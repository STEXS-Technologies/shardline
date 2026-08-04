//! On-disk chunk cache (M2b2, `docs/SDX_PLAN.md` §4.4.1 step 3).
//!
//! Mirrors `xet-client-1.5.4/src/chunk_cache/` (`disk.rs`, `cache_manager.rs`),
//! adapted to sdx conventions (thiserror [`SdxError`], `# Errors` docs):
//!
//! - **Content-addressed** by the 64-hex xorb hash (no invalidation; the hash
//!   is the content key).
//! - **Layout**: `{cache_dir}/xorbs/{prefix2}/{hash}.{start}-{end}` where
//!   `prefix2` is the first two hex characters of the hash and `(start, end)`
//!   is the exact chunk range stored (a single xorb hash is fetched under many
//!   disjoint chunk ranges, so the range is part of the content key — at most
//!   256 `prefix2` subdirectories).
//! - **Value**: the *decoded* chunk payload plus per-chunk byte offsets
//!   (`chunk_offsets`, length `chunk_end - chunk_start + 1`) and the covered
//!   chunk range — mirroring upstream `CacheRange { offsets, data, range }`.
//!   The streaming pipeline therefore skips the ranged CAS fetch **and** the
//!   decode on a hit.
//! - **Atomic writes**: a temp file is written (and `sync_all`ed) in the target
//!   directory, then `rename`d over the final path; a reader can only ever
//!   observe a complete entry.
//! - **Corruption handling**: every entry carries a CRC32 of its payload; a
//!   read that fails any structural or checksum check deletes the entry and
//!   reports a miss (never a hard error, and never a panic).
//! - **Budget/LRU**: total bytes are tracked in memory; on overflow the
//!   least-recently-accessed entry (monotonic access clock, seeded from file
//!   mtimes at startup) is evicted until the cache fits `budget_bytes`. A
//!   budget of `0` disables the cache (writes no-op, reads always miss).
//! - **Concurrency**: all state transitions are serialized behind a
//!   [`tokio::sync::Mutex`]; the file I/O under the lock is synchronous and
//!   never spans an await.
//!
//! Deviations from upstream (`xet-client-1.5.4` `chunk_cache/disk.rs`):
//! - No process-global cache-manager dedup; each [`ChunkCache`] owns its
//!   directory and is configured per client.
//! - Upstream evicts a *random* item (`random_item`); sdx evicts the oldest by
//!   access clock (a "simple LRU list", per `docs/SDX_PLAN.md` §4.4.1 step 3).
//! - `get`/`put` require the exact chunk range, whereas upstream serves any
//!   sub-range of a stored item. sdx blocks are always fetched with the exact
//!   range they were stored under, so an exact-range match is safe and simpler
//!   (the alternative — rebasing a larger stored range — would duplicate the
//!   upstream `get_range_from_cache_file` slicing logic for no current caller).
//! - The upstream default budget is 10 GiB (`DEFAULT_CHUNK_CACHE_CAPACITY`);
//!   sdx defaults to 2 GiB (`DEFAULT_CHUNK_CACHE_BUDGET_BYTES`) to keep the CLI
//!   footprint modest (`docs/SDX_PLAN.md` §4.4.4).

use std::fs::File;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::Mutex;

use crate::{error::SdxError, hash::parse_xet_hash_hex};

/// Default on-disk cache budget: 2 GiB.
///
/// Upstream `xet-runtime` defaults `chunk_cache.size_bytes` to 10 GiB; sdx
/// picks a smaller default for the CLI path (see the module docs).
pub const DEFAULT_CHUNK_CACHE_BUDGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// File magic: `SDXCHNK1`.
const MAGIC: &[u8; 8] = b"SDXCHNK1";
/// Fixed header length: magic(8) + chunk_start(8) + chunk_end(8) +
/// num_offsets(4) + data_len(8) + crc32(4) = 40 bytes.
const HEADER_LEN: usize = 40;

/// A decoded xorb range served from the on-disk cache.
///
/// `chunk_offsets` holds the start byte offset of each chunk within `data`
/// (first entry `0`, last entry `data.len()`; `chunk_end - chunk_start + 1`
/// entries), mirroring the upstream `CacheRange` contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedXorbRange {
    /// The chunk range covered by this entry, end-exclusive.
    pub chunk_range: (u64, u64),
    /// Start byte offset of each chunk within `data`.
    pub chunk_offsets: Vec<u32>,
    /// Concatenated decoded chunk payloads.
    pub data: Bytes,
}

/// Parsed on-disk entry header.
struct EntryHeader {
    chunk_range: (u64, u64),
    num_offsets: u32,
    data_len: u64,
    crc: u32,
}

/// In-memory bookkeeping for one cached entry.
#[derive(Debug, Clone, Copy)]
struct CacheEntry {
    /// Total on-disk size in bytes (header + offsets + data).
    size: u64,
    /// Access clock tick of the most recent get/put, or the file mtime when
    /// seeded from a prior process run.
    last_accessed: u64,
}

/// Cache key: the 64-hex xorb hash plus the exact chunk range.
///
/// A single xorb hash can be fetched under many disjoint chunk ranges (one per
/// term/block), so the range is part of the content key — mirroring the
/// upstream `Key`+`ChunkRange` lookup and the `docs/SDX_PLAN.md` §4.4.1 step 3
/// key `(prefix, xorb_hash) + first chunk range`.
type CacheKey = (String, u64, u64);

/// In-memory cache index, guarded by [`ChunkCache::state`].
#[derive(Debug, Default)]
struct CacheState {
    entries: std::collections::HashMap<CacheKey, CacheEntry>,
    total_bytes: u64,
}

/// Monotonic access clock shared across a process.
///
/// Based on system-time millis (so startup-seeded entries — whose
/// `last_accessed` comes from file mtimes — share a comparable scale) plus a
/// per-call counter that strictly increases within the same millisecond.
/// Millisecond clock granularity alone would produce ties that make LRU
/// eviction order arbitrary.
fn clock_tick() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_millis().try_into().unwrap_or(u64::MAX)
        });
    let counter = CALL_COUNT.fetch_add(1, Ordering::Relaxed);
    let tick = now.saturating_add(counter);
    let previous = LAST_TICK.fetch_max(tick, Ordering::Relaxed);
    previous.max(tick)
}

/// Last value handed out by [`clock_tick`] (monotonic ratchet).
static LAST_TICK: AtomicU64 = AtomicU64::new(0);
/// Strictly increasing per-call counter (disambiguates same-millisecond ticks).
static CALL_COUNT: AtomicU64 = AtomicU64::new(0);

/// On-disk chunk cache for decoded xorb ranges, keyed by the 64-hex xorb hash.
///
/// See the module docs for layout, atomicity, corruption handling, and the
/// LRU budget/eviction mechanics.
#[derive(Debug)]
pub struct ChunkCache {
    cache_dir: PathBuf,
    budget_bytes: u64,
    state: Mutex<CacheState>,
}

impl ChunkCache {
    /// Creates a cache rooted at `cache_dir` with an LRU budget of
    /// `budget_bytes` (0 disables the cache).
    ///
    /// Existing entries are scanned and validated; corrupt or oversized files
    /// are removed, and entries over the budget are evicted (oldest first).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Io`] when `cache_dir` cannot be created or scanned.
    pub fn new(cache_dir: impl Into<PathBuf>, budget_bytes: u64) -> Result<Self, SdxError> {
        let cache_dir = cache_dir.into();
        let state = scan_directory(&cache_dir, budget_bytes)?;
        Ok(Self {
            cache_dir,
            budget_bytes,
            state: Mutex::new(state),
        })
    }

    /// Returns the cache root directory.
    #[must_use]
    pub fn cache_dir(&self) -> &Path {
        &self.cache_dir
    }

    /// Returns the configured budget in bytes (0 = disabled).
    #[must_use]
    pub const fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    /// Returns the total bytes currently tracked by this cache instance.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::StreamInternal`] when the cache state lock is
    /// poisoned.
    pub async fn total_bytes(&self) -> Result<u64, SdxError> {
        let state = self.state.lock().await;
        Ok(state.total_bytes)
    }

    /// Returns the number of entries currently tracked by this cache instance.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::StreamInternal`] when the cache state lock is
    /// poisoned.
    pub async fn entry_count(&self) -> Result<usize, SdxError> {
        let state = self.state.lock().await;
        Ok(state.entries.len())
    }

    /// Returns the cached decoded xorb range for `xorb_hash` covering exactly
    /// `chunk_range`, or `None` on a miss.
    ///
    /// A corrupt or truncated entry is deleted and reported as a miss; no
    /// error is propagated for data-level corruption.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Hash`] when `xorb_hash` is not a valid 64-hex Xet
    /// CAS API hash, or [`SdxError::Io`] when reading fails.
    pub async fn get(
        &self,
        xorb_hash: &str,
        chunk_range: (u64, u64),
    ) -> Result<Option<CachedXorbRange>, SdxError> {
        if self.budget_bytes == 0 {
            return Ok(None);
        }
        validate_key(xorb_hash)?;
        let key = cache_key(xorb_hash, chunk_range);
        let path = self.item_path(&key)?;

        let (cached, was_corrupt) = match read_entry(&path) {
            Ok(Some(cached)) if cached.chunk_range == chunk_range => (Some(cached), false),
            Ok(Some(_)) => (None, false),
            Ok(None) => (None, false),
            Err(_) => (None, true),
        };
        if was_corrupt {
            drop(remove_entry_file(&path));
        }

        let mut state = self.state.lock().await;
        if was_corrupt {
            // Drop the stale index entry so `total_bytes` accounting stays
            // accurate after the corrupt file is deleted.
            if let Some(existing) = state.entries.remove(&key) {
                state.total_bytes = state.total_bytes.saturating_sub(existing.size);
            }
            return Ok(None);
        }
        let Some(cached) = cached else {
            return Ok(None);
        };
        // Re-check the index after acquiring the lock so a concurrent eviction
        // cannot leave a stale entry (a miss here is fine — the file remains
        // on disk until overwritten).
        if let Some(entry) = state.entries.get_mut(&key) {
            entry.last_accessed = clock_tick();
        }
        Ok(Some(cached))
    }

    /// Stores the decoded xorb range `data`/`chunk_offsets` under `xorb_hash`
    /// for `chunk_range`.
    ///
    /// `chunk_offsets` must have `chunk_end - chunk_start + 1` entries, the
    /// first must be `0`, and the last must equal `data.len()` (validated;
    /// invalid input is rejected with [`SdxError::StreamInternal`]). Entries
    /// larger than the budget are not stored. On overflow, the
    /// least-recently-accessed entry is evicted until the cache fits.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::Hash`] when `xorb_hash` is invalid,
    /// [`SdxError::StreamInternal`] when the entry shape is invalid, or
    /// [`SdxError::Io`] when writing or evicting fails.
    pub async fn put(
        &self,
        xorb_hash: &str,
        chunk_range: (u64, u64),
        chunk_offsets: &[u32],
        data: &[u8],
    ) -> Result<(), SdxError> {
        if self.budget_bytes == 0 {
            return Ok(());
        }
        validate_key(xorb_hash)?;
        validate_offsets(chunk_range, chunk_offsets, data)?;
        let key = cache_key(xorb_hash, chunk_range);

        let serialized = serialize_entry(chunk_range, chunk_offsets, data);
        let size = u64::try_from(serialized.len()).unwrap_or(u64::MAX);
        if size > self.budget_bytes {
            // The entry alone cannot fit; refuse to store it (mirror upstream
            // `put_impl`: "refusing to add this item as it is too large").
            return Ok(());
        }

        let path = self.item_path(&key)?;
        write_atomic(&path, &serialized)?;

        let mut state = self.state.lock().await;
        if let Some(existing) = state.entries.get(&key) {
            state.total_bytes = state.total_bytes.saturating_sub(existing.size);
        }
        state.total_bytes = state.total_bytes.saturating_add(size);
        state.entries.insert(
            key.clone(),
            CacheEntry {
                size,
                last_accessed: clock_tick(),
            },
        );

        // Evict oldest entries until the cache fits the budget. File deletion
        // happens while holding the lock; it is synchronous and quick.
        let mut evicted = Vec::new();
        while state.total_bytes > self.budget_bytes {
            let Some((oldest_key, oldest_size)) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed)
                .map(|(oldest_key, entry)| (oldest_key.clone(), entry.size))
            else {
                break;
            };
            state.entries.remove(&oldest_key);
            state.total_bytes = state.total_bytes.saturating_sub(oldest_size);
            evicted.push(oldest_key);
        }
        drop(state);

        for evicted_key in evicted {
            drop(remove_entry_file(&self.item_path(&evicted_key)?));
        }
        Ok(())
    }

    /// Resolves the on-disk path for a cache key
    /// (`{cache_dir}/xorbs/{prefix2}/{hash}.{start}-{end}`).
    fn item_path(&self, key: &CacheKey) -> Result<PathBuf, SdxError> {
        let (xorb_hash, ..) = key;
        let prefix2 = xorb_hash.get(..2).ok_or_else(|| {
            SdxError::StreamInternal("cache key shorter than two characters".to_owned())
        })?;
        Ok(self
            .cache_dir
            .join("xorbs")
            .join(prefix2)
            .join(entry_file_name(key)))
    }
}

/// Builds the cache key for `xorb_hash` / `chunk_range`.
fn cache_key(xorb_hash: &str, chunk_range: (u64, u64)) -> CacheKey {
    (xorb_hash.to_owned(), chunk_range.0, chunk_range.1)
}

/// Validates a cache key is a 64-hex Xet CAS API hash.
fn validate_key(xorb_hash: &str) -> Result<(), SdxError> {
    let _ = parse_xet_hash_hex(xorb_hash)?;
    Ok(())
}

/// Validates the `chunk_offsets`/`data` shape against `chunk_range`.
fn validate_offsets(
    chunk_range: (u64, u64),
    chunk_offsets: &[u32],
    data: &[u8],
) -> Result<(), SdxError> {
    let (start, end) = chunk_range;
    if start >= end {
        return Err(SdxError::StreamInternal(format!(
            "cache put range {start}..{end} is empty"
        )));
    }
    let expected = end
        .saturating_sub(start)
        .saturating_add(1)
        .try_into()
        .unwrap_or(usize::MAX);
    if chunk_offsets.len() != expected {
        return Err(SdxError::StreamInternal(format!(
            "cache put range {start}..{end} expects {expected} offsets, got {}",
            chunk_offsets.len()
        )));
    }
    let data_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
    if let Some(first) = chunk_offsets.first()
        && *first != 0
    {
        return Err(SdxError::StreamInternal(
            "cache put first offset must be 0".to_owned(),
        ));
    }
    if let Some(last) = chunk_offsets.last()
        && u64::from(*last) != data_len
    {
        return Err(SdxError::StreamInternal(format!(
            "cache put last offset {} must equal data length {data_len}",
            *last
        )));
    }
    for pair in chunk_offsets.windows(2) {
        // Non-decreasing: zero-length chunks (empty payloads) are legal, so
        // equal consecutive offsets are allowed.
        if let [previous, next] = pair
            && previous > next
        {
            return Err(SdxError::StreamInternal(
                "cache put offsets must be non-decreasing".to_owned(),
            ));
        }
    }
    Ok(())
}

/// Serializes an entry: fixed header followed by the payload (offsets + data).
fn serialize_entry(chunk_range: (u64, u64), chunk_offsets: &[u32], data: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(
        chunk_offsets
            .len()
            .saturating_mul(4)
            .saturating_add(data.len()),
    );
    for offset in chunk_offsets {
        payload.extend_from_slice(&offset.to_le_bytes());
    }
    payload.extend_from_slice(data);
    let crc = crc32fast::hash(&payload);
    let num_offsets = u32::try_from(chunk_offsets.len()).unwrap_or(u32::MAX);
    let data_len = u64::try_from(data.len()).unwrap_or(u64::MAX);
    let mut out = Vec::with_capacity(HEADER_LEN.saturating_add(payload.len()));
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&chunk_range.0.to_le_bytes());
    out.extend_from_slice(&chunk_range.1.to_le_bytes());
    out.extend_from_slice(&num_offsets.to_le_bytes());
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Reads and validates an entry file, returning the decoded range or `None`
/// when the file is absent. Any structural/checksum failure returns `Err`
/// (the caller deletes the file and reports a miss).
fn read_entry(path: &Path) -> Result<Option<CachedXorbRange>, SdxError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(SdxError::Io(error)),
    };
    let header = parse_header(&bytes).ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache entry header is corrupt",
        ))
    })?;
    let offsets_len = header
        .num_offsets
        .checked_mul(4)
        .and_then(|len| usize::try_from(len).ok())
        .ok_or_else(|| {
            SdxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cache entry offsets length overflow",
            ))
        })?;
    let total = HEADER_LEN
        .checked_add(offsets_len)
        .and_then(|len| u64::try_from(len).ok())
        .and_then(|len| len.checked_add(header.data_len))
        .ok_or_else(|| {
            SdxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cache entry length overflow",
            ))
        })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != total {
        return Err(SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache entry length does not match header",
        )));
    }
    // The CRC covers the payload that starts right after the fixed header:
    // the offsets table followed by the data.
    let payload = bytes.get(HEADER_LEN..).ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache payload out of bounds",
        ))
    })?;
    if crc32fast::hash(payload) != header.crc {
        return Err(SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache entry checksum mismatch",
        )));
    }
    let offsets_end = HEADER_LEN.checked_add(offsets_len).ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache offsets end overflow",
        ))
    })?;
    let offsets_bytes = bytes.get(HEADER_LEN..offsets_end).ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache offsets out of bounds",
        ))
    })?;
    let chunk_offsets: Option<Vec<u32>> = offsets_bytes
        .chunks_exact(4)
        .map(|window| window.try_into().ok().map(u32::from_le_bytes))
        .collect();
    let chunk_offsets = chunk_offsets.ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache offsets are truncated",
        ))
    })?;
    let data_start = offsets_end;
    let data_end = offsets_end
        .checked_add(usize::try_from(header.data_len).unwrap_or(usize::MAX))
        .ok_or_else(|| {
            SdxError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cache data end overflow",
            ))
        })?;
    let data = bytes.get(data_start..data_end).ok_or_else(|| {
        SdxError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "cache data out of bounds",
        ))
    })?;
    Ok(Some(CachedXorbRange {
        chunk_range: header.chunk_range,
        chunk_offsets,
        data: Bytes::copy_from_slice(data),
    }))
}

/// Parses and structurally validates the fixed entry header.
fn parse_header(bytes: &[u8]) -> Option<EntryHeader> {
    if bytes.get(..MAGIC.len())? != MAGIC.as_slice() {
        return None;
    }
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 8];
    cursor.read_exact(&mut magic).ok()?;
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf).ok()?;
    let chunk_start = u64::from_le_bytes(buf);
    cursor.read_exact(&mut buf).ok()?;
    let chunk_end = u64::from_le_bytes(buf);
    if chunk_start >= chunk_end {
        return None;
    }
    let mut buf32 = [0u8; 4];
    cursor.read_exact(&mut buf32).ok()?;
    let num_offsets = u32::from_le_bytes(buf32);
    if num_offsets == 0 {
        return None;
    }
    cursor.read_exact(&mut buf).ok()?;
    let data_len = u64::from_le_bytes(buf);
    cursor.read_exact(&mut buf32).ok()?;
    let crc = u32::from_le_bytes(buf32);
    Some(EntryHeader {
        chunk_range: (chunk_start, chunk_end),
        num_offsets,
        data_len,
        crc,
    })
}

/// Writes `data` to `path` atomically (temp file + rename).
fn write_atomic(path: &Path, data: &[u8]) -> Result<(), SdxError> {
    let dir = path
        .parent()
        .ok_or_else(|| SdxError::StreamInternal("cache path has no parent directory".to_owned()))?;
    std::fs::create_dir_all(dir)?;
    let temp_name = format!(
        "tmp.{}.{}",
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let temp_path = dir.join(temp_name);
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok::<(), std::io::Error>(())
    })();
    if let Err(error) = write_result {
        drop(std::fs::remove_file(&temp_path));
        return Err(SdxError::Io(error));
    }
    if let Err(error) = std::fs::rename(&temp_path, path) {
        drop(std::fs::remove_file(&temp_path));
        return Err(SdxError::Io(error));
    }
    Ok(())
}

/// Removes an entry file (and empty ancestor directories), ignoring `NotFound`.
fn remove_entry_file(path: &Path) -> Result<(), SdxError> {
    if let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        return Err(SdxError::Io(error));
    }
    // Remove the (now possibly empty) `{hash}` dir, then the `{prefix2}` dir.
    let mut dir = path.parent();
    for _ in 0..2 {
        let Some(current) = dir else {
            break;
        };
        let empty = std::fs::read_dir(current)
            .map(|mut read_dir| read_dir.next().is_none())
            .unwrap_or(false);
        if !empty {
            break;
        }
        drop(std::fs::remove_dir(current));
        dir = current.parent();
    }
    Ok(())
}

/// Scans `cache_dir`, validating existing entries and applying the budget.
fn scan_directory(cache_dir: &Path, budget_bytes: u64) -> Result<CacheState, SdxError> {
    let mut state = CacheState::default();
    if budget_bytes == 0 {
        return Ok(state);
    }
    std::fs::create_dir_all(cache_dir)?;
    let Some(read_dir) = read_dir_ok(cache_dir)? else {
        return Ok(state);
    };
    for prefix_dir in read_dir.flatten() {
        let path = prefix_dir.path();
        if path.file_name().is_none_or(|name| name.len() != 2) || !path.is_dir() {
            continue;
        }
        let Some(prefix_read) = read_dir_ok(&path)? else {
            continue;
        };
        for entry_dir in prefix_read.flatten() {
            let entry_path = entry_dir.path();
            if !entry_path.is_file() {
                continue;
            }
            // File names are `{hash}.{start}-{end}`; parse them back into the
            // composite key. Anything that does not parse is not ours.
            let Some(key) = entry_path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_cache_file_name)
            else {
                continue;
            };
            if parse_xet_hash_hex(&key.0).is_err() {
                drop(std::fs::remove_file(&entry_path));
                continue;
            }
            match read_entry(&entry_path) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(_) => {
                    drop(std::fs::remove_file(&entry_path));
                    continue;
                }
            }
            let size = entry_path.metadata().map_or(0, |metadata| metadata.len());
            let last_accessed = entry_path
                .metadata()
                .ok()
                .and_then(|metadata| metadata.modified().ok())
                .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |duration| {
                    duration.as_millis().try_into().unwrap_or(u64::MAX)
                });
            if state
                .entries
                .insert(
                    key,
                    CacheEntry {
                        size,
                        last_accessed,
                    },
                )
                .is_none()
            {
                state.total_bytes = state.total_bytes.saturating_add(size);
            }
        }
    }
    // Apply the budget over the scanned state, oldest first.
    while state.total_bytes > budget_bytes {
        let Some((oldest_key, size)) = state
            .entries
            .iter()
            .min_by_key(|(_, entry)| entry.last_accessed)
            .map(|(key, entry)| (key.clone(), entry.size))
        else {
            break;
        };
        state.entries.remove(&oldest_key);
        state.total_bytes = state.total_bytes.saturating_sub(size);
        let prefix2 = oldest_key.0.get(..2);
        let dir = prefix2.map(|prefix| cache_dir.join("xorbs").join(prefix));
        if let Some(dir) = dir {
            drop(std::fs::remove_file(dir.join(entry_file_name(&oldest_key))));
        }
    }
    Ok(state)
}

/// Parses a cache file name (`{hash}.{start}-{end}`) into its composite key.
fn parse_cache_file_name(name: &str) -> Option<CacheKey> {
    let (hash, suffix) = name.split_once('.')?;
    let (start, end) = suffix.split_once('-')?;
    let start = start.parse::<u64>().ok()?;
    let end = end.parse::<u64>().ok()?;
    if start >= end {
        return None;
    }
    Some((hash.to_owned(), start, end))
}

/// Formats the file name for a composite cache key.
fn entry_file_name(key: &CacheKey) -> String {
    let (hash, start, end) = key;
    format!("{hash}.{start}-{end}")
}

/// `std::fs::read_dir` that treats `NotFound` as an empty directory.
fn read_dir_ok(path: &Path) -> Result<Option<std::fs::ReadDir>, SdxError> {
    match std::fs::read_dir(path) {
        Ok(read_dir) => Ok(Some(read_dir)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(SdxError::Io(error)),
    }
}

/// Unique temp-file suffix counter (process-scoped).
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    fn hash(digit: char) -> String {
        let mut out = String::with_capacity(64);
        for _ in 0..64 {
            out.push(digit);
        }
        out
    }

    fn offsets_for(data: &[u8]) -> Vec<u32> {
        vec![0, u32::try_from(data.len()).unwrap()]
    }

    async fn put(cache: &ChunkCache, key: &str, data: &[u8]) {
        cache
            .put(key, (0, 1), &offsets_for(data), data)
            .await
            .unwrap();
    }

    async fn get(cache: &ChunkCache, key: &str) -> Option<CachedXorbRange> {
        cache.get(key, (0, 1)).await.unwrap()
    }

    #[tokio::test]
    async fn miss_before_put_hit_after() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('a');
        assert!(get(&cache, &key).await.is_none());

        put(&cache, &key, b"hello world").await;
        let cached = get(&cache, &key).await.unwrap();
        assert_eq!(cached.chunk_offsets, vec![0, 11]);
        assert_eq!(cached.data.as_ref(), b"hello world");
        assert_eq!(cached.chunk_range, (0, 1));
    }

    #[tokio::test]
    async fn put_returns_identical_data_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('b');
        let data = vec![42u8; 256];
        put(&cache, &key, &data).await;

        // A fresh instance rescans the directory and still serves the entry.
        let reopened = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let cached = get(&reopened, &key).await.unwrap();
        assert_eq!(cached.data.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn same_hash_multiple_ranges_coexist() {
        // Regression: a single xorb hash is fetched under many disjoint chunk
        // ranges; each must be stored and served independently.
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1 << 20).unwrap();
        let key = hash('a');
        cache
            .put(&key, (0, 1), &[0, 11], b"hello world")
            .await
            .unwrap();
        cache.put(&key, (1, 2), &[0, 5], b"other").await.unwrap();
        cache.put(&key, (2, 3), &[0, 3], b"abc").await.unwrap();

        let first = cache.get(&key, (0, 1)).await.unwrap().unwrap();
        assert_eq!(first.data.as_ref(), b"hello world");
        let second = cache.get(&key, (1, 2)).await.unwrap().unwrap();
        assert_eq!(second.data.as_ref(), b"other");
        let third = cache.get(&key, (2, 3)).await.unwrap().unwrap();
        assert_eq!(third.data.as_ref(), b"abc");
        assert_eq!(cache.entry_count().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn eviction_evicts_oldest_keeps_newest() {
        let dir = tempfile::tempdir().unwrap();
        // Budget 900 bytes: three 400-byte entries exceed it after the second.
        let cache = ChunkCache::new(dir.path().to_path_buf(), 900).unwrap();
        let data = vec![7u8; 400];
        let key_a = hash('a');
        let key_b = hash('b');
        let key_c = hash('c');

        put(&cache, &key_a, &data).await;
        put(&cache, &key_b, &data).await;
        // key_a is now the oldest; key_c pushes total over the budget.
        put(&cache, &key_c, &data).await;

        assert!(
            get(&cache, &key_a).await.is_none(),
            "oldest must be evicted"
        );
        assert!(get(&cache, &key_b).await.is_some(), "second must survive");
        assert!(get(&cache, &key_c).await.is_some(), "newest must survive");
    }

    #[tokio::test]
    async fn corrupt_file_returns_miss_and_is_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('d');
        put(&cache, &key, b"some cached data").await;
        assert!(get(&cache, &key).await.is_some());

        // Overwrite the entry with garbage.
        let path = cache.item_path(&cache_key(&key, (0, 1))).unwrap();
        std::fs::write(&path, b"garbage that is not a cache entry").unwrap();

        // get must report a miss and delete the corrupt file.
        assert!(get(&cache, &key).await.is_none());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn truncated_file_returns_miss_and_is_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('e');
        put(&cache, &key, b"truncate me later").await;

        let path = cache.item_path(&cache_key(&key, (0, 1))).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len().saturating_sub(10)]).unwrap();

        assert!(get(&cache, &key).await.is_none());
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn budget_zero_disables_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 0).unwrap();
        let key = hash('f');
        put(&cache, &key, b"nope").await;
        assert!(get(&cache, &key).await.is_none());
        assert_eq!(cache.total_bytes().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn concurrent_puts_and_gets_across_tasks() {
        let dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(ChunkCache::new(dir.path().to_path_buf(), 1 << 20).unwrap());
        let hex_digits = b"0123456789abcdef";
        let keys: Vec<String> = (0..16).map(|i| hash(char::from(hex_digits[i]))).collect();
        let mut tasks = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            let cache = cache.clone();
            let key = key.clone();
            tasks.push(tokio::spawn(async move {
                let data = vec![u8::try_from(index).unwrap(); 64];
                cache
                    .put(&key, (0, 1), &offsets_for(&data), &data)
                    .await
                    .unwrap();
                let cached = get(&cache, &key).await.unwrap();
                cached.data.as_ref().to_vec()
            }));
        }
        for (index, task) in tasks.into_iter().enumerate() {
            let data = task.await.unwrap();
            assert_eq!(data, vec![u8::try_from(index).unwrap(); 64]);
        }
    }

    #[tokio::test]
    async fn empty_data_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('0');
        // Chunk range [0,1) with zero bytes: offsets = [0, 0].
        cache.put(&key, (0, 1), &[0, 0], b"").await.unwrap();
        let cached = get(&cache, &key).await.unwrap();
        assert_eq!(cached.chunk_offsets, vec![0, 0]);
        assert!(cached.data.is_empty());
    }

    #[tokio::test]
    async fn invalid_offsets_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 1024).unwrap();
        let key = hash('9');
        let result = cache.put(&key, (0, 2), &[0], b"data").await;
        assert!(result.is_err());
        let result = cache.put(&key, (0, 1), &[5], b"data").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn oversize_entry_is_not_stored() {
        let dir = tempfile::tempdir().unwrap();
        let cache = ChunkCache::new(dir.path().to_path_buf(), 32).unwrap();
        let key = hash('8');
        let data = vec![0u8; 1024];
        cache
            .put(&key, (0, 1), &offsets_for(&data), &data)
            .await
            .unwrap();
        assert!(get(&cache, &key).await.is_none());
    }

    #[test]
    fn clock_tick_is_monotonic() {
        // Sanity: clock_tick strictly increases (or holds) within a process.
        let first = clock_tick();
        std::thread::sleep(Duration::from_millis(5));
        let second = clock_tick();
        assert!(second >= first);
    }
}
