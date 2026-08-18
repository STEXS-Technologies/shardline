use std::collections::HashMap;
use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::num::NonZeroU64;
use std::path::Path as FsPath;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Instant;

use axum::{
    Json,
    body::{Body, Bytes},
    extract::{FromRequestParts, Path, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE},
        request::Parts,
    },
    response::{IntoResponse, Response},
};
use futures_util::stream;
use serde_json::json;
use sha2::{Digest, Sha256};
use shardline_protocol::TokenScope;
use shardline_server_core::AuthorizedRepository;

use futures_util::StreamExt;
use shardline_storage::DeleteOutcome;

use super::{MAX_LFS_BATCH_OBJECTS, direct_object_response};
use crate::app::{AppState, authorize};
use crate::{
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectResponse,
    LfsOperation, ServerError, TransferAdapter,
    admission::weights,
    cas_headers::{ACCESS_TOKEN, TOKEN_EXPIRATION, URL},
    lfs_object_key, metrics,
    overflow::checked_add,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

/// Maximum LFS object size allowed for server-side verification (1 GiB).
/// Objects above this threshold are rejected with a 413 to prevent OOM.
const MAX_LFS_VERIFY_BYTES: u64 = 1_073_741_824; // 1 GiB
const MAX_LFS_PATCH_RANGES: usize = 65_536;
/// Journal appends tolerated before an LFS patch session's ranges file is
/// compacted back to its canonical merged form.
///
/// Range records append a line in O(1); the full read-merge-atomic-rewrite
/// only runs every [`LFS_PATCH_RANGES_COMPACTION_THRESHOLD`] appends, so a
/// large disjoint range set costs O(1) per PATCH instead of the old O(n log n)
/// rewrite-per-PATCH amplification (F-30).
const LFS_PATCH_RANGES_COMPACTION_THRESHOLD: usize = 1024;
/// Maximum declared LFS object size accepted by the chunked PATCH path (1 TiB).
///
/// Bounds the declared `total` from a `Content-Range` header so a caller cannot
/// request an arbitrary `u64` offset (which would create a multi-TiB sparse
/// staging file). One TiB is well above any legitimate dataset/model object.
const MAX_LFS_OBJECT_SIZE: u64 = 1 << 40; // 1 TiB

/// Runs the shared authorize chain and mints a typed [`AuthorizedRepository`]
/// capability for LFS requests.
///
/// LFS URLs carry no repository segment, so the repository identity comes
/// exclusively from the verified token claims (isolated via the token's
/// `RepositoryScope` namespace). This reproduces today's chain in the same
/// order: [`authorize`](crate::app::authorize) (permissive `Ok(None)` when no
/// auth provider is configured) → mint: verified context → `from_verified_context`,
/// `None` → `anonymous_full_access()`.
fn authorize_repository(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<AuthorizedRepository, ServerError> {
    authorize(state, headers, required_scope)?.map_or_else(
        || Ok(AuthorizedRepository::anonymous_full_access()),
        |ctx| {
            // The verified context (minted by the auth layer's
            // `verify_verified`) flows straight into the capability seam;
            // `from_verified_context` only re-applies the scope gate
            // idempotently — no token is re-verified here.
            AuthorizedRepository::from_verified_context(ctx, required_scope)
                .map_err(ServerError::from)
        },
    )
}

/// Read-scoped LFS authorization capability, extracted from the request.
///
/// Because LFS URLs carry no repository path segment, the capability's
/// namespace comes entirely from the verified token claims. The extractor
/// reproduces today's authorize chain exactly: `authorize` (permissive
/// `Ok(None)` when `state.auth` is `None`) → verified context → capability,
/// or `anonymous_full_access()` for permissive mode.
#[derive(Debug)]
pub struct LfsRepository {
    auth: AuthorizedRepository,
}

/// Write-scoped LFS authorization capability, extracted from the request.
#[derive(Debug)]
pub struct LfsWriteRepository {
    auth: AuthorizedRepository,
}

impl LfsRepository {
    /// Read-scoped construction from request headers.
    fn read(state: &AppState, headers: &HeaderMap) -> Result<Self, ServerError> {
        Ok(Self {
            auth: authorize_repository(state, headers, TokenScope::Read)?,
        })
    }

    /// The typed, verified authorization capability.
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.auth
    }
}

impl LfsWriteRepository {
    /// Write-scoped construction from request headers.
    fn write(state: &AppState, headers: &HeaderMap) -> Result<Self, ServerError> {
        Ok(Self {
            auth: authorize_repository(state, headers, TokenScope::Write)?,
        })
    }

    /// The typed, verified authorization capability.
    pub(crate) const fn capability(&self) -> &AuthorizedRepository {
        &self.auth
    }
}

impl FromRequestParts<Arc<AppState>> for LfsRepository {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        // Borrow (do not consume) the headers: handlers extract `HeaderMap`
        // separately for CONTENT_LENGTH / CONTENT_RANGE / range parsing.
        Self::read(state, &parts.headers)
    }
}

impl FromRequestParts<Arc<AppState>> for LfsWriteRepository {
    type Rejection = ServerError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        Self::write(state, &parts.headers)
    }
}

/// Returns a 422 UNPROCESSABLE_ENTITY response for LFS validation errors.
fn lfs_validation_response(message: &str) -> Response {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
        Json(json!({ "message": message })),
    )
        .into_response()
}

/// Per-OID mutex map to serialize PATCH operations targeting the same temp file.
///
/// The map holds **weak** values (mirroring the S3 per-key upload-lock map):
/// while any caller holds a guard, its strong [`Arc`] keeps the entry's weak
/// handle alive so concurrent PATCHes of the same OID still serialize on the
/// SAME mutex; once the last guard drops the entry dies and is evicted lazily
/// on the next acquire. This bounds the map by the number of OIDs with a
/// PATCH in flight instead of the number of distinct OIDs ever seen (F-22).
///
/// Lock order (F-31): a guard is taken while the store lock is held (or alone)
/// and is dropped before the promotion; the store lock is never re-acquired
/// while a guard is held. The guard is only held for the short staging
/// write + range-record section, so the sweep (which waits on it under the
/// store lock) can never be starved for long.
static LFS_PATCH_LOCKS: LazyLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn acquire_lfs_patch_lock(oid: &str) -> Arc<Mutex<()>> {
    // Recover from poisoning: if a previous lock-holder panicked, the map
    // contents are still valid (simple OID→lock mapping), so continue.
    let mut map = LFS_PATCH_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    // Fast path: a live weak handle exists (a guard is still being held for
    // this OID), so hand out the same strong Arc to preserve serialization.
    if let Some(live) = map.get(oid).and_then(Weak::upgrade) {
        return live;
    }
    // No live handle: drop dead entries so the map cannot grow with finished
    // OIDs (F-22), then install a fresh mutex and return its strong Arc.
    map.retain(|_oid, weak| weak.upgrade().is_some());
    let fresh = Arc::new(Mutex::new(()));
    map.insert(oid.to_owned(), Arc::downgrade(&fresh));
    fresh
}

/// Returns the number of map entries whose strong lock is still alive (i.e.
/// held by at least one guard). Test-only: asserts the map is bounded by
/// in-flight PATCHes rather than by the number of distinct OIDs ever seen.
#[cfg(test)]
fn live_lfs_patch_lock_count() -> usize {
    let map = LFS_PATCH_LOCKS.lock().unwrap_or_else(|e| e.into_inner());
    map.values().filter(|weak| weak.upgrade().is_some()).count()
}

/// The process-wide lock serializing LFS patch-store accounting (active
/// session count + aggregate staging bytes) and the expiry sweep.
///
/// Never held across a network body stream: the PATCH body is fully buffered
/// before any store mutation, so a slow client cannot stall other sessions
/// (F-10 pattern).
///
/// Lock order (F-31): the store lock is acquired FIRST, before the per-OID
/// lock, exactly like the sweep; it is dropped before the staging write. It is
/// NEVER re-acquired while a per-OID lock is held — the promotion and error
/// paths drop the per-OID guard before taking the store lock for the `.meta`
/// removal. The per-OID lock is therefore always the inner lock, and no code
/// path acquires store→per-OID and per-OID→store in the same run.
static LFS_PATCH_STORE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// In-memory merged-range bookkeeping for active LFS patch sessions.
///
/// Each session's ranges are kept merged in memory (a hole-map) and persisted
/// as an append-only journal — `+{start} {end}` lines appended after the
/// canonical `{total}` header and canonical merged `{start} {end}` lines. The
/// journal is compacted back to canonical form every
/// [`LFS_PATCH_RANGES_COMPACTION_THRESHOLD`] appends, so a large disjoint
/// range set costs O(1) per PATCH instead of a full read-sort-merge-rewrite
/// (F-30). The file is crash-recoverable: on first access (after a restart)
/// the canonical and journal lines are merged back into the in-memory set.
///
/// The map's mutex is only ever taken while holding a per-OID lock (and never
/// while holding the store lock), making it the innermost lock in the PATCH
/// path. Entries are evicted when a session is promoted or swept.
static LFS_PATCH_RANGES: LazyLock<Mutex<HashMap<PathBuf, PatchRangesState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// The merged range bookkeeping for one LFS patch session.
#[derive(Debug, Clone, Default)]
struct PatchRangesState {
    /// The declared object size from the `Content-Range` total.
    total: u64,
    /// Sorted, disjoint, merged ranges covering the assembled staging file.
    ranges: Vec<(u64, u64)>,
    /// Journal appends since the last compaction.
    journal_lines: usize,
}

#[cfg(test)]
static LFS_PATCH_RANGES_COMPACTIONS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
fn lfs_patch_ranges_compactions() -> u64 {
    LFS_PATCH_RANGES_COMPACTIONS.load(std::sync::atomic::Ordering::Relaxed)
}

/// The staging directory for in-flight LFS chunked (PATCH) uploads.
fn lfs_patch_dir(root: &FsPath) -> PathBuf {
    root.join("tmp").join("lfs-patch")
}

/// The per-OID sidecar recording the patch session's last-touched Unix time;
/// its presence marks an active (incomplete) patch session for the sweep.
fn lfs_patch_meta_path(dir: &FsPath, oid: &str) -> PathBuf {
    dir.join(format!("{oid}.meta"))
}

/// The current Unix time in seconds for the LFS patch store.
fn lfs_patch_now_seconds() -> Result<u64, ServerError> {
    shardline_server_core::unix_now_seconds_checked().map_err(|_error| ServerError::Overflow)
}

/// Reads the last-touched Unix time from a patch session's sidecar.
fn read_patch_last_touched(dir: &FsPath, oid: &str) -> Result<u64, ServerError> {
    let raw = fs::read_to_string(lfs_patch_meta_path(dir, oid))?;
    raw.trim().parse::<u64>().map_err(|_error| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid lfs patch meta",
        ))
    })
}

/// Atomically records (or refreshes) a patch session's last-touched time.
fn touch_patch_session(dir: &FsPath, oid: &str, now_unix_seconds: u64) -> Result<(), ServerError> {
    let path = lfs_patch_meta_path(dir, oid);
    let temporary = path.with_extension("meta.tmp");
    fs::write(&temporary, now_unix_seconds.to_string())?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// Returns the on-disk footprint of a staging file.
///
/// On Unix this counts ALLOCATED blocks (`st_blocks * 512`), so a sparse file
/// created by seeking past the end costs its real disk footprint against the
/// aggregate staging cap instead of its logical size (F-30). Elsewhere the
/// logical length is the only portable estimate.
#[cfg(unix)]
fn staging_file_allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    metadata.blocks().saturating_mul(512)
}

#[cfg(not(unix))]
fn staging_file_allocated_bytes(metadata: &std::fs::Metadata) -> u64 {
    metadata.len()
}

/// Returns the `(active session count, aggregate staging bytes)` across the
/// on-disk patch sessions. Caller must hold [`LFS_PATCH_STORE_LOCK`].
fn patch_store_usage(dir: &FsPath) -> Result<(usize, u64), ServerError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(error.into()),
    };
    let mut sessions = 0_usize;
    let mut bytes = 0_u64;
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some(oid) = name.strip_suffix(".meta") else {
            continue;
        };
        sessions = sessions.saturating_add(1);
        if let Ok(metadata) = fs::metadata(dir.join(oid)) {
            // F-30: count the file's ALLOCATED size (sparse files cost their
            // real disk footprint), never its logical size.
            bytes = checked_add(bytes, staging_file_allocated_bytes(&metadata))?;
        }
    }
    Ok((sessions, bytes))
}

/// Removes patch sessions whose last-touched time is older than the TTL.
///
/// Caller must hold [`LFS_PATCH_STORE_LOCK`]. Each stale session's delete is
/// serialized with an in-flight PATCH for the same OID (the per-OID lock), so
/// the sweep never removes a staging file mid-write — mirroring how the S3
/// multipart sweep takes the per-session lock (F-20).
fn sweep_lfs_patch_sessions_locked(
    dir: &FsPath,
    ttl_seconds: NonZeroU64,
    now_unix_seconds: u64,
) -> Result<usize, ServerError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut removed = 0_usize;
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        let Some(oid) = name.strip_suffix(".meta") else {
            continue;
        };
        let stale = match read_patch_last_touched(dir, oid) {
            Ok(touched) => touched.saturating_add(ttl_seconds.get()) <= now_unix_seconds,
            // An unreadable sidecar is crash debris; treat it as stale.
            Err(_error) => true,
        };
        if stale {
            let oid_lock = acquire_lfs_patch_lock(oid);
            let _guard = oid_lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let removed_data = fs::remove_file(dir.join(oid));
            let ranges_path = dir.join(format!("{oid}.ranges"));
            let removed_ranges = fs::remove_file(&ranges_path);
            let removed_meta = fs::remove_file(lfs_patch_meta_path(dir, oid));
            // Drop the session's in-memory range bookkeeping so a later PATCH
            // for the same OID re-derives it from the (now absent) file.
            evict_lfs_patch_ranges(&ranges_path);
            tracing::trace!(
                removed_data = removed_data.is_ok(),
                removed_ranges = removed_ranges.is_ok(),
                removed_meta = removed_meta.is_ok(),
                "lfs patch sweep removed stale staging session"
            );
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

/// Sweeps expired LFS chunked-patch sessions (startup + on-PATCH crash
/// recovery, mirroring how the S3 multipart sweep runs at startup and on
/// session creation).
pub(crate) fn sweep_lfs_patch_sessions(
    root: &FsPath,
    ttl_seconds: NonZeroU64,
) -> Result<usize, ServerError> {
    let _guard = LFS_PATCH_STORE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let now = lfs_patch_now_seconds()?;
    sweep_lfs_patch_sessions_locked(&lfs_patch_dir(root), ttl_seconds, now)
}

/// Builds a bounded streaming reader over a local file, feeding it in
/// fixed-size chunks with a hard total-byte ceiling.
///
/// F-21: the LFS chunked-upload promotion previously called `fs::read` on the
/// assembled staging file, loading up to `MAX_LFS_OBJECT_SIZE` (1 TiB) into
/// RAM. This streams it through the ingest path in bounded chunks instead, so
/// peak memory stays O(chunk) not O(object); the SHA-256 verification still
/// runs over the streamed bytes inside the backend ingest. The ceiling aborts
/// with `RequestBodyTooLarge` if a corrupt staging file ever exceeds it.
fn bounded_file_stream(
    path: &FsPath,
    chunk_size: usize,
    max_bytes: u64,
) -> Result<RequestBodyReader, ServerError> {
    use std::io::Read as _;
    let file = fs::File::open(path)?;
    let stream = stream::unfold(
        (file, 0_u64, max_bytes),
        move |(mut file, mut read, max_bytes)| async move {
            let mut buffer = vec![0_u8; chunk_size];
            match file.read(&mut buffer) {
                Ok(0) => None,
                Ok(read_bytes) => {
                    buffer.truncate(read_bytes);
                    match checked_add(read, read_bytes as u64) {
                        Ok(next) => {
                            read = next;
                            if read > max_bytes {
                                return Some((
                                    Err(ServerError::RequestBodyTooLarge),
                                    (file, read, max_bytes),
                                ));
                            }
                            Some((Ok(Bytes::from(buffer)), (file, read, max_bytes)))
                        }
                        Err(error) => Some((Err(error), (file, read, max_bytes))),
                    }
                }
                Err(error) => Some((Err(ServerError::Io(error)), (file, read, max_bytes))),
            }
        },
    );
    Ok(RequestBodyReader::from_stream(stream))
}

/// Inserts `(start, end_exclusive)` into a sorted, disjoint merged range list,
/// merging overlapping and adjacent neighbors so the list stays canonical.
fn insert_merged_range(ranges: &mut Vec<(u64, u64)>, start: u64, end_exclusive: u64) {
    // First range whose end reaches `start` (overlaps the new range or is
    // adjacent to it); every earlier range ends strictly before `start`.
    let merge_start = ranges.partition_point(|&(_range_start, range_end)| range_end < start);
    let mut merge_end = merge_start;
    let mut merged_end = end_exclusive;
    for &(_range_start, range_end) in ranges.iter().skip(merge_start) {
        if _range_start > merged_end {
            break;
        }
        merged_end = merged_end.max(range_end);
        merge_end = merge_end.saturating_add(1);
    }
    let merged_start = match ranges.get(merge_start) {
        Some(&(_range_start, _range_end)) => start.min(_range_start),
        None => start,
    };
    ranges.splice(merge_start..merge_end, [(merged_start, merged_end)]);
}

/// Writes a session's range bookkeeping in its canonical (fully merged) form.
///
/// The first line carries the declared total; every subsequent line is a
/// merged `{start} {end}` range with no journal prefix. The write is atomic
/// (temporary file + rename) so a crash never leaves a torn file.
fn write_lfs_patch_ranges_file(
    ranges_path: &FsPath,
    state: &PatchRangesState,
) -> Result<(), ServerError> {
    let mut encoded = format!("{}\n", state.total);
    for (range_start, range_end) in &state.ranges {
        use std::fmt::Write as _;
        writeln!(encoded, "{range_start} {range_end}").map_err(|_error| ServerError::Overflow)?;
    }
    let temporary_ranges_path = ranges_path.with_extension("ranges.tmp");
    fs::write(&temporary_ranges_path, encoded)?;
    fs::rename(temporary_ranges_path, ranges_path)?;
    Ok(())
}

/// Appends a journal line recording a newly written range.
///
/// The append is O(1): the canonical merged form is only rewritten by periodic
/// compaction, so a large disjoint range set does not trigger a
/// read-sort-merge-rewrite per PATCH (F-30). A fresh session's first append
/// writes the `{total}` header line first.
fn append_lfs_patch_range_journal(
    ranges_path: &FsPath,
    total: u64,
    start: u64,
    end_exclusive: u64,
) -> Result<(), ServerError> {
    let fresh = !ranges_path.exists();
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ranges_path)?;
    use std::io::Write as _;
    if fresh {
        writeln!(file, "{total}").map_err(|_error| ServerError::Overflow)?;
    }
    writeln!(file, "+{start} {end_exclusive}").map_err(|_error| ServerError::Overflow)?;
    Ok(())
}

/// Loads (and, when a journal is present, compacts) a session's range
/// bookkeeping from disk.
///
/// The file layout is a `{total}` header line, followed by canonical merged
/// range lines `{start} {end}` and journal lines `+{start} {end}` appended
/// since the last compaction. Canonical and journal lines are merged into one
/// sorted disjoint set. A missing file yields a fresh empty session.
fn load_lfs_patch_ranges_from_disk(
    ranges_path: &FsPath,
    total: u64,
) -> Result<PatchRangesState, ServerError> {
    let mut raw = match fs::read_to_string(ranges_path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PatchRangesState {
                total,
                ..PatchRangesState::default()
            });
        }
        Err(error) => return Err(error.into()),
    };
    // A file not ending in a newline was truncated mid-append (crash debris);
    // drop the partial trailing line rather than failing the whole session.
    if !raw.ends_with('\n') {
        match raw.rfind('\n') {
            Some(last_newline) => raw.truncate(last_newline.saturating_add(1)),
            None => raw.clear(),
        }
    }
    let mut lines = raw.lines();
    let stored_total = lines
        .next()
        .and_then(|line| line.parse::<u64>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid LFS patch range metadata",
            )
        })?;
    if stored_total != total {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "inconsistent LFS patch total length",
        )
        .into());
    }
    let mut state = PatchRangesState {
        total,
        ..PatchRangesState::default()
    };
    for line in lines {
        let range_entry = match line.strip_prefix('+') {
            Some(journal) => {
                state.journal_lines = state.journal_lines.saturating_add(1);
                journal
            }
            None => line,
        };
        let (range_start, range_end) = range_entry.split_once(' ').ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid LFS patch range entry",
            )
        })?;
        let range_start = range_start.parse::<u64>().map_err(|_error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid LFS patch range start",
            )
        })?;
        let range_end = range_end.parse::<u64>().map_err(|_error| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid LFS patch range end",
            )
        })?;
        insert_merged_range(&mut state.ranges, range_start, range_end);
    }
    if state.ranges.len() > MAX_LFS_PATCH_RANGES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "too many disjoint LFS patch ranges",
        )
        .into());
    }
    // Compact a non-empty journal back to canonical form so crash recovery
    // never has to re-read an unbounded journal more than once per session.
    if state.journal_lines > 0 {
        write_lfs_patch_ranges_file(ranges_path, &state)?;
        state.journal_lines = 0;
    }
    Ok(state)
}

/// Drops a session's in-memory range bookkeeping.
///
/// Called after promotion consumes the session, after the sweep removes it, or
/// when a staging failure tears the session down. Safe without a per-OID lock
/// in the promotion path because the concurrent sweep is blocked on the store
/// lock (F-31); otherwise the caller holds the per-OID lock.
fn evict_lfs_patch_ranges(ranges_path: &FsPath) {
    let mut map = LFS_PATCH_RANGES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    map.remove(ranges_path);
}

/// Returns an `InvalidData` IO error for a range-bookkeeping inconsistency.
fn lfs_patch_ranges_inconsistent_total() -> ServerError {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "inconsistent LFS patch total length",
    )
    .into()
}

/// Inspects a session's merged-range bookkeeping for the pre-write checks.
///
/// Returns `(high_water_mark, already_complete)` where the high-water mark is
/// the maximum recorded range end (0 for a fresh session) and
/// `already_complete` is whether the merged ranges already cover `[0, total)`.
/// Caller must hold the session's per-OID lock (so the loaded state cannot be
/// mutated or evicted in between).
fn inspect_lfs_patch_ranges(ranges_path: &FsPath, total: u64) -> Result<(u64, bool), ServerError> {
    let mut map = LFS_PATCH_RANGES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = match map.entry(ranges_path.to_owned()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(load_lfs_patch_ranges_from_disk(ranges_path, total)?)
        }
    };
    if state.total != total {
        return Err(lfs_patch_ranges_inconsistent_total());
    }
    let high_water_mark = state
        .ranges
        .last()
        .map_or(0, |&(_range_start, range_end)| range_end);
    let already_complete = state.ranges.as_slice() == [(0, total)];
    Ok((high_water_mark, already_complete))
}

/// Records a newly written range in the session's bookkeeping and returns
/// whether the merged ranges now cover `[0, total)` exactly (the promotion
/// trigger).
///
/// The merge happens in memory; the disk gets an O(1) journal append with a
/// full compaction every [`LFS_PATCH_RANGES_COMPACTION_THRESHOLD`] appends
/// (F-30). Caller must hold the session's per-OID lock.
fn record_lfs_patch_range(
    ranges_path: &FsPath,
    start: u64,
    end_exclusive: u64,
    total: u64,
) -> Result<bool, ServerError> {
    let mut map = LFS_PATCH_RANGES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let state = match map.entry(ranges_path.to_owned()) {
        std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(load_lfs_patch_ranges_from_disk(ranges_path, total)?)
        }
    };
    if state.total != total {
        return Err(lfs_patch_ranges_inconsistent_total());
    }
    insert_merged_range(&mut state.ranges, start, end_exclusive);
    if state.ranges.len() > MAX_LFS_PATCH_RANGES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "too many disjoint LFS patch ranges",
        )
        .into());
    }
    append_lfs_patch_range_journal(ranges_path, total, start, end_exclusive)?;
    state.journal_lines = state.journal_lines.saturating_add(1);
    if state.journal_lines >= LFS_PATCH_RANGES_COMPACTION_THRESHOLD {
        write_lfs_patch_ranges_file(ranges_path, state)?;
        state.journal_lines = 0;
        #[cfg(test)]
        LFS_PATCH_RANGES_COMPACTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    Ok(state.ranges.as_slice() == [(0, total)])
}

/// Consumes a completed LFS patch session's staging files.
///
/// Removes the assembled data file, the range bookkeeping, and the last-touched
/// sidecar. Called after a promotion (whether it succeeded or failed) and when
/// a completed session's object is already present in the store (F-59). The
/// store lock is taken here with NO per-OID guard held; the caller must have
/// dropped the per-OID lock first (F-31).
fn consume_lfs_patch_session(tmp_path: &FsPath, ranges_path: &FsPath, tmp_dir: &FsPath, oid: &str) {
    drop(fs::remove_file(tmp_path));
    drop(fs::remove_file(ranges_path));
    let _store_guard = LFS_PATCH_STORE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    drop(fs::remove_file(lfs_patch_meta_path(tmp_dir, oid)));
    drop(_store_guard);
    evict_lfs_patch_ranges(ranges_path);
}

/// Promotes a completed LFS patch session into the permanent store.
///
/// F-21: promote with a bounded streaming read instead of loading the entire
/// assembled object (up to 1 TiB) into RAM; the SHA-256 verification runs over
/// the streamed bytes inside the backend ingest, and the stream is capped at
/// [`MAX_LFS_OBJECT_SIZE`] (the declared-size check stays). The session is
/// consumed by the promotion: the staging files are removed whether the store
/// commit succeeded or failed.
///
/// The backend ingest is idempotent — an already-stored object is re-verified
/// and reported as `AlreadyExists` without a second write — so this is safe to
/// re-run for a session whose ranges cover `[0, total)` but whose object was
/// never committed (F-59). The store lock is only taken inside
/// [`consume_lfs_patch_session`], with NO per-OID guard held (F-31); the
/// caller must have dropped the per-OID lock first.
#[allow(clippy::too_many_arguments)]
fn promote_lfs_patch_session(
    tmp_path: &FsPath,
    ranges_path: &FsPath,
    tmp_dir: &FsPath,
    oid: &str,
    backend: &crate::ServerBackend,
    object_key: &shardline_storage::ObjectKey,
    stream_chunk_size: usize,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<(), ServerError> {
    let promotion = (|| {
        let promotion_body = bounded_file_stream(tmp_path, stream_chunk_size, MAX_LFS_OBJECT_SIZE)?;
        tokio::runtime::Handle::current().block_on(
            crate::ServerBackend::put_sha256_addressed_object_stream_if_absent(
                backend,
                object_key,
                oid,
                promotion_body,
                repository_scope,
            ),
        )
    })();
    consume_lfs_patch_session(tmp_path, ranges_path, tmp_dir, oid);
    promotion.map(|_outcome| ())
}

#[tracing::instrument(skip(state, headers, request))]
pub(crate) async fn lfs_batch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Response, ServerError> {
    let operation = match LfsOperation::from_str(&request.operation) {
        Ok(operation) => operation,
        Err(()) => return Ok(lfs_validation_response("unsupported operation")),
    };
    let requested_scope = match operation {
        LfsOperation::Download => TokenScope::Read,
        LfsOperation::Upload => TokenScope::Write,
    };
    // The batch operation field selects the scope; the capability is minted
    // from the request headers exactly as the per-object handlers do.
    let auth = authorize_repository(&state, &headers, requested_scope)?;
    if request.objects.len() > MAX_LFS_BATCH_OBJECTS {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "too many objects in batch request" })),
        )
            .into_response());
    }
    if let Some(hash_algo) = request.hash_algo.as_deref()
        && hash_algo != "sha256"
    {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported hash algorithm" })),
        )
            .into_response());
    }

    // Determine the transfer adapter. Prefer "xet" when the client supports it
    // and the server has an auth provider to mint CAS tokens. Fall back to "basic".
    let adapters: Vec<Option<TransferAdapter>> = request
        .transfers
        .iter()
        .map(|transfer| match transfer.as_str() {
            "xet" => Some(TransferAdapter::Xet),
            "basic" => Some(TransferAdapter::Basic),
            _ => None,
        })
        .collect();
    let supports_xet = adapters.contains(&Some(TransferAdapter::Xet));
    let supports_basic = adapters.contains(&Some(TransferAdapter::Basic));
    let use_xet = supports_xet && state.auth.is_some() && auth.claims().is_some();
    let transfer = if use_xet {
        "xet"
    } else if request.transfers.is_empty() || supports_basic {
        "basic"
    } else {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported transfer adapter" })),
        )
            .into_response());
    };

    // Mint a CAS token when using xet transfer. The existing claims are
    // re-signed so git-xet receives a scoped token for the CAS layer.
    let cas_token = if use_xet {
        auth.claims().and_then(|claims| {
            state
                .auth
                .as_ref()
                .and_then(|server_auth| server_auth.provider().mint_token(claims).ok())
        })
    } else {
        None
    };
    let cas_url = state
        .config
        .public_base_url()
        .trim_end_matches('/')
        .to_owned();
    let xet_action_header = cas_token.as_ref().map(|token| {
        json!({
            URL: &cas_url,
            ACCESS_TOKEN: token,
            TOKEN_EXPIRATION: "0"
        })
    });

    let mut objects = Vec::with_capacity(request.objects.len());
    for object in request.objects {
        let object_key = match lfs_object_key(&object.oid, &auth) {
            Ok(k) => k,
            Err(e) => {
                tracing::debug!(error = %e, "LFS OID parsing failed");
                return Ok(lfs_validation_response("invalid oid"));
            }
        };
        let object_length = state
            .backend
            .object_length_scoped(&object_key, auth.repository())
            .await;
        match operation {
            LfsOperation::Download => match object_length {
                Ok(length) => {
                    let action = if let Some(ref header) = xet_action_header {
                        json!({
                            "download": {
                                "href": format!(
                                    "{}/v1/lfs/objects/{}",
                                    cas_url, object.oid
                                ),
                                "header": header
                            }
                        })
                    } else {
                        json!({
                            "download": {
                                "href": format!(
                                    "{}/v1/lfs/objects/{}",
                                    cas_url, object.oid
                                )
                            }
                        })
                    };
                    objects.push(LfsObjectResponse {
                        oid: object.oid,
                        size: length,
                        authenticated: Some(auth.claims().is_some()),
                        actions: Some(action),
                        error: None,
                    });
                }
                Err(ServerError::NotFound) => objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size: object.size,
                    authenticated: None,
                    actions: None,
                    error: Some(LfsObjectError {
                        code: 404,
                        message: "Object does not exist".to_owned(),
                    }),
                }),
                Err(error) => return Err(error),
            },
            LfsOperation::Upload => {
                let (size, actions) = match object_length {
                    Ok(length) => (length, None),
                    Err(ServerError::NotFound) => {
                        let action = if let Some(ref header) = xet_action_header {
                            json!({
                                "upload": {
                                    "href": format!(
                                        "{}/v1/lfs/objects/{}",
                                        cas_url, object.oid
                                    ),
                                    "header": header
                                }
                            })
                        } else {
                            json!({
                                "upload": {
                                    "href": format!(
                                        "{}/v1/lfs/objects/{}",
                                        cas_url, object.oid
                                    )
                                }
                            })
                        };
                        (object.size, Some(action))
                    }
                    Err(error) => return Err(error),
                };
                objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size,
                    authenticated: Some(auth.claims().is_some()),
                    actions,
                    error: None,
                });
            }
        }
    }
    Ok((
        [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
        Json(LfsBatchResponse {
            transfer: transfer.to_owned(),
            objects,
            hash_algo: "sha256",
        }),
    )
        .into_response())
}

#[tracing::instrument(skip(state, headers), fields(oid))]
pub(crate) async fn lfs_get_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsRepository,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };
    metrics::record_lfs_download();
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        Some(format!("sha256:{oid}")),
        "lfs",
        repo.capability().repository(),
    )
    .await
}

#[tracing::instrument(skip(state, _headers), fields(oid))]
pub(crate) async fn lfs_head_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsRepository,
    _headers: HeaderMap,
) -> Result<Response, ServerError> {
    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };
    let total_length = state
        .backend
        .object_length_scoped(&object_key, repo.capability().repository())
        .await?;
    Ok((
        StatusCode::OK,
        [
            (CONTENT_LENGTH, total_length.to_string()),
            (CONTENT_TYPE, "application/octet-stream".to_owned()),
        ],
    )
        .into_response())
}

#[tracing::instrument(skip(state, headers, body), fields(oid))]
pub(crate) async fn lfs_put_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsWriteRepository,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let _admit = state
        .admission
        .try_acquire(weights::XORB_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;

    // The LFS specification does not require a specific Content-Type for
    // object upload. The body content is verified by its SHA-256 digest
    // regardless of Content-Type. Accept any Content-Type, including no
    // Content-Type, to interoperate with git-lfs and other LFS clients.

    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };
    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    let start = Instant::now();
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(
            &object_key,
            &oid,
            body,
            repo.capability().repository(),
        )
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("lfs", content_length, elapsed, true);
    shardline_metrics::metrics().protocol.record_lfs_upload();
    Ok(StatusCode::OK.into_response())
}

#[tracing::instrument(skip(state, _headers))]
pub(crate) async fn lfs_delete_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsWriteRepository,
    _headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };
    match state.backend.delete_object_if_present(&object_key).await? {
        DeleteOutcome::Deleted => Ok(StatusCode::ACCEPTED.into_response()),
        DeleteOutcome::NotFound => Err(ServerError::NotFound),
    }
}

/// PATCH /v1/lfs/objects/{oid} — Chunked upload (Content-Range)
///
/// Accepts a chunk of bytes and stores it at the specified offset using a temp
/// file keyed by OID. Once the persisted ranges cover the complete object, the
/// accumulated file is promoted to the permanent object store.
#[tracing::instrument(skip(state, headers, body), fields(oid))]
pub(crate) async fn lfs_patch_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsWriteRepository,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let _admit = state
        .admission
        .try_acquire(weights::XORB_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;

    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };

    // Validate Content-Range header is present.
    let content_range = match headers.get(CONTENT_RANGE) {
        Some(value) => value.to_str().unwrap_or("").to_owned(),
        None => {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
                Json(json!({ "message": "missing Content-Range header" })),
            )
                .into_response());
        }
    };

    // Parse the Content-Range header: "bytes start-end/total".
    let (offset, end, total) = match parse_content_range(&content_range) {
        Ok(range) => range,
        Err(()) => {
            return Ok((
                StatusCode::RANGE_NOT_SATISFIABLE,
                [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
                Json(json!({ "message": "invalid Content-Range header" })),
            )
                .into_response());
        }
    };
    if total == 0 || end >= total {
        return Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "Content-Range exceeds object length" })),
        )
            .into_response());
    }
    if total > MAX_LFS_OBJECT_SIZE {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "declared object size exceeds maximum allowed" })),
        )
            .into_response());
    }

    let expected_chunk_size = end
        .checked_sub(offset)
        .ok_or(ServerError::Overflow)?
        .checked_add(1)
        .ok_or(ServerError::Overflow)?;

    let content_length = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if content_length != expected_chunk_size {
        return Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "Content-Length does not match Content-Range" })),
        )
            .into_response());
    }

    let start = Instant::now();
    let mut body_reader =
        RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let chunk_bytes: Vec<u8> = read_body_to_bytes(&mut body_reader).await?;
    let chunk_size = chunk_bytes.len() as u64;

    if chunk_size != expected_chunk_size {
        return Ok((
            StatusCode::RANGE_NOT_SATISFIABLE,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "actual body length does not match Content-Range" })),
        )
            .into_response());
    }

    match state
        .backend
        .object_length_scoped(&object_key, repo.capability().repository())
        .await
    {
        Ok(_length) => {
            return Ok((
                StatusCode::CONFLICT,
                [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
                Json(json!({ "message": "object upload is already complete" })),
            )
                .into_response());
        }
        Err(ServerError::NotFound) => {}
        Err(error) => return Err(error),
    }

    // Write the chunk to a temp file at the correct offset.
    // Use a deterministic path based on OID so multiple chunks accumulate in the same file.
    // The temp directory is per-server-instance, avoiding cross-session conflicts.
    //
    // All blocking I/O is offloaded to the tokio blocking thread-pool to avoid
    // starving the async runtime.  A per-OID Mutex serializes concurrent PATCH
    // requests for the same object, preventing data corruption in the shared
    // temp file.
    let root_dir = state.config.root_dir().to_path_buf();
    let backend = state.backend.clone();
    let oid_for_closure = oid.clone();
    let object_key_for_closure = object_key.clone();
    let repository_scope = repo.capability().repository().cloned();

    let max_active_sessions = state.config.lfs_patch_max_active_sessions();
    let total_max_bytes = state.config.lfs_patch_total_max_bytes();
    let patch_ttl_seconds = state.config.lfs_patch_ttl_seconds();
    let max_seek_ahead_bytes = state.config.lfs_patch_max_seek_ahead_bytes();
    let stream_chunk_size = state.config.chunk_size().get();

    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("lfs", content_length, elapsed, true);

    tokio::task::spawn_blocking(move || {
        let tmp_dir = lfs_patch_dir(&root_dir);
        fs::create_dir_all(&tmp_dir).ok();
        let tmp_path = tmp_dir.join(&oid_for_closure);
        let ranges_path = tmp_dir.join(format!("{oid_for_closure}.ranges"));

        // F-20: sweep expired sessions first (crash recovery), then enforce
        // the active-session cap and the aggregate staging-byte cap BEFORE
        // the chunk is written. The body was fully buffered above, so no lock
        // is ever held across a network stream. The last-touched sidecar marks
        // the session active from its first byte and is refreshed on every
        // subsequent PATCH.
        //
        // F-31: the store lock is acquired first (the sweep takes the same two
        // locks in the same order) and dropped before the staging write; it is
        // NEVER re-acquired while a per-OID lock is held. The per-OID guard is
        // scoped to the pre-write checks + staging write + range record, then
        // released before the promotion; the promotion cleanup re-acquires the
        // store lock only with no per-OID guard held.
        let now = lfs_patch_now_seconds()?;
        let store_guard = LFS_PATCH_STORE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        sweep_lfs_patch_sessions_locked(&tmp_dir, patch_ttl_seconds, now)?;
        let (active_sessions, used_bytes) = patch_store_usage(&tmp_dir)?;
        // F-60: a continuation of an EXISTING session (its `.meta` sidecar is
        // already on disk, marking it active) must not be charged against the
        // active-session cap. Charging continuations made the cap a
        // chunked-PATCH denial of service: a writer holding every slot with
        // tiny sessions would permanently reject continuations of those very
        // sessions — and every other in-progress multi-chunk upload — with 429
        // mid-stream. Only NEW sessions consume a slot; the aggregate byte cap
        // below still applies to continuations, so a writer cannot bypass the
        // byte limits by reusing one OID.
        let is_continuation = lfs_patch_meta_path(&tmp_dir, &oid_for_closure).exists();
        if !is_continuation && active_sessions >= max_active_sessions.get() {
            return Err(ServerError::LfsPatchTooManySessions);
        }
        if checked_add(used_bytes, chunk_size)? > total_max_bytes.get() {
            return Err(ServerError::LfsPatchStoreFull);
        }

        // Take the per-OID lock while still holding the store lock, then drop
        // the store lock before the disk write: the per-OID lock alone
        // serializes same-OID PATCHes and protects the staging files from the
        // sweep (which holds the store lock and waits on the per-OID lock).
        let lock_arc = acquire_lfs_patch_lock(&oid_for_closure);
        // Recover from poisoning: the lock is a simple empty-token Mutex<()>,
        // so its state is trivially consistent even if a previous holder panicked.
        let lock = lock_arc.lock().unwrap_or_else(|e| e.into_inner());

        // Pre-write checks under the per-OID lock. A concurrent same-OID PATCH
        // may have already covered [0,total) and be promoting the object; and
        // the sequential-growth seek bound must be enforced against the
        // session's current high-water mark so a fresh session cannot jump to
        // a multi-TiB sparse offset (F-30).
        let (high_water_mark, already_complete) = inspect_lfs_patch_ranges(&ranges_path, total)?;
        // F-77: the seek bound is enforced BEFORE the session sidecar is
        // created (or its TTL refreshed). A rejected seek must not leave a
        // zero-byte "ghost" session ({oid}.meta with no data or ranges) behind:
        // the active-session cap counts every .meta as a session, so an
        // attacker could occupy every slot with byte-cost-free ghosts that stay
        // refreshable forever (each 416 re-touches last-touched). The fresh
        // in-memory range state this check loaded is evicted so it cannot leak
        // either; an existing session's disk state is reloaded on its next
        // PATCH. A completed session ([0,total) covered) is exempt: it goes
        // down the F-59 re-arm path below regardless of this request's offset.
        if !already_complete && offset > checked_add(high_water_mark, max_seek_ahead_bytes.get())? {
            evict_lfs_patch_ranges(&ranges_path);
            return Err(ServerError::LfsPatchRangeNotSatisfiable);
        }
        touch_patch_session(&tmp_dir, &oid_for_closure, now)?;
        drop(store_guard);
        if already_complete {
            // F-59: a session whose ranges cover [0,total) but whose object is
            // not yet in the store is the crash-left state of a promotion that
            // never finished — the window between `record_lfs_patch_range`
            // returning the promotion trigger and the store commit spans the
            // entire bounded-stream ingest of up to 1 TiB. Early-returning here
            // reported success forever (PATCH 200, HEAD/verify 404) without
            // promoting, leaving the staging files to the TTL sweep. Re-arm
            // the promotion instead: it consumes the staging files whether it
            // succeeds or fails, and if the object was already committed (a
            // concurrent same-OID promotion, or a crash after the commit but
            // before the cleanup) the backend ingest is an idempotent no-op.
            drop(lock);
            let object_present = match tokio::runtime::Handle::current().block_on(
                backend.object_length_scoped(&object_key_for_closure, repository_scope.as_ref()),
            ) {
                Ok(_length) => true,
                Err(ServerError::NotFound) => false,
                Err(error) => return Err(error),
            };
            if object_present {
                // The object made it into the store; only the staging cleanup
                // was lost (or a concurrent promotion owns the commit).
                consume_lfs_patch_session(&tmp_path, &ranges_path, &tmp_dir, &oid_for_closure);
            } else {
                promote_lfs_patch_session(
                    &tmp_path,
                    &ranges_path,
                    &tmp_dir,
                    &oid_for_closure,
                    &backend,
                    &object_key_for_closure,
                    stream_chunk_size,
                    repository_scope.as_ref(),
                )?;
            }
            return Ok(());
        }

        // The staging write + range record run under the per-OID lock; the
        // promotion (bounded streaming + backend ingest + store-lock cleanup)
        // runs AFTER the per-OID guard is released (F-31). A failure at any
        // point removes the staging files this PATCH owns (F-21: promotion
        // failure cleans its own temp; the F-20 sweep covers crashed-session
        // leftovers).
        let end_exclusive = end.checked_add(1).ok_or(ServerError::Overflow)?;
        let write_result: Result<bool, ServerError> = (|| {
            {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(&tmp_path)?;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&chunk_bytes)?;
            }
            record_lfs_patch_range(&ranges_path, offset, end_exclusive, total)
        })();
        drop(lock);
        let promote = match write_result {
            Ok(promote) => promote,
            Err(error) => {
                // A failed staging step must not leave its own temp files
                // behind. The per-OID guard has already been dropped, so the
                // store lock may be taken safely for the meta removal (F-31).
                drop(fs::remove_file(&tmp_path));
                drop(fs::remove_file(&ranges_path));
                let _store_guard = LFS_PATCH_STORE_LOCK
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                drop(fs::remove_file(lfs_patch_meta_path(
                    &tmp_dir,
                    &oid_for_closure,
                )));
                drop(_store_guard);
                evict_lfs_patch_ranges(&ranges_path);
                return Err(error);
            }
        };

        if promote {
            // F-21: promote with a bounded streaming read instead of loading
            // the entire assembled object (up to 1 TiB) into RAM; the SHA-256
            // verification runs over the streamed bytes inside the backend
            // ingest, and the stream is capped at MAX_LFS_OBJECT_SIZE (the
            // declared-size check above stays). The session is consumed by the
            // promotion (staging files removed whether the store commit
            // succeeded or failed); the store lock is taken inside with NO
            // per-OID guard held (F-31).
            promote_lfs_patch_session(
                &tmp_path,
                &ranges_path,
                &tmp_dir,
                &oid_for_closure,
                &backend,
                &object_key_for_closure,
                stream_chunk_size,
                repository_scope.as_ref(),
            )?;
        }

        Ok::<_, ServerError>(())
    })
    .await
    .map_err(ServerError::BlockingTask)??;

    Ok(StatusCode::OK.into_response())
}

/// POST /v1/lfs/objects/{oid}/verify — Upload verification
///
/// Verifies that an object exists in the store and that its SHA-256 hash
/// matches the requested OID.  Returns 200 OK on success, 404 if not found,
/// or 422 if the hash does not match.
#[tracing::instrument(skip(state, _headers), fields(oid))]
pub(crate) async fn lfs_verify_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    repo: LfsWriteRepository,
    _headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let object_key = match lfs_object_key(&oid, repo.capability()) {
        Ok(k) => k,
        Err(e) => {
            tracing::debug!(error = %e, "LFS OID parsing failed");
            return Ok(lfs_validation_response("invalid oid"));
        }
    };

    // Check object existence and size before reading.
    let total_length = match state
        .backend
        .object_length_scoped(&object_key, repo.capability().repository())
        .await
    {
        Ok(len) => len,
        Err(ServerError::NotFound) => {
            return Ok(StatusCode::NOT_FOUND.into_response());
        }
        Err(e) => return Err(e),
    };

    if total_length > MAX_LFS_VERIFY_BYTES {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "object too large for server-side verification" })),
        )
            .into_response());
    }

    // Stream the object through a SHA-256 hasher in fixed-size chunks
    // to avoid loading the entire object into memory (OOM prevention).
    let mut hasher = Sha256::new();
    let mut byte_stream = match state
        .backend
        .read_object_stream(&object_key, total_length, None)
        .await
    {
        Ok(stream) => stream,
        Err(error) => {
            tracing::warn!(%error, ?object_key, "LFS verification could not read stored object");
            return Ok(lfs_validation_response("stored object is corrupt"));
        }
    };
    while let Some(chunk_result) = byte_stream.next().await {
        let chunk = match chunk_result {
            Ok(chunk) => chunk,
            Err(error) => {
                tracing::warn!(%error, ?object_key, "LFS verification encountered corrupt storage");
                return Ok(lfs_validation_response("stored object is corrupt"));
            }
        };
        hasher.update(&chunk);
    }
    let computed_hash = hex::encode(hasher.finalize());

    if computed_hash != oid {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "SHA-256 hash mismatch" })),
        )
            .into_response());
    }

    Ok(StatusCode::OK.into_response())
}

/// Parses a `Content-Range` header value, returning `(start, end, total)`.
///
/// Accepted format:
/// - `bytes start-end/total`
fn parse_content_range(value: &str) -> Result<(u64, u64, u64), ()> {
    let value = value.trim();
    let value = value.strip_prefix("bytes ").ok_or(())?;
    let (range_part, total_part) = value.split_once('/').ok_or(())?;
    let total: u64 = total_part.parse().map_err(|_err| ())?;
    let mut parts = range_part.split('-');
    let start: u64 = parts.next().ok_or(())?.trim().parse().map_err(|_err| ())?;
    let end: u64 = parts.next().ok_or(())?.trim().parse().map_err(|_err| ())?;
    if end < start {
        return Err(());
    }
    Ok((start, end, total))
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        sync::Arc,
    };

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::{get, post},
    };
    use serde_json::{Value, json};
    use sha2::Digest;
    use shardline_protocol::TokenScope;
    use shardline_server_core::AuthProvider;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use crate::{
        ServerConfig, ServerError, ServerFrontend, ServerRole, app::AppState, lfs_object_key,
    };
    use shardline_server_core::AuthorizedRepository;

    use super::{
        LFS_PATCH_LOCKS, acquire_lfs_patch_lock, evict_lfs_patch_ranges, inspect_lfs_patch_ranges,
        lfs_batch, lfs_delete_object, lfs_get_object, lfs_head_object, lfs_patch_dir,
        lfs_patch_meta_path, lfs_patch_now_seconds, lfs_patch_object, lfs_patch_ranges_compactions,
        lfs_put_object, lfs_validation_response, lfs_verify_object, live_lfs_patch_lock_count,
        parse_content_range, patch_store_usage, record_lfs_patch_range, sweep_lfs_patch_sessions,
        touch_patch_session,
    };

    /// Test signing key matching the one used in e2e tests.
    const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    // ---------------------------------------------------------------------------
    // Test helpers
    // ---------------------------------------------------------------------------

    /// A valid 64-character lowercase hex OID suitable for LFS tests.
    fn test_oid(content: &[u8]) -> String {
        hex::encode(sha2::Sha256::digest(content))
    }

    fn test_oid_constant() -> String {
        test_oid(b"test-lfs-object")
    }

    /// Builds a minimal [`AppState`] backed by a fresh temp directory.
    ///
    /// `auth` is left as `None` so that route handlers skip authorization checks,
    /// which keeps each test self-contained without token minting.
    async fn build_test_state() -> (Arc<AppState>, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:0".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_frontends([ServerFrontend::Lfs])
        .expect("server frontends");

        let backend = crate::ServerBackend::from_config(&config)
            .await
            .expect("backend from config");

        let transfer_limiter = crate::TransferLimiter::new(chunk_size, chunk_size);

        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: crate::ReconstructionCacheService::disabled(),
            transfer_limiter,
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: crate::ProtocolMetrics::default(),
        });

        (state, tmp)
    }

    /// Builds a minimal [`AppState`] with explicit LFS chunked-patch store
    /// limits (active-session cap, aggregate byte cap, staging TTL).
    async fn build_test_state_with_lfs_options(
        max_active_sessions: std::num::NonZeroUsize,
        total_max_bytes: NonZeroU64,
        ttl_seconds: NonZeroU64,
    ) -> (Arc<AppState>, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:0".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_frontends([ServerFrontend::Lfs])
        .expect("server frontends")
        .with_lfs_patch_max_active_sessions(max_active_sessions)
        .expect("lfs patch max active sessions")
        .with_lfs_patch_total_max_bytes(total_max_bytes)
        .expect("lfs patch total max bytes")
        .with_lfs_patch_ttl_seconds(ttl_seconds)
        .expect("lfs patch ttl");

        let backend = crate::ServerBackend::from_config(&config)
            .await
            .expect("backend from config");

        let transfer_limiter = crate::TransferLimiter::new(chunk_size, chunk_size);

        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: crate::ReconstructionCacheService::disabled(),
            transfer_limiter,
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: crate::ProtocolMetrics::default(),
        });

        (state, tmp)
    }

    /// Builds a minimal [`AppState`] with an auth provider for xet transfer tests.
    async fn build_test_state_with_auth() -> (Arc<AppState>, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_frontends([ServerFrontend::Lfs])
        .expect("server frontends");

        let backend = crate::ServerBackend::from_config(&config)
            .await
            .expect("backend from config");

        let transfer_limiter = crate::TransferLimiter::new(chunk_size, chunk_size);
        let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: Some(auth),
            provider_tokens: None,
            reconstruction_cache: crate::ReconstructionCacheService::disabled(),
            transfer_limiter,
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: crate::ProtocolMetrics::default(),
        });

        (state, tmp)
    }

    /// Mints a test token for use with the auth-enabled test state.
    fn mint_test_token(scope: TokenScope) -> String {
        use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims};
        use shardline_server_core::auth::LocalHmacProvider;

        let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
        let repo = RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main"))
            .unwrap();
        let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
        provider.mint_token(&claims).unwrap()
    }

    /// Registers only the LFS routes on a fresh [`Router`] and attaches state.
    fn lfs_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/lfs/objects/batch", post(lfs_batch))
            .route(
                "/v1/lfs/objects/{oid}",
                get(lfs_get_object)
                    .head(lfs_head_object)
                    .put(lfs_put_object)
                    .patch(lfs_patch_object)
                    .delete(lfs_delete_object),
            )
            .route("/v1/lfs/objects/{oid}/verify", post(lfs_verify_object))
            .with_state(state)
    }

    // =========================================================================
    // parse_content_range tests
    // =========================================================================

    #[test]
    fn parse_content_range_accepts_standard_format() {
        assert_eq!(parse_content_range("bytes 0-99/200"), Ok((0, 99, 200)));
    }

    #[test]
    fn parse_content_range_accepts_with_whitespace() {
        // The parser trims the entire value and the prefix, but does NOT
        // trim internal whitespace between range and total parts.
        assert_eq!(parse_content_range("bytes 0-99/200"), Ok((0, 99, 200)));
    }

    #[test]
    fn parse_content_range_accepts_large_offsets() {
        assert_eq!(
            parse_content_range("bytes 1048576-2097151/4194304"),
            Ok((1048576, 2097151, 4194304))
        );
    }

    #[test]
    fn parse_content_range_rejects_missing_bytes_prefix() {
        assert_eq!(parse_content_range("0-99/200"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_missing_total() {
        assert_eq!(parse_content_range("bytes 0-99"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_non_numeric_start() {
        assert_eq!(parse_content_range("bytes abc-99/200"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_non_numeric_end() {
        assert_eq!(parse_content_range("bytes 0-xyz/200"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_non_numeric_total() {
        assert_eq!(parse_content_range("bytes 0-99/abc"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_empty_string() {
        assert_eq!(parse_content_range(""), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_total_only() {
        assert_eq!(parse_content_range("bytes /200"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_negative_numbers() {
        assert_eq!(parse_content_range("bytes -1-99/200"), Err(()));
    }

    #[test]
    fn parse_content_range_rejects_end_before_start() {
        assert_eq!(parse_content_range("bytes 100-50/200"), Err(()));
    }

    #[test]
    fn parse_content_range_accepts_end_equals_start() {
        // Single-byte chunk at offset 5.
        assert_eq!(parse_content_range("bytes 5-5/200"), Ok((5, 5, 200)));
    }

    // ── lfs_validation_response ────────────────────────────────────────────

    #[test]
    fn lfs_validation_response_returns_unprocessable_entity() {
        let response = lfs_validation_response("test error");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lfs_validation_response_includes_json_body() {
        let response = lfs_validation_response("invalid oid");
        let body = response.into_body();
        let bytes = axum::body::to_bytes(body, 1024).await.unwrap();
        let parsed: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["message"], "invalid oid");
    }

    #[test]
    fn lfs_validation_response_sets_lfs_content_type() {
        let response = lfs_validation_response("too many objects");
        assert_eq!(response.headers()["content-type"], crate::LFS_CONTENT_TYPE);
    }

    // =========================================================================
    // lfs_batch tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_download_missing_object_returns_404_error_in_objects() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let request = json!({
            "operation": "download",
            "objects": [{ "oid": oid, "size": 1024 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["transfer"], "basic");
        assert_eq!(parsed["hash_algo"], "sha256");
        let objects = parsed["objects"].as_array().unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0]["oid"], oid);
        assert_eq!(objects[0]["error"]["code"], 404);
        assert_eq!(objects[0]["error"]["message"], "Object does not exist");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_upload_missing_object_returns_upload_action() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let request = json!({
            "operation": "upload",
            "objects": [{ "oid": oid, "size": 512 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        let objects = parsed["objects"].as_array().unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0]["oid"], oid);
        assert_eq!(objects[0]["size"], 512);
        assert!(
            objects[0]["actions"]["upload"]["href"]
                .as_str()
                .unwrap()
                .contains(&oid)
        );
        assert!(objects[0]["error"].is_null());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_rejects_unsupported_operation() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "verify",
            "objects": []
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "unsupported operation");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_rejects_invalid_oid() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "objects": [{ "oid": "not-a-valid-hash", "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "invalid oid");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_rejects_unsupported_hash_algorithm() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "hash_algo": "sha512",
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "unsupported hash algorithm");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_rejects_unsupported_transfer_adapter() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "transfers": ["custom"],
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "unsupported transfer adapter");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_accepts_basic_transfer_adapter_explicitly() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["transfer"], "basic");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_accepts_empty_objects_list() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "objects": []
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        let objects = parsed["objects"].as_array().unwrap();
        assert!(objects.is_empty());
    }

    // =========================================================================
    // lfs_batch xet transfer tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_xet_transfer_without_auth_falls_back_to_basic() {
        // When no auth provider is configured, xet transfer is not available.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "transfers": ["xet", "basic"],
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        // Without auth, falls back to basic (no CAS token to return).
        assert_eq!(parsed["transfer"], "basic");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_xet_transfer_without_auth_rejects_xet_only() {
        // When no auth provider, "xet" alone is unsupported (no fallback).
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let request = json!({
            "operation": "download",
            "transfers": ["xet"],
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_xet_transfer_with_auth_returns_xet_upload_actions() {
        let (state, _tmp) = build_test_state_with_auth().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();
        let token = mint_test_token(TokenScope::Write);

        let request = json!({
            "operation": "upload",
            "transfers": ["xet", "basic"],
            "objects": [{ "oid": oid, "size": 512 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();

        // Must use xet transfer
        assert_eq!(parsed["transfer"], "xet");

        // Each object must have the CAS action headers
        let obj = &parsed["objects"][0];
        assert_eq!(obj["oid"], oid);
        let upload = &obj["actions"]["upload"];
        assert!(upload["href"].as_str().unwrap().contains(&oid));

        let header = &upload["header"];
        assert!(
            header["X-Xet-Cas-Url"]
                .as_str()
                .unwrap()
                .contains("http://127.0.0.1:8080"),
            "CAS URL should point to the server"
        );
        assert!(
            header["X-Xet-Access-Token"]
                .as_str()
                .is_some_and(|t| !t.is_empty()),
            "Access token should be present and non-empty"
        );
        assert!(
            header["X-Xet-Token-Expiration"].as_str().is_some(),
            "Token expiration should be present"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_xet_transfer_download_existing_object_includes_headers() {
        let (state, _tmp) = build_test_state_with_auth().await;
        let app = lfs_router(state);
        let content = b"xet-download-test-content";
        let oid = test_oid(content);
        let token = mint_test_token(TokenScope::Write);

        // Upload first (requires auth)
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", content.len().to_string())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);

        // Batch download with xet transfer
        let request = json!({
            "operation": "download",
            "transfers": ["xet", "basic"],
            "objects": [{ "oid": oid, "size": content.len() as u64 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(parsed["transfer"], "xet");
        let obj = &parsed["objects"][0];
        let download = &obj["actions"]["download"];

        let header = &download["header"];
        assert!(
            header["X-Xet-Cas-Url"]
                .as_str()
                .is_some_and(|u| !u.is_empty()),
            "download actions should include X-Xet-Cas-Url"
        );
        assert!(
            header["X-Xet-Access-Token"]
                .as_str()
                .is_some_and(|t| !t.is_empty()),
            "download actions should include X-Xet-Access-Token"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_xet_transfer_authenticated_with_read_token() {
        let (state, _tmp) = build_test_state_with_auth().await;
        let app = lfs_router(state);
        let token = mint_test_token(TokenScope::Read);

        let request = json!({
            "operation": "download",
            "transfers": ["xet", "basic"],
            "objects": [{ "oid": test_oid_constant(), "size": 100 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();

        // Read scope should still get xet transfer for downloads
        assert_eq!(parsed["transfer"], "xet");
    }

    // =========================================================================
    // lfs_get_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_object_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/lfs/objects/not-a-valid-oid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "invalid oid");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_object_happy_path_after_upload() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        // Use data large enough (>256 bytes) to produce multiple CDC chunks
        // with chunk_size=128 (min_chunk=16, max_chunk=256), ensuring xorb
        // packing produces a multi-chunk xorb that the download path handles.
        let content: Vec<u8> = (0u16..300u16).map(|i| (i as u8) ^ 0xAA).collect();
        let oid = test_oid(&content);

        // Upload first
        let put_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", content.len())
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        // Download
        let get_response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content);
    }

    // =========================================================================
    // lfs_head_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_object_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_object_happy_path_after_upload() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"head-test-content";
        let oid = test_oid(content);

        // Upload first
        let put_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", content.len())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_response.status(), StatusCode::OK);

        // HEAD
        let head_response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::OK);
        let content_length = head_response
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(content_length, content.len() as u64);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn head_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/v1/lfs/objects/not-a-valid-oid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // =========================================================================
    // lfs_put_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_happy_path() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let content = b"put-test-content";
        let oid = test_oid(content);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .header("content-length", content.len())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_accepts_wrong_content_type() {
        // The Content-Type check was relaxed for git-lfs compatibility.
        // Non-octet-stream Content-Types are accepted; the body is validated
        // by its SHA-256 digest regardless of Content-Type.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let content = b"test-lfs-object";
        let oid = test_oid(content);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "text/plain")
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_accepts_missing_content_type() {
        // git-lfs does not always send Content-Type; the handler accepts
        // requests without it.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let content = b"test-lfs-object";
        let oid = test_oid(content);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/v1/lfs/objects/not-a-valid-oid")
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(b"hello".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "invalid oid");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn put_object_is_idempotent() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let content = b"idempotent-content";
        let oid = test_oid(content);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
    }

    // =========================================================================
    // lfs_delete_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_object_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_object_happy_path_after_upload() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"delete-me";
        let oid = test_oid(content);

        // Upload
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);

        // Delete
        let del = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(del.status(), StatusCode::ACCEPTED);

        // Confirm deleted
        let head = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/v1/lfs/objects/not-a-valid-oid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // =========================================================================
    // lfs_patch_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_single_chunk_happy_path() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"patch-content";
        let oid = test_oid(content);
        let total = content.len() as u64;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                    .header("content-length", content.len())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        // Verify object was stored
        let head = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        let content_length = head
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(content_length, total);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_missing_content_range_returns_416() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::from(b"chunk".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "missing Content-Range header");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_invalid_content_range_returns_416() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", "invalid-format")
                    .body(Body::from(b"chunk".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "invalid Content-Range header");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/v1/lfs/objects/not-a-valid-oid")
                    .header("content-range", "bytes 0-4/8")
                    .body(Body::from(b"hello".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // =========================================================================
    // lfs_patch_object concurrency / lock tests
    // =========================================================================

    #[test]
    fn acquire_lfs_patch_lock_returns_same_lock_for_same_oid() {
        let lock1 = acquire_lfs_patch_lock("abc123");
        let lock2 = acquire_lfs_patch_lock("abc123");
        assert!(Arc::ptr_eq(&lock1, &lock2));
    }

    #[test]
    fn acquire_lfs_patch_lock_returns_different_lock_for_different_oid() {
        let lock1 = acquire_lfs_patch_lock("abc123");
        let lock2 = acquire_lfs_patch_lock("def456");
        assert!(!Arc::ptr_eq(&lock1, &lock2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_concurrent_chunks_assembles_correctly() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());

        let chunk1 = b"hello-world-part-AAAA"; // 20 bytes
        let chunk2 = b"BBBB-part-two-last!!"; // 20 bytes
        let full_content = [chunk1.as_slice(), chunk2.as_slice()].concat();
        let oid = test_oid(&full_content);
        let total = full_content.len() as u64;

        let app1 = app.clone();
        let oid1 = oid.clone();
        let h1 = tokio::spawn(async move {
            app1.oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid1}"))
                    .header(
                        "content-range",
                        format!("bytes 0-{}/{}", chunk1.len() as u64 - 1, total),
                    )
                    .header("content-length", chunk1.len())
                    .body(Body::from(chunk1.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap()
        });

        let app2 = app.clone();
        let oid2 = oid.clone();
        let h2 = tokio::spawn(async move {
            app2.oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid2}"))
                    .header(
                        "content-range",
                        format!("bytes {}-{}/{}", chunk1.len(), total - 1, total),
                    )
                    .header("content-length", chunk2.len())
                    .body(Body::from(chunk2.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap()
        });

        let (r1, r2) = tokio::join!(h1, h2);
        let r1 = r1.unwrap();
        let r2 = r2.unwrap();
        assert_eq!(r1.status(), StatusCode::OK);
        assert_eq!(r2.status(), StatusCode::OK);

        // Verify the assembled object is correct.
        let head = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head.status(), StatusCode::OK);
        let content_length = head
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(content_length, total);
    }

    // =========================================================================
    // Large / overflow size field tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_accepts_u64_max_size() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid(b"test");

        let request = json!({
            "operation": "download",
            "objects": [{ "oid": oid, "size": 18446744073709551615u64 }]
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(serde_json::to_vec(&request).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // u64::MAX is a valid u64 value → should deserialize and return 200.
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        let objects = parsed["objects"].as_array().unwrap();
        assert!(
            objects[0].get("error").is_some(),
            "u64::MAX size on missing object should give an error"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_rejects_overflow_size() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        // A JSON number larger than u64::MAX — serde_json will reject it at
        // deserialization time, so axum returns 400 before the handler runs.
        // We build the JSON as a raw string because serde_json::Value cannot
        // represent numbers beyond f64 precision.
        let overflow_body = format!(
            r#"{{"operation":"download","objects":[{{"oid":"{}","size":999999999999999999999999999999999999}}]}}"#,
            "a".repeat(64)
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(overflow_body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // serde_json deserialization failure results in a 422 Unprocessable Entity
        // (axum's default Json rejection status).
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // =========================================================================
    // lfs_verify_object tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_object_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid_constant();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/lfs/objects/{oid}/verify"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_object_happy_path_after_upload() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        // Use data large enough (>256 bytes) to produce multiple CDC chunks
        // with chunk_size=128 (min_chunk=16, max_chunk=256), ensuring xorb
        // packing produces a multi-chunk xorb that the verify path handles.
        let content: Vec<u8> = (0u16..300u16).map(|i| (i as u8) ^ 0xBB).collect();
        let oid = test_oid(&content);

        // Upload
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);

        // Verify
        let verify = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/lfs/objects/{oid}/verify"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(verify.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_object_hash_mismatch_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"mismatch-content";
        let correct_oid = test_oid(content);

        // Upload with correct OID
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{correct_oid}"))
                    .header("content-type", "application/octet-stream")
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);

        // Insert data under a *different* OID key with mismatched content.
        // We use a second valid OID (of different bytes) and store `content`
        // under it.  The verify endpoint will read the bytes, re-hash them,
        // and find that sha256(content) != second_oid, triggering a 422.
        let second_oid = test_oid(b"different-content-only-for-key");
        let object_key =
            lfs_object_key(&second_oid, &AuthorizedRepository::anonymous_full_access())
                .expect("object key");
        state
            .backend
            .put_object_bytes_if_absent(&object_key, content.to_vec())
            .await
            .expect("insert mismatched data");

        // Verify with second_oid — content hash won't match
        let verify = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/lfs/objects/{second_oid}/verify"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(verify.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body = axum::body::to_bytes(verify.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["message"], "SHA-256 hash mismatch");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_rejects_body_length_mismatch() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let oid = test_oid_constant();
        let object_key = lfs_object_key(&oid, &AuthorizedRepository::anonymous_full_access())
            .expect("object key");

        // Store an initial object with known size
        let content = b"0123456789abcdef";
        state
            .backend
            .put_object_bytes_if_absent(&object_key, content.to_vec())
            .await
            .expect("store initial object");

        // Send PATCH with Content-Range claiming 10 bytes but body only has 5
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("Content-Range", "bytes 0-4/20") // claim 5 bytes
                    .header("Content-Length", "10") // but say 10
                    .header("Content-Type", "application/octet-stream")
                    .body(Body::from(b"short-body".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Content-Length != expected_chunk_size → RangeNotSatisfiable
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_object_invalid_oid_returns_422() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/not-a-valid-oid/verify")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verify_object_too_large_returns_413() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"small-object-for-size-inflation";
        let oid = test_oid(content);

        // Store a small object under the correct OID key.
        let object_key = lfs_object_key(&oid, &AuthorizedRepository::anonymous_full_access())
            .expect("object key");
        state
            .backend
            .put_object_bytes_if_absent(&object_key, content.to_vec())
            .await
            .expect("insert object");

        // Inflate the file size on disk beyond MAX_LFS_VERIFY_BYTES.
        // The local backend stores objects at root_dir()/chunks/<key>.
        let object_path = state
            .config
            .root_dir()
            .join("chunks")
            .join(object_key.as_str());
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&object_path)
            .expect("open object file for size inflation");
        file.set_len(super::MAX_LFS_VERIFY_BYTES + 1)
            .expect("inflate file size");
        drop(file);

        // Verify should be rejected with 413 Payload Too Large.
        let verify = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/v1/lfs/objects/{oid}/verify"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(verify.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(verify.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            parsed["message"],
            "object too large for server-side verification"
        );
    }

    // =========================================================================
    // F-20 — LFS patch store: bounded sessions, aggregate byte cap, TTL sweep
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_sweep_removes_stale_staging_sessions() {
        let (state, _tmp) = build_test_state().await;
        let dir = lfs_patch_dir(state.config.root_dir());
        std::fs::create_dir_all(&dir).unwrap();

        let stale_oid = "a".repeat(64);
        let fresh_oid = "b".repeat(64);
        // A stale session: data + ranges + a backdated last-touched sidecar.
        std::fs::write(dir.join(&stale_oid), b"stale-data").unwrap();
        std::fs::write(dir.join(format!("{stale_oid}.ranges")), "10\n0 10\n").unwrap();
        touch_patch_session(&dir, &stale_oid, 1).unwrap();
        // A fresh session, touched just now.
        std::fs::write(dir.join(&fresh_oid), b"fresh-data").unwrap();
        std::fs::write(dir.join(format!("{fresh_oid}.ranges")), "10\n0 10\n").unwrap();
        let now = lfs_patch_now_seconds().unwrap();
        touch_patch_session(&dir, &fresh_oid, now).unwrap();

        let ttl = state.config.lfs_patch_ttl_seconds();
        let removed = sweep_lfs_patch_sessions(state.config.root_dir(), ttl).unwrap();
        assert_eq!(removed, 1, "only the stale session is swept");

        // The stale session's data, ranges, and sidecar are all gone.
        assert!(!dir.join(&stale_oid).exists());
        assert!(!dir.join(format!("{stale_oid}.ranges")).exists());
        assert!(!lfs_patch_meta_path(&dir, &stale_oid).exists());
        // The fresh session survives.
        assert!(dir.join(&fresh_oid).exists());
        assert!(lfs_patch_meta_path(&dir, &fresh_oid).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_rejects_when_active_session_cap_reached() {
        let (state, _tmp) = build_test_state_with_lfs_options(
            std::num::NonZeroUsize::new(2).unwrap(), // max 2 active sessions
            NonZeroU64::new(1 << 40).unwrap(),
            NonZeroU64::new(3600).unwrap(),
        )
        .await;
        let root_dir = state.config.root_dir().to_path_buf();
        let app = lfs_router(state);

        // Two partial (in-flight) sessions consume both slots.
        let oid1 = test_oid(b"cap-session-one-content");
        let oid2 = test_oid(b"cap-session-two-content");
        for oid in [&oid1, &oid2] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v1/lfs/objects/{oid}"))
                        .header("content-range", "bytes 0-9/20")
                        .header("content-length", 10)
                        .body(Body::from(b"0123456789".to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // The third session hits the active-session cap (F-20).
        let oid3 = test_oid(b"cap-session-three-content");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid3}"))
                    .header("content-range", "bytes 0-9/20")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a PATCH beyond the active-session cap must be rejected"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"], "too many active lfs patch sessions");

        // The rejected PATCH never wrote a staging file.
        let dir = lfs_patch_dir(root_dir.as_path());
        assert!(
            !dir.join(&oid3).exists(),
            "an over-cap PATCH must not write its chunk"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_rejects_over_aggregate_byte_cap_before_write() {
        let (state, _tmp) = build_test_state_with_lfs_options(
            std::num::NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(100).unwrap(), // 100-byte aggregate staging cap
            NonZeroU64::new(3600).unwrap(),
        )
        .await;
        let root_dir = state.config.root_dir().to_path_buf();
        let app = lfs_router(state);

        let oid1 = test_oid(b"aggregate-cap-first-session");
        let chunk1 = vec![0x01_u8; 80];
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid1}"))
                    .header("content-range", format!("bytes 0-{}/200", chunk1.len() - 1))
                    .header("content-length", chunk1.len())
                    .body(Body::from(chunk1))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // A second session whose chunk would push the aggregate over the cap
        // is rejected BEFORE the chunk is written (F-20).
        let oid2 = test_oid(b"aggregate-cap-second-session");
        let chunk2 = vec![0x02_u8; 30];
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid2}"))
                    .header("content-range", format!("bytes 0-{}/200", chunk2.len() - 1))
                    .header("content-length", chunk2.len())
                    .body(Body::from(chunk2))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"], "lfs patch staging byte quota exceeded");

        // The rejected chunk never materialized a staging file.
        let dir = lfs_patch_dir(root_dir.as_path());
        assert!(
            !dir.join(&oid2).exists(),
            "an over-cap PATCH must not write its chunk"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_continuations_exempt_from_active_session_cap() {
        // F-60: the active-session cap must only charge NEW sessions. With the
        // cap full, a continuation of an existing session still succeeds while
        // a brand-new session is rejected — otherwise one writer could hold all
        // slots with tiny sessions and permanently block the chunked-PATCH
        // feature (every continuation of those sessions would be rejected with
        // 429 mid-stream). The aggregate byte cap below still binds
        // continuations (see patch_continuation_still_binds_aggregate_byte_cap).
        let (state, _tmp) = build_test_state_with_lfs_options(
            std::num::NonZeroUsize::new(2).unwrap(), // max 2 active sessions
            NonZeroU64::new(1 << 40).unwrap(),
            NonZeroU64::new(3600).unwrap(),
        )
        .await;
        let app = lfs_router(state);

        // Two partial sessions consume both slots.
        let content_a = b"0123456789abcdefghij"; // 20 bytes
        let oid_a = test_oid(content_a);
        let oid_b = test_oid(b"continuation-cap-session-b");
        for oid in [&oid_a, &oid_b] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v1/lfs/objects/{oid}"))
                        .header("content-range", "bytes 0-9/20")
                        .header("content-length", 10)
                        .body(Body::from(b"0123456789".to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // A brand-new session still hits the active-session cap.
        let oid_new = test_oid(b"continuation-cap-new-session");
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid_new}"))
                    .header("content-range", "bytes 0-9/20")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a new session beyond the active-session cap must be rejected"
        );

        // A continuation of an EXISTING session succeeds despite the cap; it
        // completes [0,20) and promotes, so the OID must match the assembled
        // bytes.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid_a}"))
                    .header("content-range", "bytes 10-19/20")
                    .header("content-length", 10)
                    .body(Body::from(b"abcdefghij".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a continuation of an existing session must not be rejected by the active-session cap"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_continuation_still_binds_aggregate_byte_cap() {
        // F-60: exempting continuations from the active-session cap must not
        // weaken the aggregate staging-BYTE cap — a writer cannot bypass the
        // byte limits by reusing a single OID. A continuation that would push
        // the aggregate over the cap is still rejected BEFORE the chunk is
        // written (F-20).
        let (state, _tmp) = build_test_state_with_lfs_options(
            std::num::NonZeroUsize::new(4).unwrap(),
            NonZeroU64::new(100).unwrap(), // 100-byte aggregate staging cap
            NonZeroU64::new(3600).unwrap(),
        )
        .await;
        let root_dir = state.config.root_dir().to_path_buf();
        let app = lfs_router(state);

        let oid = test_oid(b"continuation-byte-cap-content");
        let chunk1 = vec![0x01_u8; 80];
        let chunk1_len = chunk1.len() as u64;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes 0-{}/200", chunk1.len() - 1))
                    .header("content-length", chunk1.len())
                    .body(Body::from(chunk1))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // A continuation of the SAME session whose chunk would push the
        // aggregate over the cap is rejected.
        let chunk2 = vec![0x02_u8; 30];
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header(
                        "content-range",
                        format!("bytes 80-{}/200", 80 + chunk2.len() - 1),
                    )
                    .header("content-length", chunk2.len())
                    .body(Body::from(chunk2))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["error"], "lfs patch staging byte quota exceeded");

        // The rejected continuation never extended the staging file.
        let dir = lfs_patch_dir(root_dir.as_path());
        let data_len = std::fs::metadata(dir.join(&oid)).unwrap().len();
        assert_eq!(
            data_len, chunk1_len,
            "an over-cap continuation must not write its chunk"
        );
    }

    // =========================================================================
    // F-21 — LFS promotion streams in bounded chunks and cleans its temp
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_promotion_leaves_no_staging_files() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());

        let chunk1 = b"streaming-promotion-part-AAAA"; // 27 bytes
        let chunk2 = b"BBBB-part-two-last-final"; // 22 bytes
        let full_content = [chunk1.as_slice(), chunk2.as_slice()].concat();
        let oid = test_oid(&full_content);
        let total = full_content.len() as u64;

        // Two sequential PATCHes (order preserved) assemble and promote the
        // object through the bounded streaming path (F-21).
        for (offset, chunk) in [
            (0_u64, chunk1.as_slice()),
            (chunk1.len() as u64, chunk2.as_slice()),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v1/lfs/objects/{oid}"))
                        .header(
                            "content-range",
                            format!(
                                "bytes {offset}-{}/{}",
                                offset + chunk.len() as u64 - 1,
                                total
                            ),
                        )
                        .header("content-length", chunk.len())
                        .body(Body::from(chunk.to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        // Byte-equivalence through the streamed promotion.
        let get = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), full_content);

        // The promotion consumed the staging files.
        let dir = lfs_patch_dir(state.config.root_dir());
        let entries = std::fs::read_dir(&dir)
            .map(|read| read.count())
            .unwrap_or(0);
        assert_eq!(
            entries, 0,
            "a completed promotion must not leave staging files behind"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_promotion_failure_cleans_staging_files() {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());

        // The OID claims one content, but the assembled bytes differ: the
        // promotion's SHA-256 verification fails, and the failed promotion
        // must still clean its own staging files (F-21).
        let oid = test_oid(b"expected-promotion-content");
        let wrong = b"wrong-promotion-content";
        let total = wrong.len() as u64;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                    .header("content-length", wrong.len())
                    .body(Body::from(wrong.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "a hash-mismatched promotion must fail"
        );

        let dir = lfs_patch_dir(state.config.root_dir());
        let entries = std::fs::read_dir(&dir)
            .map(|read| read.count())
            .unwrap_or(0);
        assert_eq!(
            entries, 0,
            "a failed promotion must clean its staging files"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_retry_re_promotes_crash_completed_session() {
        // F-59: a crash between the promotion trigger and the store commit
        // leaves {oid} data + {oid}.ranges showing [0,total) + a fresh
        // {oid}.meta on disk with the object ABSENT from the store. A retried
        // PATCH must re-arm the promotion instead of early-returning 200
        // forever (the old behavior left PATCH 200 / HEAD 404 until the TTL
        // sweep): the retry returns 200, the object is now present (HEAD 200),
        // and the staging files are consumed.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state.clone());
        let content = b"crash-reloaded-promotion-content";
        let oid = test_oid(content);
        let total = content.len() as u64;

        // Seed the crash-left state: data + complete ranges + fresh sidecar,
        // with NO object in the store.
        let dir = lfs_patch_dir(state.config.root_dir());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&oid), content).unwrap();
        std::fs::write(
            dir.join(format!("{oid}.ranges")),
            format!("{total}\n0 {total}\n"),
        )
        .unwrap();
        let now = lfs_patch_now_seconds().unwrap();
        touch_patch_session(&dir, &oid, now).unwrap();

        // The client retries the chunk; the server must re-promote the
        // crash-completed session.
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                    .header("content-length", content.len())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a retried PATCH on a crash-completed session must report success"
        );

        // The re-promotion stored the object and consumed the staging files.
        let head = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            head.status(),
            StatusCode::OK,
            "the object must be present after the re-promotion"
        );
        let entries = std::fs::read_dir(&dir)
            .map(|read| read.count())
            .unwrap_or(0);
        assert_eq!(
            entries, 0,
            "the re-promotion must consume the crash-left staging files"
        );
    }

    // =========================================================================
    // F-30 — sparse-file accounting, sequential-growth seek bound, incremental
    // range bookkeeping
    // =========================================================================

    #[cfg(unix)]
    #[test]
    fn patch_store_usage_counts_allocated_not_logical_bytes() {
        // A 1-byte write at a huge offset creates a SPARSE file: logical size
        // 1 GiB+, allocated footprint a few KiB. The aggregate staging cap
        // must count the ALLOCATED size so a handful of such writes can never
        // exhaust it (F-30).
        let tmp = TempDir::new().expect("tempdir");
        let dir = tmp.path().join("lfs-patch");
        std::fs::create_dir_all(&dir).unwrap();
        let oid = "a".repeat(64);
        std::fs::write(dir.join(format!("{oid}.meta")), "1").unwrap();
        {
            use std::io::{Seek, SeekFrom, Write as _};
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(dir.join(&oid))
                .unwrap();
            file.seek(SeekFrom::Start(1 << 30)).unwrap();
            file.write_all(b"x").unwrap();
        }
        let (_sessions, bytes) = patch_store_usage(&dir).unwrap();
        let allocated = {
            use std::os::unix::fs::MetadataExt;
            std::fs::metadata(dir.join(&oid)).unwrap().blocks() * 512
        };
        assert!(
            bytes < (1 << 30),
            "a sparse staging file must count allocated bytes, not its logical size: {bytes}"
        );
        assert_eq!(
            bytes, allocated,
            "usage must equal the file's allocated block footprint"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_rejects_out_of_bounds_seek() {
        // F-30: a PATCH whose Content-Range start jumps far beyond the session
        // high-water mark (here: offset 2^40-1, i.e. the sparse-file attack
        // against a fresh session) is rejected instead of creating a
        // multi-TiB sparse staging file.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid(b"seek-out-of-bounds-content");
        let total = 1_u64 << 40;
        let offset = total - 1;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                    .header("content-length", 1)
                    .body(Body::from(vec![0x01]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "a PATCH seeking to 2^40-1 on a fresh session must be rejected"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_object_rejects_seek_far_beyond_high_water_mark() {
        // F-30: even after a session has grown sequentially, a seek far beyond
        // its high-water mark + slack is rejected.
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let oid = test_oid(b"seek-past-high-water-content");
        let total = 1_u64 << 40;

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", "bytes 0-9/1099511627776")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);

        let offset = total - 1;
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                    .header("content-length", 1)
                    .body(Body::from(vec![0x01]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "a PATCH seeking past the session high-water mark + slack must be rejected"
        );
    }

    #[test]
    fn patch_ranges_disjoint_set_is_incremental() {
        // F-30: recording a large disjoint range set must not rewrite the
        // whole ranges file per PATCH. The compaction counter proves the
        // journal + periodic-compaction design keeps amortized work bounded:
        // 20k appends trigger only ~20 compactions (every 1024 appends), never
        // a full read-sort-merge-rewrite per PATCH.
        let tmp = TempDir::new().expect("tempdir");
        let ranges_path = tmp.path().join("disjoint.ranges");
        let total = 2_000_000_u64;
        let compactions_before = lfs_patch_ranges_compactions();
        let mut offset = 0_u64;
        for _ in 0..20_000 {
            let start = offset;
            let end = start + 10;
            let promote = record_lfs_patch_range(&ranges_path, start, end, total).unwrap();
            assert!(!promote, "disjoint ranges with gaps must never promote");
            offset = end + 10; // leave a 10-byte gap between ranges
        }
        let compactions = lfs_patch_ranges_compactions() - compactions_before;
        assert!(
            compactions < 64,
            "a 20k disjoint-range set must compact rarely (observed {compactions})"
        );

        // The in-memory bookkeeping still exposes the exact merged state.
        let (high_water, complete) = inspect_lfs_patch_ranges(&ranges_path, total).unwrap();
        assert_eq!(
            high_water,
            offset - 10,
            "high-water must track the last end"
        );
        assert!(!complete, "gaps must never read as complete coverage");

        // Crash recovery: evicting the entry and re-loading from the on-disk
        // file must reproduce the same high-water mark.
        evict_lfs_patch_ranges(&ranges_path);
        let (high_water_after_reload, _) = inspect_lfs_patch_ranges(&ranges_path, total).unwrap();
        assert_eq!(high_water_after_reload, offset - 10);
    }

    #[test]
    fn patch_ranges_sequential_growth_promotes_incrementally() {
        // The merged-[0,total) promotion trigger must survive the incremental
        // format: sequential growth promotes exactly when the last range
        // reaches total, and out-of-order arrival still merges to full
        // coverage.
        let tmp = TempDir::new().expect("tempdir");
        let ranges_path = tmp.path().join("sequential.ranges");
        let total = 100_u64;
        for (start, end) in [(0, 40), (40, 70)] {
            let promote = record_lfs_patch_range(&ranges_path, start, end, total).unwrap();
            assert!(!promote, "partial coverage must not promote");
        }
        let promote = record_lfs_patch_range(&ranges_path, 70, 100, total).unwrap();
        assert!(promote, "sequential growth to total must trigger promotion");

        // A second session receiving the tail before the head.
        let backfill = tmp.path().join("backfill.ranges");
        assert!(!record_lfs_patch_range(&backfill, 40, 100, 100).unwrap());
        let promote = record_lfs_patch_range(&backfill, 0, 40, 100).unwrap();
        assert!(
            promote,
            "out-of-order ranges must still merge to full coverage"
        );
    }

    #[test]
    fn patch_ranges_rejects_inconsistent_total() {
        // A client changing the declared total mid-session is rejected.
        let tmp = TempDir::new().expect("tempdir");
        let ranges_path = tmp.path().join("mismatch.ranges");
        record_lfs_patch_range(&ranges_path, 0, 10, 100).unwrap();
        let error = record_lfs_patch_range(&ranges_path, 10, 20, 200).unwrap_err();
        assert!(
            matches!(error, ServerError::Io(ref io_error) if io_error.kind() == std::io::ErrorKind::InvalidData),
            "inconsistent totals must be rejected with InvalidData, got: {error}"
        );
    }

    // =========================================================================
    // F-77 — a rejected seek must not leave a ghost patch session
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn patch_rejected_seek_leaves_no_ghost_session() {
        // F-77: a PATCH whose Content-Range start jumps beyond the
        // sequential-growth seek bound returns 416 but must NOT leave a
        // zero-byte "ghost" session behind — no {oid}.meta, no staging files.
        // Every .meta counts as a session against the active-session cap, so a
        // ghost alone would exhaust a slot while costing zero bytes against the
        // aggregate byte cap, and each repeated 416 re-touches its last-touched
        // time, keeping the ghost refreshable (immune to the TTL sweep) for the
        // whole cap lifetime.
        //
        // With the active-session cap at 1, a rejected 416 must not occupy the
        // slot: a subsequent NEW-session PATCH still succeeds (200).
        let (state, _tmp) = build_test_state_with_lfs_options(
            std::num::NonZeroUsize::new(1).unwrap(), // cap: 1 active session
            NonZeroU64::new(1 << 40).unwrap(),
            NonZeroU64::new(3600).unwrap(),
        )
        .await;
        let root_dir = state.config.root_dir().to_path_buf();
        let app = lfs_router(state);
        let dir = lfs_patch_dir(root_dir.as_path());

        let oid = test_oid(b"ghost-session-content");
        let total = 1_u64 << 30; // 1 GiB declared
        // Just past the default 64 MiB seek bound (a fresh session's high-water
        // mark is 0), so the PATCH is rejected as range-not-satisfiable.
        let offset = (1_u64 << 26) + 1;

        // Repeated 416s: none of them may materialize a session.
        for _ in 0..3 {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v1/lfs/objects/{oid}"))
                        .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                        .header("content-length", 1)
                        .body(Body::from(vec![0x01]))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "a PATCH seeking past the session high-water mark + slack must be rejected"
            );
            assert!(
                !lfs_patch_meta_path(&dir, &oid).exists(),
                "a rejected seek must not create the {oid}.meta sidecar"
            );
            let (active_sessions, used_bytes) = patch_store_usage(&dir).unwrap();
            assert_eq!(
                active_sessions, 0,
                "no ghost session may be charged against the active-session cap"
            );
            assert_eq!(used_bytes, 0, "no ghost session may hold staging bytes");
        }

        // The slot is still free: a brand-new session succeeds under the cap
        // of 1, which a ghost would otherwise have consumed.
        let oid_ok = test_oid(b"ghost-slot-still-free-content");
        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid_ok}"))
                    .header("content-range", "bytes 0-9/20")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "a rejected seek must not consume the active-session slot"
        );
    }

    // =========================================================================
    // F-31 — promotion cleanup and the sweep share a consistent lock order
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn patch_promotion_and_sweep_complete_without_deadlock() {
        // F-31: the promotion cleanup must never re-acquire the store lock
        // while holding a per-OID lock, and the sweep must never be starved by
        // a live session. Drive a sweep and a promotion-triggering PATCH
        // concurrently while the test holds the stale session's per-OID lock
        // (mimicking a PATCH that is mid-promotion): the sweep blocks on the
        // per-OID lock while holding the store lock, and the PATCH parks
        // behind the store lock. Both must finish within a hard bound — an
        // inverted lock order would hang them forever.
        let (state, _tmp) = build_test_state().await;
        let dir = lfs_patch_dir(state.config.root_dir());
        std::fs::create_dir_all(&dir).unwrap();
        let content = b"lock-order-promotion-content";
        let oid = test_oid(content);
        // A stale session for the same OID the PATCH targets.
        std::fs::write(dir.join(&oid), b"stale-data").unwrap();
        std::fs::write(dir.join(format!("{oid}.ranges")), "10\n0 10\n").unwrap();
        touch_patch_session(&dir, &oid, 1).unwrap();

        // Mimic a mid-promotion PATCH: hold the target session's per-OID lock
        // so the sweep must wait on it while holding the store lock.
        let oid_lock = acquire_lfs_patch_lock(&oid);
        let guard = oid_lock.lock().unwrap();

        let root = state.config.root_dir().to_path_buf();
        let ttl = state.config.lfs_patch_ttl_seconds();
        let app = lfs_router(state);

        let sweep_task = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(10), async move {
                sweep_lfs_patch_sessions(&root, ttl).unwrap()
            })
            .await
            .expect("sweep must complete within the deadline")
        });

        let patch_task = tokio::spawn(async move {
            tokio::time::timeout(std::time::Duration::from_secs(10), async move {
                app.oneshot(
                    Request::builder()
                        .method("PATCH")
                        .uri(format!("/v1/lfs/objects/{oid}"))
                        .header(
                            "content-range",
                            format!("bytes 0-{}/{}", content.len() - 1, content.len()),
                        )
                        .header("content-length", content.len())
                        .body(Body::from(content.to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap()
            })
            .await
            .expect("PATCH must complete within the deadline")
        });

        // Let both tasks reach their parked state (sweep on the per-OID lock,
        // PATCH on the store lock) without awaiting while the guard is held,
        // then release the per-OID guard so both can drain.
        std::thread::sleep(std::time::Duration::from_millis(200));
        drop(guard);

        let removed = sweep_task.await.unwrap();
        let patch = patch_task.await.unwrap();
        assert_eq!(removed, 1, "the stale session must be swept");
        assert_eq!(
            patch.status(),
            StatusCode::OK,
            "the PATCH must not be starved by the sweep"
        );
    }

    // =========================================================================
    // F-22 — LFS per-OID lock map stays bounded (weak values + lazy eviction)
    // =========================================================================

    #[test]
    #[serial_test::serial]
    fn acquire_lfs_patch_lock_evicts_dead_entries() {
        let baseline_live = live_lfs_patch_lock_count();
        let baseline_map = LFS_PATCH_LOCKS.lock().unwrap().len();

        {
            let _lock1 = acquire_lfs_patch_lock("evict-lock-a");
            let _lock2 = acquire_lfs_patch_lock("evict-lock-b");
            // The map is process-global and tests run in parallel, so a
            // sibling test may legitimately hold an extra live lock (e.g.
            // patch_promotion_and_sweep_complete_without_deadlock sleeps
            // while holding one). Assert the invariant rather than an exact
            // count: both guards held here must keep their entries alive.
            let live = live_lfs_patch_lock_count();
            assert!(
                live >= baseline_live + 2,
                "held guards keep their entries alive: baseline {baseline_live} -> {live}"
            );
        }

        // Both guards dropped: the strong Arcs are gone. The next acquire
        // evicts the dead entries, so the live count returns toward baseline
        // instead of growing 1:1 with the distinct OIDs (F-22). Slack for
        // concurrently-running tests, mirroring the S3 lock-map test.
        let _lock3 = acquire_lfs_patch_lock("evict-lock-c");
        assert!(
            live_lfs_patch_lock_count() <= baseline_live + 1,
            "live locks must return toward baseline once guards drop: \
             baseline {baseline_live} -> {}",
            live_lfs_patch_lock_count()
        );
        assert!(
            LFS_PATCH_LOCKS.lock().unwrap().len() <= baseline_map + 1,
            "map must not retain dead per-OID entries: baseline {baseline_map} -> {}",
            LFS_PATCH_LOCKS.lock().unwrap().len()
        );
    }
}
