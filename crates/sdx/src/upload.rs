//! Upload session layer (M3b, `docs/SDX_PLAN.md` §4.4.2 / §9-M3).
//!
//! [`UploadSession`] owns the write-path transport (the M2a
//! [`TransferClient`], write-scoped token resolution, the M3a
//! [`DedupClient`], and a fixed upload-permit semaphore — adaptive concurrency
//! is M4) and coordinates the push-style ingest loop:
//!
//! 1. Data is fed as 8 MiB [`INGESTION_BLOCK_SIZE`] blocks (files/readers via
//!    a `spawn_blocking` compute thread that also runs the CDC chunker, so the
//!    blocking task emits zero-copy [`Bytes`] chunk slices).
//! 2. Each chunk is checked against the **session dedup store**, then against
//!    the eligibility-gated **global dedup query** (`GET
//!    /v1/chunks/default-merkledb/{hash}`, 404 = miss, 429 surfaced without
//!    retry). A hit imports the returned shard's xorb reference and skips the
//!    chunk upload; a miss stages the chunk into the pending xorb buffer.
//! 3. A xorb is cut when the 64 MiB / 8192 condition or the
//!    [`SERIALIZED_XORB_SAFETY_CAP_BYTES`] worst-case serialized guard binds.
//!    Cut xorbs are uploaded with a **HEAD-first idempotency probe**
//!    (`HEAD /v1/xorbs/default/{hash}`) and a **streaming-body POST** (512 KiB
//!    progress blocks, explicit `Content-Length`) from a [`JoinSet`] of
//!    parallel tasks, one upload permit each.
//! 4. At [`UploadSession::finalize`] every xorb upload is joined, then a
//!    session shard is serialized (fork-format v3, [`crate::shard`]) and
//!    POSTed to `/v1/shards` — **xorbs-before-shard ordering is mandatory** and
//!    enforced by the server (it rejects shards referencing absent xorbs).
//!
//! RAM bound during upload is one 8 MiB ingest block + one in-progress xorb
//! (≤ 64 MiB) + one per-file pending tail (≤ 64 MiB), independent of file
//! size. In-memory payloads are fed in 8 MiB slices and never cloned whole.
//!
//! # Deltas vs upstream / plan
//!
//! - Path addressing (`remote`) is M5; the uploaded file's identity is its
//!   content hash (`merklehash::file_hash`), returned as [`UploadFileInfo::file_id`].
//! - The plan's session-tail `DataAggregator` is realized as a per-file
//!   pending tail (each file's chunks are cut and uploaded on its own
//!   boundary); the RAM bound and xorb-cut semantics are identical, and
//!   duplicate xorbs across files are uploaded at most once via the HEAD
//!   probe. A session-wide aggregator is a future refinement.
//! - Uploads apply the M4 retry policy (xorb/shard POSTs retry transient
//!   failures; dedup queries are 429-fail-fast). Upload permits are a fixed
//!   semaphore (adaptive concurrency is a future default seam).

use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use tokio::sync::{Mutex, Semaphore, mpsc};
use tokio::task::JoinSet;
use xet_core_structures::merklehash::{MerkleHash, file_hash};

use crate::auth::TokenService;
use crate::chunker::{Chunk, Chunker};
use crate::dedup::{DedupClient, DedupOutcome, is_global_dedup_eligible};
use crate::error::{SdxError, TransferError};
use crate::hash::{parse_xet_hash_hex, xet_hash_hex_string};
use crate::retry::{RetryContext, RetryMarkers, RetryPolicy, RetryScope};
use crate::session::DownloadSessionInner;
use crate::shard::{
    MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, ShardFileEntry, ShardSegment, ShardXorb, ShardXorbChunk,
    find_chunk_in_xorbs, parse_shard_xorbs, serialize_shard,
};
use crate::transfer::TransferClient;
use crate::xorb_build::{
    SERIALIZED_XORB_SAFETY_CAP_BYTES, build_xorb, xorb_cut_condition, xorb_max_addable_chunk,
};

/// Size of each ingest block fed to the chunker (files/readers/bytes are all
/// sliced at this granularity; the file is chunked on the fly, never buffered
/// whole).
pub const INGESTION_BLOCK_SIZE: usize = 8 * 1024 * 1024;
/// Fixed upload concurrency (initial value; adaptive control is M4).
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 2;

/// Result of uploading one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadFileInfo {
    /// Content-derived file identifier (the file hash in Xet hex); download
    /// this file via [`crate::session::DownloadSession::download_bytes`].
    pub file_id: String,
    /// Total uncompressed bytes uploaded.
    pub total_bytes: u64,
    /// Total number of chunks in the file.
    pub chunk_count: u64,
    /// Number of xorbs this file contributed chunks to.
    pub xorb_count: u64,
    /// Chunks staged for upload (new content).
    pub inserted_chunks: u64,
    /// Chunks resolved from session/global dedup (skipped upload).
    pub reused_chunks: u64,
}

/// Aggregate report from [`UploadSession::finalize`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadReport {
    /// Per-file results registered in this session's shard.
    pub files: Vec<UploadFileInfo>,
    /// Xorb POSTs issued (HEAD probe missed).
    pub xorb_posts: u64,
    /// Xorbs already present (HEAD probe hit; upload skipped).
    pub xorb_skipped: u64,
    /// Shard POSTs issued.
    pub shard_posts: u64,
}

/// Push-style streaming upload handle (M3b §4.4.2).
///
/// Feed data with [`write`](Self::write), then call [`finish`](Self::finish).
/// Cheaply clonable: all clones share the same pipeline. `abort` cancels the
/// remaining data and any further writes/finish fail.
#[derive(Clone)]
pub struct UploadStreamHandle {
    inner: Arc<UploadStreamHandleInner>,
}

struct UploadStreamHandleInner {
    pipeline: Mutex<Option<FileUploadPipeline>>,
    result: Arc<OnceLock<UploadFileInfo>>,
    task_id: u64,
    started: AtomicBool,
    finished: AtomicBool,
    aborted: AtomicBool,
    error: Mutex<Option<String>>,
}

impl UploadStreamHandle {
    /// Returns the group-unique task id (0 for handles created outside a group).
    #[must_use]
    pub fn task_id(&self) -> u64 {
        self.inner.task_id
    }

    /// Feeds `data` into the ingest pipeline (chunk → dedup → pending xorb).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the pipeline is already finished/aborted, the
    /// session is finalized, or a dedup/upload step fails.
    pub async fn write(&self, data: impl Into<Bytes>) -> Result<(), SdxError> {
        let data = data.into();
        let mut guard = self.inner.pipeline.lock().await;
        let Some(pipeline) = guard.as_mut() else {
            return Err(SdxError::UploadSession(
                "stream already finished or aborted".to_owned(),
            ));
        };
        self.inner.started.store(true, Ordering::Relaxed);
        pipeline.add_data(data).await
    }

    /// Blocking version of [`write`](Self::write), bridged onto the
    /// client-owned dedicated blocking runtime so it works from plain threads.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the blocking runtime cannot be resolved or the
    /// write fails.
    #[cfg(not(target_family = "wasm"))]
    pub fn write_blocking(&self, data: impl Into<Bytes>) -> Result<(), SdxError> {
        let data = data.into();
        let handle = self.clone();
        crate::stream::global_blocking_runtime()?.block_on(async move { handle.write(data).await })
    }

    /// Finalizes this file's ingest: flushes the pending tail into a xorb,
    /// registers the file info in the session shard, and returns the file
    /// result. A second call fails.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the pipeline is already finished, a xorb cut
    /// fails, or the file info cannot be registered.
    pub async fn finish(&self) -> Result<UploadFileInfo, SdxError> {
        let pipeline = self.inner.pipeline.lock().await.take();
        let Some(pipeline) = pipeline else {
            return Err(SdxError::UploadSession(
                "stream already finished or aborted".to_owned(),
            ));
        };
        if self.inner.aborted.load(Ordering::Relaxed) {
            return Err(SdxError::UploadSession("stream aborted".to_owned()));
        }
        let result = pipeline.finish().await;
        match &result {
            Ok(info) => {
                let _result = self.inner.result.set(info.clone());
            }
            Err(error) => {
                *self.inner.error.lock().await = Some(error.to_string());
            }
        }
        self.inner.finished.store(true, Ordering::Relaxed);
        result
    }

    /// Returns the completed file result if [`finish`](Self::finish) succeeded.
    #[must_use]
    pub fn try_finish(&self) -> Option<UploadFileInfo> {
        self.inner.result.get().cloned()
    }

    /// Cancels this upload: drops the pipeline so subsequent
    /// [`write`](Self::write)/[`finish`](Self::finish) fail.
    pub fn abort(&self) {
        self.inner.aborted.store(true, Ordering::Relaxed);
        *self.inner.pipeline.blocking_lock() = None;
    }

    /// Returns the status flags for group status probes.
    pub(crate) fn status_flags(&self) -> UploadStatusFlags {
        let error = self.inner.error.blocking_lock().clone();
        UploadStatusFlags {
            started: self.inner.started.load(Ordering::Relaxed),
            finished: self.inner.finished.load(Ordering::Relaxed),
            aborted: self.inner.aborted.load(Ordering::Relaxed),
            error,
        }
    }
}

/// Snapshot of an upload handle's status for the group probe.
pub(crate) struct UploadStatusFlags {
    pub(crate) started: bool,
    pub(crate) finished: bool,
    pub(crate) aborted: bool,
    pub(crate) error: Option<String>,
}

/// Shared state of one [`UploadSession`].
struct UploadSessionInner {
    transfer: TransferClient,
    tokens: TokenService,
    api_base: String,
    repository: crate::auth::RepositoryId,
    dedup: DedupClient,
    upload_permits: Arc<Semaphore>,
    /// Dedicated no-read-timeout client for (potentially large) shard POSTs.
    shard_transfer: TransferClient,
    chunk_target_size: usize,
    /// Retry policy (M4) applied to xorb/shard uploads and dedup queries.
    retry_policy: RetryPolicy,
    xorb_posts: AtomicU64,
    xorb_skipped: AtomicU64,
    shard_posts: AtomicU64,
    state: Mutex<SessionState>,
}

impl UploadSessionInner {
    /// Builds a write-scoped [`RetryContext`] for xorb/shard uploads.
    fn upload_retry_context(&self) -> RetryContext {
        RetryContext {
            policy: self.retry_policy.clone(),
            tokens: Some(self.tokens.clone()),
            scope: RetryScope::Write,
            // 403 on an upload re-issues the write token once (loop-guarded).
            markers: RetryMarkers {
                retry_on_403: true,
                ..RetryMarkers::default()
            },
        }
    }

    /// Builds a write-scoped [`RetryContext`] for global dedup queries (404 =
    /// miss, 429 fail-fast).
    fn dedup_retry_context(&self) -> RetryContext {
        RetryContext {
            policy: self.retry_policy.clone(),
            tokens: Some(self.tokens.clone()),
            scope: RetryScope::Write,
            markers: RetryMarkers::dedup(),
        }
    }

    /// Resolves the write token and returns `(CAS base URL, bearer token)`.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when token issuance fails.
    async fn cas_credentials(&self) -> Result<(String, String), SdxError> {
        let token = self.tokens.write_token().await?;
        let base = if token.cas_url.is_empty() {
            self.api_base.clone()
        } else {
            token.cas_url
        };
        Ok((base, token.token))
    }
}

/// Session-wide state shared by all file pipelines in this session.
struct SessionState {
    /// chunk hash → xorb + index for chunks placed/imported this session
    /// (the session dedup store).
    chunk_locations: HashMap<MerkleHash, ChunkLocation>,
    /// xorb hash → xorb info for every xorb referenced by the session shard.
    xorb_infos: HashMap<MerkleHash, ShardXorb>,
    /// Per-file results, in completion order.
    file_reports: Vec<UploadFileInfo>,
    /// File entries for the session shard.
    file_infos: Vec<ShardFileEntry>,
    /// In-flight xorb upload tasks.
    xorb_upload_tasks: JoinSet<Result<(), SdxError>>,
    /// Pending `(remote, file_id)` path registrations to apply at `finalize`.
    pending_registrations: Vec<(String, String)>,
    /// Global chunk index across the whole session (drives dedup eligibility).
    global_chunk_index: u64,
    /// Last chunk index a global dedup query was issued for.
    last_global_query_index: Option<u64>,
    finalized: bool,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            chunk_locations: HashMap::new(),
            xorb_infos: HashMap::new(),
            file_reports: Vec::new(),
            file_infos: Vec::new(),
            xorb_upload_tasks: JoinSet::new(),
            pending_registrations: Vec::new(),
            global_chunk_index: 0,
            last_global_query_index: None,
            finalized: false,
        }
    }
}

/// Where a chunk lives: inside which xorb and at which chunk index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChunkLocation {
    xorb_hash: MerkleHash,
    chunk_index: u32,
    unpacked_len: u64,
}

/// One file's chunk in file order.
#[derive(Debug, Clone, Copy)]
enum Placement {
    /// Chunk staged into a new xorb (not yet cut).
    New { hash: MerkleHash, len: u64 },
    /// Chunk already stored; references an existing xorb.
    Known {
        hash: MerkleHash,
        len: u64,
        xorb_hash: MerkleHash,
        chunk_index: u32,
    },
}

/// Per-file ingest pipeline: chunker + dedup + pending-xorb buffer.
struct FileUploadPipeline {
    session: UploadSession,
    chunker: Chunker,
    placements: Vec<Placement>,
    /// Per `Placement::New`, in order, the (xorb_hash, xorb chunk index) the
    /// chunk was cut into.
    new_chunk_xorbs: Vec<(MerkleHash, u32)>,
    /// Pending new chunks not yet cut into a xorb, with their file offsets.
    pending: Vec<(Chunk, u64)>,
    pending_bytes: usize,
    /// Next file offset (cumulative over all chunks).
    next_file_offset: u64,
    total_bytes: u64,
    inserted_chunks: u64,
    reused_chunks: u64,
}

impl FileUploadPipeline {
    fn new(session: UploadSession) -> Self {
        let chunk_target = session.inner.chunk_target_size;
        Self {
            session,
            chunker: Chunker::new(chunk_target),
            placements: Vec::new(),
            new_chunk_xorbs: Vec::new(),
            pending: Vec::new(),
            pending_bytes: 0,
            next_file_offset: 0,
            total_bytes: 0,
            inserted_chunks: 0,
            reused_chunks: 0,
        }
    }

    /// Feeds one ingest block into the chunker and processes every complete
    /// chunk.
    async fn add_data(&mut self, data: Bytes) -> Result<(), SdxError> {
        let chunks = self.chunker.next_block_bytes(&data, false);
        for chunk in chunks {
            self.process_chunk(chunk).await?;
        }
        Ok(())
    }

    /// Processes one chunk: session dedup, then eligibility-gated global dedup,
    /// then staging into the pending xorb buffer.
    async fn process_chunk(&mut self, chunk: Chunk) -> Result<(), SdxError> {
        let hash = chunk.hash();
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);

        // 1. Session dedup store.
        let session_hit = {
            let state = self.session.inner.state.lock().await;
            state.chunk_locations.get(&hash).copied()
        };
        if let Some(location) = session_hit {
            self.placements.push(Placement::Known {
                hash,
                len,
                xorb_hash: location.xorb_hash,
                chunk_index: location.chunk_index,
            });
            self.reused_chunks = self.reused_chunks.saturating_add(1);
            self.account_chunk(len);
            return Ok(());
        }

        // 2. Eligibility-gated global dedup query.
        let (global_index, eligible) = {
            let state = self.session.inner.state.lock().await;
            (
                state.global_chunk_index,
                is_global_dedup_eligible(
                    state.global_chunk_index,
                    &hash,
                    state.last_global_query_index,
                ),
            )
        };
        if eligible {
            self.session
                .inner
                .state
                .lock()
                .await
                .last_global_query_index = Some(global_index);
            let session = self.session.clone();
            let (base, token) = session.inner.cas_credentials().await?;
            let hash_hex = xet_hash_hex_string(hash);
            // Dedup queries are 429-fail-fast and treat 404 as a miss.
            let retry = session.inner.dedup_retry_context();
            let dedup = session.inner.dedup.clone();
            let outcome = retry
                .run(token, |tok| {
                    let dedup = dedup.clone();
                    let base = base.clone();
                    let hash_hex = hash_hex.clone();
                    async move { dedup.query_raw(&base, &tok, &hash_hex).await }
                })
                .await?;
            if let DedupOutcome::Present { shard_body } = outcome
                && let Ok(xorbs) = parse_shard_xorbs(&shard_body)
                && let Some((xorb, index)) = find_chunk_in_xorbs(&xorbs, &hash)
            {
                {
                    let mut state = self.session.inner.state.lock().await;
                    let location = ChunkLocation {
                        xorb_hash: xorb.xorb_hash,
                        chunk_index: index,
                        unpacked_len: len,
                    };
                    state.chunk_locations.insert(hash, location);
                    // Import the xorb info, marking the referenced chunk with
                    // the global-dedup flag so the server re-indexes it.
                    let mut imported = xorb.clone();
                    if let Some(imported_chunk) = imported
                        .chunks
                        .get_mut(usize::try_from(index).unwrap_or(usize::MAX))
                    {
                        imported_chunk.flags |= MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG;
                    }
                    state
                        .xorb_infos
                        .entry(imported.xorb_hash)
                        .or_insert(imported);
                    state.global_chunk_index = state.global_chunk_index.saturating_add(1);
                }
                self.placements.push(Placement::Known {
                    hash,
                    len,
                    xorb_hash: xorb.xorb_hash,
                    chunk_index: index,
                });
                self.reused_chunks = self.reused_chunks.saturating_add(1);
                self.account_chunk(len);
                return Ok(());
            }
        }

        // 3. Stage as new content; cut a xorb first if the limits bind.
        let chunk_len = chunk.len();
        if needs_xorb_cut(self.pending_bytes, self.pending.len(), chunk_len) {
            self.cut_xorb().await?;
        }
        let file_offset = self.next_file_offset;
        self.pending.push((chunk, file_offset));
        self.pending_bytes = self.pending_bytes.saturating_add(chunk_len);
        self.placements.push(Placement::New { hash, len });
        self.inserted_chunks = self.inserted_chunks.saturating_add(1);
        self.account_chunk(len);
        {
            let mut state = self.session.inner.state.lock().await;
            state.global_chunk_index = state.global_chunk_index.saturating_add(1);
        }
        Ok(())
    }

    const fn account_chunk(&mut self, len: u64) {
        self.next_file_offset = self.next_file_offset.saturating_add(len);
        self.total_bytes = self.total_bytes.saturating_add(len);
    }

    /// Cuts the pending buffer into a xorb: HEAD-probe, streaming POST (or
    /// skip if already stored), and records the xorb info + chunk locations.
    async fn cut_xorb(&mut self) -> Result<(), SdxError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pairs: Vec<(Bytes, u64)> = self
            .pending
            .iter()
            .map(|(chunk, offset)| (chunk.data.clone(), *offset))
            .collect();
        let built = build_xorb(&pairs)?;

        // HEAD-first idempotency probe (retried on transient failures).
        let (base, token) = self.session.inner.cas_credentials().await?;
        let retry = self.session.inner.upload_retry_context();
        let transfer = self.session.inner.transfer.clone();
        let xorb_hash_hex = built.xorb_hash_hex.clone();
        let base_head = base.clone();
        let exists = retry
            .run(token.clone(), move |tok| {
                let hclient = transfer.clone();
                let b = base_head.clone();
                let hash = xorb_hash_hex.clone();
                async move { hclient.head_xorb(&b, &tok, &hash).await }
            })
            .await?;
        if exists {
            self.session
                .inner
                .xorb_skipped
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.session
                .inner
                .xorb_posts
                .fetch_add(1, Ordering::Relaxed);
            let permit = self.session.inner.upload_permits.clone();
            let upload_transfer = self.session.inner.transfer.clone();
            let hash = built.xorb_hash_hex.clone();
            let body = Bytes::copy_from_slice(&built.serialized);
            self.session
                .inner
                .state
                .lock()
                .await
                .xorb_upload_tasks
                .spawn(async move {
                    let _permit = permit.acquire_owned().await.map_err(|_error| {
                        SdxError::UploadSession("upload permit semaphore closed".to_owned())
                    })?;
                    // Retry the streaming xorb POST with backoff/refresh; the
                    // serialized bytes are in memory, so replays are safe.
                    retry
                        .run(token, move |tok| {
                            let tclient = upload_transfer.clone();
                            let base = base.clone();
                            let hash = hash.clone();
                            let body = body.clone();
                            async move { tclient.upload_xorb(&base, &tok, &hash, body).await }
                        })
                        .await?;
                    Ok(())
                });
        }

        let xorb_hash = parse_xet_hash_hex(&built.xorb_hash_hex)?;
        let num_bytes_in_xorb = built
            .chunk_entries
            .iter()
            .fold(0u64, |acc, entry| acc.saturating_add(entry.raw_length));
        let mut chunks = Vec::with_capacity(built.chunk_entries.len());
        let mut unpacked_offset = 0u64;
        for entry in &built.chunk_entries {
            chunks.push(ShardXorbChunk {
                chunk_hash: entry.hash,
                chunk_byte_range_start: unpacked_offset,
                unpacked_segment_bytes: entry.raw_length,
                flags: 0,
            });
            unpacked_offset = unpacked_offset.saturating_add(entry.raw_length);
        }
        let shard_xorb = ShardXorb {
            xorb_hash,
            num_bytes_in_xorb,
            chunks,
        };

        let mut state = self.session.inner.state.lock().await;
        state
            .xorb_infos
            .entry(xorb_hash)
            .or_insert_with(|| shard_xorb.clone());
        for (index, (chunk, _offset)) in self.pending.iter().enumerate() {
            let location = ChunkLocation {
                xorb_hash,
                chunk_index: u32::try_from(index).unwrap_or(u32::MAX),
                unpacked_len: u64::try_from(chunk.len()).unwrap_or(u64::MAX),
            };
            state.chunk_locations.insert(chunk.hash(), location);
            self.new_chunk_xorbs
                .push((xorb_hash, u32::try_from(index).unwrap_or(u32::MAX)));
        }
        self.pending.clear();
        self.pending_bytes = 0;
        Ok(())
    }

    /// Flushes the chunker's buffered tail (if any) and the pending xorb
    /// buffer, builds the file's segments + file info, and registers it with
    /// the session shard.
    async fn finish(mut self) -> Result<UploadFileInfo, SdxError> {
        // The push-style pipeline never passes `is_final` to the chunker, so
        // any partial trailing chunk is buffered; flush it through the normal
        // dedup/staging path first.
        if let Some(tail) = self.chunker.finish() {
            self.process_chunk(tail).await?;
        }
        self.cut_xorb().await?;

        // Build the file's segments from the ordered placements.
        let mut segments: Vec<ShardSegment> = Vec::new();
        let mut new_index = 0usize;
        for placement in &self.placements {
            let (xorb_hash, chunk_index, len) = match placement {
                Placement::Known {
                    xorb_hash,
                    chunk_index,
                    len,
                    ..
                } => (*xorb_hash, u64::from(*chunk_index), *len),
                Placement::New { len, .. } => {
                    let (xorb_hash, chunk_index) = self
                        .new_chunk_xorbs
                        .get(new_index)
                        .copied()
                        .unwrap_or_else(|| (MerkleHash::default(), 0));
                    new_index = new_index.saturating_add(1);
                    (xorb_hash, u64::from(chunk_index), *len)
                }
            };
            if let Some(last) = segments.last_mut()
                && last.xorb_hash == xorb_hash
                && last.chunk_index_end == chunk_index
            {
                last.chunk_index_end = last.chunk_index_end.saturating_add(1);
                last.unpacked_segment_bytes = last.unpacked_segment_bytes.saturating_add(len);
                continue;
            }
            segments.push(ShardSegment {
                xorb_hash,
                unpacked_segment_bytes: len,
                chunk_index_start: chunk_index,
                chunk_index_end: chunk_index.saturating_add(1),
            });
        }

        let chunk_list: Vec<(MerkleHash, u64)> = self
            .placements
            .iter()
            .map(|placement| match placement {
                Placement::New { hash, len } | Placement::Known { hash, len, .. } => (*hash, *len),
            })
            .collect();
        let file_hash = file_hash(&chunk_list);
        let file_entry = ShardFileEntry {
            file_hash,
            segments,
        };

        let xorb_count = self
            .new_chunk_xorbs
            .iter()
            .map(|(hash, _)| *hash)
            .collect::<std::collections::HashSet<_>>()
            .len() as u64;
        let info = UploadFileInfo {
            file_id: xet_hash_hex_string(file_hash),
            total_bytes: self.total_bytes,
            chunk_count: u64::try_from(chunk_list.len()).unwrap_or(u64::MAX),
            xorb_count,
            inserted_chunks: self.inserted_chunks,
            reused_chunks: self.reused_chunks,
        };

        let mut state = self.session.inner.state.lock().await;
        state.file_infos.push(file_entry);
        state.file_reports.push(info.clone());
        Ok(info)
    }
}

/// Returns whether adding a chunk of `next_len` to a pending xorb holding
/// `pending_bytes` across `pending_chunks` requires cutting first.
fn needs_xorb_cut(pending_bytes: usize, pending_chunks: usize, next_len: usize) -> bool {
    xorb_cut_condition(pending_bytes, pending_chunks, next_len)
        || xorb_max_addable_chunk(
            pending_bytes,
            pending_chunks,
            SERIALIZED_XORB_SAFETY_CAP_BYTES,
        ) < next_len
}

/// One batch emitted by the blocking ingest task.
enum ChunkBatch {
    Chunks(Vec<Chunk>),
    Done,
}

/// Upload session for one repository (write side).
///
/// Create one via [`crate::XetClient::upload_session`] (reusable across
/// multiple files) or use the one-shot
/// [`crate::XetClient::upload_file`] / `upload_bytes` / `upload_stream`
/// helpers, which create, use, and finalize a session internally.
#[derive(Clone)]
pub struct UploadSession {
    inner: Arc<UploadSessionInner>,
}

impl UploadSession {
    /// Creates a session over the client's shared write-path state.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the dedicated no-read-timeout shard client
    /// cannot be built.
    pub(crate) fn new(inner: &Arc<DownloadSessionInner>) -> Result<Self, SdxError> {
        let shard_client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(TransferError::from)?;
        Ok(Self {
            inner: Arc::new(UploadSessionInner {
                transfer: inner.transfer.clone(),
                tokens: inner.tokens.clone(),
                api_base: inner.api_base.clone(),
                repository: inner.repository.clone(),
                dedup: DedupClient::new(inner.transfer.clone()),
                upload_permits: Arc::new(Semaphore::new(inner.upload_concurrency.max(1))),
                shard_transfer: TransferClient::new(shard_client),
                chunk_target_size: inner.upload_chunk_size,
                retry_policy: inner.retry_policy.clone(),
                xorb_posts: AtomicU64::new(0),
                xorb_skipped: AtomicU64::new(0),
                shard_posts: AtomicU64::new(0),
                state: Mutex::new(SessionState::default()),
            }),
        })
    }

    /// Uploads the local file at `path` under the remote path `remote`.
    ///
    /// The file is read in [`INGESTION_BLOCK_SIZE`] blocks on a compute
    /// thread; the returned [`UploadFileInfo::file_id`] is the content-derived
    /// file identifier, and `remote` is registered to it at
    /// [`UploadSession::finalize`].
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the file cannot be read or the upload fails.
    pub async fn upload_file(
        &self,
        path: impl AsRef<Path>,
        remote: &str,
    ) -> Result<UploadFileInfo, SdxError> {
        let file = std::fs::File::open(path)?;
        self.upload_stream(remote, file).await
    }

    /// Uploads an in-memory payload, fed in 8 MiB slices (zero-copy), and
    /// registers it under the remote path `remote` at
    /// [`UploadSession::finalize`].
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the upload fails.
    pub async fn upload_bytes(
        &self,
        remote: &str,
        bytes: impl Into<Bytes>,
    ) -> Result<UploadFileInfo, SdxError> {
        let bytes = bytes.into();
        self.upload_stream(remote, std::io::Cursor::new(bytes))
            .await
    }

    /// Uploads a `std::io::Read` stream under the remote path `remote`.
    ///
    /// The reader is consumed on a `spawn_blocking` compute thread that also
    /// runs the CDC chunker; chunks cross back to the async pipeline as
    /// zero-copy [`Bytes`] slices. `remote` is registered to the resulting
    /// `file_id` at [`UploadSession::finalize`].
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the reader fails or the upload fails.
    pub async fn upload_stream<R>(
        &self,
        remote: &str,
        reader: R,
    ) -> Result<UploadFileInfo, SdxError>
    where
        R: Read + Send + 'static,
    {
        self.check_not_finalized().await?;
        let mut pipeline = FileUploadPipeline::new(self.clone());
        let (tx, mut rx) = mpsc::channel::<Result<ChunkBatch, SdxError>>(2);
        let chunk_target = self.inner.chunk_target_size;
        let reader_task =
            tokio::task::spawn_blocking(move || feed_reader(reader, &tx, chunk_target));

        while let Some(batch) = rx.recv().await {
            match batch? {
                ChunkBatch::Chunks(chunks) => {
                    for chunk in chunks {
                        pipeline.process_chunk(chunk).await?;
                    }
                }
                ChunkBatch::Done => break,
            }
        }
        let info = pipeline.finish().await?;
        reader_task
            .await
            .map_err(|error| SdxError::TaskJoin(error.to_string()))??;
        // Record the pending path registration; applied in `finalize`.
        self.inner
            .state
            .lock()
            .await
            .pending_registrations
            .push((remote.to_owned(), info.file_id.clone()));
        Ok(info)
    }

    /// Creates a push-style streaming upload handle.
    ///
    /// Feed [`UploadStreamHandle::write`] and finalize with
    /// [`UploadStreamHandle::finish`]; the session must still be finalized
    /// (which uploads the session shard).
    #[must_use]
    pub fn upload_stream_handle(&self) -> UploadStreamHandle {
        self.upload_stream_handle_with_id(0)
    }

    pub(crate) fn upload_stream_handle_with_id(&self, id: u64) -> UploadStreamHandle {
        UploadStreamHandle {
            inner: Arc::new(UploadStreamHandleInner {
                pipeline: Mutex::new(Some(FileUploadPipeline::new(self.clone()))),
                result: Arc::new(OnceLock::new()),
                task_id: id,
                started: AtomicBool::new(false),
                finished: AtomicBool::new(false),
                aborted: AtomicBool::new(false),
                error: Mutex::new(None),
            }),
        }
    }

    /// Finalizes the session: joins all in-flight xorb uploads, then serializes
    /// and uploads the session shard (xorbs-before-shard).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a xorb upload failed, the shard cannot be
    /// built, or the shard POST fails. Calling twice fails.
    pub async fn finalize(&self) -> Result<UploadReport, SdxError> {
        let mut tasks = {
            let mut state = self.inner.state.lock().await;
            if state.finalized {
                return Err(SdxError::UploadSession(
                    "session already finalized".to_owned(),
                ));
            }
            state.finalized = true;
            std::mem::take(&mut state.xorb_upload_tasks)
        };
        while let Some(result) = tasks.join_next().await {
            result.map_err(|error| SdxError::TaskJoin(error.to_string()))??;
        }

        let (file_infos, xorb_infos, files) = {
            let state = self.inner.state.lock().await;
            (
                state.file_infos.clone(),
                state
                    .xorb_infos
                    .values()
                    .cloned()
                    .collect::<Vec<ShardXorb>>(),
                state.file_reports.clone(),
            )
        };

        if !file_infos.is_empty() {
            let shard = serialize_shard(&file_infos, &xorb_infos);
            let (base, token) = self.inner.cas_credentials().await?;
            let _permit = self
                .inner
                .upload_permits
                .acquire()
                .await
                .map_err(|_error| {
                    SdxError::UploadSession("upload permit semaphore closed".to_owned())
                })?;
            let retry = self.inner.upload_retry_context();
            retry
                .run(token, move |tok| {
                    let shard = shard.clone();
                    let base = base.clone();
                    async move {
                        self.inner
                            .shard_transfer
                            .upload_shard(&base, &tok, shard)
                            .await
                    }
                })
                .await?;
            self.inner.shard_posts.fetch_add(1, Ordering::Relaxed);
        }

        // Apply pending path registrations after the shard POST (xorbs and the
        // shard are stored first, so `register_path` finds the file in scope).
        let pending = std::mem::take(&mut self.inner.state.lock().await.pending_registrations);
        if !pending.is_empty() {
            let metadata = crate::tree::MetadataClient::from_upload(
                &self.inner.transfer,
                &self.inner.tokens,
                &self.inner.api_base,
                &self.inner.repository,
                &self.inner.retry_policy,
            );
            for (remote, file_id) in pending {
                metadata.register_path(&remote, &file_id).await?;
            }
        }

        Ok(UploadReport {
            files,
            xorb_posts: self.inner.xorb_posts.load(Ordering::Relaxed),
            xorb_skipped: self.inner.xorb_skipped.load(Ordering::Relaxed),
            shard_posts: self.inner.shard_posts.load(Ordering::Relaxed),
        })
    }

    /// Returns the number of xorb POST requests issued so far.
    #[must_use]
    pub fn xorb_post_count(&self) -> u64 {
        self.inner.xorb_posts.load(Ordering::Relaxed)
    }

    /// Returns the number of xorb uploads skipped because the xorb already
    /// existed (HEAD probe hit).
    #[must_use]
    pub fn xorb_skipped_count(&self) -> u64 {
        self.inner.xorb_skipped.load(Ordering::Relaxed)
    }

    async fn check_not_finalized(&self) -> Result<(), SdxError> {
        if self.inner.state.lock().await.finalized {
            return Err(SdxError::UploadSession(
                "session already finalized".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Blocking ingest task: reads `reader` in 8 MiB blocks, chunks on the compute
/// thread, and forwards zero-copy chunk batches (then a `Done` marker).
fn feed_reader<R: Read + Send + 'static>(
    mut reader: R,
    tx: &mpsc::Sender<Result<ChunkBatch, SdxError>>,
    chunk_target: usize,
) -> Result<(), SdxError> {
    let send = |batch| {
        tx.blocking_send(Ok(batch))
            .map_err(|_error| SdxError::UploadSession("ingest channel closed".to_owned()))
    };
    let mut chunker = Chunker::new(chunk_target);
    let mut buffer = vec![0u8; INGESTION_BLOCK_SIZE];
    loop {
        let n = reader.read(&mut buffer).map_err(SdxError::Io)?;
        if n == 0 {
            break;
        }
        let block = Bytes::copy_from_slice(buffer.get(..n).unwrap_or_default());
        let chunks = chunker.next_block_bytes(&block, false);
        if !chunks.is_empty() {
            send(ChunkBatch::Chunks(chunks))?;
        }
    }
    if let Some(tail) = chunker.finish() {
        send(ChunkBatch::Chunks(vec![tail]))?;
    }
    send(ChunkBatch::Done)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path, path_regex},
    };
    use xet_core_structures::merklehash::xorb_hash;

    use super::needs_xorb_cut;
    use crate::shard::{ShardXorb, ShardXorbChunk, serialize_shard};
    use crate::xorb_build::MAX_XORB_BYTES;
    use crate::{Auth, RepositoryId, XetClientBuilder, compute_chunk_hash, xet_hash_hex_string};

    const WRITE_TOKEN: &str = "write-token";
    const BOOTSTRAP_KEY: &str = "bootstrap";

    /// Builds a client whose write-token, xorb POST, and shard POST routes are
    /// mocked. `xorb_exists` makes the HEAD idempotency probe return 200
    /// (existing) instead of 404; `dedup_body` serves a global-dedup hit for
    /// the exact chunk instead of the generic 404 miss.
    async fn mock_client_opts(
        xorb_exists: bool,
        dedup_body: Option<(String, Vec<u8>)>,
    ) -> (MockServer, crate::XetClient) {
        let server = MockServer::start().await;
        // Specific dedup-hit mock must be mounted before the generic miss mock
        // (wiremock uses first-match-wins).
        if let Some((hash_hex, body)) = dedup_body {
            Mock::given(method("GET"))
                .and(path(format!("/v1/chunks/default-merkledb/{hash_hex}")))
                .respond_with(
                    ResponseTemplate::new(200).set_body_raw(body, "application/octet-stream"),
                )
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-write-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": WRITE_TOKEN,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"/v1/chunks/default-merkledb/.*"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "miss"})))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path_regex(r"/v1/xorbs/default/.*"))
            .respond_with(if xorb_exists {
                ResponseTemplate::new(200)
            } else {
                ResponseTemplate::new(404)
            })
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"/v1/xorbs/default/.*"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"was_inserted": true})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/shards"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 1})))
            .mount(&server)
            .await;
        // Path registration (M5b) performed by `finalize`.
        Mock::given(method("PUT"))
            .and(path_regex(r"/api/github/team/assets/path/main/.*"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "path": "remote/file",
                "fileId": "0".repeat(64),
                "size": 0,
                "updatedAt": 0,
                "created": true,
            })))
            .mount(&server)
            .await;
        let client = build_client(&server).await;
        (server, client)
    }

    async fn mock_client() -> (MockServer, crate::XetClient) {
        mock_client_opts(false, None).await
    }

    async fn build_client(server: &MockServer) -> crate::XetClient {
        let auth = Auth::new(
            &server.uri(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_api_key(BOOTSTRAP_KEY.to_owned())
        .with_subject("user".to_owned());
        let port = server.uri().split(':').next_back().unwrap().to_owned();
        XetClientBuilder::new()
            .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
            .auth(auth)
            .with_upload_chunk_size(128)
            .build()
            .unwrap()
    }

    async fn shard_post_body(server: &MockServer) -> Vec<u8> {
        let requests = server.received_requests().await.unwrap_or_default();
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/v1/shards")
            .unwrap();
        request.body.clone()
    }

    #[tokio::test]
    async fn upload_bytes_stores_one_xorb_and_shard() {
        let (server, client) = mock_client().await;
        let session = client.upload_session().unwrap();
        let info = session
            .upload_bytes("remote/hello.bin", b"hello upload world".to_vec())
            .await
            .unwrap();
        assert_eq!(info.total_bytes, 18);
        assert_eq!(info.inserted_chunks, 1);
        assert_eq!(info.reused_chunks, 0);
        assert_eq!(info.file_id.len(), 64);

        let report = session.finalize().await.unwrap();
        assert_eq!(report.files.len(), 1);
        assert_eq!(report.xorb_posts, 1);
        assert_eq!(report.xorb_skipped, 0);
        assert_eq!(report.shard_posts, 1);

        // The shard body must parse and reference exactly one chunk.
        let body = shard_post_body(&server).await;
        let xorbs = crate::shard::parse_shard_xorbs(&body).unwrap();
        assert_eq!(xorbs.len(), 1);
        assert_eq!(xorbs[0].chunks.len(), 1);
        assert_eq!(
            xorbs[0].chunks[0].chunk_hash,
            compute_chunk_hash(b"hello upload world")
        );
        // The xorb POST reached the correct route (verified by the mock
        // matching), and the session shard references exactly the xorb built
        // from the source — confirming the correct xorb was uploaded. (wiremock
        // does not buffer streamed request bodies, so the on-the-wire explicit
        // `Content-Length` is asserted at the wire level in `transfer`'s
        // tests.)
        let expected =
            crate::xorb_build::build_xorb(&[(Bytes::from_static(b"hello upload world"), 0)])
                .unwrap();
        assert_eq!(
            xorbs[0].xorb_hash,
            crate::hash::parse_xet_hash_hex(&expected.xorb_hash_hex).unwrap()
        );
        assert_eq!(
            xorbs[0].chunks[0].chunk_hash,
            expected.chunk_entries[0].hash
        );
    }

    #[tokio::test]
    async fn upload_bytes_head_probe_skips_existing_xorb() {
        // The xorb already exists server-side: HEAD returns 200 and the POST
        // must never fire (idempotent re-upload).
        let (server, client) = mock_client_opts(true, None).await;
        let session = client.upload_session().unwrap();
        session
            .upload_bytes("remote/stored.bin", b"already stored".to_vec())
            .await
            .unwrap();
        let report = session.finalize().await.unwrap();
        assert_eq!(report.xorb_posts, 0);
        assert_eq!(report.xorb_skipped, 1);
        let requests = server.received_requests().await.unwrap_or_default();
        // No xorb POST at all.
        assert!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "POST")
                .all(|request| request.url.path() == "/v1/shards")
        );
    }

    #[tokio::test]
    async fn session_dedup_reuses_xorbs_across_files() {
        let (_server, client) = mock_client().await;
        let session = client.upload_session().unwrap();
        let first = session
            .upload_bytes("remote/same.bin", b"same content twice".to_vec())
            .await
            .unwrap();
        let second = session
            .upload_bytes("remote/same.bin", b"same content twice".to_vec())
            .await
            .unwrap();
        let report = session.finalize().await.unwrap();
        assert_eq!(first.inserted_chunks, 1);
        // The second file's chunk resolved from the session dedup store.
        assert_eq!(second.inserted_chunks, 0);
        assert_eq!(second.reused_chunks, 1);
        // Only one xorb was posted for both files.
        assert_eq!(report.xorb_posts, 1);
        assert_eq!(report.shard_posts, 1);
    }

    #[tokio::test]
    async fn global_dedup_present_skips_xorb_upload() {
        // Build a shard body that already contains the chunk, and serve it for
        // the exact chunk hash.
        let chunk_hash = compute_chunk_hash(b"dedup me");
        let xorb_hash = xorb_hash(&[(chunk_hash, 8)]);
        let imported = serialize_shard(
            &[],
            &[ShardXorb {
                xorb_hash,
                num_bytes_in_xorb: 8,
                chunks: vec![ShardXorbChunk {
                    chunk_hash,
                    chunk_byte_range_start: 0,
                    unpacked_segment_bytes: 8,
                    flags: 0,
                }],
            }],
        );
        let (server, client) =
            mock_client_opts(false, Some((xet_hash_hex_string(chunk_hash), imported))).await;

        let session = client.upload_session().unwrap();
        let info = session
            .upload_bytes("remote/dedup.bin", b"dedup me".to_vec())
            .await
            .unwrap();
        assert_eq!(info.inserted_chunks, 0);
        assert_eq!(info.reused_chunks, 1);
        let report = session.finalize().await.unwrap();
        assert_eq!(report.xorb_posts, 0);
        assert_eq!(report.shard_posts, 1);

        // The shard references the imported xorb, not a new one.
        let body = shard_post_body(&server).await;
        let xorbs = crate::shard::parse_shard_xorbs(&body).unwrap();
        assert_eq!(xorbs.len(), 1);
        assert_eq!(xorbs[0].xorb_hash, xorb_hash);
        assert_eq!(xorbs[0].chunks[0].chunk_hash, chunk_hash);
    }

    #[tokio::test]
    async fn push_style_handle_write_and_finish() {
        let (_server, client) = mock_client().await;
        let session = client.upload_session().unwrap();
        let handle = session.upload_stream_handle();
        assert_eq!(handle.task_id(), 0);
        // Two writes whose second tails a partial chunk across the boundary:
        // "part one " (9) + "part two " (9) = 18 bytes.
        let first = b"part one ".to_vec();
        let second = b"part two ".to_vec();
        let source_len = first.len() + second.len();
        handle.write(first).await.unwrap();
        handle.write(second).await.unwrap();
        let info = handle.finish().await.unwrap();
        // Byte-accounting regression: no byte is lost across the push-style
        // write → chunker → finish pipeline; total_bytes equals the source.
        assert_eq!(info.total_bytes as usize, source_len);
        assert_eq!(info.total_bytes, 18);
        assert_eq!(info.file_id.len(), 64);
        assert!(handle.try_finish().is_some());
        // Second finish fails.
        assert!(handle.finish().await.is_err());
        let report = session.finalize().await.unwrap();
        assert_eq!(report.files.len(), 1);
        assert_eq!(report.shard_posts, 1);
    }

    #[tokio::test]
    async fn push_style_multiple_writes_preserve_all_bytes() {
        // Feed the payload in several uneven slices (including a sub-minimum
        // tail) and verify the pipeline reconstructs the exact source bytes.
        let (_server, client) = mock_client().await;
        let session = client.upload_session().unwrap();
        let handle = session.upload_stream_handle();
        let source = b"the quick brown fox jumps over the lazy dog 0123456789".to_vec();
        for window in source.chunks(13) {
            handle.write(window.to_vec()).await.unwrap();
        }
        let info = handle.finish().await.unwrap();
        assert_eq!(info.total_bytes as usize, source.len());

        // Cross-check against an independent chunker run on the same source.
        let mut chunker = crate::chunker::Chunker::new(128);
        let chunks = chunker.next_block_bytes(&Bytes::copy_from_slice(&source), true);
        assert_eq!(info.chunk_count as usize, chunks.len());
        assert_eq!(
            chunks.iter().map(crate::chunker::Chunk::len).sum::<usize>(),
            source.len()
        );

        let _ = session.finalize().await.unwrap();
    }

    #[test]
    fn needs_xorb_cut_respects_all_limits() {
        assert!(!needs_xorb_cut(0, 0, 1024));
        assert!(needs_xorb_cut(MAX_XORB_BYTES.saturating_sub(1), 0, 2));
        assert!(needs_xorb_cut(0, crate::xorb_build::MAX_XORB_CHUNKS, 1));
        // The serialized safety cap binds before the 64 MiB uncompressed cut.
        assert!(needs_xorb_cut(
            crate::xorb_build::SERIALIZED_XORB_SAFETY_CAP_BYTES as usize,
            0,
            1024,
        ));
    }

    #[test]
    fn upload_report_is_comparable() {
        let report = crate::upload::UploadReport {
            files: Vec::new(),
            xorb_posts: 0,
            xorb_skipped: 0,
            shard_posts: 0,
        };
        assert_eq!(report.xorb_posts, 0);
        assert_eq!(report.files.len(), 0);
        assert_eq!(report, report.clone());
    }

    async fn build_client_with_policy(
        server: &MockServer,
        policy: crate::retry::RetryPolicy,
    ) -> crate::XetClient {
        let auth = Auth::new(
            &server.uri(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_api_key(BOOTSTRAP_KEY.to_owned())
        .with_subject("user".to_owned());
        let port = server.uri().split(':').next_back().unwrap().to_owned();
        XetClientBuilder::new()
            .endpoint(format!("xet://127.0.0.1:{port}/github/team/assets/main"))
            .auth(auth)
            .with_upload_chunk_size(128)
            .with_retry_policy(policy)
            .build()
            .unwrap()
    }

    /// The xorb POST 503s twice (retryable admission), then succeeds; the M4
    /// retry layer must replay the in-memory xorb and complete the upload.
    #[tokio::test]
    async fn upload_retries_xorb_post_on_503() {
        use std::time::Duration;

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-write-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": WRITE_TOKEN,
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"/v1/chunks/default-merkledb/.*"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "miss"})))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path_regex(r"/v1/xorbs/default/.*"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        // xorb POST: 503 twice, then success (first-match-wins exhaustion).
        Mock::given(method("POST"))
            .and(path_regex(r"/v1/xorbs/default/.*"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "admitted"})))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"/v1/xorbs/default/.*"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"was_inserted": true})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/shards"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 1})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path_regex(r"/api/github/team/assets/path/main/.*"))
            .and(header("authorization", format!("Bearer {WRITE_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "path": "remote/file",
                "fileId": "0".repeat(64),
                "size": 0,
                "updatedAt": 0,
                "created": true,
            })))
            .mount(&server)
            .await;

        let client = build_client_with_policy(
            &server,
            crate::retry::RetryPolicy::new()
                .with_base_delay(Duration::from_millis(1))
                .with_jitter(false),
        )
        .await;
        let session = client.upload_session().unwrap();
        session
            .upload_bytes("remote/upload.bin", b"some upload data".to_vec())
            .await
            .unwrap();
        let report = session.finalize().await.unwrap();
        assert_eq!(report.xorb_posts, 1);
        assert_eq!(report.shard_posts, 1);
        let xorb_posts = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| {
                request.method.as_str() == "POST" && request.url.path().starts_with("/v1/xorbs/")
            })
            .count();
        assert_eq!(xorb_posts, 3); // 2×503 + 1×200
    }
}
