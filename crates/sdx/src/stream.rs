//! Pull-based streaming download core (M2b1, `docs/SDX_PLAN.md` §4.4.1).
//!
//! Mirrors `xet-data-1.5.4/src/file_reconstruction/` (`file_reconstructor.rs`,
//! `reconstruction_terms/`, `data_writer/`) and the hf-xet `TaskRuntime`
//! blocking bridge (§4.4.3):
//!
//! - [`DownloadStream`] / [`UnorderedDownloadStream`] are **pull-based**
//!   streams: `next()` (async) and `blocking_next()` (sync, for CLI threads)
//!   return `Option<Bytes>` / `Option<(u64, Bytes)>`, never buffering the file
//!   whole. `Ok(None)` means end-of-stream **or** cancellation.
//! - The `FileReconstructor` pipeline builder's background task is
//!   **spawned at construction, paused, and auto-starts on the first**
//!   `next()`/`blocking_next()` (mirror `download_stream_handle.rs`); dropping
//!   the stream cancels promptly via the shared run state.
//! - [`BufferSemaphore`] is the **byte-denominated memory bound** (mirror
//!   `xet-runtime` `AdjustableSemaphore` + `reconstruction_download_buffer`):
//!   every in-flight term carries a byte-permit (`acquire_many(term_size)`)
//!   released **only after the consumer consumed those bytes**.
//! - [`DataWriter`] (mirror `data_writer.rs`) with `SequentialWriter` and
//!   `UnorderedWriter`; `reconstruct_to_writer` runs any `std::io::Write`
//!   sink on a background thread.
//! - Term-metadata prefetch (`GET /v1|v2/reconstructions/{file_id}` with a
//!   `Range` header) keeps `prefetched_pos - active_pos ≥ min_prefetch_buffer`,
//!   block sizes clamped `[min_reconstruction_fetch_size,
//!   max_reconstruction_fetch_size]` (estimator-driven, not fixed).
//! - The on-disk chunk cache ([`crate::cache`], M2b2) is checked on every xorb
//!   block fetch **before** the download permit / network; successful fetches
//!   are stored back (best-effort, spawned). Cached data still counts against
//!   the buffer semaphore while in flight — the cache is a disk copy, not a
//!   memory bypass.
//!
//! Explicitly **not** in this milestone (later): the upload/commit group
//! (§4.4.3, M3), and retry/backoff/adaptive concurrency (M4). The
//! download-permit semaphore is a fixed count (default 4).
//!
//! # Blocking / async-runtime bridge
//!
//! [`DownloadStream::blocking_next`] and
//! [`UnorderedDownloadStream::blocking_next`] run the async `next()` future on
//! a dedicated multi-threaded [`tokio::runtime::Runtime`] owned by the client
//! (mirror hf-xet `TaskRuntime`), so CLI threads can block safely **without**
//! an ambient runtime. Upstream `xet-data` instead calls `blocking_recv()`,
//! which **panics inside an async runtime**; sdx's dedicated-runtime bridge
//! does not panic, but calling `blocking_next()` from within an async context
//! still blocks that executor thread until the next chunk arrives — prefer
//! [`DownloadStream::next`] there. `blocking_next()` is compiled only on
//! non-wasm targets (wasm has no multi-thread runtime).

use std::collections::{BTreeMap, HashMap, VecDeque, hash_map::Entry};
use std::future::Future;
use std::io::{IoSlice, Write};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use shardline_xet_adapter::{ReconstructionFetchInfo, ReconstructionMultiRangeFetch};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, OnceCell, OwnedSemaphorePermit, Semaphore, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;
use xet_core_structures::ExpWeightedMovingAvg;
use xet_core_structures::merklehash::MerkleHash;

use crate::{
    cache::ChunkCache,
    error::{SdxError, TransferError},
    hash::parse_xet_hash_hex,
    reconstruction::{ReconstructionResponse, fetch_reconstruction_response},
    transfer::{ByteRange, TransferClient},
    xorb::XorbReader,
};

/// Base download-buffer capacity shared across active downloads (2 GiB,
/// mirroring upstream `download_buffer_size`).
pub const DEFAULT_DOWNLOAD_BUFFER_SIZE: u64 = 2_147_483_648;
/// Additional download-buffer capacity allocated per active download
/// (512 MiB, mirroring upstream `download_buffer_perfile_size`).
pub const DEFAULT_DOWNLOAD_BUFFER_PERFILE_SIZE: u64 = 536_870_912;
/// Hard cap on the total download-buffer capacity (8 GiB, mirroring upstream
/// `download_buffer_limit`).
pub const DEFAULT_DOWNLOAD_BUFFER_LIMIT: u64 = 8_589_934_592;
/// Minimum size of a single term-metadata prefetch block (256 MiB).
pub const DEFAULT_MIN_RECONSTRUCTION_FETCH_SIZE: u64 = 268_435_456;
/// Maximum size of a single term-metadata prefetch block (8 GiB).
pub const DEFAULT_MAX_RECONSTRUCTION_FETCH_SIZE: u64 = 8_589_934_592;
/// Minimum term-metadata prefetch lead (1 GiB).
pub const DEFAULT_MIN_PREFETCH_BUFFER: u64 = 1_073_741_824;
/// Target block completion time for the prefetch estimator (15 minutes).
pub const DEFAULT_TARGET_BLOCK_COMPLETION_TIME_SECS: f64 = 900.0;
/// EWMA half-life (in samples) for the completion-rate estimator.
pub const DEFAULT_COMPLETION_RATE_ESTIMATOR_HALF_LIFE: f64 = 4.0;
/// Default fixed CAS connection-permit count (adaptive controller is M4).
pub const DEFAULT_DOWNLOAD_CONCURRENCY: usize = 4;

/// Tunable prefetch/buffer limits for the streaming download pipeline.
///
/// Defaults mirror the upstream `xet-runtime` `ReconstructionConfig`
/// (`xet-runtime-1.5.4/src/config/groups/reconstruction.rs`). The CLI `cat`
/// path should run with a modest buffer cap (64–256 MiB) per
/// `docs/SDX_PLAN.md` §4.4.4.
#[derive(Debug, Clone)]
pub struct StreamLimits {
    /// Minimum term-metadata prefetch block size, in bytes.
    pub min_reconstruction_fetch_size: u64,
    /// Maximum term-metadata prefetch block size, in bytes.
    pub max_reconstruction_fetch_size: u64,
    /// Minimum prefetch lead (`prefetched_pos - active_pos`), in bytes.
    pub min_prefetch_buffer: u64,
    /// Target completion time for a prefetch block, in seconds (drives the
    /// completion-rate-based block sizing).
    pub target_block_completion_time_secs: f64,
    /// Half-life (in samples) of the completion-rate EWMA.
    pub completion_rate_estimator_half_life: f64,
}

impl Default for StreamLimits {
    fn default() -> Self {
        Self {
            min_reconstruction_fetch_size: DEFAULT_MIN_RECONSTRUCTION_FETCH_SIZE,
            max_reconstruction_fetch_size: DEFAULT_MAX_RECONSTRUCTION_FETCH_SIZE,
            min_prefetch_buffer: DEFAULT_MIN_PREFETCH_BUFFER,
            target_block_completion_time_secs: DEFAULT_TARGET_BLOCK_COMPLETION_TIME_SECS,
            completion_rate_estimator_half_life: DEFAULT_COMPLETION_RATE_ESTIMATOR_HALF_LIFE,
        }
    }
}

/// Shared state for the streaming pipeline, derived from a
/// [`crate::XetClient`]'s `DownloadSessionInner`.
#[derive(Clone)]
pub(crate) struct StreamContext {
    pub transfer: TransferClient,
    pub api_base: String,
    pub buffer_semaphore: Arc<BufferSemaphore>,
    pub active_downloads: Arc<AtomicU64>,
    pub download_permits: Arc<Semaphore>,
    pub limits: StreamLimits,
    /// Optional on-disk chunk cache (M2b2); checked before every xorb fetch.
    pub chunk_cache: Option<Arc<ChunkCache>>,
    /// Count of ranged xorb transfer requests issued (network fetches), for
    /// observability/E2E request-counting.
    pub xorb_fetch_count: Arc<AtomicU64>,
    #[cfg(not(target_family = "wasm"))]
    pub blocking_runtime: Arc<tokio::runtime::Runtime>,
}

/// The process-global dedicated runtime used for blocking bridges
/// ([`DownloadStream::blocking_next`]).
///
/// A `tokio::runtime::Runtime` panics when dropped from within an async
/// runtime context, and it cannot be safely owned by a client that may be
/// dropped inside `#[tokio::main]`-style code. sdx therefore keeps a single
/// multi-threaded runtime in a process-global `OnceLock` (created lazily on the
/// first stream construction, errors propagated to the caller). It is never
/// dropped, so no thread can hit the panic; the cost is one idle blocking
/// runtime for the process lifetime once any stream has been used. This mirrors
/// hf-xet's shared `TaskRuntime` (the per-session variant arrives with the
/// stream-group layer, M2b2).
#[cfg(not(target_family = "wasm"))]
pub(crate) fn global_blocking_runtime() -> Result<Arc<tokio::runtime::Runtime>, SdxError> {
    static RUNTIME: OnceLock<Result<Arc<tokio::runtime::Runtime>, String>> = OnceLock::new();
    let slot = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map(Arc::new)
            .map_err(|error| error.to_string())
    });
    match slot {
        Ok(runtime) => Ok(runtime.clone()),
        Err(message) => Err(SdxError::StreamInternal(format!(
            "blocking runtime failed to start: {message}"
        ))),
    }
}

// ============================================================================
// Byte-denominated buffer semaphore
// ============================================================================

/// A byte-denominated semaphore for bounding in-flight download buffering.
///
/// Mirrors the upstream `xet-runtime` `AdjustableSemaphore`
/// (`adjustable_semaphore.rs`): the total permit count can be adjusted at any
/// time between a minimum and maximum bound (`increment_permits_to_target` on
/// download entry, `decrement_permits_to_target` via an exit guard). Permit
/// decreases are resolved lazily as issued permits drop.
///
/// Permit counts are **bytes**. Every in-flight reconstruction term acquires
/// `term_size` permits that are released only after the consumer consumed the
/// term's bytes, so in-flight buffered bytes never exceed the capacity.
///
/// Internally, permits are scaled by a power-of-two `basis` so the logical
/// count (up to the 8 GiB default limit) fits within tokio's `u32` per-acquire
/// limit; on 64-bit platforms the basis is 1 for any practical cap.
pub struct BufferSemaphore {
    semaphore: Arc<Semaphore>,
    total_permits: AtomicU64,
    enqueued_permit_decreases: AtomicU64,
    min_physical_permits: u64,
    max_physical_permits: u64,
    basis: u64,
    adjustment_lock: Mutex<()>,
}

/// A permit issued by a [`BufferSemaphore`], releasing its capacity on drop.
///
/// A permit can be split ([`split`](Self::split)) so a freshly incremented
/// (virtual) permit can be carved into per-term permits, giving a new download
/// immediate access without queueing behind existing acquires.
pub struct BufferPermit {
    permit: Option<OwnedSemaphorePermit>,
    num_physical_permits: u32,
    parent: Arc<BufferSemaphore>,
}

impl BufferSemaphore {
    /// Creates a byte-denominated semaphore with `initial_permits` capacity,
    /// adjustable within `[min_permits, max_permits]`.
    ///
    /// A fixed-capacity semaphore (`min == initial == max`) disables the
    /// dynamic per-download scaling.
    #[must_use]
    pub fn new(min_permits: u64, initial_permits: u64, max_permits: u64) -> Self {
        let basis = Self::compute_basis(max_permits);
        let min_physical = min_permits.div_ceil(basis);
        let max_physical = max_permits.div_ceil(basis);
        let initial_physical = initial_permits
            .div_ceil(basis)
            .clamp(min_physical, max_physical);
        Self {
            semaphore: Arc::new(Semaphore::new(
                usize::try_from(initial_physical).unwrap_or(usize::MAX),
            )),
            total_permits: AtomicU64::new(initial_physical),
            enqueued_permit_decreases: AtomicU64::new(0),
            min_physical_permits: min_physical,
            max_physical_permits: max_physical,
            basis,
            adjustment_lock: Mutex::new(()),
        }
    }

    /// The current total capacity in bytes.
    #[must_use]
    pub fn total_permits(&self) -> u64 {
        self.total_permits
            .load(Ordering::Relaxed)
            .saturating_mul(self.basis)
    }

    /// The current unissued capacity in bytes.
    #[must_use]
    pub fn available_permits(&self) -> u64 {
        u64::try_from(self.semaphore.available_permits())
            .unwrap_or(u64::MAX)
            .saturating_mul(self.basis)
    }

    /// Acquires `n` byte-permits.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::StreamInternal`] when the semaphore is closed.
    pub async fn acquire_many(self: &Arc<Self>, n: u64) -> Result<BufferPermit, SdxError> {
        let physical = self.to_physical_acquire(n);
        let permit = self
            .semaphore
            .clone()
            .acquire_many_owned(physical)
            .await
            .map_err(|error| {
                SdxError::StreamInternal(format!("buffer permit acquire failed: {error}"))
            })?;
        Ok(BufferPermit {
            permit: Some(permit),
            num_physical_permits: physical,
            parent: self.clone(),
        })
    }

    /// Grows the total capacity to `target` bytes if it is currently below it,
    /// returning a virtual permit holding the newly added capacity.
    ///
    /// The returned permit can be [`split`](BufferPermit::split) for immediate
    /// term-level access that bypasses the FIFO acquire queue; when it drops,
    /// its capacity enters the semaphore.
    pub fn increment_permits_to_target(self: &Arc<Self>, target: u64) -> Option<BufferPermit> {
        let _lock = self
            .adjustment_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.total_permits();
        if target <= current {
            return None;
        }
        self.increment_total_permits_impl(target.saturating_sub(current))
    }

    /// Shrinks the total capacity toward `target` bytes if it is currently
    /// above it. Issued permits remain valid; the decrease is resolved lazily.
    ///
    /// Returns the number of bytes by which the capacity was reduced.
    pub fn decrement_permits_to_target(&self, target: u64) -> Option<u64> {
        let _lock = self
            .adjustment_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current = self.total_permits();
        if target >= current {
            return None;
        }
        let n = current.saturating_sub(target);
        let physical_n = n.div_ceil(self.basis);
        if physical_n == 0 {
            return None;
        }
        let removed = attempt_sub(&self.total_permits, physical_n, self.min_physical_permits);
        if removed == 0 {
            return None;
        }
        let removed_u32 = u32::try_from(removed).unwrap_or(u32::MAX);
        if let Ok(permit) = self.semaphore.clone().try_acquire_many_owned(removed_u32) {
            permit.forget();
        } else {
            self.enqueued_permit_decreases
                .fetch_add(removed, Ordering::Relaxed);
        }
        Some(removed.saturating_mul(self.basis))
    }

    fn increment_total_permits_impl(self: &Arc<Self>, n: u64) -> Option<BufferPermit> {
        let physical_n = n.div_ceil(self.basis);
        if physical_n == 0 {
            return None;
        }
        let added = attempt_add(&self.total_permits, physical_n, self.max_physical_permits);
        if added == 0 {
            return None;
        }
        let cancelled = attempt_sub(&self.enqueued_permit_decreases, added, 0);
        let to_hold = added.saturating_sub(cancelled);
        Some(BufferPermit {
            permit: None,
            num_physical_permits: u32::try_from(to_hold).unwrap_or(u32::MAX),
            parent: self.clone(),
        })
    }

    fn to_physical_acquire(&self, n: u64) -> u32 {
        let total = self.total_permits.load(Ordering::Relaxed).max(1);
        u32::try_from(n.div_ceil(self.basis).clamp(1, total)).unwrap_or(u32::MAX)
    }

    /// Smallest power-of-two basis so that `max_permits / basis` fits within
    /// the platform per-acquire limit.
    const fn compute_basis(max_permits: u64) -> u64 {
        let mut basis: u64 = 1;
        while max_permits.div_ceil(basis) > u32::MAX as u64 {
            basis = basis.saturating_mul(2);
        }
        basis
    }
}

impl BufferPermit {
    /// The number of byte-permits held by this permit.
    #[must_use]
    pub fn num_permits(&self) -> u64 {
        u64::from(self.num_physical_permits).saturating_mul(self.parent.basis)
    }

    /// Splits `n` byte-permits off this permit into a new permit.
    ///
    /// Returns `None` when `n` is zero or exceeds the permits held.
    pub fn split(&mut self, n: u64) -> Option<BufferPermit> {
        let physical_n = n.div_ceil(self.parent.basis);
        if physical_n > u64::from(self.num_physical_permits) {
            return None;
        }
        let physical_n = u32::try_from(physical_n).unwrap_or(u32::MAX);
        self.num_physical_permits = self.num_physical_permits.saturating_sub(physical_n);
        if physical_n > 0 {
            let permit = self
                .permit
                .as_mut()
                .and_then(|permit| permit.split(usize::try_from(physical_n).unwrap_or(usize::MAX)));
            Some(BufferPermit {
                permit,
                num_physical_permits: physical_n,
                parent: self.parent.clone(),
            })
        } else {
            None
        }
    }
}

impl Drop for BufferPermit {
    fn drop(&mut self) {
        let parent = &self.parent;
        let num_permits = u64::from(self.num_physical_permits);
        let resolved = attempt_sub(&parent.enqueued_permit_decreases, num_permits, 0);
        if let Some(mut permit) = self.permit.take() {
            if resolved > 0 {
                // Consume the enqueued decrease; the remainder returns normally.
                if let Some(split) = permit.split(usize::try_from(resolved).unwrap_or(usize::MAX)) {
                    split.forget();
                }
            }
        } else {
            // Virtual permit: release the non-consumed portion into the semaphore.
            let to_return = num_permits.saturating_sub(resolved);
            if to_return > 0 {
                parent
                    .semaphore
                    .add_permits(usize::try_from(to_return).unwrap_or(usize::MAX));
            }
        }
    }
}

/// Adds up to `n`, clamped at `max_value`; returns the amount actually added.
fn attempt_add(counter: &AtomicU64, n: u64, max_value: u64) -> u64 {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            if current >= max_value {
                None
            } else {
                Some(current.saturating_add(n).min(max_value))
            }
        })
        .map_or(0, |previous| {
            previous
                .saturating_add(n)
                .min(max_value)
                .saturating_sub(previous)
        })
}

/// Subtracts up to `n`, clamped at `min_value`; returns the amount actually
/// subtracted.
fn attempt_sub(counter: &AtomicU64, n: u64, min_value: u64) -> u64 {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            if current <= min_value {
                None
            } else {
                Some(current.saturating_sub(n).max(min_value))
            }
        })
        .map_or(0, |previous| {
            let after = previous.saturating_sub(n).max(min_value);
            previous.saturating_sub(after)
        })
}

/// Runs `f` when the guard drops (used to shrink the download buffer on exit).
struct ExitGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> ExitGuard<F> {
    const fn new(f: F) -> Self {
        Self(Some(f))
    }
}

impl<F: FnOnce()> Drop for ExitGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}

// ============================================================================
// RunState: cancellation + error propagation
// ============================================================================

/// Internal error for the reconstruction run loop: separates genuine
/// cancellation (mapped to `Ok(0)`) from real errors (propagated as `Err`).
pub(crate) enum RunError {
    Cancelled,
    Error(SdxError),
}

impl From<SdxError> for RunError {
    fn from(error: SdxError) -> Self {
        RunError::Error(error)
    }
}

/// Shared cancellation/error state for one reconstruction pipeline.
///
/// Mirrors upstream `run_state.rs`: any background task that fails calls
/// [`set_error`](Self::set_error), which stores the first error **and** cancels
/// the token, immediately waking every `select!` branch listening on
/// [`cancelled`](Self::cancelled). Errors surface to the consumer via
/// [`check_error`](Self::check_error) at item boundaries.
pub(crate) struct RunState {
    cancellation_token: CancellationToken,
    has_error: AtomicBool,
    stored_error: Mutex<Option<SdxError>>,
    total_bytes_scheduled: AtomicU64,
    total_bytes_delivered: AtomicU64,
}

impl RunState {
    pub(crate) fn new(cancellation_token: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            cancellation_token,
            has_error: AtomicBool::new(false),
            stored_error: Mutex::new(None),
            total_bytes_scheduled: AtomicU64::new(0),
            total_bytes_delivered: AtomicU64::new(0),
        })
    }

    /// Stores the first error and cancels the token. Subsequent calls are
    /// ignored (first error wins).
    pub(crate) fn set_error(&self, error: SdxError) {
        let mut guard = self
            .stored_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_none() {
            *guard = Some(error);
            self.has_error.store(true, Ordering::Release);
        }
        drop(guard);
        self.cancellation_token.cancel();
    }

    /// Returns the stored error, if any, and clears it (the stream reports it
    /// once at an item boundary).
    pub(crate) fn check_error(&self) -> Result<(), SdxError> {
        if self.has_error.load(Ordering::Acquire) {
            let mut guard = self
                .stored_error
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let error = guard.take().unwrap_or_else(|| {
                SdxError::StreamInternal("unknown error occurred in background task".to_owned())
            });
            self.has_error.store(false, Ordering::Release);
            return Err(error);
        }
        Ok(())
    }

    /// Checks errors first (so error-triggered cancellation returns the
    /// underlying error), then cancellation.
    pub(crate) fn check_run_state(&self) -> Result<(), RunError> {
        if let Err(error) = self.check_error() {
            return Err(RunError::Error(error));
        }
        if self.cancellation_token.is_cancelled() {
            return Err(RunError::Cancelled);
        }
        Ok(())
    }

    /// Cancels without an error (genuine external cancellation).
    pub(crate) fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Future that resolves when cancelled; for use in `select!`.
    pub(crate) async fn cancelled(&self) {
        self.cancellation_token.cancelled().await;
    }

    /// Records a term of `size` bytes as scheduled for delivery.
    pub(crate) fn record_new_term(&self, size: u64) {
        self.total_bytes_scheduled
            .fetch_add(size, Ordering::Relaxed);
    }

    /// Reports `size` bytes delivered to the consumer.
    pub(crate) fn report_bytes_written(&self, size: u64) {
        self.total_bytes_delivered
            .fetch_add(size, Ordering::Relaxed);
    }

    pub(crate) fn total_bytes_scheduled(&self) -> u64 {
        self.total_bytes_scheduled.load(Ordering::Relaxed)
    }

    pub(crate) fn total_bytes_delivered(&self) -> u64 {
        self.total_bytes_delivered.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of the stored error message, if any (does not take
    /// the error out; [`check_error`](Self::check_error) still works).
    pub(crate) fn error_message(&self) -> Option<String> {
        if !self.has_error.load(Ordering::Acquire) {
            return None;
        }
        let guard = self
            .stored_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.as_ref().map(ToString::to_string)
    }

    /// Returns `true` when the cancellation token has been cancelled.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation_token.is_cancelled()
    }
}

// ============================================================================
// DataWriter abstraction
// ============================================================================

/// A future that produces the data bytes for one file term.
pub type DataFuture = Pin<Box<dyn Future<Output = Result<Bytes, SdxError>> + Send + 'static>>;

/// Writer abstraction for the reconstruction pipeline (mirror `data_writer.rs`).
///
/// The reconstruction loop hands each term's `(relative_byte_range, permit,
/// data_future)` to the writer; `finish()` waits for all data to be written
/// and returns the number of bytes written.
#[async_trait::async_trait]
pub trait DataWriter: Send {
    /// Sets the data source for the next term.
    ///
    /// `byte_range` is relative to the requested download range. The optional
    /// buffer permit is released only after the data has been consumed/written.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the range is not sequential, the writer is
    /// finished, a prior background task failed, or the writer channel closed.
    async fn set_next_term_data_source(
        &mut self,
        byte_range: Range<u64>,
        permit: Option<BufferPermit>,
        data_future: DataFuture,
    ) -> Result<(), SdxError>;

    /// Consumes the writer, waiting for all data to be written, and returns
    /// the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background task failed or the written byte
    /// count disagrees with the scheduled byte count.
    async fn finish(self: Box<Self>) -> Result<u64, SdxError>;
}

// ============================================================================
// SequentialWriter
// ============================================================================

/// Item sent through the sequential writer queue.
pub(crate) enum SequentialRetrievalItem {
    Data {
        receiver: oneshot::Receiver<Bytes>,
        permit: Option<BufferPermit>,
    },
    Finish,
}

/// A pending write with its buffer permit.
type PendingWrite = (Bytes, Option<BufferPermit>);

/// `write_vectored` iovec cap (mirror upstream `WRITEV_MAX_SLICE`).
const WRITEV_MAX_SLICE: usize = 24;

/// Background thread that drains the sequential writer queue into a `Write`
/// sink (the `reconstruct_to_writer` path).
struct SyncWriterThread {
    rx: UnboundedReceiver<SequentialRetrievalItem>,
    bytes_written: Arc<AtomicU64>,
    run_state: Arc<RunState>,
    pending: Option<SequentialRetrievalItem>,
    finished: bool,
}

impl SyncWriterThread {
    /// Returns the next write item, optionally blocking for it.
    ///
    /// When `should_block` is false and the data is not ready, the item is put
    /// back into `pending` and `None` is returned.
    fn next_write(&mut self, should_block: bool) -> Result<Option<PendingWrite>, SdxError> {
        if self.pending.is_none() {
            self.pending = if should_block {
                self.rx.blocking_recv()
            } else {
                self.rx.try_recv().ok()
            };
        }
        match self.pending.take() {
            Some(SequentialRetrievalItem::Data {
                mut receiver,
                permit,
            }) => {
                if should_block {
                    let data = match receiver.blocking_recv() {
                        Ok(data) => data,
                        Err(_) => {
                            self.run_state.check_error()?;
                            return Err(SdxError::StreamInternal(
                                "data sender was dropped before sending data".to_owned(),
                            ));
                        }
                    };
                    Ok(Some((data, permit)))
                } else {
                    match receiver.try_recv() {
                        Ok(data) => Ok(Some((data, permit))),
                        Err(oneshot::error::TryRecvError::Empty) => {
                            self.pending = Some(SequentialRetrievalItem::Data { receiver, permit });
                            Ok(None)
                        }
                        Err(oneshot::error::TryRecvError::Closed) => {
                            self.run_state.check_error()?;
                            Err(SdxError::StreamInternal(
                                "data sender was dropped before sending data".to_owned(),
                            ))
                        }
                    }
                }
            }
            Some(SequentialRetrievalItem::Finish) => {
                self.finished = true;
                Ok(None)
            }
            None => Ok(None),
        }
    }

    /// Runs the non-vectorized writer loop: `write_all` per term, then `flush`.
    fn run(mut self, mut writer: impl Write) -> Result<(), SdxError> {
        while let Some((data, permit)) = self.next_write(true)? {
            let len = u64::try_from(data.len()).unwrap_or(u64::MAX);
            writer.write_all(&data).map_err(SdxError::Io)?;
            self.bytes_written.fetch_add(len, Ordering::Relaxed);
            // The buffer permit is released only after the data is written.
            drop(permit);
            if self.finished {
                break;
            }
        }
        writer.flush().map_err(SdxError::Io)?;
        Ok(())
    }

    /// Runs the vectorized writer loop, batching pending writes into
    /// `write_vectored` calls of at most 24 iovecs (mirror upstream
    /// `WRITEV_MAX_SLICE`).
    fn run_vectorized(mut self, mut writer: impl Write) -> Result<(), SdxError> {
        let mut pending_writes: VecDeque<PendingWrite> = VecDeque::new();
        while !self.finished || !pending_writes.is_empty() {
            if pending_writes.is_empty() {
                let Some(write) = self.next_write(true)? else {
                    break;
                };
                pending_writes.push_back(write);
            }
            while let Some(write) = self.next_write(false)? {
                pending_writes.push_back(write);
            }
            let io_slices: Vec<IoSlice<'_>> = pending_writes
                .iter()
                .take(WRITEV_MAX_SLICE)
                .map(|(data, _)| IoSlice::new(data))
                .collect();
            let written = match writer.write_vectored(&io_slices) {
                Ok(0) if !io_slices.is_empty() => {
                    return Err(SdxError::StreamInternal(
                        "write_vectored returned 0 with non-empty buffers".to_owned(),
                    ));
                }
                Ok(n) => n,
                Err(ref error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(SdxError::Io(error)),
            };
            self.bytes_written.fetch_add(
                u64::try_from(written).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            let mut remaining = written;
            while remaining > 0 && !pending_writes.is_empty() {
                let front_len = pending_writes
                    .front()
                    .map(|(data, _)| data.len())
                    .unwrap_or(0);
                if remaining >= front_len {
                    remaining = remaining.saturating_sub(front_len);
                    pending_writes.pop_front();
                } else {
                    if let Some(front) = pending_writes.front_mut() {
                        front.0 = front.0.slice(remaining..);
                    }
                    remaining = 0;
                }
            }
        }
        writer.flush().map_err(SdxError::Io)?;
        Ok(())
    }
}

/// Writes data sequentially to an output sink from async data futures.
///
/// In streaming mode (`new_streaming`) no background thread is spawned: items
/// are pulled directly by a [`DownloadStream`]. In writer mode (`new`) a
/// background blocking thread performs the writes, enforcing strict byte-range
/// contiguity before any bytes hit the sink.
pub struct SequentialWriter {
    sender: UnboundedSender<SequentialRetrievalItem>,
    next_position: u64,
    background_handle: Option<JoinHandle<()>>,
    run_state: Arc<RunState>,
    bytes_written: Arc<AtomicU64>,
    active_tasks: JoinSet<Result<(), SdxError>>,
    finished: bool,
}

impl Drop for SequentialWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.run_state.cancel();
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for SequentialWriter {
    async fn set_next_term_data_source(
        &mut self,
        byte_range: Range<u64>,
        permit: Option<BufferPermit>,
        data_future: DataFuture,
    ) -> Result<(), SdxError> {
        self.run_state.check_error()?;
        while let Some(result) = self.active_tasks.try_join_next() {
            result.map_err(|error| SdxError::TaskJoin(error.to_string()))??;
        }
        if self.finished {
            return Err(SdxError::StreamInternal(
                "writer has already finished".to_owned(),
            ));
        }
        if byte_range.start != self.next_position {
            return Err(SdxError::StreamInternal(format!(
                "byte range not sequential: expected start at {}, got {}",
                self.next_position, byte_range.start
            )));
        }
        let expected_size = byte_range.end.saturating_sub(byte_range.start);
        self.next_position = byte_range.end;

        let (sender, receiver) = oneshot::channel();
        if self
            .sender
            .send(SequentialRetrievalItem::Data { receiver, permit })
            .is_err()
        {
            self.run_state.check_error()?;
            return Err(SdxError::StreamInternal(
                "background writer channel closed".to_owned(),
            ));
        }

        let run_state = self.run_state.clone();
        let task = async move {
            let result = async {
                run_state.check_error()?;
                let data = data_future.await?;
                if u64::try_from(data.len()).unwrap_or(u64::MAX) != expected_size {
                    return Err(SdxError::StreamInternal(format!(
                        "data size mismatch: expected {expected_size} bytes, got {} bytes",
                        data.len()
                    )));
                }
                if sender.send(data).is_err() {
                    run_state.check_error()?;
                    return Err(SdxError::StreamInternal(
                        "failed to send data: receiver dropped".to_owned(),
                    ));
                }
                Ok(())
            }
            .await;
            if let Err(error) = result {
                // `SdxError` is not `Clone`, so store the real error in the run
                // state (first error wins) and surface a generic marker through
                // the task result; `check_error` returns the real error first
                // at every boundary.
                run_state.set_error(error);
                Err(SdxError::StreamInternal(
                    "term data task failed; see run state".to_owned(),
                ))
            } else {
                result
            }
        };
        self.active_tasks.spawn(task);
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<u64, SdxError> {
        self.run_state.check_error()?;
        if self.finished {
            return Err(SdxError::StreamInternal(
                "writer has already finished".to_owned(),
            ));
        }
        self.finished = true;
        if self.sender.send(SequentialRetrievalItem::Finish).is_err() {
            self.run_state.check_error()?;
            return Err(SdxError::StreamInternal(
                "background writer channel closed".to_owned(),
            ));
        }
        let expected_bytes = self.next_position;
        while let Some(result) = self.active_tasks.join_next().await {
            result.map_err(|error| SdxError::TaskJoin(error.to_string()))??;
        }
        match self.background_handle.take() {
            Some(handle) => {
                handle.await.map_err(|error| {
                    SdxError::StreamInternal(format!("background writer task failed: {error}"))
                })?;
                self.run_state.check_error()?;
                let actual_bytes = self.bytes_written.load(Ordering::Relaxed);
                if actual_bytes != expected_bytes {
                    return Err(SdxError::StreamInternal(format!(
                        "bytes written mismatch: expected {expected_bytes} bytes, wrote {actual_bytes} bytes"
                    )));
                }
                Ok(actual_bytes)
            }
            None => {
                // Streaming mode: no background writer thread; the consumer
                // (DownloadStream) reads items directly from the channel.
                Ok(expected_bytes)
            }
        }
    }
}

impl SequentialWriter {
    /// Creates a streaming sequential writer that exposes its internal queue.
    ///
    /// No background writer thread is spawned; the returned receiver yields
    /// [`SequentialRetrievalItem`]s consumed by a [`DownloadStream`].
    pub(crate) fn new_streaming(
        run_state: Arc<RunState>,
    ) -> (
        Box<dyn DataWriter>,
        UnboundedReceiver<SequentialRetrievalItem>,
    ) {
        let (sender, receiver) = unbounded_channel();
        let writer = Box::new(Self {
            sender,
            next_position: 0,
            background_handle: None,
            run_state,
            bytes_written: Arc::new(AtomicU64::new(0)),
            active_tasks: JoinSet::new(),
            finished: false,
        });
        (writer, receiver)
    }

    /// Creates a sequential writer backed by the given `Write` impl.
    ///
    /// When `use_vectorized` is true, the background thread batches pending
    /// writes and uses `write_vectored` (≤ 24 iovecs). The writer is moved to a
    /// `spawn_blocking` thread for blocking I/O.
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new<W: Write + Send + 'static>(
        writer: W,
        use_vectorized: bool,
        run_state: Arc<RunState>,
    ) -> Box<dyn DataWriter> {
        let (sender, receiver) = unbounded_channel();
        let bytes_written = Arc::new(AtomicU64::new(0));
        let run_state_thread = run_state.clone();
        let run_state_clone = run_state.clone();
        let bytes_written_clone = bytes_written.clone();

        let handle = tokio::task::spawn_blocking(move || {
            let writer_thread = SyncWriterThread {
                rx: receiver,
                bytes_written: bytes_written_clone,
                run_state: run_state_thread,
                pending: None,
                finished: false,
            };
            let result = if use_vectorized {
                writer_thread.run_vectorized(writer)
            } else {
                writer_thread.run(writer)
            };
            if let Err(error) = result {
                run_state_clone.set_error(error);
            }
        });

        Box::new(Self {
            sender,
            next_position: 0,
            background_handle: Some(handle),
            run_state,
            bytes_written,
            active_tasks: JoinSet::new(),
            finished: false,
        })
    }
}

// ============================================================================
// UnorderedWriter
// ============================================================================

/// A completed term ready for consumption.
pub(crate) struct CompletedTerm {
    pub byte_range: Range<u64>,
    pub data: Bytes,
    pub permit: Option<BufferPermit>,
}

/// Atomic progress counters shared between the writer, its spawned tasks, and
/// the consumer stream.
pub(crate) struct UnorderedWriterProgress {
    pub terms_in_progress: AtomicU64,
    pub bytes_in_progress: AtomicU64,
}

impl UnorderedWriterProgress {
    pub(crate) fn terms_in_progress(&self) -> u64 {
        self.terms_in_progress.load(Ordering::Acquire)
    }

    pub(crate) fn bytes_in_progress(&self) -> u64 {
        self.bytes_in_progress.load(Ordering::Relaxed)
    }
}

/// Writer that delivers completed data terms in arbitrary completion order.
///
/// Each `set_next_term_data_source` spawns a task that resolves the data
/// future and sends the result through an unbounded channel; the consumer
/// (an [`UnorderedDownloadStream`]) reads in completion order.
pub struct UnorderedWriter {
    result_tx: UnboundedSender<Result<CompletedTerm, SdxError>>,
    run_state: Arc<RunState>,
    progress: Arc<UnorderedWriterProgress>,
    task_set: JoinSet<Result<u64, SdxError>>,
    total_bytes_sent: u64,
    finished: bool,
}

impl Drop for UnorderedWriter {
    fn drop(&mut self) {
        if !self.finished {
            self.run_state.cancel();
        }
    }
}

#[async_trait::async_trait]
impl DataWriter for UnorderedWriter {
    async fn set_next_term_data_source(
        &mut self,
        byte_range: Range<u64>,
        permit: Option<BufferPermit>,
        data_future: DataFuture,
    ) -> Result<(), SdxError> {
        self.run_state.check_error()?;
        while let Some(result) = self.task_set.try_join_next() {
            self.total_bytes_sent = self
                .total_bytes_sent
                .saturating_add(result.map_err(|error| SdxError::TaskJoin(error.to_string()))??);
        }
        if self.finished {
            return Err(SdxError::StreamInternal(
                "writer has already finished".to_owned(),
            ));
        }
        let expected_size = byte_range.end.saturating_sub(byte_range.start);
        self.progress
            .terms_in_progress
            .fetch_add(1, Ordering::Relaxed);
        self.progress
            .bytes_in_progress
            .fetch_add(expected_size, Ordering::Relaxed);

        let result_tx = self.result_tx.clone();
        let run_state = self.run_state.clone();
        let progress = self.progress.clone();

        self.task_set.spawn(async move {
            let result = async {
                run_state.check_error()?;
                let data = data_future.await?;
                if u64::try_from(data.len()).unwrap_or(u64::MAX) != expected_size {
                    return Err(SdxError::StreamInternal(format!(
                        "data size mismatch: expected {expected_size} bytes, got {} bytes",
                        data.len()
                    )));
                }
                Ok(CompletedTerm {
                    byte_range,
                    data,
                    permit,
                })
            }
            .await;

            let completed_bytes = result
                .as_ref()
                .map(|term| u64::try_from(term.data.len()).unwrap_or(u64::MAX))
                .unwrap_or(0);
            match result {
                Ok(term) => {
                    drop(result_tx.send(Ok(term)));
                }
                Err(error) => {
                    // `SdxError` is not `Clone`: store the real error in the
                    // run state (which cancels the token, waking the consumer's
                    // `cancelled()` branch) instead of sending a marker. The
                    // consumer's `check_error` then surfaces the real error.
                    run_state.set_error(error);
                }
            }

            progress
                .bytes_in_progress
                .fetch_sub(expected_size, Ordering::Relaxed);
            progress.terms_in_progress.fetch_sub(1, Ordering::Release);

            if completed_bytes > 0 {
                Ok(completed_bytes)
            } else {
                run_state.check_error()?;
                Ok(0)
            }
        });

        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<u64, SdxError> {
        self.run_state.check_error()?;
        while let Some(result) = self.task_set.join_next().await {
            self.total_bytes_sent = self
                .total_bytes_sent
                .saturating_add(result.map_err(|error| SdxError::TaskJoin(error.to_string()))??);
        }
        self.finished = true;
        Ok(self.total_bytes_sent)
    }
}

/// The three-part result of building an unordered streaming writer.
type UnorderedStreamingWriter = (
    Box<dyn DataWriter>,
    UnboundedReceiver<Result<CompletedTerm, SdxError>>,
    Arc<UnorderedWriterProgress>,
);

impl UnorderedWriter {
    /// Creates an unordered writer for streaming use.
    ///
    /// Returns the writer (for the reconstruction task), the receiver end of
    /// the completion channel, and the shared progress counters.
    pub(crate) fn new_streaming(run_state: Arc<RunState>) -> UnorderedStreamingWriter {
        let (result_tx, result_rx) = unbounded_channel();
        let progress = Arc::new(UnorderedWriterProgress {
            terms_in_progress: AtomicU64::new(0),
            bytes_in_progress: AtomicU64::new(0),
        });
        let writer = Box::new(Self {
            result_tx,
            run_state,
            progress: progress.clone(),
            task_set: JoinSet::new(),
            total_bytes_sent: 0,
            finished: false,
        });
        (writer, result_rx, progress)
    }
}

// ============================================================================
// Term / xorb block model and reconstruction normalization
// ============================================================================

/// A single file term: a contiguous output byte range sourced from a slice of
/// one xorb block's decoded data.
#[derive(Clone)]
pub(crate) struct FileTerm {
    /// Absolute byte range in the output file.
    pub byte_range: Range<u64>,
    /// Flattened index into the xorb block's `chunk_offsets` for this term's
    /// starting chunk.
    pub xorb_block_start_index: usize,
    /// Byte offset into the first chunk of the block, non-zero only for the
    /// first term when the query range starts mid-chunk.
    pub offset_into_first_range: u64,
    /// The xorb block sourcing this term.
    pub xorb_block: Arc<XorbBlock>,
}

/// Decoded xorb block data with per-chunk byte offsets for zero-copy slicing.
pub(crate) struct XorbBlockData {
    /// `(chunk_index, byte_offset)` pairs mapping each chunk to its start
    /// position within `data`.
    pub chunk_offsets: Vec<(usize, usize)>,
    /// The concatenated decompressed chunk data for this block.
    pub data: Bytes,
}

/// A downloadable xorb byte range, with data cached for sharing across terms.
pub(crate) struct XorbBlock {
    /// 64-hex xorb hash (the content-addressed cache key).
    pub hash: String,
    /// Chunk range within the xorb, end-exclusive.
    pub chunk_range: (u64, u64),
    /// The transfer URL.
    pub url: String,
    /// The inclusive byte range to fetch from `url`.
    pub bytes: ByteRange,
    /// Cached decoded data; the first term to request it triggers the fetch.
    pub data: OnceCell<Arc<XorbBlockData>>,
}

impl XorbBlock {
    /// Retrieves (or reuses) the decoded block data, checking the on-disk
    /// chunk cache first and fetching from the CAS on a miss under a download
    /// permit.
    ///
    /// On a cache hit the download permit is never acquired and no network
    /// request is issued; successful ranged fetches are stored back to the
    /// cache best-effort (spawned, failures ignored).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the download permit cannot be acquired, the
    /// xorb fetch fails, or the serialized bytes cannot be decoded.
    async fn retrieve_data(
        self: Arc<Self>,
        ctx: &StreamContext,
        token: &str,
    ) -> Result<Arc<XorbBlockData>, SdxError> {
        self.data
            .get_or_try_init(|| async {
                // M2b2: check the on-disk chunk cache before acquiring the
                // download permit / hitting the CAS (key: xorb hash + exact
                // chunk range, mirror `xet-data-1.5.4` `xorb_block.rs`).
                if let Some(cache) = &ctx.chunk_cache
                    && let Some(cached) = cache.get(&self.hash, self.chunk_range).await?
                {
                    let base = usize::try_from(self.chunk_range.0).unwrap_or(usize::MAX);
                    let chunk_offsets = cached
                        .chunk_offsets
                        .iter()
                        .enumerate()
                        .map(|(index, offset)| {
                            (
                                base.saturating_add(index),
                                usize::try_from(*offset).unwrap_or(usize::MAX),
                            )
                        })
                        .collect();
                    return Ok(Arc::new(XorbBlockData {
                        chunk_offsets,
                        data: cached.data,
                    }));
                }
                let _download_permit =
                    ctx.download_permits
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|error| {
                            SdxError::StreamInternal(format!(
                                "download permit acquire failed: {error}"
                            ))
                        })?;
                let ranged = ctx
                    .transfer
                    .fetch_xorb_range(&self.url, token, self.bytes)
                    .await?;
                ctx.xorb_fetch_count.fetch_add(1, Ordering::Relaxed);
                let chunk_data = XorbReader::new(ranged.data).decode_chunk_data()?;
                // Best-effort async cache put (mirror upstream `xorb_block.rs`
                // `tokio::spawn` + warn); failures must not fail the download.
                if let Some(cache) = &ctx.chunk_cache {
                    let cache = cache.clone();
                    let hash = self.hash.clone();
                    let chunk_range = self.chunk_range;
                    let data = chunk_data.data.clone();
                    let chunk_offsets = chunk_data
                        .chunk_offsets
                        .iter()
                        .map(|offset| u32::try_from(*offset).unwrap_or(u32::MAX))
                        .collect::<Vec<u32>>();
                    tokio::spawn(async move {
                        drop(cache.put(&hash, chunk_range, &chunk_offsets, &data).await);
                    });
                }
                let start_chunk = self.chunk_range.0;
                let base = usize::try_from(start_chunk).unwrap_or(usize::MAX);
                let chunk_offsets = chunk_data
                    .chunk_offsets
                    .iter()
                    .enumerate()
                    .map(|(index, offset)| (base.saturating_add(index), *offset))
                    .collect();
                Ok(Arc::new(XorbBlockData {
                    chunk_offsets,
                    data: chunk_data.data,
                }))
            })
            .await
            .cloned()
    }
}

impl FileTerm {
    /// Zero-copy slices this term's bytes out of the block's decoded data.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::StreamInternal`] when the block data does not cover
    /// the term's byte range.
    fn extract_bytes(&self, block_data: &XorbBlockData) -> Result<Bytes, SdxError> {
        let (_, start_byte_offset) = block_data
            .chunk_offsets
            .get(self.xorb_block_start_index)
            .copied()
            .ok_or_else(|| {
                SdxError::StreamInternal(format!(
                    "chunk offset {} missing from xorb block",
                    self.xorb_block_start_index
                ))
            })?;
        let start = start_byte_offset
            .checked_add(usize::try_from(self.offset_into_first_range).unwrap_or(usize::MAX))
            .ok_or_else(|| SdxError::StreamInternal("term start offset overflow".to_owned()))?;
        let expected_size =
            usize::try_from(self.byte_range.end.saturating_sub(self.byte_range.start))
                .unwrap_or(usize::MAX);
        let end = start
            .checked_add(expected_size)
            .ok_or_else(|| SdxError::StreamInternal("term end offset overflow".to_owned()))?;
        if end > block_data.data.len() {
            return Err(SdxError::StreamInternal(format!(
                "term byte range {start}..{end} exceeds xorb block data length {}",
                block_data.data.len()
            )));
        }
        Ok(block_data.data.slice(start..end))
    }

    /// Returns a future that retrieves and extracts this term's data bytes.
    ///
    /// If the xorb data is already cached, the future resolves immediately.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the data-retrieval task cannot be spawned.
    async fn get_data_task(
        &self,
        ctx: &StreamContext,
        token: &str,
    ) -> Result<DataFuture, SdxError> {
        if let Some(block_data) = self.xorb_block.data.get() {
            let bytes = self.extract_bytes(block_data)?;
            return Ok(Box::pin(async move { Ok(bytes) }));
        }
        let file_term = self.clone();
        let xorb_block = self.xorb_block.clone();
        let token = token.to_owned();
        let ctx = ctx.clone();
        let task = tokio::task::spawn(async move {
            let block_data = xorb_block.retrieve_data(&ctx, &token).await?;
            file_term.extract_bytes(&block_data)
        });
        Ok(Box::pin(async move {
            task.await
                .map_err(|error| SdxError::TaskJoin(error.to_string()))?
        }))
    }
}

/// A block of file terms returned by one prefetched reconstruction request.
pub(crate) struct TermBlock {
    pub file_terms: Vec<FileTerm>,
    /// The end of the actual byte range covered (may be < requested end when
    /// the end of file was reached).
    pub actual_end: u64,
}

/// A normalized fetch descriptor for one xorb byte range.
#[derive(Debug, Clone)]
struct XorbDescriptor {
    url: String,
    bytes: ByteRange,
    chunks: (u64, u64),
}

/// Raw reconstruction fetch metadata in either wire version.
enum XorbDescriptorSource {
    V2(BTreeMap<String, Vec<ReconstructionMultiRangeFetch>>),
    V1(BTreeMap<String, Vec<ReconstructionFetchInfo>>),
}

impl XorbDescriptorSource {
    fn for_hash(&self, hash: &str) -> Vec<XorbDescriptor> {
        match self {
            XorbDescriptorSource::V2(xorbs) => xorbs
                .get(hash)
                .map(|entries| {
                    entries
                        .iter()
                        .flat_map(|entry| {
                            entry.ranges.iter().map(|range| XorbDescriptor {
                                url: entry.url.clone(),
                                bytes: ByteRange::new(range.bytes.start, range.bytes.end),
                                chunks: (range.chunks.start, range.chunks.end),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
            XorbDescriptorSource::V1(fetch_info) => fetch_info
                .get(hash)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|entry| XorbDescriptor {
                            url: entry.url.clone(),
                            bytes: ByteRange::new(entry.url_range.start, entry.url_range.end),
                            chunks: (entry.range.start, entry.range.end),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

/// Fetches the term block covering `query_range`, returning `None` when the
/// range is at or past the end of the file (HTTP 416 is treated as EOF).
///
/// # Errors
///
/// Returns [`SdxError`] when the reconstruction request fails for a non-EOF
/// reason or the response cannot be normalized.
async fn fetch_term_block(
    ctx: &StreamContext,
    token: &str,
    file_id: &str,
    query_range: Range<u64>,
) -> Result<Option<TermBlock>, SdxError> {
    if query_range.start >= query_range.end {
        return Ok(None);
    }
    let wire_range = ByteRange::new(query_range.start, query_range.end.saturating_sub(1));
    let response = match fetch_reconstruction_response(
        &ctx.transfer,
        &ctx.api_base,
        token,
        file_id,
        Some(wire_range),
    )
    .await
    {
        Ok(response) => response,
        // Past-EOF reconstruction requests surface as 416; the prefetch loop
        // treats that as end-of-stream (`docs/SDX_PLAN.md` §4.4.4
        // `.with_expected_416()`).
        Err(SdxError::Transfer(TransferError::RangeNotSatisfiable(_))) => return Ok(None),
        Err(error) => return Err(error),
    };
    normalize_block(response, query_range.start, query_range.end)
}

/// Normalizes a raw reconstruction response into file terms sharing deduplicated
/// xorb blocks (dedup map `(xorb_hash, first_chunk_start) → XorbBlock`, mirror
/// `xet-data-1.5.4` `reconstruction_terms/file_term.rs`).
fn normalize_block(
    response: ReconstructionResponse,
    query_start: u64,
    query_end: u64,
) -> Result<Option<TermBlock>, SdxError> {
    let (offset_into_first_range, terms, descriptors) = match response {
        ReconstructionResponse::V2(resp) => (
            resp.offset_into_first_range,
            resp.terms,
            XorbDescriptorSource::V2(resp.xorbs),
        ),
        ReconstructionResponse::V1(resp) => (
            resp.offset_into_first_range,
            resp.terms,
            XorbDescriptorSource::V1(resp.fetch_info),
        ),
    };
    if terms.is_empty() {
        return Ok(None);
    }
    // Index fetch descriptors per xorb hash (sorted by chunk start) so the
    // per-term lookup below is O(log n) rather than the O(n) linear scan that
    // would otherwise make large term blocks (e.g. 32k terms × 16k descriptors)
    // take tens of seconds.
    let mut descriptor_index: HashMap<String, Vec<XorbDescriptor>> = HashMap::new();
    let mut xorb_blocks: Vec<Arc<XorbBlock>> = Vec::new();
    let mut xorb_index: HashMap<(MerkleHash, u64), usize> = HashMap::new();
    let mut file_terms = Vec::with_capacity(terms.len());
    let mut current_offset = query_start;
    for (term_index, term) in terms.iter().enumerate() {
        let xorb_hash = parse_xet_hash_hex(&term.hash)?;
        let sorted = descriptor_index
            .entry(term.hash.clone())
            .or_insert_with(|| {
                let mut list = descriptors.for_hash(&term.hash);
                list.sort_by_key(|descriptor| descriptor.chunks.0);
                list
            });
        // The covering descriptor is the one with the largest `chunks.0 <=
        // term.range.start` that also reaches `term.range.end` (mirror the
        // previous linear `.find` semantics).
        let position = sorted.partition_point(|descriptor| descriptor.chunks.0 <= term.range.start);
        let descriptor = match position.checked_sub(1).and_then(|index| sorted.get(index)) {
            Some(descriptor) if descriptor.chunks.1 >= term.range.end => descriptor.clone(),
            _ => {
                return Err(SdxError::MissingFetchInfo {
                    term_index,
                    hash: term.hash.clone(),
                });
            }
        };
        let block_index = match xorb_index.entry((xorb_hash, descriptor.chunks.0)) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                let index = xorb_blocks.len();
                xorb_blocks.push(Arc::new(XorbBlock {
                    hash: term.hash.clone(),
                    chunk_range: descriptor.chunks,
                    url: descriptor.url.clone(),
                    bytes: descriptor.bytes,
                    data: OnceCell::new(),
                }));
                entry.insert(index);
                index
            }
        };
        // Only the first term can start mid-chunk (query range start offset).
        let offset = if term_index == 0 {
            offset_into_first_range
        } else {
            0
        };
        if offset > term.unpacked_length {
            return Err(SdxError::UnpackedLengthMismatch {
                term_index,
                expected: term.unpacked_length,
                actual: offset,
            });
        }
        let term_byte_size = term.unpacked_length.saturating_sub(offset);
        let block =
            xorb_blocks
                .get(block_index)
                .cloned()
                .ok_or_else(|| SdxError::MissingFetchInfo {
                    term_index,
                    hash: term.hash.clone(),
                })?;
        let start_chunk_index =
            usize::try_from(term.range.start.saturating_sub(descriptor.chunks.0))
                .unwrap_or(usize::MAX);
        file_terms.push(FileTerm {
            byte_range: current_offset..current_offset.saturating_add(term_byte_size),
            xorb_block_start_index: start_chunk_index,
            offset_into_first_range: offset,
            xorb_block: block,
        });
        current_offset = current_offset.saturating_add(term_byte_size);
    }
    // The last term may extend beyond the requested range when the query ends
    // mid-chunk; trim it to the query boundary.
    if current_offset > query_end {
        let shrink = current_offset.saturating_sub(query_end);
        if let Some(last) = file_terms.last_mut() {
            last.byte_range.end = last.byte_range.end.saturating_sub(shrink);
        }
        current_offset = query_end;
    }
    Ok(Some(TermBlock {
        file_terms,
        actual_end: current_offset,
    }))
}

// ============================================================================
// ReconstructionTermManager (term-metadata prefetch)
// ============================================================================

/// Manages iteration over file term blocks with adaptive prefetching.
///
/// Mirrors `xet-data-1.5.4` `reconstruction_terms/manager.rs`: prefetches
/// reconstruction blocks ahead of consumption so `prefetched_pos - active_pos`
/// stays above `min_prefetch_buffer`, sizing each block by the observed
/// completion rate clamped to
/// `[min_reconstruction_fetch_size, max_reconstruction_fetch_size]`.
struct ReconstructionTermManager {
    ctx: StreamContext,
    file_id: String,
    token: String,
    requested_byte_range: Range<u64>,
    last_block_info: Option<(std::time::Instant, Range<u64>)>,
    known_final_byte_position: Arc<AtomicU64>,
    prefetched_byte_position: u64,
    current_active_byte_position: u64,
    prefetch_queue: VecDeque<JoinHandle<Result<Option<TermBlock>, SdxError>>>,
    completion_rate_estimator: ExpWeightedMovingAvg,
}

impl ReconstructionTermManager {
    async fn new(
        ctx: StreamContext,
        file_id: String,
        token: String,
        requested_byte_range: Range<u64>,
    ) -> Result<Self, SdxError> {
        let mut manager = Self {
            ctx,
            file_id,
            token,
            requested_byte_range: requested_byte_range.clone(),
            last_block_info: None,
            known_final_byte_position: Arc::new(AtomicU64::new(requested_byte_range.end)),
            prefetched_byte_position: requested_byte_range.start,
            current_active_byte_position: requested_byte_range.start,
            prefetch_queue: VecDeque::new(),
            completion_rate_estimator: ExpWeightedMovingAvg::new_count_decay(
                DEFAULT_COMPLETION_RATE_ESTIMATOR_HALF_LIFE,
            ),
        };
        // Seed the pipeline with two blocks so the first block's completion
        // time can be measured against the second.
        let initial = manager.ctx.limits.min_reconstruction_fetch_size;
        manager.prefetch_block(initial).await?;
        manager.prefetch_block(initial.saturating_mul(2)).await?;
        Ok(manager)
    }

    /// Returns the next block of file terms, or `None` when the file has been
    /// fully covered.
    async fn next_file_terms(&mut self) -> Result<Option<TermBlock>, SdxError> {
        // Update the completion-rate estimator from the previous block.
        if let Some((start_time, block_range)) = self.last_block_info.take() {
            let completion_time = start_time.elapsed().as_secs_f64();
            let block_size = block_range.end.saturating_sub(block_range.start) as f64;
            if block_size != 0.0 {
                // Scoped allow mirrors the upstream estimator's float math.
                #[allow(clippy::float_arithmetic)]
                {
                    self.completion_rate_estimator
                        .update(block_size / completion_time.max(1e-6));
                }
            }
        }
        self.check_prefetch_buffer().await?;
        let Some(join_handle) = self.prefetch_queue.pop_front() else {
            return Ok(None);
        };
        let maybe_block = join_handle
            .await
            .map_err(|error| SdxError::TaskJoin(error.to_string()))??;
        match maybe_block {
            Some(block) => {
                let block_start = block
                    .file_terms
                    .first()
                    .map(|t| t.byte_range.start)
                    .unwrap_or(0);
                let block_end = block.actual_end;
                self.last_block_info = Some((std::time::Instant::now(), block_start..block_end));
                self.current_active_byte_position = block_end;
                Ok(Some(block))
            }
            None => {
                // End of file: remember where iteration stopped.
                self.known_final_byte_position
                    .store(self.prefetched_byte_position, Ordering::Relaxed);
                Ok(None)
            }
        }
    }

    fn is_done_fetching(&self) -> bool {
        self.prefetched_byte_position >= self.known_final_byte_position.load(Ordering::Relaxed)
    }

    /// Ensures enough term metadata is prefetched to keep the pipeline fed.
    async fn check_prefetch_buffer(&mut self) -> Result<(), SdxError> {
        if self.is_done_fetching() {
            return Ok(());
        }
        // Scoped allow mirrors the upstream estimator's float math
        // (`manager.rs` `check_prefetch_buffer`).
        #[allow(clippy::float_arithmetic)]
        {
            let completion_rate = self.completion_rate_estimator.value();
            let target_time = self.ctx.limits.target_block_completion_time_secs;
            let prefetch_buffer_target = target_time * completion_rate;
            let min_buffer = self.ctx.limits.min_prefetch_buffer as f64;
            let target_buffer = prefetch_buffer_target.max(min_buffer);

            let current = self
                .prefetched_byte_position
                .saturating_sub(self.current_active_byte_position);
            if !self.prefetch_queue.is_empty() && target_buffer <= current as f64 {
                return Ok(());
            }
            // No `saturating_sub` for f64: clamp the deficit to >= 0.
            let deficit = (target_buffer - current as f64).max(0.0);
            let next_block_size = (deficit as u64).clamp(
                self.ctx.limits.min_reconstruction_fetch_size,
                self.ctx.limits.max_reconstruction_fetch_size,
            );
            self.prefetch_block(next_block_size).await
        }
    }

    /// Schedules a prefetch of `block_size` bytes starting at the current
    /// prefetch position.
    async fn prefetch_block(&mut self, block_size: u64) -> Result<(), SdxError> {
        let block_size = block_size.clamp(
            self.ctx.limits.min_reconstruction_fetch_size,
            self.ctx.limits.max_reconstruction_fetch_size,
        );
        let mut prefetch_range =
            self.prefetched_byte_position..self.prefetched_byte_position.saturating_add(block_size);

        let last_byte_position = self
            .known_final_byte_position
            .load(Ordering::Relaxed)
            .min(self.requested_byte_range.end);
        if prefetch_range.end > last_byte_position {
            prefetch_range.end = last_byte_position;
        }
        let min_fetch_size = self.ctx.limits.min_reconstruction_fetch_size;
        if prefetch_range.end.saturating_add(min_fetch_size) > self.requested_byte_range.end {
            prefetch_range.end = self.requested_byte_range.end;
        }
        // Empty range (empty file, or already at/past the end).
        if prefetch_range.start >= prefetch_range.end {
            return Ok(());
        }
        self.prefetched_byte_position = prefetch_range.end;

        let ctx = self.ctx.clone();
        let token = self.token.clone();
        let file_id = self.file_id.clone();
        let known_final_byte_position = self.known_final_byte_position.clone();
        let join_handle = tokio::task::spawn(async move {
            match fetch_term_block(&ctx, &token, &file_id, prefetch_range.clone()).await {
                Ok(Some(block)) => {
                    // The server clamped the range to the file end.
                    if block.actual_end < prefetch_range.end {
                        known_final_byte_position.store(block.actual_end, Ordering::Relaxed);
                    }
                    Ok(Some(block))
                }
                Ok(None) => {
                    // Past the end of the file.
                    known_final_byte_position.fetch_min(prefetch_range.start, Ordering::Relaxed);
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        });
        self.prefetch_queue.push_back(join_handle);
        Ok(())
    }
}

// ============================================================================
// FileReconstructor (pipeline)
// ============================================================================

/// Reconstructs a file from its content-addressed chunks by downloading xorb
/// blocks and streaming the reassembled data to a writer or pull-based stream.
///
/// Mirrors `xet-data-1.5.4` `file_reconstructor.rs`. The background task is
/// spawned at construction, paused, and auto-starts on the first
/// `next()`/`blocking_next()`; dropping the stream cancels promptly.
///
/// The on-disk chunk cache ([`ChunkCache`], M2b2) is checked before every xorb
/// fetch and populated on successful ranged fetches; it defaults to the
/// client-configured cache (see [`crate::XetClientBuilder::with_chunk_cache_dir`])
/// and can be overridden per reconstructor via [`with_chunk_cache`](Self::with_chunk_cache).
pub(crate) struct FileReconstructor {
    ctx: StreamContext,
    file_id: String,
    token: String,
    byte_range: Option<Range<u64>>,
    chunk_cache: Option<Arc<ChunkCache>>,
    cancellation_token: CancellationToken,
}

impl FileReconstructor {
    pub(crate) fn new(ctx: StreamContext, file_id: String, token: String) -> Self {
        let chunk_cache = ctx.chunk_cache.clone();
        Self {
            ctx,
            file_id,
            token,
            byte_range: None,
            chunk_cache,
            cancellation_token: CancellationToken::new(),
        }
    }

    pub(crate) const fn with_byte_range(mut self, byte_range: Range<u64>) -> Self {
        self.byte_range = Some(byte_range);
        self
    }

    /// Replaces the default cancellation token with the given one, for
    /// coordinated external cancellation (the stream-group layer, M2b2).
    pub(crate) fn with_cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = token;
        self
    }

    /// Overrides the on-disk chunk cache used by this reconstructor (defaults
    /// to the client-configured cache).
    ///
    /// The pipeline reads the cache from the shared [`StreamContext`] by
    /// default; this override exists for callers that build reconstructors
    /// directly (the CLI and future milestones).
    #[must_use]
    #[allow(dead_code)]
    pub fn with_chunk_cache(mut self, cache: Arc<ChunkCache>) -> Self {
        self.chunk_cache = Some(cache);
        self
    }

    /// Reconstructs the file to the given `Write` sink on a background
    /// `spawn_blocking` thread, returning the number of bytes written.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when reconstruction, a fetch, or a write fails.
    pub(crate) async fn reconstruct_to_writer<W: Write + Send + 'static>(
        self,
        writer: W,
    ) -> Result<u64, SdxError> {
        let run_state = RunState::new(self.cancellation_token.clone());
        let data_writer = SequentialWriter::new(writer, true, run_state.clone());
        self.run(data_writer, run_state).await
    }

    /// Reconstructs the file as a pull-based stream, spawning the (paused)
    /// reconstruction task on the client's dedicated blocking runtime.
    pub(crate) fn reconstruct_to_stream(self) -> DownloadStream {
        let run_state = RunState::new(self.cancellation_token.clone());
        let runtime = self.ctx.blocking_runtime.clone();
        DownloadStream::new(self, run_state, runtime)
    }

    /// Reconstructs the file as an unordered stream, yielding `(offset, Bytes)`
    /// in completion order.
    pub(crate) fn reconstruct_to_unordered_stream(self) -> UnorderedDownloadStream {
        let run_state = RunState::new(self.cancellation_token.clone());
        let runtime = self.ctx.blocking_runtime.clone();
        UnorderedDownloadStream::new(self, run_state, runtime)
    }

    async fn run(
        self,
        data_writer: Box<dyn DataWriter>,
        run_state: Arc<RunState>,
    ) -> Result<u64, SdxError> {
        match self.run_impl(data_writer, &run_state).await {
            Ok(value) => Ok(value),
            Err(RunError::Cancelled) => {
                // Genuine cancellation: surface any stored error without
                // consuming it (the stream path reports the typed error via
                // `check_error`).
                if let Some(message) = run_state.error_message() {
                    return Err(SdxError::StreamInternal(message));
                }
                Ok(0)
            }
            Err(RunError::Error(error)) => {
                // Store the error (first-wins) so the stream path can surface
                // the typed error via `check_error`; return an error to the
                // writer caller WITHOUT consuming the stored copy (consuming
                // it would make the stream path report "unknown error
                // occurred in background task").
                run_state.set_error(error);
                Err(run_state
                    .error_message()
                    .map(SdxError::StreamInternal)
                    .unwrap_or_else(|| SdxError::StreamInternal("download failed".to_owned())))
            }
        }
    }

    async fn run_impl(
        self,
        mut data_writer: Box<dyn DataWriter>,
        run_state: &RunState,
    ) -> Result<u64, RunError> {
        let Self {
            ctx,
            file_id,
            token,
            byte_range,
            chunk_cache,
            cancellation_token: _,
        } = self;
        // Per-reconstructor cache override wins; otherwise the client-configured
        // cache (already seeded into `ctx`) applies.
        let ctx = StreamContext {
            chunk_cache: chunk_cache.or_else(|| ctx.chunk_cache.clone()),
            ..ctx
        };

        run_state.check_run_state()?;
        let requested_range = byte_range.unwrap_or(0..u64::MAX);
        let mut term_manager = ReconstructionTermManager::new(
            ctx.clone(),
            file_id,
            token.clone(),
            requested_range.clone(),
        )
        .await?;

        // Dynamic buffer scaling: target = (base + n * perfile).min(limit).
        // `increment_permits_to_target` may return a virtual permit this
        // download can split from immediately; the exit guard recomputes the
        // target for the reduced download count and shrinks back if needed.
        let base = DEFAULT_DOWNLOAD_BUFFER_SIZE;
        let perfile = DEFAULT_DOWNLOAD_BUFFER_PERFILE_SIZE;
        let limit = DEFAULT_DOWNLOAD_BUFFER_LIMIT;
        let active = ctx
            .active_downloads
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let target = base
            .saturating_add(active.saturating_mul(perfile))
            .min(limit);
        let mut seed_buffer_permit = ctx.buffer_semaphore.increment_permits_to_target(target);
        let semaphore = ctx.buffer_semaphore.clone();
        let active_downloads = ctx.active_downloads.clone();
        let _download_count_guard = ExitGuard::new(move || {
            let n = active_downloads
                .fetch_sub(1, Ordering::Relaxed)
                .saturating_sub(1);
            let shrunk_target = base.saturating_add(n.saturating_mul(perfile)).min(limit);
            semaphore.decrement_permits_to_target(shrunk_target);
        });

        // The range start offset: writer ranges are relative to this.
        let range_start_offset = requested_range.start;

        // Outer loop: retrieve blocks of file terms, aborting promptly on
        // cancellation via `select!`.
        loop {
            let maybe_block = tokio::select! {
                biased;
                () = run_state.cancelled() => {
                    run_state.check_run_state()?;
                    return Ok(0);
                }
                result = term_manager.next_file_terms() => result?,
            };
            let Some(block) = maybe_block else {
                break;
            };
            run_state.check_run_state()?;

            // Inner loop: process each file term in the block.
            for file_term in block.file_terms {
                run_state.check_run_state()?;
                let term_size = file_term
                    .byte_range
                    .end
                    .saturating_sub(file_term.byte_range.start);

                // Split from the reserved (virtual) permit first so this
                // download gets immediate access; fall back to the shared
                // semaphore with prompt cancellation.
                let buffer_permit = match seed_buffer_permit
                    .as_mut()
                    .and_then(|reserved| reserved.split(term_size))
                {
                    Some(permit) => permit,
                    None => {
                        seed_buffer_permit = None;
                        tokio::select! {
                            biased;
                            () = run_state.cancelled() => {
                                run_state.check_run_state()?;
                                return Ok(0);
                            }
                            result = ctx.buffer_semaphore.acquire_many(term_size) => result?,
                        }
                    }
                };

                let data_future = file_term.get_data_task(&ctx, &token).await?;
                let relative_start = file_term
                    .byte_range
                    .start
                    .saturating_sub(range_start_offset);
                let relative_end = file_term.byte_range.end.saturating_sub(range_start_offset);
                data_writer
                    .set_next_term_data_source(
                        relative_start..relative_end,
                        Some(buffer_permit),
                        data_future,
                    )
                    .await?;
                run_state.record_new_term(term_size);
            }
        }

        // Finish the writer and wait for all data to be delivered.
        let bytes_written = data_writer.finish().await?;
        Ok(bytes_written)
    }
}

// ============================================================================
// DownloadStream
// ============================================================================

/// A streaming download handle that yields data chunks as they are
/// reconstructed (mirror `data_writer/download_stream.rs`).
///
/// The reconstruction task is spawned at construction but pauses until
/// [`start`](Self::start) is called (or the first [`next`](Self::next) /
/// [`blocking_next`](Self::blocking_next)). Data is pulled from the sequential
/// writer's internal queue, bypassing the synchronous writer thread.
///
/// Each call to `next()`/`blocking_next()` returns the next sequential chunk,
/// or `Ok(None)` at end-of-stream **or** cancellation. Any reconstruction error
/// surfaces on the call that would have returned the next chunk (or on the
/// final `None`) via the shared run state.
pub struct DownloadStream {
    receiver: UnboundedReceiver<SequentialRetrievalItem>,
    finished: bool,
    run_state: Arc<RunState>,
    start_signal: Option<Arc<Notify>>,
    #[cfg(not(target_family = "wasm"))]
    runtime: Arc<tokio::runtime::Runtime>,
}

impl DownloadStream {
    /// Creates a new `DownloadStream`, spawning the (paused) reconstruction
    /// task on the client's dedicated runtime.
    pub(crate) fn new(
        reconstructor: FileReconstructor,
        run_state: Arc<RunState>,
        #[cfg(not(target_family = "wasm"))] runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let (data_writer, receiver) = SequentialWriter::new_streaming(run_state.clone());
        let start_signal = Arc::new(Notify::new());
        let signal = start_signal.clone();
        let rs = run_state.clone();
        let task = async move {
            signal.notified().await;
            drop(reconstructor.run(data_writer, rs).await);
        };
        #[cfg(not(target_family = "wasm"))]
        {
            runtime.spawn(task);
        }
        #[cfg(target_family = "wasm")]
        {
            tokio::task::spawn(task);
        }
        Self {
            receiver,
            finished: false,
            run_state,
            start_signal: Some(start_signal),
            #[cfg(not(target_family = "wasm"))]
            runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next). This
    /// method is non-async and does not require a tokio runtime context.
    pub fn start(&mut self) {
        if let Some(signal) = self.start_signal.take() {
            signal.notify_one();
        }
    }

    fn ensure_started(&mut self) {
        if self.start_signal.is_some() {
            self.start();
        }
    }

    fn cancel_reconstruction(&self) {
        self.run_state.cancel();
        if let Some(signal) = self.start_signal.as_ref() {
            signal.notify_one();
        }
    }

    /// Returns the next chunk of downloaded data asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    pub async fn next(&mut self) -> Result<Option<Bytes>, SdxError> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        let item = if let Ok(item) = self.receiver.try_recv() {
            Some(item)
        } else {
            tokio::select! {
                biased;
                recv = self.receiver.recv() => recv,
                () = self.run_state.cancelled() => None,
            }
        };

        match item {
            Some(SequentialRetrievalItem::Data { receiver, permit }) => {
                // The term's bytes may still be in flight (the data future is
                // resolving the xorb fetch/decode), so awaiting the oneshot
                // must race cancellation: a group/session abort has to surface
                // as a prompt `Ok(None)` rather than hanging on the fetch
                // (M2b2 stream-group abort semantics).
                let data = tokio::select! {
                    biased;
                    data = receiver => match data {
                        Ok(data) => data,
                        Err(_) => {
                            self.run_state.check_error()?;
                            return Err(SdxError::StreamInternal(
                                "data sender was dropped before sending data".to_owned(),
                            ));
                        }
                    },
                    () = self.run_state.cancelled() => {
                        self.finished = true;
                        return Ok(None);
                    }
                };
                self.run_state
                    .report_bytes_written(u64::try_from(data.len()).unwrap_or(u64::MAX));
                // The buffer permit is released only after the consumer has
                // received these bytes.
                drop(permit);
                Ok(Some(data))
            }
            Some(SequentialRetrievalItem::Finish) | None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            }
        }
    }

    /// Returns the next chunk of downloaded data, blocking the current thread
    /// until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Runtime requirements
    ///
    /// This runs the async [`next`](Self::next) on a dedicated multi-threaded
    /// runtime owned by the client, so it works from plain CLI threads and
    /// from [`tokio::task::spawn_blocking`] **without** panicking (upstream
    /// `xet-data` panics inside an async runtime via `blocking_recv`). Calling
    /// it from within an async context is supported but blocks that executor
    /// thread until the next chunk arrives — prefer [`next`](Self::next)
    /// there.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<Bytes>, SdxError> {
        let runtime = self.runtime.clone();
        runtime.block_on(self.next())
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Subsequent calls to [`next`](Self::next) / [`blocking_next`](Self::blocking_next)
    /// return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.cancel_reconstruction();
        drop(self.start_signal.take());
        self.receiver.close();
        self.finished = true;
    }

    /// Returns the shared run state (for the stream-group status probe).
    pub(crate) fn run_state(&self) -> Arc<RunState> {
        self.run_state.clone()
    }

    /// Returns the not-yet-fired start signal, if the stream is still paused
    /// (the stream-group layer wakes paused tasks during abort).
    pub(crate) fn pending_start_signal(&self) -> Option<Arc<Notify>> {
        self.start_signal.clone()
    }
}

impl Drop for DownloadStream {
    fn drop(&mut self) {
        self.cancel_reconstruction();
        self.receiver.close();
    }
}

// ============================================================================
// UnorderedDownloadStream
// ============================================================================

/// A streaming download handle that yields data chunks in completion order,
/// each tagged with its byte offset in the output file (mirror
/// `data_writer/unordered_download_stream.rs`).
///
/// The reconstruction task is spawned at construction but pauses until
/// [`start`](Self::start) is called (or the first [`next`](Self::next) /
/// [`blocking_next`](Self::blocking_next)). Progress can be monitored via
/// [`total_bytes_expected`](Self::total_bytes_expected),
/// [`bytes_in_progress`](Self::bytes_in_progress), and
/// [`bytes_completed`](Self::bytes_completed).
pub struct UnorderedDownloadStream {
    /// Shared atomic progress counters (also held by the writer and its tasks).
    progress: Arc<UnorderedWriterProgress>,
    /// Channel receiver for completed terms from spawned tasks.
    receiver: UnboundedReceiver<Result<CompletedTerm, SdxError>>,
    /// Whether the stream has finished (no more data).
    finished: bool,
    /// Shared run state with the `FileReconstructor`.
    run_state: Arc<RunState>,
    /// Signal to unblock the spawned reconstruction task.
    start_signal: Option<Arc<Notify>>,
    #[cfg(not(target_family = "wasm"))]
    runtime: Arc<tokio::runtime::Runtime>,
}

impl UnorderedDownloadStream {
    /// Creates a new `UnorderedDownloadStream`, spawning the (paused)
    /// reconstruction task on the client's dedicated runtime.
    pub(crate) fn new(
        reconstructor: FileReconstructor,
        run_state: Arc<RunState>,
        #[cfg(not(target_family = "wasm"))] runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        let (writer, receiver, progress) = UnorderedWriter::new_streaming(run_state.clone());
        let start_signal = Arc::new(Notify::new());
        let signal = start_signal.clone();
        let rs = run_state.clone();
        let task = async move {
            signal.notified().await;
            drop(reconstructor.run(writer, rs).await);
        };
        #[cfg(not(target_family = "wasm"))]
        {
            runtime.spawn(task);
        }
        #[cfg(target_family = "wasm")]
        {
            tokio::task::spawn(task);
        }
        Self {
            progress,
            receiver,
            finished: false,
            run_state,
            start_signal: Some(start_signal),
            #[cfg(not(target_family = "wasm"))]
            runtime,
        }
    }

    /// Unblocks the reconstruction task so it begins producing data.
    ///
    /// If already started, this is a no-op. Called automatically on the first
    /// [`next`](Self::next) / [`blocking_next`](Self::blocking_next).
    pub fn start(&mut self) {
        if let Some(signal) = self.start_signal.take() {
            signal.notify_one();
        }
    }

    fn ensure_started(&mut self) {
        if self.start_signal.is_some() {
            self.start();
        }
    }

    fn cancel_reconstruction(&self) {
        self.run_state.cancel();
        if let Some(signal) = self.start_signal.as_ref() {
            signal.notify_one();
        }
    }

    /// Returns the next `(file_offset, chunk)` pair asynchronously.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    pub async fn next(&mut self) -> Result<Option<(u64, Bytes)>, SdxError> {
        if self.finished {
            return Ok(None);
        }
        self.ensure_started();

        if let Ok(result) = self.receiver.try_recv() {
            return self.process_term(result);
        }
        let next_item = tokio::select! {
            biased;
            recv = self.receiver.recv() => recv,
            () = self.run_state.cancelled() => None,
        };
        match next_item {
            Some(result) => self.process_term(result),
            None => {
                self.finished = true;
                self.run_state.check_error()?;
                Ok(None)
            }
        }
    }

    /// Returns the next `(file_offset, chunk)` pair, blocking the current
    /// thread until data is available.
    ///
    /// Returns `Ok(None)` when the download is complete or cancelled.
    ///
    /// # Runtime requirements
    ///
    /// See [`DownloadStream::blocking_next`] for the dedicated-runtime bridge
    /// and the upstream panic-in-async-runtime documentation.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when a background reconstruction task failed.
    #[cfg(not(target_family = "wasm"))]
    pub fn blocking_next(&mut self) -> Result<Option<(u64, Bytes)>, SdxError> {
        let runtime = self.runtime.clone();
        runtime.block_on(self.next())
    }

    fn process_term(
        &mut self,
        result: Result<CompletedTerm, SdxError>,
    ) -> Result<Option<(u64, Bytes)>, SdxError> {
        let term = result?;
        let offset = term.byte_range.start;
        let data = term.data;
        // The buffer permit is released only after the consumer has received
        // these bytes.
        drop(term.permit);
        self.run_state
            .report_bytes_written(u64::try_from(data.len()).unwrap_or(u64::MAX));
        Ok(Some((offset, data)))
    }

    /// Cancels the in-progress (or not-yet-started) download.
    ///
    /// Subsequent calls to [`next`](Self::next) / [`blocking_next`](Self::blocking_next)
    /// return `Ok(None)`.
    pub fn cancel(&mut self) {
        self.cancel_reconstruction();
        drop(self.start_signal.take());
        self.receiver.close();
        self.finished = true;
    }

    /// Total bytes scheduled for delivery so far (grows as term metadata is
    /// prefetched; equals the full file/range size at end-of-stream).
    #[must_use]
    pub fn total_bytes_expected(&self) -> u64 {
        self.run_state.total_bytes_scheduled()
    }

    /// Bytes currently being fetched by in-progress tasks.
    #[must_use]
    pub fn bytes_in_progress(&self) -> u64 {
        self.progress.bytes_in_progress()
    }

    /// Bytes delivered to the consumer through [`next`](Self::next) /
    /// [`blocking_next`](Self::blocking_next).
    #[must_use]
    pub fn bytes_completed(&self) -> u64 {
        self.run_state.total_bytes_delivered()
    }

    /// Number of tasks currently resolving data futures.
    #[must_use]
    pub fn terms_in_progress(&self) -> u64 {
        self.progress.terms_in_progress()
    }

    /// Returns the shared run state (for the stream-group status probe).
    pub(crate) fn run_state(&self) -> Arc<RunState> {
        self.run_state.clone()
    }

    /// Returns the not-yet-fired start signal, if the stream is still paused
    /// (the stream-group layer wakes paused tasks during abort).
    pub(crate) fn pending_start_signal(&self) -> Option<Arc<Notify>> {
        self.start_signal.clone()
    }
}

impl Drop for UnorderedDownloadStream {
    fn drop(&mut self) {
        self.cancel_reconstruction();
        self.receiver.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use serde_json::json;
    use tokio::runtime::Runtime;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path},
    };

    use super::*;
    use crate::{reconstruction, transfer::TransferClient};

    const FILE_ID: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const XORB_HASH: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const READ_TOKEN: &str = "read-token";

    /// Serializes chunk payloads with the pinned upstream serializer.
    fn serialize_payload(chunks: &[&[u8]]) -> Vec<u8> {
        use xet_core_structures::xorb_object::{CompressionScheme, serialize_chunk};
        let mut payload = Vec::new();
        for chunk in chunks {
            serialize_chunk(chunk, &mut payload, CompressionScheme::None).unwrap();
        }
        payload
    }

    fn v2_response_body(
        offset: u64,
        terms: serde_json::Value,
        xorbs: serde_json::Value,
    ) -> serde_json::Value {
        let mut body = serde_json::Map::new();
        body.insert(
            "offset_into_first_range".to_owned(),
            serde_json::Value::from(offset),
        );
        body.insert("terms".to_owned(), terms);
        body.insert("xorbs".to_owned(), xorbs);
        serde_json::Value::Object(body)
    }

    /// Small prefetch limits so unit tests exercise multiple term blocks
    /// without huge JSON responses.
    fn test_limits() -> StreamLimits {
        StreamLimits {
            min_reconstruction_fetch_size: 4096,
            max_reconstruction_fetch_size: 4096,
            min_prefetch_buffer: 8192,
            ..StreamLimits::default()
        }
    }

    /// Shared dedicated runtime for the streaming tests.
    ///
    /// Creating/dropping a `tokio::runtime::Runtime` inside an async test
    /// context panics ("cannot drop a runtime in a context where blocking is
    /// not allowed"), so a single runtime is created once and shared across
    /// all tests; it is dropped at process exit on a clean thread.
    fn test_runtime() -> Arc<Runtime> {
        static RUNTIME: std::sync::OnceLock<Arc<Runtime>> = std::sync::OnceLock::new();
        RUNTIME
            .get_or_init(|| Arc::new(Runtime::new().unwrap()))
            .clone()
    }

    fn test_stream_context(server: &MockServer, buffer_cap: u64) -> StreamContext {
        StreamContext {
            transfer: TransferClient::new(reqwest::Client::new()),
            api_base: server.uri(),
            buffer_semaphore: Arc::new(BufferSemaphore::new(buffer_cap, buffer_cap, buffer_cap)),
            active_downloads: Arc::new(AtomicU64::new(0)),
            download_permits: Arc::new(Semaphore::new(DEFAULT_DOWNLOAD_CONCURRENCY)),
            limits: test_limits(),
            chunk_cache: None,
            xorb_fetch_count: Arc::new(AtomicU64::new(0)),
            blocking_runtime: test_runtime(),
        }
    }

    /// Mounts a 200 reconstruction response for `file_hash` scoped to exactly
    /// the `Range` header `bytes=start-end`; every other range gets 416
    /// (past-EOF), which the prefetch loop treats as end-of-stream.
    async fn reconstruction_mock(
        server: &MockServer,
        start: u64,
        end: u64,
        offset: u64,
        terms: serde_json::Value,
        xorbs: serde_json::Value,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .and(header("range", format!("bytes={start}-{end}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(v2_response_body(offset, terms, xorbs)),
            )
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .respond_with(ResponseTemplate::new(416).set_body_json(json!({"error": "past eof"})))
            .mount(server)
            .await;
    }

    async fn xorb_range_mock(server: &MockServer, start: u64, end: u64, body: Vec<u8>) {
        Mock::given(method("GET"))
            .and(path(format!("/transfer/xorb/default/{XORB_HASH}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .and(header("range", format!("bytes={start}-{end}")))
            .respond_with(
                ResponseTemplate::new(206)
                    .insert_header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", body.len()),
                    )
                    .set_body_raw(body, "application/octet-stream"),
            )
            .mount(server)
            .await;
    }

    fn make_stream(ctx: StreamContext, range: Option<Range<u64>>) -> DownloadStream {
        let mut reconstructor =
            FileReconstructor::new(ctx, FILE_ID.to_owned(), READ_TOKEN.to_owned());
        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }
        reconstructor.reconstruct_to_stream()
    }

    fn make_unordered_stream(
        ctx: StreamContext,
        range: Option<Range<u64>>,
    ) -> UnorderedDownloadStream {
        let mut reconstructor =
            FileReconstructor::new(ctx, FILE_ID.to_owned(), READ_TOKEN.to_owned());
        if let Some(range) = range {
            reconstructor = reconstructor.with_byte_range(range);
        }
        reconstructor.reconstruct_to_unordered_stream()
    }

    async fn drain(mut stream: DownloadStream) -> (Vec<u8>, Vec<u64>) {
        let mut out = Vec::new();
        let mut sizes = Vec::new();
        while let Some(chunk) = stream.next().await.unwrap() {
            sizes.push(u64::try_from(chunk.len()).unwrap());
            out.extend_from_slice(&chunk);
        }
        (out, sizes)
    }

    // ── BufferSemaphore ─────────────────────────────────────────────────────

    #[test]
    fn buffer_semaphore_acquire_release_bounds_in_flight() {
        let sem = Arc::new(BufferSemaphore::new(100, 100, 100));
        assert_eq!(sem.total_permits(), 100);
        assert_eq!(sem.available_permits(), 100);

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let p1 = sem.acquire_many(60).await.unwrap();
            assert_eq!(sem.available_permits(), 40);
            let p2 = sem.acquire_many(40).await.unwrap();
            assert_eq!(sem.available_permits(), 0);
            drop(p1);
            assert_eq!(sem.available_permits(), 60);
            drop(p2);
            assert_eq!(sem.available_permits(), 100);
        });
    }

    #[test]
    fn buffer_semaphore_scales_with_active_download_target() {
        let base = 100u64;
        let perfile = 50u64;
        let limit = 250u64;
        let sem = Arc::new(BufferSemaphore::new(base, base, limit));
        assert_eq!(sem.total_permits(), base);

        // One active download: target = base + perfile.
        let seed = sem.increment_permits_to_target(base + perfile);
        assert!(seed.is_some());
        assert_eq!(sem.total_permits(), 150);

        // Splitting the virtual permit gives immediate (queue-bypassing) access.
        let mut seed = seed.unwrap();
        let term = seed.split(40).unwrap();
        assert_eq!(term.num_permits(), 40);
        assert_eq!(sem.total_permits(), 150);
        drop(term);
        // Dropping the virtual remainder enters the semaphore.
        drop(seed);
        assert_eq!(sem.available_permits(), 150);

        // Two active downloads: target = base + 2*perfile = 200.
        let _seed2 = sem.increment_permits_to_target(base + perfile.saturating_mul(2));
        assert_eq!(sem.total_permits(), 200);

        // Shrink back to base on exit.
        assert!(sem.decrement_permits_to_target(base).is_some());
        assert_eq!(sem.total_permits(), 100);
    }

    #[test]
    fn buffer_semaphore_clamps_to_max_and_min() {
        let sem = Arc::new(BufferSemaphore::new(50, 50, 100));
        let _ = sem.increment_permits_to_target(10_000);
        assert_eq!(sem.total_permits(), 100);
        assert!(sem.increment_permits_to_target(100).is_none());
        let _ = sem.decrement_permits_to_target(0);
        assert_eq!(sem.total_permits(), 50);
    }

    #[test]
    fn buffer_semaphore_large_capacity_uses_scaling_basis() {
        // 8 GiB exceeds u32::MAX, so a basis > 1 is required; acquisitions of
        // arbitrarily large term sizes must still succeed.
        let sem = Arc::new(BufferSemaphore::new(
            DEFAULT_DOWNLOAD_BUFFER_SIZE,
            DEFAULT_DOWNLOAD_BUFFER_SIZE,
            DEFAULT_DOWNLOAD_BUFFER_LIMIT,
        ));
        // ceil(8 GiB / basis) must fit u32::MAX: basis = 4.
        assert_eq!(sem.basis, 4);
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let p = sem
                .acquire_many(DEFAULT_DOWNLOAD_BUFFER_SIZE)
                .await
                .unwrap();
            assert!(p.num_permits() >= DEFAULT_DOWNLOAD_BUFFER_SIZE);
            drop(p);
            assert_eq!(sem.available_permits(), DEFAULT_DOWNLOAD_BUFFER_SIZE);
        });
    }

    // ── RunState ────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_state_error_surfaces_via_check_error() {
        let state = RunState::new(CancellationToken::new());
        assert!(state.check_error().is_ok());
        state.set_error(SdxError::StreamInternal("boom".to_owned()));
        let error = state.check_error().unwrap_err();
        assert!(matches!(error, SdxError::StreamInternal(_)));
        // Cancellation is triggered by the error.
        state.cancelled().await;
    }

    #[tokio::test]
    async fn run_state_cancel_returns_cancelled() {
        let state = RunState::new(CancellationToken::new());
        state.cancel();
        assert!(matches!(state.check_run_state(), Err(RunError::Cancelled)));
    }

    #[tokio::test]
    async fn run_state_tracks_scheduled_and_delivered_bytes() {
        let state = RunState::new(CancellationToken::new());
        state.record_new_term(100);
        state.record_new_term(50);
        assert_eq!(state.total_bytes_scheduled(), 150);
        state.report_bytes_written(75);
        assert_eq!(state.total_bytes_delivered(), 75);
    }

    // ── SequentialWriter (unit) ─────────────────────────────────────────────

    fn immediate_future(data: Bytes) -> DataFuture {
        Box::pin(async move { Ok(data) })
    }

    fn delayed_future(data: Bytes, delay: Duration) -> DataFuture {
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(data)
        })
    }

    #[tokio::test]
    async fn sequential_writer_streaming_orders_chunks() {
        let state = RunState::new(CancellationToken::new());
        let (mut writer, mut receiver) = SequentialWriter::new_streaming(state);

        writer
            .set_next_term_data_source(
                0..5,
                None,
                delayed_future(Bytes::from("Hello"), Duration::from_millis(50)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(
                5..6,
                None,
                delayed_future(Bytes::from(" "), Duration::from_millis(10)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(6..11, None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();
        let total = writer.finish().await.unwrap();
        assert_eq!(total, 11);

        let mut out = Vec::new();
        while let Some(item) = receiver.recv().await {
            match item {
                SequentialRetrievalItem::Data { receiver, permit } => {
                    let data = receiver.await.unwrap();
                    out.extend_from_slice(&data);
                    drop(permit);
                }
                SequentialRetrievalItem::Finish => break,
            }
        }
        assert_eq!(&out, b"Hello World");
    }

    #[tokio::test]
    async fn sequential_writer_rejects_non_sequential_range() {
        let state = RunState::new(CancellationToken::new());
        let (mut writer, _receiver) = SequentialWriter::new_streaming(state);
        writer
            .set_next_term_data_source(0..5, None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        let result = writer
            .set_next_term_data_source(10..15, None, immediate_future(Bytes::from("World")))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sequential_writer_reports_size_mismatch() {
        let state = RunState::new(CancellationToken::new());
        let (mut writer, _receiver) = SequentialWriter::new_streaming(state);
        writer
            .set_next_term_data_source(0..10, None, immediate_future(Bytes::from("Hi")))
            .await
            .unwrap();
        let result = writer.finish().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn sequential_writer_releases_permit_after_write() {
        let sem = Arc::new(BufferSemaphore::new(10, 10, 10));
        let state = RunState::new(CancellationToken::new());
        let (mut writer, mut receiver) = SequentialWriter::new_streaming(state);

        let permit1 = sem.acquire_many(5).await.unwrap();
        let permit2 = sem.acquire_many(5).await.unwrap();
        assert_eq!(sem.available_permits(), 0);

        writer
            .set_next_term_data_source(0..5, Some(permit1), immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(5..6, Some(permit2), immediate_future(Bytes::from(" ")))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        // Permits are only released after the consumer pulls the bytes.
        assert_eq!(sem.available_permits(), 0);
        while let Some(item) = receiver.recv().await {
            match item {
                SequentialRetrievalItem::Data { receiver, permit } => {
                    let _ = receiver.await.unwrap();
                    drop(permit);
                }
                SequentialRetrievalItem::Finish => break,
            }
        }
        assert_eq!(sem.available_permits(), 10);
    }

    #[tokio::test]
    async fn sequential_writer_background_thread_writes_and_flushes() {
        let state = RunState::new(CancellationToken::new());
        let mut writer = SequentialWriter::new(Vec::new(), false, state);
        writer
            .set_next_term_data_source(0..5, None, immediate_future(Bytes::from("Hello")))
            .await
            .unwrap();
        writer
            .set_next_term_data_source(5..11, None, immediate_future(Bytes::from(" World")))
            .await
            .unwrap();
        let total = writer.finish().await.unwrap();
        assert_eq!(total, 11);
    }

    // ── UnorderedWriter (unit) ──────────────────────────────────────────────

    #[tokio::test]
    async fn unordered_writer_yields_completion_order_with_offsets() {
        let state = RunState::new(CancellationToken::new());
        let (mut writer, mut receiver, _progress) = UnorderedWriter::new_streaming(state);

        writer
            .set_next_term_data_source(
                0..5,
                None,
                delayed_future(Bytes::from("Hello"), Duration::from_millis(80)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(
                5..6,
                None,
                delayed_future(Bytes::from(" "), Duration::from_millis(40)),
            )
            .await
            .unwrap();
        writer
            .set_next_term_data_source(6..11, None, immediate_future(Bytes::from("World")))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut items = Vec::new();
        while let Some(result) = receiver.recv().await {
            let term = result.unwrap();
            items.push((term.byte_range.start, term.data.to_vec()));
            drop(term.permit);
        }
        items.sort_by_key(|(offset, _)| *offset);
        let out: Vec<u8> = items.into_iter().flat_map(|(_, data)| data).collect();
        assert_eq!(&out, b"Hello World");
    }

    // ── Full-stream wiremock tests ──────────────────────────────────────────

    #[tokio::test]
    async fn download_stream_yields_chunks_in_order() {
        let server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let chunk_c = vec![3u8; 64];
        let payload = serialize_payload(&[&chunk_a, &chunk_b, &chunk_c]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 2, "end": 3}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 3}, "bytes": {"start": 0, "end": 300}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 300, payload).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let (out, _sizes) = drain(make_stream(ctx, None)).await;
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);
        expected.extend_from_slice(&chunk_c);
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn download_stream_with_chunk_cache_serves_second_download_from_disk() {
        let server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let payload = serialize_payload(&[&chunk_a, &chunk_b]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 200}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 200, payload).await;

        let dir = tempfile::tempdir().unwrap();
        let cache =
            Arc::new(crate::cache::ChunkCache::new(dir.path().to_path_buf(), 1 << 20).unwrap());
        let mut ctx = test_stream_context(&server, 1_048_576);
        ctx.chunk_cache = Some(cache.clone());
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);

        // First download: cache miss, one network fetch.
        let (first, _) = drain(make_stream(ctx.clone(), None)).await;
        assert_eq!(first, expected);
        assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 1);
        // The cache put is spawned best-effort; wait for it to land.
        for _ in 0..100 {
            if cache.entry_count().await.unwrap() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(cache.entry_count().await.unwrap(), 1);

        // Second download: cache hit, no additional network fetch.
        let (second, _) = drain(make_stream(ctx.clone(), None)).await;
        assert_eq!(second, expected);
        assert_eq!(ctx.xorb_fetch_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn download_stream_eof_after_last_chunk() {
        let server = MockServer::start().await;
        let chunk = vec![5u8; 64];
        let payload = serialize_payload(&[&chunk]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 100, payload).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let mut stream = make_stream(ctx, None);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, chunk);
        // EOF at the end.
        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn download_stream_empty_file_yields_immediate_eof() {
        let server = MockServer::start().await;
        reconstruction_mock(&server, 0, 4095, 0, json!([]), json!({})).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let (out, _sizes) = drain(make_stream(ctx, None)).await;
        assert!(out.is_empty());
    }

    #[tokio::test]
    async fn download_stream_byte_range_honors_start_offset() {
        let server = MockServer::start().await;
        let chunk = vec![7u8; 64];
        let payload = serialize_payload(&[&chunk]);

        // The requested range 16..64 is clamped into a single prefetch block,
        // so the only wire request is `bytes=16-63`.
        reconstruction_mock(
            &server,
            16,
            63,
            16,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 100, payload).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let (out, _sizes) = drain(make_stream(ctx, Some(16..64))).await;
        assert_eq!(out, chunk[16..64]);
    }

    #[tokio::test]
    async fn download_stream_buffer_semaphore_bounds_in_flight_bytes() {
        let server = MockServer::start().await;
        // 32 chunks × 64 bytes = 2 KiB, with a 256-byte buffer cap: only a few
        // terms may be buffered at once, yet the stream must complete.
        let chunks: Vec<Vec<u8>> = (0..32u8).map(|i| vec![i; 64]).collect();
        let _payload = serialize_payload(
            chunks
                .iter()
                .map(Vec::as_slice)
                .collect::<Vec<_>>()
                .as_slice(),
        );

        let terms: serde_json::Value = chunks
            .iter()
            .enumerate()
            .map(|(i, chunk)| {
                json!({
                    "hash": XORB_HASH,
                    "unpacked_length": chunk.len(),
                    "range": {"start": i as u64, "end": i as u64 + 1}
                })
            })
            .collect();
        let ranges: serde_json::Value = (0..32u64)
            .map(|i| {
                json!({
                    "chunks": {"start": i, "end": i + 1},
                    "bytes": {"start": i * 100, "end": i * 100 + 99}
                })
            })
            .collect();

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            terms,
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": ranges
                }]
            }),
        )
        .await;
        // One mock per distinct (byte range) request.
        for i in 0..32u64 {
            let start = i * 100;
            let end = start + 99;
            let slice = serialize_payload(&[chunks[i as usize].as_slice()]);
            xorb_range_mock(&server, start, end, slice).await;
        }

        let cap = 256u64;
        let ctx = test_stream_context(&server, cap);
        let mut stream = make_unordered_stream(ctx.clone(), None);
        let mut expected = Vec::new();
        for chunk in &chunks {
            expected.extend_from_slice(chunk);
        }
        let mut total = 0u64;
        let mut peak_in_progress = 0u64;
        while let Some((_offset, chunk)) = stream.next().await.unwrap() {
            total = total.saturating_add(u64::try_from(chunk.len()).unwrap());
            peak_in_progress = peak_in_progress.max(stream.bytes_in_progress());
            // The byte-denominated semaphore bounds in-flight buffered bytes.
            assert!(stream.bytes_in_progress() <= cap);
        }
        assert_eq!(total, u64::try_from(expected.len()).unwrap());
        assert!(peak_in_progress > 0);
        // After full consumption the semaphore is back to capacity.
        assert_eq!(ctx.buffer_semaphore.available_permits(), cap);
    }

    #[tokio::test]
    async fn download_unordered_stream_yields_completion_order() {
        let server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let _payload = serialize_payload(&[&chunk_a, &chunk_b]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 99}},
                        {"chunks": {"start": 1, "end": 2}, "bytes": {"start": 100, "end": 199}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 99, serialize_payload(&[&chunk_a])).await;
        xorb_range_mock(&server, 100, 199, serialize_payload(&[&chunk_b])).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let mut stream = make_unordered_stream(ctx, None);
        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);
        let mut pieces: Vec<(u64, Vec<u8>)> = Vec::new();
        while let Some((offset, chunk)) = stream.next().await.unwrap() {
            pieces.push((offset, chunk.to_vec()));
        }
        // The stream yields completion order (not file order): reassemble by
        // offset to verify byte-identity.
        pieces.sort_by_key(|(offset, _)| *offset);
        let mut out = Vec::new();
        let offsets: Vec<u64> = pieces.iter().map(|(offset, _)| *offset).collect();
        for (_, data) in pieces {
            out.extend_from_slice(&data);
        }
        assert_eq!(offsets, vec![0, 64]);
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn download_stream_cancel_stops_promptly() {
        let server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let payload = serialize_payload(&[&chunk_a, &chunk_b]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 200}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 200, payload).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let mut stream = make_stream(ctx, None);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, chunk_a);
        stream.cancel();
        // After cancel, subsequent calls return Ok(None) and stay finished.
        assert!(stream.next().await.unwrap().is_none());
        assert!(stream.next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn download_stream_drop_cancels_background_task() {
        let server = MockServer::start().await;
        let chunk = vec![5u8; 64];
        let payload = serialize_payload(&[&chunk]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                    ]
                }]
            }),
        )
        .await;
        xorb_range_mock(&server, 0, 100, payload).await;

        let ctx = test_stream_context(&server, 1_048_576);
        let mut stream = make_stream(ctx, None);
        // Pull one chunk, then drop mid-stream: the background task must be
        // cancelled rather than leaking.
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first, chunk);
        drop(stream);
        // Allow any in-flight task to observe the cancellation.
        tokio::task::yield_now().await;
    }

    #[test]
    fn blocking_next_works_from_plain_thread() {
        let rt = Runtime::new().unwrap();
        let (server, mut stream) = rt.block_on(async {
            let server = MockServer::start().await;
            let chunk = vec![5u8; 64];
            let payload = serialize_payload(&[&chunk]);
            reconstruction_mock(
                &server,
                0,
                4095,
                0,
                json!([
                    {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
                ]),
                json!({
                    XORB_HASH: [{
                        "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                        "ranges": [
                            {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                        ]
                    }]
                }),
            )
            .await;
            xorb_range_mock(&server, 0, 100, payload).await;
            let ctx = test_stream_context(&server, 1_048_576);
            (server, make_stream(ctx, None))
        });
        drop(rt);

        // Consume on a plain (non-async) thread via the dedicated runtime.
        let handle = std::thread::spawn(move || {
            let mut out = Vec::new();
            while let Some(chunk) = stream.blocking_next().unwrap() {
                out.extend_from_slice(&chunk);
            }
            (out, stream)
        });
        let (out, _stream) = handle.join().unwrap();
        assert_eq!(out, vec![5u8; 64]);
        drop(server);
    }

    #[tokio::test]
    async fn download_stream_matches_reconstruct_output() {
        // Use two separate mock servers: the M2a `reconstruct` path sends no
        // Range header, while the streaming path sends `bytes=0-4095` (with a
        // 416 fallback), so the mock sets must not overlap.
        let full_server = MockServer::start().await;
        let stream_server = MockServer::start().await;
        let chunk_a = vec![7u8; 64];
        let chunk_b = vec![9u8; 64];
        let payload = serialize_payload(&[&chunk_a, &chunk_b]);

        let terms = json!([
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}},
            {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 1, "end": 2}}
        ]);
        let full_xorbs = json!({
            XORB_HASH: [{
                "url": format!("{}/transfer/xorb/default/{XORB_HASH}", full_server.uri()),
                "ranges": [
                    {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 200}}
                ]
            }]
        });
        let stream_xorbs = json!({
            XORB_HASH: [{
                "url": format!("{}/transfer/xorb/default/{XORB_HASH}", stream_server.uri()),
                "ranges": [
                    {"chunks": {"start": 0, "end": 2}, "bytes": {"start": 0, "end": 200}}
                ]
            }]
        });

        // Full-file reconstruction (no Range header) via the M2a path.
        Mock::given(method("GET"))
            .and(path(format!("/v2/reconstructions/{FILE_ID}")))
            .and(header("authorization", format!("Bearer {READ_TOKEN}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(v2_response_body(
                0,
                terms.clone(),
                full_xorbs,
            )))
            .mount(&full_server)
            .await;
        xorb_range_mock(&full_server, 0, 200, payload.clone()).await;

        // Streaming path uses the ranged request; map the prefetch range.
        reconstruction_mock(&stream_server, 0, 4095, 0, terms, stream_xorbs).await;
        xorb_range_mock(&stream_server, 0, 200, payload.clone()).await;

        let mut expected = chunk_a.clone();
        expected.extend_from_slice(&chunk_b);

        // M2a sequential reconstruction.
        let transfer = TransferClient::new(reqwest::Client::new());
        let file =
            reconstruction::reconstruct(&transfer, &full_server.uri(), READ_TOKEN, FILE_ID, None)
                .await
                .unwrap();
        assert_eq!(file.data, expected);

        // Streaming path.
        let ctx = test_stream_context(&stream_server, 1_048_576);
        let (out, _sizes) = drain(make_stream(ctx, None)).await;
        assert_eq!(out, expected);
    }

    #[tokio::test]
    async fn download_stream_surfaces_background_errors() {
        let server = MockServer::start().await;
        let chunk = vec![5u8; 64];
        let payload = serialize_payload(&[&chunk]);

        reconstruction_mock(
            &server,
            0,
            4095,
            0,
            json!([
                {"hash": XORB_HASH, "unpacked_length": 64, "range": {"start": 0, "end": 1}}
            ]),
            json!({
                XORB_HASH: [{
                    "url": format!("{}/transfer/xorb/default/{XORB_HASH}", server.uri()),
                    "ranges": [
                        {"chunks": {"start": 0, "end": 1}, "bytes": {"start": 0, "end": 100}}
                    ]
                }]
            }),
        )
        .await;
        // No xorb mock: the fetch 404s, and the error must surface via the
        // stream's check_error at an item boundary.
        let _ = payload;

        let ctx = test_stream_context(&server, 1_048_576);
        let mut stream = make_stream(ctx, None);
        let result = stream.next().await;
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(SdxError::Transfer(TransferError::NotFound(_)))
        ));
    }
}
