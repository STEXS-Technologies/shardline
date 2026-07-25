use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

/// Weighted admission controller for request work.
///
/// Grants permits with configurable weight. Large requests (uploads, reconstructions)
/// consume more capacity than small requests (metadata lookups).
#[derive(Debug, Clone)]
pub struct WeightedAdmission {
    inner: Arc<Semaphore>,
    max_weight: u64,
    counters: AdmissionCounters,
}

impl WeightedAdmission {
    /// Creates a new admission controller with the given maximum concurrent weight.
    pub fn new(max_weight: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(max_weight.get())),
            max_weight: max_weight.get() as u64,
            counters: AdmissionCounters::default(),
        }
    }

    /// Returns the configured maximum weight.
    pub fn max_weight(&self) -> u64 {
        self.max_weight
    }

    /// Attempts to acquire a permit with the given weight.
    /// Returns `None` if `weight` is zero, the semaphore is closed, or the weight exceeds max.
    pub fn try_acquire(&self, weight: u64) -> Option<OwnedSemaphorePermit> {
        if weight == 0 {
            self.counters.record_rejected();
            return None;
        }
        let permits = u32::try_from(weight.min(self.max_weight)).ok()?;
        let result = self.inner.clone().try_acquire_many_owned(permits).ok();
        if result.is_some() {
            self.counters.record_admitted();
        } else {
            self.counters.record_rejected();
        }
        result
    }

    /// Acquires a permit with the given weight, waiting if necessary.
    /// Returns `None` if `weight` is zero.
    pub async fn acquire(&self, weight: u64) -> Option<OwnedSemaphorePermit> {
        if weight == 0 {
            self.counters.record_rejected();
            return None;
        }
        let permits = u32::try_from(weight.min(self.max_weight)).ok()?;
        let start = std::time::Instant::now();
        let result = self.inner.clone().acquire_many_owned(permits).await.ok();
        if result.is_some() {
            if start.elapsed() > std::time::Duration::from_millis(1) {
                self.counters.record_queued();
            } else {
                self.counters.record_admitted();
            }
        } else {
            self.counters.record_rejected();
        }
        result
    }

    /// Returns the number of available permits.
    pub fn available_permits(&self) -> u32 {
        self.inner.available_permits() as u32
    }

    /// Returns a reference to the admission counters.
    pub fn counters(&self) -> &AdmissionCounters {
        &self.counters
    }
}

/// Bounded execution pool for specific work types (hashing, parsing, blocking I/O).
///
/// Each pool has a maximum concurrency. Attempts beyond the limit are rejected
/// immediately rather than queued (to avoid head-of-line blocking).
#[derive(Debug, Clone)]
pub struct BoundedPool {
    inner: Arc<Semaphore>,
    capacity: u32,
}

impl BoundedPool {
    /// Creates a new bounded pool with the given maximum concurrency.
    pub fn new(max_concurrent: NonZeroUsize) -> Self {
        let cap = max_concurrent.get() as u32;
        Self {
            inner: Arc::new(Semaphore::new(cap as usize)),
            capacity: cap,
        }
    }

    /// Attempts to acquire a permit without waiting.
    /// Returns `None` if the pool is saturated.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.clone().try_acquire_owned().ok()
    }

    /// Returns the number of currently available permits.
    pub fn available_permits(&self) -> u32 {
        self.inner.available_permits() as u32
    }

    /// Returns the total pool capacity.
    pub fn capacity(&self) -> u32 {
        self.capacity
    }
}

/// Collection of bounded execution pools for the server.
#[derive(Debug, Clone)]
pub struct ExecutionPools {
    /// Pool for CPU-intensive hashing operations.
    pub hashing: BoundedPool,
    /// Pool for parsing and deserialization.
    pub parsing: BoundedPool,
    /// Pool for blocking filesystem I/O.
    pub blocking_io: BoundedPool,
}

impl ExecutionPools {
    /// Creates execution pools with default sizes.
    pub fn default_sizes() -> Self {
        Self {
            hashing: BoundedPool::new(NonZeroUsize::new(8).unwrap()),
            parsing: BoundedPool::new(NonZeroUsize::new(8).unwrap()),
            blocking_io: BoundedPool::new(NonZeroUsize::new(16).unwrap()),
        }
    }

    /// Creates execution pools with custom sizes from configuration.
    pub fn with_sizes(
        hashing: NonZeroUsize,
        parsing: NonZeroUsize,
        blocking_io: NonZeroUsize,
    ) -> Self {
        Self {
            hashing: BoundedPool::new(hashing),
            parsing: BoundedPool::new(parsing),
            blocking_io: BoundedPool::new(blocking_io),
        }
    }
}

/// Default timeout durations for operations.
pub mod timeouts {
    use std::time::Duration;

    /// Timeout for object storage read operations (S3 GET, local file read).
    pub const STORAGE_READ: Duration = Duration::from_secs(30);
    /// Timeout for object storage write operations (S3 PUT, local file write).
    pub const STORAGE_WRITE: Duration = Duration::from_secs(60);
    /// Timeout for metadata database queries.
    pub const DATABASE_QUERY: Duration = Duration::from_secs(10);
    /// Timeout for metadata database writes (transactions).
    pub const DATABASE_WRITE: Duration = Duration::from_secs(30);
    /// Timeout for HTTP request body headers.
    pub const REQUEST_HEADERS: Duration = Duration::from_secs(10);
    /// Timeout for the total HTTP request (upload or download).
    pub const REQUEST_TOTAL: Duration = Duration::from_secs(300);
}

/// Admission metrics counters tracked via atomics (used when prometheus metrics are unavailable).
#[derive(Debug, Default)]
pub struct AdmissionCounters {
    pub admitted: AtomicU64,
    pub queued: AtomicU64,
    pub rejected: AtomicU64,
}

impl AdmissionCounters {
    pub fn record_admitted(&self) {
        self.admitted.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_queued(&self) {
        self.queued.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }
}

// Manual Clone implementation because AtomicU64 is not Clone.
impl Clone for AdmissionCounters {
    fn clone(&self) -> Self {
        Self {
            admitted: AtomicU64::new(self.admitted.load(Ordering::Relaxed)),
            queued: AtomicU64::new(self.queued.load(Ordering::Relaxed)),
            rejected: AtomicU64::new(self.rejected.load(Ordering::Relaxed)),
        }
    }
}

/// Tracks storage quotas per repository scope.
#[derive(Debug, Clone)]
pub struct QuotaTracker {
    inner: Arc<Mutex<QuotaState>>,
}

#[derive(Debug, Default)]
struct QuotaState {
    /// Bytes stored per repository (key format: "{provider}/{owner}/{repo}").
    storage_bytes: HashMap<String, u64>,
    /// Upload operations count per repository.
    upload_count: HashMap<String, u64>,
}

impl QuotaTracker {
    /// Creates a new quota tracker.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(QuotaState::default())),
        }
    }

    /// Records a storage operation for a repository.
    pub fn record_store(&self, repo_key: &str, bytes: u64) {
        let mut state = self.inner.lock().unwrap();
        *state.storage_bytes.entry(repo_key.to_owned()).or_insert(0) += bytes;
        *state.upload_count.entry(repo_key.to_owned()).or_insert(0) += 1;
    }

    /// Returns the total bytes stored for a repository.
    pub fn storage_bytes(&self, repo_key: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .storage_bytes
            .get(repo_key)
            .copied()
            .unwrap_or(0)
    }

    /// Returns the upload count for a repository.
    pub fn upload_count(&self, repo_key: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .upload_count
            .get(repo_key)
            .copied()
            .unwrap_or(0)
    }

    /// Checks if a storage operation would exceed the per-repo quota (default 100 GiB).
    pub fn would_exceed_quota(&self, repo_key: &str, additional_bytes: u64, max_bytes: u64) -> bool {
        self.storage_bytes(repo_key)
            .saturating_add(additional_bytes)
            > max_bytes
    }
}

impl Default for QuotaTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Standard request weights for admission control.
pub mod weights {
    /// Weight for a simple metadata lookup (stats, exists check).
    pub const METADATA_LOOKUP: u64 = 1;
    /// Weight for a chunk read (small object download).
    pub const CHUNK_READ: u64 = 2;
    /// Weight for a xorb upload (stores chunks + metadata).
    pub const XORB_UPLOAD: u64 = 4;
    /// Weight for a shard upload (parsing + metadata commit).
    pub const SHARD_UPLOAD: u64 = 8;
    /// Weight for a file reconstruction (complex multi-chunk read).
    pub const RECONSTRUCTION: u64 = 16;
    /// Weight for a batch operation (reconstruction batch, LFS batch).
    pub const BATCH_OPERATION: u64 = 32;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    // ── AdmissionCounters tests ──────────────────────────────────────────

    #[test]
    fn admission_counters_record() {
        let counters = AdmissionCounters::default();
        counters.record_admitted();
        counters.record_queued();
        counters.record_rejected();
        assert_eq!(counters.admitted.load(Ordering::Relaxed), 1);
        assert_eq!(counters.queued.load(Ordering::Relaxed), 1);
        assert_eq!(counters.rejected.load(Ordering::Relaxed), 1);
    }

    // ── QuotaTracker tests ──────────────────────────────────────────────

    #[test]
    fn quota_tracker_tracks_bytes() {
        let qt = QuotaTracker::new();
        qt.record_store("github/owner/repo", 1000);
        assert_eq!(qt.storage_bytes("github/owner/repo"), 1000);
    }

    #[test]
    fn quota_tracker_would_exceed() {
        let qt = QuotaTracker::new();
        qt.record_store("github/owner/repo", 90);
        assert!(!qt.would_exceed_quota("github/owner/repo", 5, 100));
        assert!(qt.would_exceed_quota("github/owner/repo", 15, 100));
    }

    #[test]
    fn quota_tracker_default_zero_for_unknown_key() {
        let qt = QuotaTracker::new();
        assert_eq!(qt.storage_bytes("nonexistent"), 0);
        assert_eq!(qt.upload_count("nonexistent"), 0);
    }

    #[test]
    fn quota_tracker_upload_count_increments() {
        let qt = QuotaTracker::new();
        qt.record_store("gitlab/team/project", 500);
        qt.record_store("gitlab/team/project", 300);
        assert_eq!(qt.upload_count("gitlab/team/project"), 2);
    }

    // ── WeightedAdmission tests ─────────────────────────────────────────

    #[test]
    fn admission_grants_permit_within_capacity() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let permit = ctrl.try_acquire(5);
        assert!(permit.is_some());
    }

    #[test]
    fn admission_counters_reflect_try_acquire_admitted() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let _permit = ctrl.try_acquire(5);
        assert_eq!(ctrl.counters().admitted.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn admission_counters_reflect_zero_weight_rejected() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let _permit = ctrl.try_acquire(0);
        assert_eq!(ctrl.counters().rejected.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn admission_denies_when_exhausted() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(3).unwrap());
        let p1 = ctrl.try_acquire(2);
        let p2 = ctrl.try_acquire(2);
        assert!(p1.is_some());
        assert!(p2.is_none());
    }

    #[test]
    fn admission_releases_permit() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(5).unwrap());
        let permit = ctrl.try_acquire(5);
        assert!(permit.is_some());
        assert_eq!(ctrl.available_permits(), 0);
        drop(permit);
        assert_eq!(ctrl.available_permits(), 5);
    }

    #[test]
    fn admission_weight_capped_at_max() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        // Weight 100 should be capped to max_weight=10
        let permit = ctrl.try_acquire(100);
        assert!(permit.is_some());
        assert_eq!(ctrl.available_permits(), 0);
    }

    #[test]
    fn admission_zero_weight_returns_none() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let permit = ctrl.try_acquire(0);
        assert!(permit.is_none());
    }

    // ── BoundedPool tests ──────────────────────────────────────────────────

    #[test]
    fn bounded_pool_acquires_within_capacity() {
        let pool = BoundedPool::new(NonZeroUsize::new(5).unwrap());
        let p1 = pool.try_acquire();
        let p2 = pool.try_acquire();
        assert!(p1.is_some());
        assert!(p2.is_some());
    }

    #[test]
    fn bounded_pool_rejects_when_full() {
        let pool = BoundedPool::new(NonZeroUsize::new(1).unwrap());
        let p1 = pool.try_acquire();
        let p2 = pool.try_acquire();
        assert!(p1.is_some());
        assert!(p2.is_none());
    }

    #[test]
    fn execution_pools_default_sizes() {
        let pools = ExecutionPools::default_sizes();
        assert_eq!(pools.hashing.capacity(), 8);
        assert_eq!(pools.parsing.capacity(), 8);
        assert_eq!(pools.blocking_io.capacity(), 16);
    }
}
