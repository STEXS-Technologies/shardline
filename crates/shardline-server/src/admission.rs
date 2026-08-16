use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
    #[must_use]
    pub fn new(max_weight: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(max_weight.get())),
            max_weight: max_weight.get() as u64,
            counters: AdmissionCounters::default(),
        }
    }

    /// Returns the configured maximum weight.
    pub const fn max_weight(&self) -> u64 {
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
    pub const fn counters(&self) -> &AdmissionCounters {
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
    pub const fn capacity(&self) -> u32 {
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
    #[must_use]
    pub fn default_sizes() -> Self {
        Self {
            hashing: BoundedPool::new(NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN)),
            parsing: BoundedPool::new(NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN)),
            blocking_io: BoundedPool::new(NonZeroUsize::new(16).unwrap_or(NonZeroUsize::MIN)),
        }
    }

    /// Creates execution pools with custom sizes from configuration.
    #[must_use]
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

/// Operation deadlines enforced at the server boundary.
pub mod timeouts {
    use std::time::Duration;

    /// Upper bound for work performed while serving one HTTP request.
    ///
    /// This does not replace lower-level client and database deadlines; it is
    /// the final server-side guard that prevents an accepted request from
    /// running indefinitely.
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

/// Standard request weights for admission control.
pub mod weights {
    /// Weight for a lightweight xorb/chunk read (a repository-reference
    /// metadata scan). Read handlers enumerate the repo's latest + version
    /// records per request (O(N) in record count), so they are admission-gated
    /// like the upload/reconstruction paths to bound concurrent scans.
    pub const XORB_READ: u64 = 1;
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

    #[tokio::test]
    async fn admission_async_acquire_succeeds() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let permit = ctrl.acquire(3).await;
        assert!(
            permit.is_some(),
            "async acquire should return permit when capacity available"
        );
        assert_eq!(ctrl.available_permits(), 7);
    }

    #[tokio::test]
    async fn admission_async_acquire_zero_weight_returns_none() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let permit = ctrl.acquire(0).await;
        assert!(permit.is_none(), "acquire with weight 0 should return None");
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
    fn bounded_pool_releases_permit_on_drop() {
        let pool = BoundedPool::new(NonZeroUsize::new(2).unwrap());
        let p1 = pool.try_acquire();
        assert_eq!(pool.available_permits(), 1);
        drop(p1);
        assert_eq!(pool.available_permits(), 2);
    }

    #[test]
    fn execution_pools_default_sizes() {
        let pools = ExecutionPools::default_sizes();
        assert_eq!(pools.hashing.capacity(), 8);
        assert_eq!(pools.parsing.capacity(), 8);
        assert_eq!(pools.blocking_io.capacity(), 16);
    }

    #[test]
    fn execution_pools_with_sizes() {
        let pools = ExecutionPools::with_sizes(
            NonZeroUsize::new(4).unwrap(),
            NonZeroUsize::new(6).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        );
        assert_eq!(pools.hashing.capacity(), 4);
        assert_eq!(pools.parsing.capacity(), 6);
        assert_eq!(pools.blocking_io.capacity(), 8);
    }

    #[test]
    fn admission_available_permits_reflects_usage() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        assert_eq!(ctrl.available_permits(), 10);
        let p1 = ctrl.try_acquire(3).unwrap();
        assert_eq!(ctrl.available_permits(), 7);
        drop(p1);
        assert_eq!(ctrl.available_permits(), 10);
    }

    #[test]
    fn admission_weighted_max_weight_accessor() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(50).unwrap());
        assert_eq!(ctrl.max_weight(), 50);
    }
}
