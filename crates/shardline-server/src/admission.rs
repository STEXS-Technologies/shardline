use std::sync::Arc;
use std::num::NonZeroUsize;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

/// Weighted admission controller for request work.
///
/// Grants permits with configurable weight. Large requests (uploads, reconstructions)
/// consume more capacity than small requests (metadata lookups).
#[derive(Debug, Clone)]
pub struct WeightedAdmission {
    inner: Arc<Semaphore>,
    max_weight: u64,
}

impl WeightedAdmission {
    /// Creates a new admission controller with the given maximum concurrent weight.
    pub fn new(max_weight: NonZeroUsize) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(max_weight.get())),
            max_weight: max_weight.get() as u64,
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
            return None;
        }
        let permits = u32::try_from(weight.min(self.max_weight)).ok()?;
        self.inner.clone().try_acquire_many_owned(permits).ok()
    }

    /// Acquires a permit with the given weight, waiting if necessary.
    /// Returns `None` if `weight` is zero.
    pub async fn acquire(&self, weight: u64) -> Option<OwnedSemaphorePermit> {
        if weight == 0 {
            return None;
        }
        let permits = u32::try_from(weight.min(self.max_weight)).ok()?;
        self.inner.clone().acquire_many_owned(permits).await.ok()
    }

    /// Returns the number of available permits.
    pub fn available_permits(&self) -> u32 {
        self.inner.available_permits() as u32
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

    #[test]
    fn admission_grants_permit_within_capacity() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(10).unwrap());
        let permit = ctrl.try_acquire(5);
        assert!(permit.is_some());
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
