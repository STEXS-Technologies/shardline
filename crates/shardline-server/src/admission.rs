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
}
