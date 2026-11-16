use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ServerConfigError;

mod blocking;
mod bytes;
pub(crate) use blocking::{blocking_work, blocking_work_with_buffer, blocking_work_with_permit};
pub(crate) use blocking::{cancellable_work_with_permit, cancellable_work_with_setup};
pub(crate) use bytes::retain_bytes;

/// Largest capacity representable by batch permit counts and Tokio's semaphore.
pub(crate) fn maximum_counted_capacity() -> usize {
    (u32::MAX as usize).min(Semaphore::MAX_PERMITS)
}

pub(crate) const fn validate_capacity(
    name: &'static str,
    capacity: NonZeroUsize,
    maximum: usize,
) -> Result<(), ServerConfigError> {
    if capacity.get() > maximum {
        return Err(ServerConfigError::ResourceCapacityOutOfRange {
            name,
            capacity: capacity.get(),
            maximum,
        });
    }
    Ok(())
}

/// Weighted admission controller for request work.
///
/// Grants permits with configurable weight. Large requests (uploads, reconstructions)
/// consume more capacity than small requests (metadata lookups).
#[derive(Debug, Clone)]
pub struct WeightedAdmission {
    inner: Arc<Semaphore>,
    max_weight: u64,
    counters: Arc<AdmissionCounters>,
}

impl WeightedAdmission {
    /// Creates a new admission controller with the given maximum concurrent weight.
    ///
    /// # Panics
    /// Panics if capacity exceeds the smaller of `u32::MAX` and Tokio's
    /// `Semaphore::MAX_PERMITS`. Use [`Self::try_new`] for configuration inputs.
    #[must_use]
    #[allow(clippy::panic)] // The documented infallible convenience API rejects invalid capacity.
    pub fn new(max_weight: NonZeroUsize) -> Self {
        match Self::try_new(max_weight) {
            Ok(controller) => controller,
            Err(error) => panic!("{error}"),
        }
    }

    /// Creates a controller after checking its capacity without allocating permits.
    ///
    /// # Errors
    /// Returns a configuration error if capacity exceeds the representable limit.
    pub fn try_new(max_weight: NonZeroUsize) -> Result<Self, ServerConfigError> {
        validate_capacity(
            "admission_max_weight",
            max_weight,
            maximum_counted_capacity(),
        )?;
        Ok(Self {
            inner: Arc::new(Semaphore::new(max_weight.get())),
            max_weight: max_weight.get() as u64,
            counters: Arc::new(AdmissionCounters::default()),
        })
    }

    /// Returns the configured maximum weight.
    #[must_use]
    pub const fn max_weight(&self) -> u64 {
        self.max_weight
    }

    /// Attempts to acquire a permit with the given weight.
    /// Caps the requested weight at the configured capacity. Returns `None`
    /// if the weight is zero, the semaphore is closed, or capacity is unavailable.
    #[must_use]
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
    #[must_use]
    pub fn available_permits(&self) -> u32 {
        self.inner.available_permits() as u32
    }

    /// Returns a reference to the admission counters.
    #[must_use]
    pub fn counters(&self) -> &AdmissionCounters {
        &self.counters
    }
}

/// Bounded execution pool for specific work types (hashing, parsing, blocking I/O).
///
/// Each pool has a maximum concurrency. Public admission attempts reject excess
/// work immediately; internal stream operations can wait cooperatively.
#[derive(Debug, Clone)]
pub struct BoundedPool {
    inner: Arc<Semaphore>,
    retained_buffers: Arc<Semaphore>,
    capacity: u32,
}

impl BoundedPool {
    /// Creates a new bounded pool with the given maximum concurrency.
    ///
    /// # Panics
    /// Panics if capacity exceeds the smaller of `u32::MAX` and Tokio's
    /// `Semaphore::MAX_PERMITS`. Use [`Self::try_new`] for configuration inputs.
    #[allow(clippy::panic)] // The documented infallible convenience API rejects invalid capacity.
    pub fn new(max_concurrent: NonZeroUsize) -> Self {
        match Self::try_new(max_concurrent) {
            Ok(pool) => pool,
            Err(error) => panic!("{error}"),
        }
    }

    /// Creates a pool after checking its capacity.
    ///
    /// # Errors
    /// Returns a configuration error instead of truncating an oversized capacity.
    pub fn try_new(max_concurrent: NonZeroUsize) -> Result<Self, ServerConfigError> {
        validate_capacity(
            "execution_pool_size",
            max_concurrent,
            maximum_counted_capacity(),
        )?;
        Ok(Self {
            inner: Arc::new(Semaphore::new(max_concurrent.get())),
            retained_buffers: Arc::new(Semaphore::new(max_concurrent.get())),
            capacity: max_concurrent.get() as u32,
        })
    }

    /// Attempts to acquire a permit without waiting.
    /// Returns `None` if the pool is saturated.
    pub fn try_acquire(&self) -> Option<OwnedSemaphorePermit> {
        self.inner.clone().try_acquire_owned().ok()
    }

    /// Waits cooperatively for stream work, without spawning a
    /// blocking job while queued. The running job must own the returned permit.
    pub(crate) async fn acquire(&self) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.inner.clone().acquire_owned().await
    }

    /// Separately bounds retained, individually size-limited buffers. Hold this
    /// charge with the actual bytes while releasing execution between jobs.
    pub(crate) async fn acquire_buffer(
        &self,
    ) -> Result<OwnedSemaphorePermit, tokio::sync::AcquireError> {
        self.retained_buffers.clone().acquire_owned().await
    }

    #[cfg(test)]
    pub(crate) fn available_buffer_permits(&self) -> usize {
        self.retained_buffers.available_permits()
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
    ///
    /// # Panics
    /// Panics if any capacity is unrepresentable. Use [`Self::try_with_sizes`]
    /// to reject invalid configuration without panicking.
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

    /// Creates execution pools with checked custom sizes.
    ///
    /// # Errors
    /// Returns a configuration error if any capacity is unrepresentable.
    pub fn try_with_sizes(
        hashing: NonZeroUsize,
        parsing: NonZeroUsize,
        blocking_io: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        Ok(Self {
            hashing: BoundedPool::try_new(hashing)?,
            parsing: BoundedPool::try_new(parsing)?,
            blocking_io: BoundedPool::try_new(blocking_io)?,
        })
    }
}

/// Operation deadlines enforced at the server boundary.
pub mod timeouts {
    use std::time::Duration;

    /// Upper bound for admission and handler work through response headers.
    ///
    /// This complements lower-level client and database deadlines.
    /// Response producers have a separate no-progress deadline after headers.
    pub const REQUEST_TOTAL: Duration = Duration::from_secs(300);

    /// Maximum interval without nonempty response data. This is an idle limit,
    /// not a limit on total transfer duration or a TCP acknowledgement timer.
    pub(crate) const RESPONSE_IDLE: Duration = Duration::from_secs(300);

    /// Maximum stall while a connection has an outstanding socket write.
    /// Idle keep-alive connections and progressing writes do not expire here.
    pub(crate) const WRITE_IDLE: Duration = Duration::from_secs(300);
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
    /// Weight for bounded, multi-container native deduplication metadata.
    /// Uses the existing controller's capacity clamp on small deployments.
    pub const XET_DEDUP: u64 = 4;
    /// Weight for a xorb upload (stores chunks + metadata).
    pub const XORB_UPLOAD: u64 = 4;
    /// Weight for a shard upload (parsing + metadata commit).
    pub const SHARD_UPLOAD: u64 = 8;
    /// Weight for a file reconstruction (complex multi-chunk read).
    pub const RECONSTRUCTION: u64 = 16;
    /// Weight for a whole-store stats scan.
    ///
    /// `stats` walks EVERY object in the store (a full dir walk or paginated S3
    /// LIST) plus a full latest-record traversal, with no LIMIT and no cache.
    /// The scan is O(N) in store size (measured ~2.7µs/object), so concurrent
    /// scans are admission-bounded like the read/upload/reconstruction paths to
    /// prevent a request flood from driving unbounded store I/O.
    pub const STATS: u64 = 8;
    /// Weight for a batch operation (reconstruction batch, LFS batch).
    pub const BATCH_OPERATION: u64 = 32;
}

#[cfg(test)]
mod tests {
    #[test]
    fn checked_capacity_boundaries_preserve_capacity() {
        let maximum = super::maximum_counted_capacity();
        let valid = NonZeroUsize::new(maximum).unwrap();
        let invalid = NonZeroUsize::new(maximum + 1).unwrap();
        let controller = WeightedAdmission::try_new(valid).unwrap();
        assert_eq!(
            controller.available_permits(),
            u32::try_from(maximum).unwrap()
        );
        let permit = controller.try_acquire(u64::MAX).unwrap();
        assert_eq!(controller.available_permits(), 0);
        drop(permit);
        assert_eq!(
            controller.available_permits(),
            u32::try_from(maximum).unwrap()
        );
        let pool = BoundedPool::try_new(valid).unwrap();
        assert_eq!(pool.capacity(), u32::try_from(maximum).unwrap());
        let permit = pool.try_acquire().unwrap();
        assert_eq!(
            pool.available_permits(),
            u32::try_from(maximum - 1).unwrap()
        );
        drop(permit);
        assert!(WeightedAdmission::try_new(invalid).is_err());
        assert!(BoundedPool::try_new(invalid).is_err());
        assert!(ExecutionPools::try_with_sizes(valid, invalid, valid).is_err());
        assert!(ExecutionPools::try_with_sizes(valid, valid, invalid).is_err());
        assert!(ExecutionPools::try_with_sizes(invalid, valid, valid).is_err());
    }

    #[test]
    #[should_panic(expected = "execution_pool_size capacity")]
    fn convenience_pool_rejects_unrepresentable_capacity() {
        let _pool =
            BoundedPool::new(NonZeroUsize::new(super::maximum_counted_capacity() + 1).unwrap());
    }

    #[test]
    #[should_panic(expected = "admission_max_weight capacity")]
    fn convenience_admission_rejects_unrepresentable_capacity() {
        let _controller = WeightedAdmission::new(
            NonZeroUsize::new(super::maximum_counted_capacity() + 1).unwrap(),
        );
    }

    use super::*;
    use std::num::NonZeroUsize;

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

    #[tokio::test]
    async fn weighted_admission_preserves_capacity_counters_and_release() {
        for (capacity, weight) in [
            (10, 0),
            (10, 3),
            (10, 5),
            (5, 5),
            (3, 2),
            (10, 100),
            (50, 50),
        ] {
            for asynchronous in [false, true] {
                let controller = WeightedAdmission::new(NonZeroUsize::new(capacity).unwrap());
                assert_eq!(controller.max_weight(), capacity as u64);
                let permit = if asynchronous {
                    controller.acquire(weight).await
                } else {
                    controller.try_acquire(weight)
                };
                let used = weight.min(capacity as u64) as u32;
                assert_eq!(permit.is_some(), weight != 0);
                assert_eq!(controller.available_permits(), capacity as u32 - used);
                let counters = controller.counters();
                assert_eq!(
                    counters.admitted.load(Ordering::Relaxed)
                        + counters.queued.load(Ordering::Relaxed),
                    u64::from(weight != 0),
                );
                if !asynchronous {
                    assert_eq!(
                        counters.admitted.load(Ordering::Relaxed),
                        u64::from(weight != 0)
                    );
                    assert_eq!(counters.queued.load(Ordering::Relaxed), 0);
                }
                if weight == 0 {
                    assert_eq!(counters.rejected.load(Ordering::Relaxed), 1);
                } else {
                    assert_eq!(counters.rejected.load(Ordering::Relaxed), 0);
                    assert!(controller.try_acquire(capacity as u64).is_none());
                    assert_eq!(counters.rejected.load(Ordering::Relaxed), 1);
                }
                drop(permit);
                assert_eq!(controller.available_permits(), capacity as u32);
            }
        }
    }

    #[test]
    fn bounded_pools_exhaust_and_release_all_capacity() {
        for capacity in [1, 2, 5] {
            let pool = BoundedPool::new(NonZeroUsize::new(capacity).unwrap());
            let permits: Vec<_> = (0..capacity)
                .map(|index| {
                    let permit = pool.try_acquire().unwrap();
                    assert_eq!(pool.available_permits(), (capacity - index - 1) as u32);
                    permit
                })
                .collect();
            assert_eq!(pool.available_permits(), 0);
            assert!(pool.try_acquire().is_none());
            drop(permits);
            assert_eq!(pool.available_permits(), capacity as u32);
        }
    }

    #[test]
    fn cloned_admission_shares_capacity_and_counters() {
        let ctrl = WeightedAdmission::new(NonZeroUsize::new(2).unwrap());
        let clone = ctrl.clone();
        let permit = clone.try_acquire(2).unwrap();
        assert_eq!(ctrl.available_permits(), 0);
        assert!(ctrl.try_acquire(1).is_none());
        assert_eq!(ctrl.counters().admitted.load(Ordering::Relaxed), 1);
        assert_eq!(clone.counters().rejected.load(Ordering::Relaxed), 1);
        drop(permit);
        assert_eq!(clone.available_permits(), 2);
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
}
