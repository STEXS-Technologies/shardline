use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ServerError;

/// Default timeout for acquiring chunk transfer permits.
pub const DEFAULT_TRANSFER_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(60);

/// Weighted transfer concurrency limiter based on chunk-equivalent cost.
#[derive(Debug, Clone)]
pub struct TransferLimiter {
    chunk_size_bytes: NonZeroUsize,
    max_in_flight_chunks: NonZeroUsize,
    semaphore: Arc<Semaphore>,
    acquire_timeout: Duration,
}

impl TransferLimiter {
    /// Creates a transfer limiter that budgets concurrent response work in
    /// chunk-equivalent permits.
    #[must_use]
    pub fn new(chunk_size_bytes: NonZeroUsize, max_in_flight_chunks: NonZeroUsize) -> Self {
        Self {
            chunk_size_bytes,
            max_in_flight_chunks,
            semaphore: Arc::new(Semaphore::new(max_in_flight_chunks.get())),
            acquire_timeout: DEFAULT_TRANSFER_ACQUIRE_TIMEOUT,
        }
    }

    /// Sets a custom acquire timeout.
    #[must_use]
    pub const fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Acquires permits for a transfer with the supplied byte length.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the limiter capacity cannot be represented,
    /// the semaphore has been closed, or the acquire timeout elapses.
    pub async fn acquire_bytes(
        &self,
        total_bytes: u64,
    ) -> Result<OwnedSemaphorePermit, ServerError> {
        let permits = permits_for_bytes(
            total_bytes,
            self.chunk_size_bytes,
            self.max_in_flight_chunks,
        )?;
        let semaphore = self.semaphore.clone();
        tokio::time::timeout(self.acquire_timeout, semaphore.acquire_many_owned(permits))
            .await
            .map_err(|_elapsed| ServerError::TransferLimiterTimedOut)?
            .map_err(|_error| ServerError::TransferLimiterClosed)
    }
}

fn permits_for_bytes(
    total_bytes: u64,
    chunk_size_bytes: NonZeroUsize,
    max_in_flight_chunks: NonZeroUsize,
) -> Result<u32, ServerError> {
    let effective_bytes = total_bytes.max(1);
    let chunk_size = u64::try_from(chunk_size_bytes.get())?;
    let required_chunks = effective_bytes.div_ceil(chunk_size);
    let max_chunks = u64::try_from(max_in_flight_chunks.get())?;
    let bounded_chunks = required_chunks.min(max_chunks);
    u32::try_from(bounded_chunks).map_err(ServerError::from)
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use tokio::time::timeout;

    use crate::ServerError;

    use super::{TransferLimiter, permits_for_bytes};

    const CHUNK_SIZE: NonZeroUsize = match NonZeroUsize::new(4) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

    const MAX_IN_FLIGHT: NonZeroUsize = match NonZeroUsize::new(3) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

    #[test]
    fn permits_scale_with_bytes_and_cap_at_capacity() {
        for (bytes, chunk_size, capacity, expected) in [
            (0, CHUNK_SIZE, MAX_IN_FLIGHT, 1),
            (1, CHUNK_SIZE, MAX_IN_FLIGHT, 1),
            (4, CHUNK_SIZE, MAX_IN_FLIGHT, 1),
            (5, CHUNK_SIZE, MAX_IN_FLIGHT, 2),
            (20, CHUNK_SIZE, MAX_IN_FLIGHT, 3),
            (100, NonZeroUsize::MIN, NonZeroUsize::new(10).unwrap(), 10),
            (
                u64::MAX,
                NonZeroUsize::MIN,
                NonZeroUsize::new(u32::MAX as usize).unwrap(),
                u32::MAX,
            ),
        ] {
            assert_eq!(
                permits_for_bytes(bytes, chunk_size, capacity).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn transfer_limiter_new_stores_configuration() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, MAX_IN_FLIGHT);
        assert_eq!(limiter.chunk_size_bytes, CHUNK_SIZE);
        assert_eq!(limiter.max_in_flight_chunks, MAX_IN_FLIGHT);
        assert_eq!(limiter.semaphore.available_permits(), MAX_IN_FLIGHT.get());
        assert_eq!(limiter.acquire_timeout, Duration::from_secs(60));
    }

    #[test]
    fn with_acquire_timeout_overrides_default() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, MAX_IN_FLIGHT)
            .with_acquire_timeout(Duration::from_secs(10));
        assert_eq!(limiter.acquire_timeout, Duration::from_secs(10));
    }

    #[test]
    fn permits_for_bytes_overflow_detected() {
        // Use a max_in_flight that exceeds u32::MAX to trigger the TryFrom error.
        let huge_capacity = NonZeroUsize::new(usize::MAX).unwrap_or(NonZeroUsize::MIN);
        let small_chunk = NonZeroUsize::new(1).unwrap_or(NonZeroUsize::MIN);

        let result = permits_for_bytes(u64::MAX, small_chunk, huge_capacity);
        assert!(matches!(result, Err(ServerError::NumericConversion(_))));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn limiter_blocks_until_capacity_returns() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, NonZeroUsize::MIN);
        let first = limiter.acquire_bytes(4).await;
        let first = first.expect("expected valid first");

        let blocked = timeout(Duration::from_millis(50), limiter.acquire_bytes(4)).await;
        assert!(blocked.is_err());

        drop(first);

        let released = timeout(Duration::from_secs(1), limiter.acquire_bytes(4)).await;
        let released = released.expect("expected valid released");
        assert!(released.is_ok());
    }

    #[tokio::test]
    async fn limiter_returns_closed_error_when_semaphore_closed() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, MAX_IN_FLIGHT);
        limiter.semaphore.close();
        assert!(matches!(
            limiter.acquire_bytes(1).await,
            Err(ServerError::TransferLimiterClosed)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_times_out_deterministically_with_paused_time() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, NonZeroUsize::MIN)
            .with_acquire_timeout(Duration::from_millis(100));
        let _permit = limiter.acquire_bytes(4).await.unwrap();
        // Tokio advances the paused clock once the blocked acquire is idle.
        assert!(matches!(
            limiter.acquire_bytes(4).await,
            Err(ServerError::TransferLimiterTimedOut)
        ));
    }
}
