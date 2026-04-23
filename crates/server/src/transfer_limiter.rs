use std::{num::NonZeroUsize, sync::Arc};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::ServerError;

/// Weighted transfer concurrency limiter based on chunk-equivalent cost.
#[derive(Debug, Clone)]
pub(crate) struct TransferLimiter {
    chunk_size_bytes: NonZeroUsize,
    max_in_flight_chunks: NonZeroUsize,
    semaphore: Arc<Semaphore>,
}

impl TransferLimiter {
    /// Creates a transfer limiter that budgets concurrent response work in
    /// chunk-equivalent permits.
    pub(crate) fn new(chunk_size_bytes: NonZeroUsize, max_in_flight_chunks: NonZeroUsize) -> Self {
        Self {
            chunk_size_bytes,
            max_in_flight_chunks,
            semaphore: Arc::new(Semaphore::new(max_in_flight_chunks.get())),
        }
    }

    /// Acquires permits for a transfer with the supplied byte length.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the limiter capacity cannot be represented or the
    /// semaphore has been closed.
    pub(crate) async fn acquire_bytes(
        &self,
        total_bytes: u64,
    ) -> Result<OwnedSemaphorePermit, ServerError> {
        let permits = permits_for_bytes(
            total_bytes,
            self.chunk_size_bytes,
            self.max_in_flight_chunks,
        )?;
        self.semaphore
            .clone()
            .acquire_many_owned(permits)
            .await
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
        let one_byte = permits_for_bytes(1, CHUNK_SIZE, MAX_IN_FLIGHT);
        let exact_chunk = permits_for_bytes(4, CHUNK_SIZE, MAX_IN_FLIGHT);
        let two_chunks = permits_for_bytes(5, CHUNK_SIZE, MAX_IN_FLIGHT);
        let capped = permits_for_bytes(20, CHUNK_SIZE, MAX_IN_FLIGHT);

        assert!(one_byte.is_ok());
        assert!(exact_chunk.is_ok());
        assert!(two_chunks.is_ok());
        assert!(capped.is_ok());
        assert_eq!(one_byte.ok(), Some(1));
        assert_eq!(exact_chunk.ok(), Some(1));
        assert_eq!(two_chunks.ok(), Some(2));
        assert_eq!(capped.ok(), Some(3));
    }

    #[test]
    fn zero_byte_transfers_still_reserve_one_permit() {
        let permits = permits_for_bytes(0, CHUNK_SIZE, MAX_IN_FLIGHT);

        assert!(permits.is_ok());
        assert_eq!(permits.ok(), Some(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn limiter_blocks_until_capacity_returns() {
        let limiter = TransferLimiter::new(CHUNK_SIZE, NonZeroUsize::MIN);
        let first = limiter.acquire_bytes(4).await;
        assert!(first.is_ok());
        let Ok(first) = first else {
            return;
        };

        let blocked = timeout(Duration::from_millis(50), limiter.acquire_bytes(4)).await;
        assert!(blocked.is_err());

        drop(first);

        let released = timeout(Duration::from_secs(1), limiter.acquire_bytes(4)).await;
        assert!(released.is_ok());
        let Ok(released) = released else {
            return;
        };
        assert!(released.is_ok());
    }
}
