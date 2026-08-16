use std::{future::Future, pin::Pin};

use crate::{ReconstructionCacheError, ReconstructionCacheKey};

/// Boxed asynchronous reconstruction-cache operation.
pub type ReconstructionCacheFuture<'operation, T> =
    Pin<Box<dyn Future<Output = Result<T, ReconstructionCacheError>> + Send + 'operation>>;

/// Asynchronous reconstruction-cache adapter contract.
pub trait AsyncReconstructionCache: Send + Sync {
    /// Verifies that the adapter is configured correctly and can serve requests.
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()>;

    /// Loads one cached reconstruction payload.
    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>>;

    /// Stores one reconstruction payload.
    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()>;

    /// Deletes one cached reconstruction payload.
    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool>;

    /// Refreshes the aliveness stamp of any in-flight loading latch for `key`.
    ///
    /// The service layer runs the reconstruction loader between `get()` and
    /// `put()`. A concurrent waiter at the orphan bound must be able to tell a
    /// slow-but-alive loader (fresh stamp) from a dead one (stale stamp), so
    /// the loader's caller refreshes the stamp while the load is in flight
    /// (F-90). Adapters without local loading latches (Redis, Disabled) have
    /// nothing to refresh and return `Ok(false)`.
    ///
    /// Returns `true` when a loading latch existed and was refreshed.
    fn touch_loading<'operation>(
        &'operation self,
        _key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async { Ok(false) })
    }
}
