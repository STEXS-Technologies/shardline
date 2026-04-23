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
}
