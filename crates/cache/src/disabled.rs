use crate::{AsyncReconstructionCache, ReconstructionCacheFuture, ReconstructionCacheKey};

/// No-op reconstruction cache adapter.
#[derive(Debug, Clone, Default)]
pub struct DisabledReconstructionCache;

impl DisabledReconstructionCache {
    /// Creates a disabled reconstruction cache adapter.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl AsyncReconstructionCache for DisabledReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn get<'operation>(
        &'operation self,
        _key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async { Ok(None) })
    }

    fn put<'operation>(
        &'operation self,
        _key: &'operation ReconstructionCacheKey,
        _payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async { Ok(()) })
    }

    fn delete<'operation>(
        &'operation self,
        _key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async { Ok(false) })
    }
}
