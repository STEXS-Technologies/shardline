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

#[cfg(test)]
mod tests {
    use crate::{AsyncReconstructionCache, ReconstructionCacheKey};

    use super::*;

    #[test]
    fn new_constructs_without_panic() {
        let cache = DisabledReconstructionCache::new();
        drop(cache);
    }

    #[test]
    fn default_constructs_without_panic() {
        let cache = DisabledReconstructionCache::default();
        drop(cache);
    }

    #[tokio::test]
    async fn ready_returns_ok() {
        let cache = DisabledReconstructionCache::new();
        let result = cache.ready().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn get_returns_none() {
        let cache = DisabledReconstructionCache::new();
        let key = ReconstructionCacheKey::latest("test", None);
        let result = cache.get(&key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn put_returns_ok() {
        let cache = DisabledReconstructionCache::new();
        let key = ReconstructionCacheKey::latest("test", None);
        let result = cache.put(&key, b"payload").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_returns_false() {
        let cache = DisabledReconstructionCache::new();
        let key = ReconstructionCacheKey::latest("test", None);
        let result = cache.delete(&key).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
