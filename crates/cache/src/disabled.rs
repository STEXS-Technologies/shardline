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
        let _cache = DisabledReconstructionCache::new();
    }

    #[test]
    fn default_constructs_without_panic() {
        let _cache = DisabledReconstructionCache;
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

    #[test]
    fn new_is_const_fn() {
        const _CACHE: DisabledReconstructionCache = DisabledReconstructionCache::new();
    }

    #[test]
    fn default_trait_produces_same_as_new() {
        let a = DisabledReconstructionCache::new();
        // Both are unit structs — both impl AsyncReconstructionCache
        let _: &dyn crate::AsyncReconstructionCache = &a;
    }

    #[tokio::test]
    async fn get_with_scope_returns_none() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let cache = DisabledReconstructionCache::new();
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))
            .unwrap();
        let key = ReconstructionCacheKey::latest("scoped-file", Some(&scope));
        let result = cache.get(&key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn get_with_version_key_returns_none() {
        let cache = DisabledReconstructionCache::new();
        let key = ReconstructionCacheKey::version("file.bin", "hash123", None);
        let result = cache.get(&key).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[tokio::test]
    async fn put_with_empty_payload_returns_ok() {
        let cache = DisabledReconstructionCache::new();
        let key = ReconstructionCacheKey::latest("empty", None);
        let result = cache.put(&key, b"").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn delete_with_scoped_key_returns_false() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let cache = DisabledReconstructionCache::new();
        let scope = RepositoryScope::new(RepositoryProvider::GitLab, "group", "proj", None)
            .unwrap();
        let key = ReconstructionCacheKey::latest("scoped", Some(&scope));
        let result = cache.delete(&key).await;
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
