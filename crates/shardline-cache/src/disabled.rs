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
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::*;
    use crate::ReconstructionCacheLookup;

    #[tokio::test]
    async fn disabled_cache_never_retains_payloads() {
        let result = async {
            const CACHE: DisabledReconstructionCache = DisabledReconstructionCache::new();
            let cache: &dyn AsyncReconstructionCache = &CACHE;
            let github =
                RepositoryScope::new(RepositoryProvider::GitHub, "org", "repo", Some("main"))?;
            let gitlab = RepositoryScope::new(RepositoryProvider::GitLab, "group", "proj", None)?;
            cache.ready().await?;

            for key in [
                ReconstructionCacheKey::latest("test", None),
                ReconstructionCacheKey::latest("scoped-file", Some(&github)),
                ReconstructionCacheKey::version("file.bin", "hash123", None),
                ReconstructionCacheKey::latest("empty", None),
                ReconstructionCacheKey::latest("scoped", Some(&gitlab)),
            ] {
                assert_eq!(cache.get(&key).await?, None);
                assert!(!cache.delete(&key).await?);
                for payload in [b"payload".as_slice(), b""] {
                    cache.put(&key, payload).await?;
                    assert_eq!(cache.get(&key).await?, None);
                    assert!(!cache.delete(&key).await?);

                    let ReconstructionCacheLookup::Reserved(reservation) =
                        cache.get_or_reserve(&key).await?
                    else {
                        return Err("disabled cache returned a cached payload".into());
                    };
                    cache.put_reserved(&key, payload, &reservation).await?;
                    assert_eq!(cache.get(&key).await?, None);
                    assert!(!cache.touch_loading(&key).await?);
                    assert!(!cache.touch_reservation(&key, &reservation).await?);
                    assert!(!cache.delete_reserved(&key, &reservation).await?);
                    cache.release_loading(&key);
                    cache.release_reservation(&key, &reservation);
                }
            }
            Ok::<(), Box<dyn std::error::Error>>(())
        }
        .await;
        assert!(result.is_ok(), "{result:?}");
    }
}
