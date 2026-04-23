use std::{fmt, num::NonZeroU64};

use redis::AsyncCommands;

use crate::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheFuture,
    ReconstructionCacheKey,
};

const RECONSTRUCTION_CACHE_PREFIX: &str = "shardline:reconstruction:v1";

/// Redis-backed reconstruction cache adapter.
#[derive(Clone)]
pub struct RedisReconstructionCache {
    client: redis::Client,
    ttl_seconds: NonZeroU64,
}

impl fmt::Debug for RedisReconstructionCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisReconstructionCache")
            .field("client", &"***")
            .field("ttl_seconds", &self.ttl_seconds)
            .finish()
    }
}

impl RedisReconstructionCache {
    /// Creates a Redis-backed reconstruction cache adapter.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the URL is empty or invalid.
    pub fn new(redis_url: &str, ttl_seconds: NonZeroU64) -> Result<Self, ReconstructionCacheError> {
        if redis_url.trim().is_empty() {
            return Err(ReconstructionCacheError::EmptyRedisUrl);
        }

        Ok(Self {
            client: redis::Client::open(redis_url)?,
            ttl_seconds,
        })
    }

    fn redis_key(key: &ReconstructionCacheKey) -> String {
        let scope = key.repository_scope().map_or_else(
            || "global".to_owned(),
            |scope| {
                let revision = scope
                    .revision()
                    .map_or_else(|| "head".to_owned(), encode_component);
                format!(
                    "{}:{}:{}:{}",
                    scope.provider(),
                    encode_component(scope.owner()),
                    encode_component(scope.repo()),
                    revision
                )
            },
        );
        let content = key
            .content_hash()
            .map_or_else(|| "latest".to_owned(), encode_component);

        format!(
            "{RECONSTRUCTION_CACHE_PREFIX}:{scope}:{content}:{}",
            encode_component(key.file_id())
        )
    }
}

impl AsyncReconstructionCache for RedisReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async move {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            let _pong: String = redis::cmd("PING").query_async(&mut connection).await?;
            Ok(())
        })
    }

    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            let redis_key = Self::redis_key(key);
            let value: Option<Vec<u8>> = connection.get(redis_key).await?;
            Ok(value)
        })
    }

    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async move {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            let redis_key = Self::redis_key(key);
            let ttl_seconds = self.ttl_seconds.get();
            let _: () = connection
                .set_ex(redis_key, payload.to_vec(), ttl_seconds)
                .await?;
            Ok(())
        })
    }

    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let mut connection = self.client.get_multiplexed_async_connection().await?;
            let redis_key = Self::redis_key(key);
            let deleted: usize = connection.del(redis_key).await?;
            Ok(deleted > 0)
        })
    }
}

fn encode_component(value: &str) -> String {
    hex::encode(value.as_bytes())
}

#[cfg(test)]
mod tests {
    use std::{env::var as env_var, error::Error as StdError, num::NonZeroU64};

    use redis::AsyncCommands;

    use super::{RECONSTRUCTION_CACHE_PREFIX, RedisReconstructionCache};
    use crate::{AsyncReconstructionCache, ReconstructionCacheKey};

    #[test]
    fn redis_cache_debug_redacts_connection_url() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let cache = RedisReconstructionCache::new(
            "redis://:cache-secret@cache.example.test:6379/0",
            ttl_seconds,
        );
        assert!(cache.is_ok());
        let Ok(cache) = cache else {
            return;
        };

        let rendered = format!("{cache:?}");

        assert!(!rendered.contains("cache-secret"));
        assert!(rendered.contains("***"));
    }

    #[tokio::test]
    async fn redis_cache_roundtrips_payload_when_live_url_is_available() {
        let Some(redis_url) = env_var("STEXS_REDIS_CACHE_TEST_URL").ok() else {
            return;
        };

        let ttl_seconds = NonZeroU64::new(60).map_or(NonZeroU64::MIN, |value| value);
        let cache = RedisReconstructionCache::new(&redis_url, ttl_seconds);
        assert!(cache.is_ok());
        let Ok(cache) = cache else {
            return;
        };
        let initial_flush = flush_matching_keys(&redis_url).await;
        assert!(initial_flush.is_ok());
        let key = ReconstructionCacheKey::latest("asset.bin", None);

        let put = cache.put(&key, b"payload").await;
        assert!(put.is_ok());
        let value = cache.get(&key).await;
        assert!(value.is_ok());
        assert_eq!(value.ok().flatten(), Some(b"payload".to_vec()));

        let final_flush = flush_matching_keys(&redis_url).await;
        assert!(final_flush.is_ok());
    }

    async fn flush_matching_keys(redis_url: &str) -> Result<(), Box<dyn StdError>> {
        let client = redis::Client::open(redis_url)?;
        let mut connection = client.get_multiplexed_async_connection().await?;
        let pattern = format!("{RECONSTRUCTION_CACHE_PREFIX}:*");
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut connection)
            .await?;
        if !keys.is_empty() {
            let _: usize = connection.del(keys).await?;
        }
        Ok(())
    }
}
