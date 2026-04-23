use std::{
    fmt::{Debug, Formatter, Result as FmtResult},
    future::Future,
    num::{NonZeroU64, NonZeroUsize},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::{from_slice, to_vec};
use shardline_cache::{
    AsyncReconstructionCache, DisabledReconstructionCache, MemoryReconstructionCache,
    ReconstructionCacheKey, RedisReconstructionCache,
};
use shardline_protocol::RepositoryScope;

use crate::{
    FileReconstructionResponse, LocalBackend, ServerConfig, ServerConfigError, ServerError,
};

type SharedReconstructionCache = Arc<dyn AsyncReconstructionCache>;
const MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES: u64 = 67_108_864;

/// Benchmarks one cold reconstruction load followed by one hot cache hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconstructionCacheBenchReport {
    /// Cold reconstruction latency, including the backend load and cache insert.
    pub cold_load_micros: u64,
    /// Hot reconstruction latency served from cache.
    pub hot_load_micros: u64,
    /// Serialized reconstruction-response payload size used by the cache adapter.
    pub response_bytes: u64,
    /// Whether the second lookup avoided the backend loader.
    pub cache_hit: bool,
}

/// Runtime reconstruction-cache service.
#[derive(Clone)]
pub(crate) struct ReconstructionCacheService {
    adapter_name: &'static str,
    adapter: SharedReconstructionCache,
}

impl Debug for ReconstructionCacheService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("ReconstructionCacheService")
            .field("adapter_name", &self.adapter_name)
            .finish()
    }
}

impl ReconstructionCacheService {
    pub(crate) fn disabled() -> Self {
        Self {
            adapter_name: ReconstructionCacheAdapter::Disabled.as_str(),
            adapter: Arc::new(DisabledReconstructionCache::new()),
        }
    }

    /// Builds the configured reconstruction-cache service.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the configured adapter cannot initialize.
    pub(crate) fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
        match config.reconstruction_cache_adapter() {
            ReconstructionCacheAdapter::Disabled => Ok(Self::disabled()),
            ReconstructionCacheAdapter::Memory => Ok(Self {
                adapter_name: ReconstructionCacheAdapter::Memory.as_str(),
                adapter: Arc::new(MemoryReconstructionCache::new(
                    config.reconstruction_cache_ttl_seconds(),
                    config.reconstruction_cache_memory_max_entries(),
                )),
            }),
            ReconstructionCacheAdapter::Redis => {
                let redis_url = config
                    .reconstruction_cache_redis_url()
                    .ok_or(ServerError::MissingReconstructionCacheRedisUrl)?;
                let adapter = RedisReconstructionCache::new(
                    redis_url,
                    config.reconstruction_cache_ttl_seconds(),
                )?;
                Ok(Self {
                    adapter_name: ReconstructionCacheAdapter::Redis.as_str(),
                    adapter: Arc::new(adapter),
                })
            }
        }
    }

    fn for_tests(adapter_name: &'static str, adapter: SharedReconstructionCache) -> Self {
        Self {
            adapter_name,
            adapter,
        }
    }

    pub(crate) const fn backend_name(&self) -> &'static str {
        self.adapter_name
    }

    pub(crate) async fn ready(&self) -> Result<(), ServerError> {
        self.adapter.ready().await.map_err(ServerError::from)
    }

    pub(crate) async fn get_or_load<Load, LoadFuture>(
        &self,
        key: &ReconstructionCacheKey,
        load: Load,
    ) -> Result<FileReconstructionResponse, ServerError>
    where
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<FileReconstructionResponse, ServerError>>,
    {
        let cached = self.adapter.get(key).await;
        if let Ok(Some(payload)) = cached
            && payload_within_bound(&payload)
        {
            let parsed = from_slice::<FileReconstructionResponse>(&payload);
            if let Ok(response) = parsed {
                return Ok(response);
            }
        }

        let response = load().await?;
        let payload = to_vec(&response)?;
        if payload_within_bound(&payload) {
            let _ignored = self.adapter.put(key, &payload).await;
        }
        Ok(response)
    }

    pub(crate) fn version_key(
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> ReconstructionCacheKey {
        ReconstructionCacheKey::version(file_id, content_hash, repository_scope)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconstructionCacheAdapter {
    Disabled,
    Memory,
    Redis,
}

impl ReconstructionCacheAdapter {
    pub(crate) fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "memory" => Ok(Self::Memory),
            "redis" => Ok(Self::Redis),
            _ => Err(ServerConfigError::InvalidReconstructionCacheAdapter),
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Memory => "memory",
            Self::Redis => "redis",
        }
    }
}

pub(crate) const DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(30) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};

pub(crate) const DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES: NonZeroUsize =
    match NonZeroUsize::new(4096) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

fn payload_within_bound(payload: &[u8]) -> bool {
    let observed_bytes = u64::try_from(payload.len()).unwrap_or(u64::MAX);
    observed_bytes <= MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES
}

pub(crate) async fn benchmark_memory_reconstruction_cache_with_loader<Load, LoadFuture>(
    file_id: &str,
    content_hash: &str,
    repository_scope: Option<&RepositoryScope>,
    load: Load,
) -> Result<ReconstructionCacheBenchReport, ServerError>
where
    Load: Fn() -> LoadFuture,
    LoadFuture: Future<Output = Result<FileReconstructionResponse, ServerError>>,
{
    let ttl_seconds = NonZeroU64::new(60).map_or(NonZeroU64::MIN, |value| value);
    let max_entries = NonZeroUsize::new(8).map_or(NonZeroUsize::MIN, |value| value);
    let adapter: SharedReconstructionCache =
        Arc::new(MemoryReconstructionCache::new(ttl_seconds, max_entries));
    let cache = ReconstructionCacheService::for_tests("memory-bench", adapter);
    let key = ReconstructionCacheService::version_key(file_id, content_hash, repository_scope);
    let loader_calls = AtomicUsize::new(0);

    let cold_started = Instant::now();
    let cold = cache
        .get_or_load(&key, || {
            loader_calls.fetch_add(1, Ordering::SeqCst);
            load()
        })
        .await?;
    let cold_load_micros = duration_micros(cold_started.elapsed())?;

    let hot_started = Instant::now();
    let hot = cache
        .get_or_load(&key, || {
            loader_calls.fetch_add(1, Ordering::SeqCst);
            load()
        })
        .await?;
    let hot_load_micros = duration_micros(hot_started.elapsed())?;

    debug_assert_eq!(cold, hot);
    let response_bytes = u64::try_from(to_vec(&hot)?.len())?;

    Ok(ReconstructionCacheBenchReport {
        cold_load_micros,
        hot_load_micros,
        response_bytes,
        cache_hit: loader_calls.load(Ordering::SeqCst) == 1,
    })
}

/// Measures cold and hot reconstruction-cache behavior over the local backend path.
///
/// # Errors
///
/// Returns [`ServerError`] when reconstruction loading or cache serialization fails.
pub async fn benchmark_memory_reconstruction_cache(
    backend: &LocalBackend,
    file_id: &str,
    content_hash: &str,
    repository_scope: Option<&RepositoryScope>,
) -> Result<ReconstructionCacheBenchReport, ServerError> {
    benchmark_memory_reconstruction_cache_with_loader(
        file_id,
        content_hash,
        repository_scope,
        || async move {
            backend
                .reconstruction(file_id, Some(content_hash), None, repository_scope)
                .await
        },
    )
    .await
}

fn duration_micros(duration: Duration) -> Result<u64, ServerError> {
    u64::try_from(duration.as_micros()).map_err(ServerError::from)
}

#[cfg(test)]
mod tests {
    use std::{
        num::{NonZeroU64, NonZeroUsize},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use shardline_cache::{
        AsyncReconstructionCache, MemoryReconstructionCache, ReconstructionCacheError,
        ReconstructionCacheFuture, ReconstructionCacheKey,
    };
    use tokio::sync::Mutex;

    use super::{
        MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES, ReconstructionCacheService,
        SharedReconstructionCache,
    };
    use crate::{
        FileReconstructionResponse, ReconstructionChunkRange, ReconstructionFetchInfo,
        ReconstructionTerm, ReconstructionUrlRange,
    };

    #[derive(Debug)]
    struct BrokenCache;

    #[derive(Debug)]
    struct StaticCache {
        payload: Option<Vec<u8>>,
        put_calls: Arc<AtomicUsize>,
    }

    impl AsyncReconstructionCache for BrokenCache {
        fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
            Box::pin(async { Err(ReconstructionCacheError::Operation) })
        }

        fn get<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
        ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
            Box::pin(async { Err(ReconstructionCacheError::Operation) })
        }

        fn put<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
            _payload: &'operation [u8],
        ) -> ReconstructionCacheFuture<'operation, ()> {
            Box::pin(async { Err(ReconstructionCacheError::Operation) })
        }

        fn delete<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
        ) -> ReconstructionCacheFuture<'operation, bool> {
            Box::pin(async { Err(ReconstructionCacheError::Operation) })
        }
    }

    impl AsyncReconstructionCache for StaticCache {
        fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
            Box::pin(async { Ok(()) })
        }

        fn get<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
        ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
            let payload = self.payload.clone();
            Box::pin(async move { Ok(payload) })
        }

        fn put<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
            _payload: &'operation [u8],
        ) -> ReconstructionCacheFuture<'operation, ()> {
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }

        fn delete<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
        ) -> ReconstructionCacheFuture<'operation, bool> {
            Box::pin(async { Ok(false) })
        }
    }

    #[derive(Debug)]
    struct CaptureCache {
        put_calls: Arc<AtomicUsize>,
        stored_payloads: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl AsyncReconstructionCache for CaptureCache {
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
            payload: &'operation [u8],
        ) -> ReconstructionCacheFuture<'operation, ()> {
            let payload = payload.to_vec();
            let stored_payloads = Arc::clone(&self.stored_payloads);
            self.put_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                stored_payloads.lock().await.push(payload);
                Ok(())
            })
        }

        fn delete<'operation>(
            &'operation self,
            _key: &'operation ReconstructionCacheKey,
        ) -> ReconstructionCacheFuture<'operation, bool> {
            Box::pin(async { Ok(false) })
        }
    }

    #[tokio::test]
    async fn cache_service_uses_cached_payload_after_first_load() {
        let ttl_seconds = NonZeroU64::new(60).map_or(NonZeroU64::MIN, |value| value);
        let adapter: SharedReconstructionCache = Arc::new(MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::MIN,
        ));
        let cache = ReconstructionCacheService::for_tests("memory", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let loader_calls = AtomicUsize::new(0);

        let first = cache
            .get_or_load(&key, || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("chunk-1")) }
            })
            .await;
        let second = cache
            .get_or_load(&key, || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("chunk-2")) }
            })
            .await;

        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            second
                .ok()
                .and_then(|response| response.terms.first().map(|term| term.hash.clone())),
            Some("chunk-1".to_owned())
        );
    }

    #[tokio::test]
    async fn cache_service_falls_back_to_loader_when_cache_adapter_errors() {
        let adapter: SharedReconstructionCache = Arc::new(BrokenCache);
        let cache = ReconstructionCacheService::for_tests("broken", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);

        let response = cache
            .get_or_load(&key, || async { Ok(sample_response("chunk-1")) })
            .await;

        assert!(response.is_ok());
        assert_eq!(
            response
                .ok()
                .and_then(|value| value.terms.first().map(|term| term.unpacked_length)),
            Some(4)
        );
    }

    #[tokio::test]
    async fn cache_service_falls_back_to_loader_when_cached_payload_exceeds_bound() {
        let put_calls = Arc::new(AtomicUsize::new(0));
        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: Some(vec![
                b'{';
                MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES as usize + 1
            ]),
            put_calls: Arc::clone(&put_calls),
        });
        let cache = ReconstructionCacheService::for_tests("static", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let loader_calls = AtomicUsize::new(0);

        let response = cache
            .get_or_load(&key, || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("chunk-1")) }
            })
            .await;

        assert!(response.is_ok());
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(put_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cache_service_skips_put_when_loaded_payload_exceeds_bound() {
        let put_calls = Arc::new(AtomicUsize::new(0));
        let stored_payloads = Arc::new(Mutex::new(Vec::new()));
        let adapter: SharedReconstructionCache = Arc::new(CaptureCache {
            put_calls: Arc::clone(&put_calls),
            stored_payloads: Arc::clone(&stored_payloads),
        });
        let cache = ReconstructionCacheService::for_tests("capture", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let oversized_hash =
            "h".repeat(usize::try_from(MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES).unwrap_or(0));

        let response = cache
            .get_or_load(&key, || async { Ok(sample_response(&oversized_hash)) })
            .await;

        assert!(response.is_ok());
        assert_eq!(put_calls.load(Ordering::SeqCst), 0);
        assert_eq!(stored_payloads.lock().await.len(), 0);
    }

    fn sample_response(hash: &str) -> FileReconstructionResponse {
        FileReconstructionResponse {
            offset_into_first_range: 0,
            terms: vec![ReconstructionTerm {
                hash: hash.to_owned(),
                unpacked_length: 4,
                range: ReconstructionChunkRange { start: 0, end: 1 },
            }],
            fetch_info: [(
                hash.to_owned(),
                vec![ReconstructionFetchInfo {
                    range: ReconstructionChunkRange { start: 0, end: 1 },
                    url: format!("https://cas.example.test/{hash}"),
                    url_range: ReconstructionUrlRange { start: 0, end: 3 },
                }],
            )]
            .into_iter()
            .collect(),
        }
    }
}
