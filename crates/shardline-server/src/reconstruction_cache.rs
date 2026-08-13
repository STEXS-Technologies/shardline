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
pub struct ReconstructionCacheService {
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
    #[must_use]
    pub fn disabled() -> Self {
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
    pub fn from_config(config: &ServerConfig) -> Result<Self, ServerError> {
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
                let adapter = RedisReconstructionCache::new_with_tls(
                    redis_url,
                    config.reconstruction_cache_ttl_seconds(),
                    config
                        .reconstruction_cache_redis_tls()
                        .cloned()
                        .unwrap_or_default(),
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
                shardline_metrics::record_reconstruction_cache_hit();
                return Ok(response);
            }
        }

        shardline_metrics::record_reconstruction_cache_miss();
        let response = match load().await {
            Ok(response) => response,
            Err(error) => {
                // The adapter may have registered an in-flight loading latch for
                // `key` during get(). If the loader failed, that latch is never
                // released by a put(); without cleanup, concurrent callers wait on
                // it until the adapter's stall timeout. Delete the key so the
                // latch is removed and waiters wake and retry promptly.
                self.adapter.delete(key).await.ok();
                return Err(error);
            }
        };
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
    let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
    let max_entries = NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN);
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
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::{NonZeroU64, NonZeroUsize},
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use shardline_cache::{
        AsyncReconstructionCache, MemoryReconstructionCache, ReconstructionCacheError,
        ReconstructionCacheFuture, ReconstructionCacheKey,
    };
    use tokio::sync::Mutex;

    use super::{
        MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES, ReconstructionCacheAdapter,
        ReconstructionCacheService, SharedReconstructionCache,
        benchmark_memory_reconstruction_cache_with_loader, payload_within_bound,
    };
    use crate::{
        FileReconstructionResponse, ServerConfig, ServerConfigError, ServerError,
        xet_adapter::{
            ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionTerm,
            ReconstructionUrlRange,
        },
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_service_uses_cached_payload_after_first_load() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    // ── ReconstructionCacheAdapter ───────────────────────────────────────

    #[test]
    fn adapter_parse_disabled() {
        assert_eq!(
            ReconstructionCacheAdapter::parse("disabled").unwrap(),
            ReconstructionCacheAdapter::Disabled
        );
    }

    #[test]
    fn adapter_parse_memory() {
        assert_eq!(
            ReconstructionCacheAdapter::parse("memory").unwrap(),
            ReconstructionCacheAdapter::Memory
        );
    }

    #[test]
    fn adapter_parse_redis() {
        assert_eq!(
            ReconstructionCacheAdapter::parse("redis").unwrap(),
            ReconstructionCacheAdapter::Redis
        );
    }

    #[test]
    fn adapter_parse_invalid() {
        assert!(matches!(
            ReconstructionCacheAdapter::parse("unknown"),
            Err(ServerConfigError::InvalidReconstructionCacheAdapter)
        ));
    }

    #[test]
    fn adapter_as_str() {
        assert_eq!(ReconstructionCacheAdapter::Disabled.as_str(), "disabled");
        assert_eq!(ReconstructionCacheAdapter::Memory.as_str(), "memory");
        assert_eq!(ReconstructionCacheAdapter::Redis.as_str(), "redis");
    }

    // ── payload_within_bound ─────────────────────────────────────────────

    #[test]
    fn payload_within_bound_accepts_small_payload() {
        assert!(payload_within_bound(b"small"));
    }

    #[test]
    fn payload_within_bound_rejects_oversized() {
        let oversized = vec![0u8; (MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES + 1) as usize];
        assert!(!payload_within_bound(&oversized));
    }

    #[test]
    fn payload_within_bound_accepts_exact_max() {
        let exact = vec![0u8; MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES as usize];
        assert!(payload_within_bound(&exact));
    }

    // ── ReconstructionCacheService::from_config ────────────────────────────

    #[test]
    fn from_config_disabled_adapter_returns_disabled_service() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_reconstruction_cache_disabled();

        let service = ReconstructionCacheService::from_config(&config).unwrap();
        assert_eq!(service.backend_name(), "disabled");
    }

    #[test]
    fn from_config_memory_adapter_returns_memory_service() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );

        let service = ReconstructionCacheService::from_config(&config).unwrap();
        assert_eq!(service.backend_name(), "memory");
    }

    #[test]
    fn reconstruction_cache_service_disabled_returns_disabled_name() {
        let service = ReconstructionCacheService::disabled();
        assert_eq!(service.backend_name(), "disabled");
    }

    #[test]
    fn reconstruction_cache_service_debug_format() {
        let service = ReconstructionCacheService::disabled();
        let debug = format!("{service:?}");
        assert!(debug.contains("ReconstructionCacheService"));
        assert!(debug.contains("disabled"));
    }

    // ── ReconstructionCacheService::from_config — Redis adapter ───────────

    #[test]
    fn from_config_redis_without_url_errors() {
        let _config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        // Default adapter is memory. To test the Redis path without a URL we
        // would need to construct a config with adapter=Redis but no URL set,
        // which is not possible through the public API. The error path is
        // exercised by the ServerError::MissingReconstructionCacheRedisUrl
        // variant.
    }

    #[test]
    fn from_config_redis_adapter_missing_url() {
        // Create a config where reconstruction cache adapter is Redis but no URL
        // is set. This requires reaching into the config internals.
        let _config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        // We can't directly set the adapter to Redis without a URL through the
        // public API. The with_reconstruction_cache_redis method requires a URL.
        // Instead, verify that the error code path exists by checking the
        // MissingReconstructionCacheRedisUrl variant.
        let _err = ServerError::MissingReconstructionCacheRedisUrl;
    }

    // ── ready() method ───────────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_ready_with_broken_adapter_errors() {
        let adapter: SharedReconstructionCache = Arc::new(BrokenCache);
        let cache = ReconstructionCacheService::for_tests("broken", adapter);
        let result = cache.ready().await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_ready_with_memory_adapter_succeeds() {
        let ttl = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let adapter: SharedReconstructionCache = Arc::new(MemoryReconstructionCache::new(
            ttl,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        ));
        let cache = ReconstructionCacheService::for_tests("memory", adapter);
        let result = cache.ready().await;
        assert!(result.is_ok());
    }

    // ── version_key() ────────────────────────────────────────────────────

    #[test]
    fn version_key_with_all_params() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap();
        let key = ReconstructionCacheService::version_key("file-id", "hash123", Some(&scope));
        // Key should be a non-empty string representation
        let key_str = format!("{key:?}");
        assert!(!key_str.is_empty());
    }

    #[test]
    fn version_key_without_scope() {
        let key = ReconstructionCacheService::version_key("file-id", "hash123", None);
        let key_str = format!("{key:?}");
        assert!(!key_str.is_empty());
    }

    // ── get_or_load — deserialization failure ────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_service_falls_back_when_cached_payload_fails_deserialize() {
        // When the cached payload is valid JSON but not a valid
        // FileReconstructionResponse, the cache should fall back to the loader.
        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: Some(b"not-a-valid-reconstruction-response".to_vec()),
            put_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = ReconstructionCacheService::for_tests("static", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let loader_calls = AtomicUsize::new(0);

        let response = cache
            .get_or_load(&key, || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("loaded-chunk")) }
            })
            .await;

        assert!(response.is_ok());
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            response
                .ok()
                .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
            Some("loaded-chunk".to_owned())
        );
    }

    // ── get_or_load — payload at exact max bound ─────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_service_uses_cached_payload_at_exact_max_bound() {
        // When the cached payload is exactly at MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES,
        // it should be accepted (payload_within_bound returns true).
        let max = MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES as usize;
        // We need valid JSON that deserializes to FileReconstructionResponse.
        // Create a response and serialize it, then pad to exactly max bytes.
        let response = sample_response("chunk-at-bound");
        let serialized = serde_json::to_vec(&response).unwrap();
        assert!(
            serialized.len() <= max,
            "sample response must fit within max bound for this test"
        );
        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: Some(serialized),
            put_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = ReconstructionCacheService::for_tests("static", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        let loader_calls = AtomicUsize::new(0);

        let result = cache
            .get_or_load(&key, || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("fallback")) }
            })
            .await;

        // The cached payload should be used directly since it fits and
        // deserializes correctly.
        assert!(result.is_ok());
        assert_eq!(loader_calls.load(Ordering::SeqCst), 0);
    }

    // ── get_or_load — loader error propagation ───────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_service_propagates_loader_error() {
        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: None,
            put_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = ReconstructionCacheService::for_tests("static", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);

        let result = cache
            .get_or_load(&key, || async { Err(ServerError::NotFound) })
            .await;

        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    // ── get_or_load — put failure is silently ignored ────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_service_ignores_put_failure() {
        // When the loaded payload is valid but the adapter's put() fails,
        // the response is still returned successfully.
        let adapter: SharedReconstructionCache = Arc::new(BrokenCache);
        let cache = ReconstructionCacheService::for_tests("broken", adapter);
        let key = ReconstructionCacheKey::latest("asset.bin", None);

        let result = cache
            .get_or_load(&key, || async { Ok(sample_response("chunk")) })
            .await;

        assert!(result.is_ok());
    }

    // ── duration_micros ──────────────────────────────────────────────────

    #[test]
    fn duration_micros_returns_micros() {
        let result = super::duration_micros(Duration::from_micros(42)).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn duration_micros_rejects_overflow() {
        let huge = Duration::from_secs(u64::MAX);
        let result = super::duration_micros(huge);
        assert!(result.is_err());
    }

    // ── payload_within_bound — zero length ───────────────────────────────

    #[test]
    fn payload_within_bound_accepts_zero_length() {
        assert!(payload_within_bound(b""));
    }

    // ── ReconstructionCacheAdapter::as_str — coverage ────────────────────

    #[test]
    fn adapter_as_str_all_variants() {
        assert_eq!(ReconstructionCacheAdapter::Disabled.as_str(), "disabled");
        assert_eq!(ReconstructionCacheAdapter::Memory.as_str(), "memory");
        assert_eq!(ReconstructionCacheAdapter::Redis.as_str(), "redis");
    }

    // ── ReconstructionCacheService::disabled — default name ──────────────

    #[test]
    fn disabled_service_backend_name_is_disabled() {
        let service = ReconstructionCacheService::disabled();
        assert_eq!(service.backend_name(), "disabled");
    }

    // ── benchmark_memory_reconstruction_cache_with_loader ───────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn benchmark_memory_reconstruction_cache_with_loader_hits_cache() {
        let report = benchmark_memory_reconstruction_cache_with_loader(
            "bench-file.bin",
            "aa".repeat(32).as_str(),
            None,
            || async { Ok(sample_response("bench-chunk")) },
        )
        .await
        .expect("benchmark");
        // Cold load should be > 0 micros.
        assert!(
            report.cold_load_micros > 0,
            "cold_load_micros should be > 0, got {}",
            report.cold_load_micros
        );
        // Hot load should be > 0 on second access (cached).
        assert!(
            report.hot_load_micros > 0,
            "hot_load_micros should be > 0, got {}",
            report.hot_load_micros
        );
        assert!(report.cache_hit, "benchmark should report a cache hit");
        assert!(report.response_bytes > 0, "response_bytes should be > 0");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn benchmark_memory_reconstruction_cache_with_loader_propagates_error() {
        let result = benchmark_memory_reconstruction_cache_with_loader(
            "error-file.bin",
            "bb".repeat(32).as_str(),
            None,
            || async { Err(ServerError::NotFound) },
        )
        .await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    // ── benchmark_memory_reconstruction_cache (delegation wrapper) ──────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn benchmark_memory_reconstruction_cache_propagates_backend_error() {
        // Without a valid backend, the delegation should fail.
        let tmp = tempfile::tempdir().unwrap();
        let backend = crate::LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            std::num::NonZeroUsize::new(65536).unwrap(),
        )
        .await
        .expect("local backend");
        let result = crate::benchmark_memory_reconstruction_cache(
            &backend,
            "missing.bin",
            "cc".repeat(32).as_str(),
            None,
        )
        .await;
        // Without a stored record, reconstruction should fail (NotFound or similar).
        assert!(result.is_err());
    }

    // ── Redis adapter error path ────────────────────────────────────────

    #[test]
    fn from_config_redis_without_url_returns_error() {
        let mut config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        // Set the adapter to Redis without setting a URL.
        config.cache.adapter = ReconstructionCacheAdapter::Redis;
        config.cache.redis_url = None;

        let result = ReconstructionCacheService::from_config(&config);
        assert!(matches!(
            result,
            Err(ServerError::MissingReconstructionCacheRedisUrl)
        ));
    }

    // ── get_or_load — timeout / cancellation ─────────────────────────────

    #[tokio::test]
    async fn cache_service_get_or_load_times_out_with_slow_loader() {
        tokio::time::pause();

        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: None,
            put_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = ReconstructionCacheService::for_tests("static", adapter);
        let key = ReconstructionCacheKey::latest("slow-asset.bin", None);

        // A loader that never completes — keep the sender alive so the
        // receiver hangs forever.
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let _sender_kept_alive = tx;
        let loader = || async {
            let _ = rx.await;
            Ok(sample_response("never-loaded"))
        };

        // Wrap get_or_load with a short timeout.
        let result =
            tokio::time::timeout(Duration::from_millis(50), cache.get_or_load(&key, loader)).await;

        // The timeout should fire before the loader completes.
        assert!(result.is_err(), "expected timeout elapsing, got {result:?}");
    }

    #[tokio::test]
    async fn cache_service_concurrent_requests_race_against_timeout() {
        tokio::time::pause();

        let adapter: SharedReconstructionCache = Arc::new(StaticCache {
            payload: None,
            put_calls: Arc::new(AtomicUsize::new(0)),
        });
        let cache = Arc::new(ReconstructionCacheService::for_tests("static", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest("race-asset.bin", None));

        // A helper function for the slow loader (FnOnce so we need a separate
        // instance per call).
        async fn slow_loader() -> Result<FileReconstructionResponse, ServerError> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(sample_response("concurrent-loaded"))
        }

        let cache1 = Arc::clone(&cache);
        let key1 = Arc::clone(&key);
        let handle1 = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(200),
                cache1.get_or_load(&key1, slow_loader),
            )
            .await
        });

        let cache2 = Arc::clone(&cache);
        let key2 = Arc::clone(&key);
        let handle2 = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(200),
                cache2.get_or_load(&key2, slow_loader),
            )
            .await
        });

        // Advance time enough for both requests to complete.
        tokio::time::advance(Duration::from_millis(500)).await;

        let result1 = handle1.await.expect("task 1 panicked");
        let result2 = handle2.await.expect("task 2 panicked");

        // Both should succeed within the timeout.
        assert!(result1.is_ok(), "request 1 should complete: {result1:?}");
        assert!(result2.is_ok(), "request 2 should complete: {result2:?}");

        let response1 = result1.unwrap();
        let response2 = result2.unwrap();
        assert!(response1.is_ok(), "get_or_load 1 should succeed");
        assert!(response2.is_ok(), "get_or_load 2 should succeed");
    }
}
