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
    AsyncReconstructionCache, MemoryReconstructionCache, ReconstructionCacheKey,
    ReconstructionCacheLookup, ReconstructionCacheReservation, RedisReconstructionCache,
};
use shardline_protocol::RepositoryScope;

use crate::{
    FileReconstructionResponse, LocalBackend, ServerConfig, ServerConfigError, ServerError,
};

type SharedReconstructionCache = Arc<dyn AsyncReconstructionCache>;
const MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES: u64 = 67_108_864;

/// Interval at which the service layer refreshes the loading latch's aliveness
/// stamp while its loader is running (F-90). Kept well below the memory
/// adapter's aliveness grace so a waiter at the orphan bound always observes a
/// fresh stamp for a live loader.
const LOADER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

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
    adapter: Option<SharedReconstructionCache>,
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
            adapter: None,
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
                adapter: Some(Arc::new(MemoryReconstructionCache::new(
                    config.reconstruction_cache_ttl_seconds(),
                    config.reconstruction_cache_memory_max_entries(),
                ))),
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
                    adapter: Some(Arc::new(adapter)),
                })
            }
        }
    }

    fn for_tests(adapter_name: &'static str, adapter: SharedReconstructionCache) -> Self {
        Self {
            adapter_name,
            adapter: Some(adapter),
        }
    }

    pub(crate) const fn backend_name(&self) -> &'static str {
        self.adapter_name
    }

    pub(crate) async fn ready(&self) -> Result<(), ServerError> {
        match &self.adapter {
            Some(adapter) => adapter.ready().await.map_err(ServerError::from),
            None => Ok(()),
        }
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
        let Some(adapter) = &self.adapter else {
            shardline_metrics::record_reconstruction_cache_miss();
            return load().await;
        };
        let reservation = match adapter.get_or_reserve(key).await {
            Ok(ReconstructionCacheLookup::Hit(payload)) => {
                if let Some(response) = decode_cache_hit(&payload) {
                    return Ok(response);
                }
                // A cache value is never authoritative. Remove malformed or
                // oversized data, and compete for a fenced reconstruction
                // reservation instead of allowing every replica to rebuild it.
                adapter.delete(key).await.ok();
                match adapter.get_or_reserve(key).await {
                    Ok(ReconstructionCacheLookup::Hit(refetched)) => {
                        if let Some(response) = decode_cache_hit(&refetched) {
                            return Ok(response);
                        }
                        None
                    }
                    Ok(ReconstructionCacheLookup::Reserved(reservation)) => Some(reservation),
                    Err(_) => None,
                }
            }
            Ok(ReconstructionCacheLookup::Reserved(reservation)) => Some(reservation),
            Err(_) => None,
        };

        shardline_metrics::record_reconstruction_cache_miss();
        // Release this reservation synchronously on cancellation so waiters
        // can retry without waiting for the orphan timeout (F-112).
        let _latch_release = reservation
            .as_ref()
            .map(|reservation| LoadingLatchReleaseGuard {
                adapter,
                key,
                reservation,
            });
        // Refresh the fenced reservation while loading so a slow, live loader
        // is not mistaken for an orphan and duplicated by a waiter (F-90).
        let response =
            match Self::run_load_with_heartbeat(adapter, key, reservation.as_ref(), load).await {
                Ok(response) => response,
                Err(error) => {
                    // Wake waiters after failure, without deleting a newer owner's reservation.
                    if let Some(reservation) = reservation.as_ref() {
                        adapter.delete_reserved(key, reservation).await.ok();
                    }
                    return Err(error);
                }
            };
        let payload = to_vec(&response)?;
        if payload_within_bound(&payload) {
            if let Some(reservation) = reservation.as_ref() {
                let _ignored = adapter.put_reserved(key, &payload, reservation).await;
            } else {
                let _ignored = adapter.put(key, &payload).await;
            }
        } else {
            // Skipping an oversized write must still release our loading
            // reservation so waiters can retry promptly (F-91).
            if let Some(reservation) = reservation.as_ref() {
                adapter.delete_reserved(key, reservation).await.ok();
            }
        }
        Ok(response)
    }

    /// Runs `load` while refreshing the loading latch's aliveness stamp at
    /// intervals, mirroring the memory adapter's own loader heartbeat so the
    /// production service-layer path gets the same protection (F-90).
    async fn run_load_with_heartbeat<Load, LoadFuture>(
        adapter: &SharedReconstructionCache,
        key: &ReconstructionCacheKey,
        reservation: Option<&ReconstructionCacheReservation>,
        load: Load,
    ) -> Result<FileReconstructionResponse, ServerError>
    where
        Load: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<FileReconstructionResponse, ServerError>>,
    {
        let Some(reservation) = reservation else {
            return load().await;
        };
        let mut loader_future = std::pin::pin!(load());
        let mut heartbeat = tokio::time::interval(LOADER_HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                result = &mut loader_future => return result,
                _ = heartbeat.tick() => {
                    // Refresh the latch the adapter registered during get(), so
                    // a concurrent waiter at the orphan bound sees a fresh
                    // stamp instead of stealing the latch of a live loader.
                    let _ignored = adapter.touch_reservation(key, reservation).await;
                }
            }
        }
    }

    pub(crate) fn version_key(
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> ReconstructionCacheKey {
        ReconstructionCacheKey::version(file_id, content_hash, repository_scope)
    }
}

/// Releases a loader reservation on cancellation. Memory-cache waiters
/// can retry immediately; adapters without synchronous release use lease expiry.
/// The ownership fence prevents an old loader from releasing a newer reservation.
struct LoadingLatchReleaseGuard<'request> {
    adapter: &'request SharedReconstructionCache,
    key: &'request ReconstructionCacheKey,
    reservation: &'request ReconstructionCacheReservation,
}

impl Drop for LoadingLatchReleaseGuard<'_> {
    fn drop(&mut self) {
        self.adapter.release_reservation(self.key, self.reservation);
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

fn decode_cache_hit(payload: &[u8]) -> Option<FileReconstructionResponse> {
    if !payload_within_bound(payload) {
        return None;
    }
    let response = from_slice(payload).ok()?;
    shardline_metrics::record_reconstruction_cache_hit();
    Some(response)
}

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
    let counted_load = || {
        loader_calls.fetch_add(1, Ordering::SeqCst);
        load()
    };

    let cold_started = Instant::now();
    let cold = cache.get_or_load(&key, &counted_load).await?;
    let cold_load_micros = duration_micros(cold_started.elapsed())?;

    let hot_started = Instant::now();
    let hot = cache.get_or_load(&key, &counted_load).await?;
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
    async fn cache_service_ignores_corrupt_cached_payload_and_repairs_entry() {
        for payload in [
            b"not a reconstruction response".as_slice(),
            b"not-a-valid-reconstruction-response",
            b"{}",
        ] {
            let put_calls = Arc::new(AtomicUsize::new(0));
            let adapter: SharedReconstructionCache = Arc::new(StaticCache {
                payload: Some(payload.to_vec()),
                put_calls: Arc::clone(&put_calls),
            });
            let cache = ReconstructionCacheService::for_tests("static", adapter);
            let key = ReconstructionCacheKey::latest("asset.bin", None);
            let loader_calls = AtomicUsize::new(0);

            let response = cache
                .get_or_load(&key, || {
                    loader_calls.fetch_add(1, Ordering::SeqCst);
                    async { Ok(sample_response("chunk-repaired")) }
                })
                .await;

            assert!(response.is_ok());
            assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
            assert_eq!(put_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                response
                    .ok()
                    .and_then(|value| value.terms.first().map(|term| term.hash.clone())),
                Some("chunk-repaired".to_owned())
            );
        }
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
    fn adapter_parse_known_variants() {
        for (name, adapter) in [
            ("disabled", ReconstructionCacheAdapter::Disabled),
            ("memory", ReconstructionCacheAdapter::Memory),
            ("redis", ReconstructionCacheAdapter::Redis),
        ] {
            assert_eq!(ReconstructionCacheAdapter::parse(name).unwrap(), adapter);
            assert_eq!(adapter.as_str(), name);
        }
    }

    #[test]
    fn adapter_parse_invalid() {
        assert!(matches!(
            ReconstructionCacheAdapter::parse("unknown"),
            Err(ServerConfigError::InvalidReconstructionCacheAdapter)
        ));
    }

    #[test]
    fn payload_bound_includes_empty_small_and_exact_maximum() {
        assert!(payload_within_bound(b""));
        assert!(payload_within_bound(b"small"));
        let max = MAX_RECONSTRUCTION_CACHE_PAYLOAD_BYTES as usize;
        let oversized = vec![0u8; max + 1];
        assert!(!payload_within_bound(&oversized));
        assert!(payload_within_bound(&oversized[..max]));
    }

    // ── ReconstructionCacheService::from_config ────────────────────────────

    #[test]
    fn configured_memory_and_disabled_adapters_have_expected_names() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        for (config, expected) in [
            (config.clone(), "memory"),
            (config.with_reconstruction_cache_disabled(), "disabled"),
        ] {
            let service = ReconstructionCacheService::from_config(&config).unwrap();
            assert_eq!(service.backend_name(), expected);
        }
    }

    #[tokio::test]
    async fn disabled_cache_loads_each_request_and_propagates_errors() {
        let service = ReconstructionCacheService::disabled();
        assert_eq!(service.backend_name(), "disabled");
        service.ready().await.unwrap();
        let key = ReconstructionCacheKey::latest("asset.bin", None);
        for hash in ["first", "second"] {
            let response = service
                .get_or_load(&key, || async { Ok(sample_response(hash)) })
                .await
                .unwrap();
            assert_eq!(response.terms.first().unwrap().hash, hash);
        }
        assert!(matches!(
            service
                .get_or_load(&key, || async { Err(ServerError::NotFound) })
                .await,
            Err(ServerError::NotFound)
        ));
    }

    #[test]
    fn reconstruction_cache_service_debug_format() {
        let service = ReconstructionCacheService::disabled();
        let debug = format!("{service:?}");
        assert!(debug.contains("ReconstructionCacheService"));
        assert!(debug.contains("disabled"));
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
    fn version_keys_preserve_file_identity_and_repository_scope() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None).unwrap();
        let scoped = ReconstructionCacheService::version_key("file-id", "hash123", Some(&scope));
        let unscoped = ReconstructionCacheService::version_key("file-id", "hash123", None);
        for key in [&scoped, &unscoped] {
            assert_eq!(key.file_id(), "file-id");
            assert_eq!(key.content_hash(), Some("hash123"));
        }
        let actual_scope = scoped.repository_scope().unwrap();
        assert_eq!(actual_scope.owner(), "owner");
        assert_eq!(actual_scope.repo(), "repo");
        assert_eq!(
            actual_scope.clone().provider_kind(),
            RepositoryProvider::GitHub
        );
        assert_eq!(actual_scope.revision(), None);
        assert!(unscoped.repository_scope().is_none());
        assert_ne!(scoped, unscoped);
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
        let mut serialized = serde_json::to_vec(&response).unwrap();
        assert!(
            serialized.len() <= max,
            "sample response must fit within max bound for this test"
        );
        serialized.resize(max, b' ');
        assert_eq!(serialized.len(), max);
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
    fn duration_micros_preserves_precision_and_rejects_overflow() {
        for (duration, expected) in [
            (Duration::ZERO, 0),
            (Duration::from_nanos(999), 0),
            (Duration::from_micros(42), 42),
        ] {
            assert_eq!(super::duration_micros(duration).unwrap(), expected);
        }
        assert!(super::duration_micros(Duration::from_secs(u64::MAX)).is_err());
    }

    // ── benchmark_memory_reconstruction_cache_with_loader ───────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn benchmark_memory_reconstruction_cache_with_loader_hits_cache() {
        let loader_calls = AtomicUsize::new(0);
        let report = benchmark_memory_reconstruction_cache_with_loader(
            "bench-file.bin",
            "aa".repeat(32).as_str(),
            None,
            || {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(sample_response("bench-chunk")) }
            },
        )
        .await
        .expect("benchmark");
        assert_eq!(loader_calls.load(Ordering::SeqCst), 1);
        assert!(report.cache_hit, "benchmark should report a cache hit");
        assert_eq!(
            report.response_bytes,
            serde_json::to_vec(&sample_response("bench-chunk"))
                .unwrap()
                .len() as u64
        );
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

    // ── get_or_load — caller cancellation releases the loading latch (F-112) ─

    #[tokio::test(start_paused = true)]
    async fn cache_service_get_or_load_caller_drop_releases_loading_latch() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory);
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest(
            "service-caller-drop.bin",
            None,
        ));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // R1: a reconstruction through the service layer whose loader never
        // completes on its own — the caller stays in flight until we drop it.
        let (never_tx, never_rx) = tokio::sync::oneshot::channel::<()>();
        let _keep_sender_alive = never_tx;
        let cache_1 = Arc::clone(&cache);
        let key_1 = Arc::clone(&key);
        let loader_calls_1 = Arc::clone(&loader_calls);
        let task1 = tokio::spawn(async move {
            cache_1
                .get_or_load(&key_1, || {
                    loader_calls_1.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        let _ = never_rx.await;
                        Ok(sample_response("cancelled"))
                    })
                })
                .await
        });

        // Let R1 start its load (the adapter's get() has registered the latch).
        tokio::time::advance(Duration::from_millis(10)).await;
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "first reconstruction must start before cancellation"
        );

        // Drop the caller future mid-load (client disconnect / timeout).
        task1.abort();
        tokio::time::advance(Duration::from_millis(1)).await;
        let _ = task1.await;

        // R2: a next caller for the same key must NOT stall for the full
        // orphan bound — it runs its own load promptly. Advance past the 5s
        // probe bound so a leaked zombie latch surfaces as a timeout instead
        // of hanging the test.
        let cache_2 = Arc::clone(&cache);
        let key_2 = Arc::clone(&key);
        let loader_calls_2 = Arc::clone(&loader_calls);
        let retry = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_secs(5),
                cache_2.get_or_load(&key_2, || {
                    loader_calls_2.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(sample_response("retry")) })
                }),
            )
            .await
        });
        tokio::time::advance(Duration::from_secs(5)).await;
        let retried = retry.await.expect("retry task panicked");
        let response = retried.expect("next caller stalled on a zombie loading latch (F-112)");
        let response = response.expect("next caller returned an unexpected error");
        assert_eq!(
            response.terms.first().map(|term| term.hash.as_str()),
            Some("retry"),
            "next caller must load its own value after a cancelled caller"
        );
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            2,
            "the retry must run its own load (the cancelled load's latch is gone)"
        );
    }

    #[tokio::test]
    async fn cache_service_stale_cancellation_keeps_new_reservation_alive() {
        let memory = MemoryReconstructionCache::new(
            NonZeroU64::new(60).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory);
        let cache = Arc::new(ReconstructionCacheService::for_tests(
            "memory",
            Arc::clone(&adapter),
        ));
        let key = ReconstructionCacheKey::latest("stale-service-cancellation", None);
        let task_cache = Arc::clone(&cache);
        let task_key = key.clone();
        let (started_send, started_receive) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            task_cache
                .get_or_load(&task_key, || async move {
                    started_send.send(()).unwrap();
                    std::future::pending::<Result<FileReconstructionResponse, ServerError>>().await
                })
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), started_receive)
            .await
            .unwrap()
            .unwrap();
        // Invalidation releases the old caller's latch while its future remains alive.
        adapter.delete(&key).await.unwrap();
        let lookup = adapter.get_or_reserve(&key).await.unwrap();
        assert!(matches!(
            lookup,
            shardline_cache::ReconstructionCacheLookup::Reserved(_)
        ));
        let shardline_cache::ReconstructionCacheLookup::Reserved(current) = lookup else {
            return;
        };
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        // Actual service Drop cleanup must leave the newer generation intact.
        assert!(adapter.touch_reservation(&key, &current).await.unwrap());
        let response = sample_response("current");
        let payload = serde_json::to_vec(&response).unwrap();
        adapter
            .put_reserved(&key, &payload, &current)
            .await
            .unwrap();
        let observed = cache
            .get_or_load(&key, || async { Ok(sample_response("unexpected-loader")) })
            .await
            .unwrap();
        assert_eq!(
            observed.terms.first().map(|term| term.hash.as_str()),
            Some("current")
        );
    }

    // ── get_or_load — slow (>30s) loader keeps its latch (F-68) ─────────
    //
    // Reconstruction of large files from S3-backed CAS routinely exceeds the
    // memory adapter's old 30s stall interval. A concurrent service-layer
    // request must keep waiting for the in-flight loader instead of giving up
    // at 30s, reporting a miss, and starting a second full reconstruction.

    #[tokio::test(start_paused = true)]
    async fn cache_service_waits_for_slow_in_flight_loader_without_second_load() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory.clone());
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest("slow-service.bin", None));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // R1: the adapter's get_or_load registers the loading latch and runs a
        // 35s reconstruction (longer than the old 30s stall interval).
        let memory_1 = memory.clone();
        let key_1 = Arc::clone(&key);
        let loader_calls_1 = Arc::clone(&loader_calls);
        let task1 = tokio::spawn(async move {
            memory_1
                .get_or_load(&key_1, || {
                    loader_calls_1.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_secs(35)).await;
                        let payload = serde_json::to_vec(&sample_response("slow-chunk"))
                            .expect("serialize sample response");
                        Ok::<_, ReconstructionCacheError>(payload)
                    })
                })
                .await
        });

        // Give R1 time to register its latch.
        tokio::time::advance(Duration::from_millis(100)).await;

        // R2: the service layer's get() finds the latch and must wait for R1
        // instead of returning a miss at 30s and starting a second load.
        let cache_2 = Arc::clone(&cache);
        let key_2 = Arc::clone(&key);
        let loader_calls_2 = Arc::clone(&loader_calls);
        let task2 = tokio::spawn(async move {
            cache_2
                .get_or_load(&key_2, || {
                    loader_calls_2.fetch_add(1, Ordering::SeqCst);
                    // If this ever runs, the `loader_calls == 1` assertion
                    // below fails (a second reconstruction was started).
                    async { Ok(sample_response("should-not-run")) }
                })
                .await
        });

        // Advance past the 30s stall interval and through the loader's 35s
        // completion: the waiter re-enters instead of giving up.
        tokio::time::advance(Duration::from_secs(36)).await;

        let result1 = task1.await.expect("task 1 panicked");
        let result2 = task2.await.expect("task 2 panicked");
        assert!(result1.is_ok(), "loader should succeed");
        assert!(result2.is_ok(), "waiter should succeed");
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "concurrent reconstructions must be deduplicated"
        );
        assert_eq!(
            result2
                .ok()
                .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
            Some("slow-chunk".to_owned())
        );
    }

    // ── get_or_load — slow (>60s) loader keeps its latch past the total
    //    orphan bound (F-78) ──────────────────────────────────────────────
    //
    // F-68 moved the latch-steal boundary to a 60s total orphan bound, but a
    // reconstruction longer than 60s STILL had its latch stolen: the waiter's
    // deadline fired while the loader was alive, the latch was released, and
    // the service layer treated the resulting miss as a second full load()
    // (measured: loads total 2). The adapter's loader now refreshes an
    // aliveness stamp at intervals, and the waiter extends its wait at the
    // deadline instead of stealing.

    #[tokio::test(start_paused = true)]
    async fn cache_service_waits_for_slow_in_flight_loader_beyond_total_bound_without_second_load()
    {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory.clone());
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest("slow-service-61s.bin", None));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // R1: the adapter's get_or_load registers the loading latch and runs a
        // 61s reconstruction — past the 60s total orphan bound. The adapter
        // heartbeats the latch while the loader is in flight.
        let memory_1 = memory.clone();
        let key_1 = Arc::clone(&key);
        let loader_calls_1 = Arc::clone(&loader_calls);
        let task1 = tokio::spawn(async move {
            memory_1
                .get_or_load(&key_1, || {
                    loader_calls_1.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async {
                        tokio::time::sleep(Duration::from_secs(61)).await;
                        let payload = serde_json::to_vec(&sample_response("slow-chunk-61"))
                            .expect("serialize sample response");
                        Ok::<_, ReconstructionCacheError>(payload)
                    })
                })
                .await
        });

        // Give R1 time to register its latch and start its 61s sleep.
        tokio::time::advance(Duration::from_millis(100)).await;

        // R2: a concurrent service request finds the latch and must keep
        // waiting past the 60s orphan bound instead of reporting a miss and
        // starting a second reconstruction.
        let cache_2 = Arc::clone(&cache);
        let key_2 = Arc::clone(&key);
        let loader_calls_2 = Arc::clone(&loader_calls);
        let task2 = tokio::spawn(async move {
            cache_2
                .get_or_load(&key_2, || {
                    loader_calls_2.fetch_add(1, Ordering::SeqCst);
                    // If this ever runs, the `loader_calls == 1` assertion
                    // below fails (a second reconstruction was started).
                    async { Ok(sample_response("should-not-run")) }
                })
                .await
        });

        // A tiny advance so R2 registers its wait (deadline ≈ 60.1s).
        tokio::time::advance(Duration::from_millis(1)).await;

        // Cross R2's 60s deadline while R1's loader is still alive: the
        // loader's aliveness stamp stays fresh, so R2 must extend its wait
        // instead of stealing the latch.
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            !task2.is_finished(),
            "waiter must not give up at the orphan bound while the loader is alive"
        );

        // Advance through the loader's completion at 61s: the value is stored
        // and R2 observes it.
        tokio::time::advance(Duration::from_secs(2)).await;

        let result1 = task1.await.expect("task 1 panicked");
        let result2 = task2.await.expect("task 2 panicked");
        assert!(result1.is_ok(), "loader should succeed");
        assert!(result2.is_ok(), "waiter should succeed");
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "concurrent reconstructions must be deduplicated"
        );
        assert_eq!(
            result2
                .ok()
                .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
            Some("slow-chunk-61".to_owned())
        );
    }

    // ── get_or_load — slow (>60s) loader through the SERVICE layer keeps its
    //    latch (F-90) ──────────────────────────────────────────────────────
    //
    // The F-78 heartbeat was wired only into the memory adapter's get_or_load,
    // which the production service layer does not use: it runs
    // adapter.get() -> load().await -> put() and never refreshed the latch's
    // aliveness stamp. A >60s reconstruction therefore had its latch stolen at
    // the orphan bound and a second full reconstruction started (loads = 2).
    // The service layer now heartbeats the latch itself while its loader runs,
    // so BOTH requests going through ReconstructionCacheService::get_or_load
    // dedup on the single reconstruction.

    #[tokio::test(start_paused = true)]
    async fn cache_service_dedups_slow_loads_through_service_layer() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory);
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest(
            "slow-service-layer.bin",
            None,
        ));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // R1: a 61s reconstruction through the service layer (past the 60s
        // total orphan bound). The service layer heartbeats the latch while
        // the loader runs.
        let cache_1 = Arc::clone(&cache);
        let key_1 = Arc::clone(&key);
        let loader_calls_1 = Arc::clone(&loader_calls);
        let task1 = tokio::spawn(async move {
            cache_1
                .get_or_load(&key_1, || {
                    loader_calls_1.fetch_add(1, Ordering::SeqCst);
                    async {
                        tokio::time::sleep(Duration::from_secs(61)).await;
                        Ok(sample_response("slow-chunk-service"))
                    }
                })
                .await
        });

        // Give R1 time to register its latch and start its 61s sleep.
        tokio::time::advance(Duration::from_millis(100)).await;

        // R2: a concurrent service-layer request must keep waiting past the
        // 60s orphan bound instead of reporting a miss and starting a second
        // reconstruction.
        let cache_2 = Arc::clone(&cache);
        let key_2 = Arc::clone(&key);
        let loader_calls_2 = Arc::clone(&loader_calls);
        let task2 = tokio::spawn(async move {
            cache_2
                .get_or_load(&key_2, || {
                    loader_calls_2.fetch_add(1, Ordering::SeqCst);
                    // If this ever runs, the `loader_calls == 1` assertion
                    // below fails (a second reconstruction was started).
                    async { Ok(sample_response("should-not-run")) }
                })
                .await
        });

        // A tiny advance so R2 registers its wait (deadline ≈ 60.1s).
        tokio::time::advance(Duration::from_millis(1)).await;

        // Cross R2's 60s deadline while R1's loader is still alive: the
        // service-layer heartbeat keeps the latch's stamp fresh, so R2 must
        // extend its wait instead of stealing the latch.
        tokio::time::advance(Duration::from_secs(60)).await;
        assert!(
            !task2.is_finished(),
            "waiter must not give up at the orphan bound while the service-layer loader is alive"
        );

        // Advance through the loader's completion at 61s: the value is stored
        // and R2 observes it.
        tokio::time::advance(Duration::from_secs(2)).await;

        let result1 = task1.await.expect("task 1 panicked");
        let result2 = task2.await.expect("task 2 panicked");
        assert!(result1.is_ok(), "loader should succeed");
        assert!(result2.is_ok(), "waiter should succeed");
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "concurrent reconstructions must be deduplicated"
        );
        assert_eq!(
            result2
                .ok()
                .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
            Some("slow-chunk-service".to_owned())
        );
    }

    // ── get_or_load — cold-cache concurrent requests dedup on one load
    //    (F-91) ───────────────────────────────────────────────────────────
    //
    // get() previously returned Ok(None) on a cold miss WITHOUT registering a
    // loading latch, so N concurrent service-layer requests for an uncached
    // key each ran their own reconstruction (measured: loads = 3 for 3
    // requests). get() now registers a latch on the cold-miss path so
    // concurrent callers wait for the single in-flight load.

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cache_service_cold_miss_concurrent_requests_run_loader_once() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory);
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest("cold-dedup.bin", None));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // N concurrent cold requests released at once against an empty cache.
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(4));
        let mut handles = Vec::new();
        for _ in 0..3 {
            let cache = Arc::clone(&cache);
            let key = Arc::clone(&key);
            let loader_calls = Arc::clone(&loader_calls);
            let barrier = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(&key, || {
                        loader_calls.fetch_add(1, Ordering::SeqCst);
                        async {
                            tokio::time::sleep(Duration::from_millis(300)).await;
                            Ok(sample_response("cold-shared"))
                        }
                    })
                    .await
            }));
        }

        // Release all requests at once.
        barrier.wait().await;

        for handle in handles {
            let result = handle.await.expect("task panicked");
            assert!(result.is_ok(), "request should succeed");
            assert_eq!(
                result
                    .ok()
                    .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
                Some("cold-shared".to_owned()),
                "every concurrent request must observe the single load's value"
            );
        }

        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            1,
            "cold-cache concurrent requests must dedup on a single reconstruction"
        );
    }

    // ── get_or_load — a wedged-but-pollable loader cannot pin waiters beyond
    //    the bounded total (F-100) ─────────────────────────────────────────
    //
    // The F-78 deadline extension let a loader that keeps refreshing its
    // aliveness stamp extend every waiter's bound by 60s forever. The service
    // layer heartbeats the latch itself, so a never-completing loader stays
    // "alive" indefinitely — the extension cap bounds the total wait to
    // (1 + cap) × 60s, after which the waiter escapes, releases the latch, and
    // runs its own reconstruction.

    #[tokio::test(start_paused = true)]
    async fn cache_service_waiter_escapes_wedged_loader_and_loads_itself() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let memory = MemoryReconstructionCache::new(
            ttl_seconds,
            NonZeroUsize::new(8).unwrap_or(NonZeroUsize::MIN),
        );
        let adapter: SharedReconstructionCache = Arc::new(memory);
        let cache = Arc::new(ReconstructionCacheService::for_tests("memory", adapter));
        let key = Arc::new(ReconstructionCacheKey::latest("wedged-service.bin", None));
        let loader_calls = std::sync::Arc::new(AtomicUsize::new(0));

        // R1: a loader that never completes but stays pollable (yields via 1s
        // sleeps), so the service-layer heartbeat keeps its latch fresh
        // forever.
        let cache_1 = Arc::clone(&cache);
        let key_1 = Arc::clone(&key);
        let loader_calls_1 = Arc::clone(&loader_calls);
        let wedged_task = tokio::spawn(async move {
            cache_1
                .get_or_load(&key_1, || {
                    loader_calls_1.fetch_add(1, Ordering::SeqCst);
                    async {
                        loop {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        #[allow(unreachable_code)]
                        Ok(sample_response("never"))
                    }
                })
                .await
        });

        // Give R1 time to register its latch.
        tokio::time::advance(Duration::from_millis(100)).await;

        // R2: a concurrent service-layer request that must escape the wedged
        // latch at the bounded total and complete its own reconstruction.
        let cache_2 = Arc::clone(&cache);
        let key_2 = Arc::clone(&key);
        let loader_calls_2 = Arc::clone(&loader_calls);
        let waiter_task = tokio::spawn(async move {
            cache_2
                .get_or_load(&key_2, || {
                    loader_calls_2.fetch_add(1, Ordering::SeqCst);
                    async { Ok(sample_response("r2-value")) }
                })
                .await
        });

        // Poll the waiter once so it registers its wait (deadline ≈ now + 60s).
        tokio::task::yield_now().await;

        // Advance in 60s stages, yielding between stages. `tokio::time::advance`
        // only marks timer-woken tasks ready; the yield polls them so R1's
        // service-layer heartbeat keeps the stamp fresh AND R2's extension loop
        // progresses one interval per stage. After (1 + cap) stages — 60s × 5 =
        // 300s, matching the server's 300s request budget — the extension cap
        // saturates and R2 must escape, run its own load, and complete.
        for _ in 0..=5 {
            tokio::time::advance(Duration::from_secs(60)).await;
            tokio::task::yield_now().await;
        }

        let result2 = waiter_task.await.expect("task 2 panicked");
        assert!(result2.is_ok(), "waiter should complete its own load");
        assert_eq!(
            result2
                .ok()
                .and_then(|r| r.terms.first().map(|t| t.hash.clone())),
            Some("r2-value".to_owned())
        );
        assert_eq!(
            loader_calls.load(Ordering::SeqCst),
            2,
            "the waiter must run its own reconstruction after escaping the wedged latch"
        );

        // R1 is still wedged; abort it so the test can finish.
        wedged_task.abort();
    }
}
