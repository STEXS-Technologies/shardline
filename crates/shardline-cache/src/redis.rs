use std::{
    fmt,
    future::Future,
    num::NonZeroU64,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use redis::AsyncCommands;
use shardline_protocol::SecretBytes;

use crate::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheFuture,
    ReconstructionCacheKey, ReconstructionCacheLookup, ReconstructionCacheReservation,
};

const RECONSTRUCTION_CACHE_PREFIX: &str = "shardline:reconstruction:v1";
const DEFAULT_REDIS_OPERATION_TIMEOUT: Duration = Duration::from_secs(1);
const REDIS_LOADING_LEASE: Duration = Duration::from_secs(30);
const REDIS_LOADING_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// TLS material for a Redis connection.
///
/// Configure a root certificate for private certificate authorities. Configure
/// a client certificate and key together when the Redis server requires mTLS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RedisTlsConfig {
    root_cert: Option<SecretBytes>,
    client_cert: Option<SecretBytes>,
    client_key: Option<SecretBytes>,
}

impl RedisTlsConfig {
    /// Creates TLS configuration with an optional PEM-encoded root certificate.
    #[must_use]
    pub const fn new(root_cert: Option<SecretBytes>) -> Self {
        Self {
            root_cert,
            client_cert: None,
            client_key: None,
        }
    }

    /// Adds a PEM-encoded client certificate and private key for mTLS.
    #[must_use]
    pub fn with_client_identity(
        mut self,
        client_cert: SecretBytes,
        client_key: SecretBytes,
    ) -> Self {
        self.client_cert = Some(client_cert);
        self.client_key = Some(client_key);
        self
    }

    const fn is_empty(&self) -> bool {
        self.root_cert.is_none() && self.client_cert.is_none() && self.client_key.is_none()
    }

    fn into_redis_tls_certificates(
        self,
    ) -> Result<redis::TlsCertificates, ReconstructionCacheError> {
        let client_tls = match (self.client_cert, self.client_key) {
            (None, None) => None,
            (Some(client_cert), Some(client_key)) => Some(redis::ClientTlsConfig {
                client_cert: client_cert.expose_secret().to_vec(),
                client_key: client_key.expose_secret().to_vec(),
            }),
            (Some(_), None) | (None, Some(_)) => {
                return Err(ReconstructionCacheError::IncompleteRedisTlsClientIdentity);
            }
        };

        Ok(redis::TlsCertificates {
            client_tls,
            root_cert: self.root_cert.map(|c| c.expose_secret().to_vec()),
        })
    }
}

/// Redis-backed reconstruction cache adapter.
#[derive(Clone)]
pub struct RedisReconstructionCache {
    client: redis::Client,
    connection: Arc<tokio::sync::OnceCell<redis::aio::ConnectionManager>>,
    ttl_seconds: NonZeroU64,
    operation_timeout: Duration,
}

impl fmt::Debug for RedisReconstructionCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisReconstructionCache")
            .field("client", &"***")
            .field("ttl_seconds", &self.ttl_seconds)
            .field("operation_timeout", &self.operation_timeout)
            .finish()
    }
}

impl RedisReconstructionCache {
    /// Creates a Redis-backed reconstruction cache adapter.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the URL is empty or invalid, or the TTL exceeds Redis's expiration range.
    pub fn new(redis_url: &str, ttl_seconds: NonZeroU64) -> Result<Self, ReconstructionCacheError> {
        Self::new_with_tls_and_timeout(
            redis_url,
            ttl_seconds,
            RedisTlsConfig::default(),
            DEFAULT_REDIS_OPERATION_TIMEOUT,
        )
    }

    /// Creates a Redis-backed reconstruction cache adapter with TLS or mTLS material.
    ///
    /// The Redis URL must use the `rediss://` scheme whenever TLS material is
    /// supplied. An empty config still permits `rediss://` URLs that use the
    /// platform trust store.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the URL or TLS material is invalid, or the TTL exceeds Redis's expiration range.
    pub fn new_with_tls(
        redis_url: &str,
        ttl_seconds: NonZeroU64,
        tls_config: RedisTlsConfig,
    ) -> Result<Self, ReconstructionCacheError> {
        Self::new_with_tls_and_timeout(
            redis_url,
            ttl_seconds,
            tls_config,
            DEFAULT_REDIS_OPERATION_TIMEOUT,
        )
    }

    /// Creates a Redis-backed cache with an explicit per-operation latency bound.
    ///
    /// The timeout covers connection acquisition and the Redis command. Bounding the
    /// complete operation ensures an unavailable cache cannot indefinitely delay the
    /// durable reconstruction fallback.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the URL or TLS material is invalid or
    /// when `operation_timeout` is zero or the TTL exceeds Redis's expiration range.
    pub fn new_with_tls_and_timeout(
        redis_url: &str,
        ttl_seconds: NonZeroU64,
        tls_config: RedisTlsConfig,
        operation_timeout: Duration,
    ) -> Result<Self, ReconstructionCacheError> {
        if redis_url.trim().is_empty() {
            return Err(ReconstructionCacheError::EmptyRedisUrl);
        }
        validate_expiration(ttl_seconds, 0, 0)?;
        if operation_timeout.is_zero() {
            return Err(ReconstructionCacheError::InvalidRedisOperationTimeout);
        }

        if redis_url.trim_start().starts_with("rediss://") || !tls_config.is_empty() {
            install_rustls_crypto_provider();
        }

        let client = if tls_config.is_empty() {
            redis::Client::open(redis_url)?
        } else {
            redis::Client::build_with_tls(redis_url, tls_config.into_redis_tls_certificates()?)?
        };

        Ok(Self {
            client,
            connection: Arc::new(tokio::sync::OnceCell::new()),
            ttl_seconds,
            operation_timeout,
        })
    }

    /// Reuses a lazily initialized connection manager shared by adapter clones.
    /// The manager reconnects after transport failures; outer operation bounds
    /// also cover initialization and waiting for a reconnect.
    async fn get_connection(
        &self,
    ) -> Result<redis::aio::ConnectionManager, ReconstructionCacheError> {
        let connection = self
            .connection
            .get_or_try_init(|| async {
                let config = redis::aio::ConnectionManagerConfig::new()
                    .set_connection_timeout(Some(self.operation_timeout))
                    .set_response_timeout(Some(self.operation_timeout));
                self.client.get_connection_manager_with_config(config).await
            })
            .await?;
        Ok(connection.clone())
    }

    async fn with_operation_timeout<T, Operation>(
        &self,
        operation: Operation,
    ) -> Result<T, ReconstructionCacheError>
    where
        Operation: Future<Output = Result<T, ReconstructionCacheError>>,
    {
        tokio::time::timeout(self.operation_timeout, operation)
            .await
            .map_err(|_elapsed| ReconstructionCacheError::RedisTimeout)?
    }

    pub(crate) fn redis_key(key: &ReconstructionCacheKey) -> String {
        let scope = key.repository_scope().map_or_else(
            || "global".to_owned(),
            |scope| {
                let revision = scope
                    .revision()
                    .map_or_else(|| "head".to_owned(), hex::encode);
                format!(
                    "{}:{}:{}:{}",
                    scope.provider(),
                    hex::encode(scope.owner()),
                    hex::encode(scope.repo()),
                    revision
                )
            },
        );
        let content = key
            .content_hash()
            .map_or_else(|| "latest".to_owned(), hex::encode);

        format!(
            "{RECONSTRUCTION_CACHE_PREFIX}:{scope}:{content}:{}",
            hex::encode(key.file_id())
        )
    }

    fn loading_key(redis_key: &str) -> String {
        format!("{redis_key}:loading")
    }

    fn new_reservation_token() -> Result<String, ReconstructionCacheError> {
        let mut bytes = [0_u8; 16];
        getrandom::fill(&mut bytes).map_err(|_error| ReconstructionCacheError::Operation)?;
        Ok(hex::encode(bytes))
    }

    async fn compare_delete_loading(
        connection: &mut redis::aio::ConnectionManager,
        loading_key: &str,
        token: &str,
    ) -> Result<bool, ReconstructionCacheError> {
        static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
            redis::Script::new(
                "if redis.call('GET', KEYS[1]) == ARGV[1] then \
             return redis.call('DEL', KEYS[1]) else return 0 end",
            )
        });
        let deleted: i64 = SCRIPT
            .key(loading_key)
            .arg(token)
            .invoke_async(connection)
            .await?;
        Ok(deleted > 0)
    }
}

fn install_rustls_crypto_provider() {
    let _already_installed = rustls::crypto::ring::default_provider()
        .install_default()
        .is_err();
}

impl AsyncReconstructionCache for RedisReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async move {
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let _pong: String = redis::cmd("PING").query_async(&mut connection).await?;
                let (seconds, micros): (u64, u64) =
                    redis::cmd("TIME").query_async(&mut connection).await?;
                validate_expiration(self.ttl_seconds, seconds, micros)?;
                Ok(())
            })
            .await
        })
    }

    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async move {
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let redis_key = Self::redis_key(key);
                Ok(connection.get(redis_key).await?)
            })
            .await
        })
    }

    fn get_or_reserve<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, ReconstructionCacheLookup> {
        Box::pin(async move {
            let redis_key = Self::redis_key(key);
            let loading_key = Self::loading_key(&redis_key);
            let reservation_token = Self::new_reservation_token()?;
            let deadline = Instant::now()
                .checked_add(self.operation_timeout)
                .ok_or(ReconstructionCacheError::Operation)?;
            tokio::time::timeout_at(deadline.into(), async {
                loop {
                    let mut connection = self.get_connection().await?;
                    let lease_millis = u64::try_from(REDIS_LOADING_LEASE.as_millis())?;
                    static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
                        redis::Script::new(
                        "local value = redis.call('GET', KEYS[1]); \
                         if value then return {1, value} end; \
                         local acquired = redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]); \
                         if acquired then return {2, false} else return {0, false} end",
                        )
                    });
                    let (state, payload): (i64, Option<Vec<u8>>) = SCRIPT
                    .key(&redis_key)
                    .key(&loading_key)
                    .arg(&reservation_token)
                    .arg(lease_millis)
                    .invoke_async(&mut connection)
                    .await?;
                    if state == 1
                        && let Some(payload) = payload
                    {
                        return Ok(ReconstructionCacheLookup::Hit(payload));
                    }
                    if state == 2 {
                        return Ok(ReconstructionCacheLookup::Reserved(
                            ReconstructionCacheReservation::distributed(reservation_token),
                        ));
                    }
                    tokio::time::sleep(REDIS_LOADING_POLL_INTERVAL).await;
                }
            })
            .await
            .map_err(|_elapsed| ReconstructionCacheError::RedisTimeout)?
        })
    }

    fn put<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async move {
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let ttl_seconds = self.ttl_seconds.get();
                let _: () = connection
                    .set_ex(Self::redis_key(key), payload, ttl_seconds)
                    .await
                    .map_err(expiration_error)?;
                Ok(())
            })
            .await
        })
    }

    fn put_reserved<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        payload: &'operation [u8],
        reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, ()> {
        Box::pin(async move {
            let Some(token) = reservation.owner_token() else {
                return Err(ReconstructionCacheError::LostLoadingReservation);
            };
            let redis_key = Self::redis_key(key);
            let loading_key = Self::loading_key(&redis_key);
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let ttl_seconds = self.ttl_seconds.get();
                static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
                    redis::Script::new(
                        "if redis.call('GET', KEYS[2]) == ARGV[1] then \
                     redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); \
                     redis.call('DEL', KEYS[2]); return 1 else return 0 end",
                    )
                });
                let published: i64 = SCRIPT
                    .key(&redis_key)
                    .key(&loading_key)
                    .arg(token)
                    .arg(payload)
                    .arg(ttl_seconds)
                    .invoke_async(&mut connection)
                    .await
                    .map_err(expiration_error)?;
                if published == 0 {
                    return Err(ReconstructionCacheError::LostLoadingReservation);
                }
                Ok(())
            })
            .await
        })
    }

    fn delete<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let deleted: usize = connection.del(Self::redis_key(key)).await?;
                Ok(deleted > 0)
            })
            .await
        })
    }

    fn delete_reserved<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let redis_key = Self::redis_key(key);
            let Some(token) = reservation.owner_token() else {
                return Ok(false);
            };
            let loading_key = Self::loading_key(&redis_key);
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
                    redis::Script::new(
                        "if redis.call('GET', KEYS[2]) == ARGV[1] then \
                     local deleted = redis.call('DEL', KEYS[1]); \
                     redis.call('DEL', KEYS[2]); return deleted else return 0 end",
                    )
                });
                let released: i64 = SCRIPT
                    .key(&redis_key)
                    .key(&loading_key)
                    .arg(token)
                    .invoke_async(&mut connection)
                    .await?;
                Ok(released > 0)
            })
            .await
        })
    }

    fn touch_reservation<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
        reservation: &'operation ReconstructionCacheReservation,
    ) -> ReconstructionCacheFuture<'operation, bool> {
        Box::pin(async move {
            let Some(token) = reservation.owner_token() else {
                return Ok(false);
            };
            let loading_key = Self::loading_key(&Self::redis_key(key));
            self.with_operation_timeout(async {
                let mut connection = self.get_connection().await?;
                let lease_millis = u64::try_from(REDIS_LOADING_LEASE.as_millis())?;
                static SCRIPT: LazyLock<redis::Script> = LazyLock::new(|| {
                    redis::Script::new(
                        "if redis.call('GET', KEYS[1]) == ARGV[1] then \
                     return redis.call('PEXPIRE', KEYS[1], ARGV[2]) else return 0 end",
                    )
                });
                let refreshed: i64 = SCRIPT
                    .key(loading_key)
                    .arg(token)
                    .arg(lease_millis)
                    .invoke_async(&mut connection)
                    .await?;
                Ok(refreshed > 0)
            })
            .await
        })
    }

    fn release_reservation(
        &self,
        key: &ReconstructionCacheKey,
        reservation: &ReconstructionCacheReservation,
    ) {
        let Some(token) = reservation.owner_token().map(ToOwned::to_owned) else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let loading_key = Self::loading_key(&Self::redis_key(key));
        let cache = self.clone();
        handle.spawn(async move {
            let _ignored = cache
                .with_operation_timeout(async {
                    let mut connection = cache.get_connection().await?;
                    Self::compare_delete_loading(&mut connection, &loading_key, &token).await
                })
                .await;
        });
    }
}

fn validate_expiration(
    ttl: NonZeroU64,
    server_seconds: u64,
    server_micros: u64,
) -> Result<(), ReconstructionCacheError> {
    let expiration_ms = u128::from(ttl.get())
        .saturating_mul(1_000)
        .saturating_add(u128::from(server_seconds).saturating_mul(1_000))
        .saturating_add(u128::from(server_micros) / 1_000);
    if expiration_ms > u128::from(u64::MAX / 2) {
        return Err(ReconstructionCacheError::InvalidRedisTtl);
    }
    Ok(())
}

fn expiration_error(error: redis::RedisError) -> ReconstructionCacheError {
    // Redis validates relative expiration atomically before changing the value.
    // A near-limit TTL can cease to fit between ready() and the later write.
    if error.kind() == redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError)
        && error
            .detail()
            .is_some_and(|detail| detail.starts_with("invalid expire time in "))
    {
        ReconstructionCacheError::InvalidRedisTtl
    } else {
        ReconstructionCacheError::Redis(error)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::{env::var as env_var, error::Error as StdError, num::NonZeroU64, time::Duration};

    use redis::AsyncCommands;

    use super::{RECONSTRUCTION_CACHE_PREFIX, RedisReconstructionCache, RedisTlsConfig};
    use crate::{AsyncReconstructionCache, ReconstructionCacheKey};
    use shardline_protocol::{RepositoryProvider, RepositoryScope, SecretBytes};

    #[test]
    fn redis_ttl_validation_preserves_exact_server_time_boundary() {
        let ttl = NonZeroU64::MIN;
        let last_valid_time = u64::MAX / 2 - 1_000;
        assert!(
            super::validate_expiration(
                ttl,
                last_valid_time / 1_000,
                (last_valid_time % 1_000) * 1_000
            )
            .is_ok()
        );
        let invalid_time = last_valid_time + 1;
        assert!(matches!(
            super::validate_expiration(ttl, invalid_time / 1_000, (invalid_time % 1_000) * 1_000),
            Err(crate::ReconstructionCacheError::InvalidRedisTtl)
        ));
        assert!(matches!(
            super::validate_expiration(ttl, u64::MAX, u64::MAX),
            Err(crate::ReconstructionCacheError::InvalidRedisTtl)
        ));
    }

    #[test]
    fn redis_constructor_rejects_fundamentally_unrepresentable_ttl() {
        for seconds in [u64::MAX, (u64::MAX / 2) / 1_000 + 1] {
            assert!(matches!(
                RedisReconstructionCache::new(
                    "redis://localhost",
                    NonZeroU64::new(seconds).unwrap_or(NonZeroU64::MIN)
                ),
                Err(crate::ReconstructionCacheError::InvalidRedisTtl)
            ));
        }
        assert!(
            RedisReconstructionCache::new(
                "redis://localhost",
                NonZeroU64::new((u64::MAX / 2) / 1_000).unwrap_or(NonZeroU64::MIN)
            )
            .is_ok()
        );
    }

    #[test]
    fn redis_expiration_error_mapping_preserves_unrelated_server_errors() {
        for detail in [
            "invalid expire time in 'setex' command",
            "invalid expire time in 'set' command script: example",
        ] {
            let error = redis::RedisError::from((
                redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
                "response",
                detail.to_owned(),
            ));
            assert!(matches!(
                super::expiration_error(error),
                crate::ReconstructionCacheError::InvalidRedisTtl
            ));
        }
        let error = redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "response",
            "unrelated server failure".to_owned(),
        ));
        assert!(matches!(
            super::expiration_error(error),
            crate::ReconstructionCacheError::Redis(_)
        ));
    }

    #[tokio::test]
    async fn redis_ownerless_reserved_mutations_do_not_fall_back_to_raw_commands() {
        let cache = RedisReconstructionCache::new("redis://127.0.0.1:9", NonZeroU64::MIN);
        let cache = cache.unwrap();
        let key = ReconstructionCacheKey::latest("ownerless", None);
        let token = crate::ReconstructionCacheReservation::default();
        assert!(matches!(
            cache.put_reserved(&key, b"wrong", &token).await,
            Err(crate::ReconstructionCacheError::LostLoadingReservation)
        ));
        assert!(matches!(
            cache.delete_reserved(&key, &token).await,
            Ok(false)
        ));
    }

    #[tokio::test]
    async fn redis_clones_reuse_one_live_connection_when_url_is_available() {
        let Ok(url) = env_var("STEXS_REDIS_CACHE_TEST_URL") else {
            return;
        };
        let cache = RedisReconstructionCache::new(&url, NonZeroU64::MIN);
        let cache = cache.unwrap();
        let original = cache.get_connection().await;
        let mut original = original.unwrap();
        let original_id: u64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut original)
            .await
            .unwrap_or_default();
        assert_ne!(original_id, 0);
        for _ in 0..16 {
            let cloned = cache.clone().get_connection().await;
            let mut cloned = cloned.unwrap();
            let id: u64 = redis::cmd("CLIENT")
                .arg("ID")
                .query_async(&mut cloned)
                .await
                .unwrap_or_default();
            assert_eq!(id, original_id);
        }
    }

    #[tokio::test]
    async fn redis_manager_recovers_after_its_own_connection_is_closed() {
        let Ok(url) = env_var("STEXS_REDIS_CACHE_TEST_URL") else {
            return;
        };
        let cache = RedisReconstructionCache::new(&url, NonZeroU64::MIN);
        let cache = cache.unwrap();
        let original = cache.get_connection().await;
        let mut original = original.unwrap();
        let old_id: u64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut original)
            .await
            .unwrap_or_default();
        assert_ne!(old_id, 0);
        // Close only this test's managed connection, through a separate controller.
        let controller = redis::Client::open(url);
        let controller_client = controller.unwrap();
        let connection = controller_client.get_multiplexed_async_connection().await;
        let mut controller_connection = connection.unwrap();
        let killed: u64 = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(old_id)
            .query_async(&mut controller_connection)
            .await
            .unwrap_or_default();
        assert_eq!(killed, 1);
        let recovered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if cache.ready().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        assert!(recovered.is_ok());
        let reconnected = cache.clone().get_connection().await;
        let mut reconnected = reconnected.unwrap();
        let new_id: u64 = redis::cmd("CLIENT")
            .arg("ID")
            .query_async(&mut reconnected)
            .await
            .unwrap_or_default();
        assert_ne!(new_id, 0);
        assert_ne!(new_id, old_id);
    }

    #[test]
    fn redis_cache_debug_redacts_connection_url() {
        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let cache = RedisReconstructionCache::new(
            "redis://:cache-secret@cache.example.test:6379/0",
            ttl_seconds,
        );
        let cache = cache.unwrap();

        let rendered = format!("{cache:?}");

        assert!(!rendered.contains("cache-secret"));
        assert!(rendered.contains("***"));
    }

    #[test]
    fn redis_cache_rejects_zero_operation_timeout() {
        let result = RedisReconstructionCache::new_with_tls_and_timeout(
            "redis://127.0.0.1:6379",
            NonZeroU64::MIN,
            RedisTlsConfig::default(),
            Duration::ZERO,
        );
        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::InvalidRedisOperationTimeout)
        ));
    }

    #[tokio::test]
    async fn redis_cache_bounds_stalled_connection_operations() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await;
        let listener = listener.unwrap();
        let address = listener.local_addr();
        let address = address.unwrap();
        let stalled_peer = tokio::spawn(async move {
            let accepted = listener.accept().await;
            if accepted.is_ok() {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        let cache = RedisReconstructionCache::new_with_tls_and_timeout(
            &format!("redis://{address}"),
            NonZeroU64::MIN,
            RedisTlsConfig::default(),
            Duration::from_millis(50),
        );
        let cache = cache.unwrap();

        let result = cache.ready().await;
        stalled_peer.abort();

        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::RedisTimeout)
        ));
    }

    #[test]
    fn redis_tls_config_debug_redacts_certificate_material() {
        let tls = RedisTlsConfig::new(Some(SecretBytes::from_slice(b"root-secret")))
            .with_client_identity(
                SecretBytes::from_slice(b"client-secret"),
                SecretBytes::from_slice(b"key-secret"),
            );

        let rendered = format!("{tls:?}");

        assert!(!rendered.contains("root-secret"));
        assert!(!rendered.contains("client-secret"));
        assert!(!rendered.contains("key-secret"));
        assert!(rendered.contains("***"));
    }

    #[test]
    fn redis_tls_client_identity_must_include_certificate_and_key() {
        let tls = RedisTlsConfig {
            root_cert: None,
            client_cert: Some(SecretBytes::from_slice(b"certificate")),
            client_key: None,
        };
        let result =
            RedisReconstructionCache::new_with_tls("rediss://localhost:6379", NonZeroU64::MIN, tls);

        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::IncompleteRedisTlsClientIdentity)
        ));
    }

    #[tokio::test]
    async fn redis_cache_roundtrips_payload_when_live_url_is_available() {
        let Some(redis_url) = env_var("STEXS_REDIS_CACHE_TEST_URL").ok() else {
            return;
        };

        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let cache = RedisReconstructionCache::new(&redis_url, ttl_seconds);
        let cache = cache.unwrap();
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

    #[allow(clippy::unwrap_used)]
    #[tokio::test]
    async fn redis_cache_ready_succeeds_when_live_url_is_available() {
        let Some(redis_url) = env_var("STEXS_REDIS_CACHE_TEST_URL").ok() else {
            return;
        };

        let ttl_seconds = NonZeroU64::new(60).unwrap_or(NonZeroU64::MIN);
        let cache = RedisReconstructionCache::new(&redis_url, ttl_seconds).unwrap();
        let result = cache.ready().await;
        assert!(result.is_ok());
    }

    #[test]
    fn redis_key_serialization_preserves_scope_and_content_identity() {
        let cases = [
            (
                Some((
                    RepositoryProvider::GitHub,
                    "my-org",
                    "my-repo",
                    Some("main"),
                )),
                "file.bin",
                None,
                "shardline:reconstruction:v1:github:6d792d6f7267:6d792d7265706f:6d61696e:latest:66696c652e62696e",
            ),
            (
                None,
                "file.bin",
                None,
                "shardline:reconstruction:v1:global:latest:66696c652e62696e",
            ),
            (
                None,
                "file.bin",
                Some("abc123"),
                "shardline:reconstruction:v1:global:616263313233:66696c652e62696e",
            ),
            (
                Some((RepositoryProvider::GitLab, "group", "project", None)),
                "doc.pdf",
                None,
                "shardline:reconstruction:v1:gitlab:67726f7570:70726f6a656374:head:latest:646f632e706466",
            ),
            (
                Some((RepositoryProvider::Gitea, "gitea-org", "gitea-repo", None)),
                "f1.bin",
                Some("hash1"),
                "shardline:reconstruction:v1:gitea:67697465612d6f7267:67697465612d7265706f:head:6861736831:66312e62696e",
            ),
            (
                Some((RepositoryProvider::Codeberg, "user", "repo", Some("v2"))),
                "data.bin",
                None,
                "shardline:reconstruction:v1:codeberg:75736572:7265706f:7632:latest:646174612e62696e",
            ),
            (
                Some((RepositoryProvider::Generic, "ns", "repo-name", None)),
                "generic.bin",
                None,
                "shardline:reconstruction:v1:generic:6e73:7265706f2d6e616d65:head:latest:67656e657269632e62696e",
            ),
            (
                Some((
                    RepositoryProvider::GitHub,
                    "my-org/team",
                    "asset.repo_v2",
                    Some("feature/branch"),
                )),
                "my file (1).bin",
                None,
                "shardline:reconstruction:v1:github:6d792d6f72672f7465616d:61737365742e7265706f5f7632:666561747572652f6272616e6368:latest:6d792066696c65202831292e62696e",
            ),
            (None, "", None, "shardline:reconstruction:v1:global:latest:"),
            (
                None,
                "f",
                Some(""),
                "shardline:reconstruction:v1:global::66",
            ),
        ];
        for (scope, file_id, content_hash, expected) in cases {
            let scope = scope.map(|(provider, owner, repo, revision)| {
                RepositoryScope::new(provider, owner, repo, revision).unwrap()
            });
            let key = content_hash.map_or_else(
                || ReconstructionCacheKey::latest(file_id, scope.as_ref()),
                |hash| ReconstructionCacheKey::version(file_id, hash, scope.as_ref()),
            );
            assert_eq!(RedisReconstructionCache::redis_key(&key), expected);
        }
    }

    #[test]
    fn redis_url_validation_preserves_empty_and_parse_errors() {
        for url in [
            "",
            "   ",
            "\n",
            "\t\t",
            "localhost:6379",
            "redis://localhost:abc",
        ] {
            let error = RedisReconstructionCache::new(url, NonZeroU64::MIN).unwrap_err();
            if url.trim().is_empty() {
                assert!(matches!(
                    error,
                    crate::ReconstructionCacheError::EmptyRedisUrl
                ));
            } else {
                assert!(matches!(error, crate::ReconstructionCacheError::Redis(_)));
            }
        }
        for url in [
            "redis://127.0.0.1:6379",
            "redis://localhost",
            "redis+unix:///tmp/redis.sock",
            "redis+unix:///run/redis.sock",
        ] {
            assert!(RedisReconstructionCache::new(url, NonZeroU64::MIN).is_ok());
        }
    }

    #[test]
    fn redis_cache_clones_share_connection_initialization_and_preserve_settings() {
        let cache = RedisReconstructionCache::new_with_tls_and_timeout(
            "redis://localhost",
            NonZeroU64::new(60).unwrap(),
            RedisTlsConfig::default(),
            Duration::from_millis(250),
        )
        .unwrap();
        let cloned = cache.clone();
        assert!(std::sync::Arc::ptr_eq(
            &cache.connection,
            &cloned.connection
        ));
        assert_eq!(cache.ttl_seconds, cloned.ttl_seconds);
        assert_eq!(cache.operation_timeout, cloned.operation_timeout);
    }

    // ── More redis_key format edge cases ──────────────────────────────────

    #[test]
    fn redis_key_format_special_chars_in_names() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "org.name",
            "repo-name",
            Some("rev_sion"),
        );
        assert!(scope.is_ok());
        if let Ok(scope) = scope {
            let key = ReconstructionCacheKey::latest("file.bin", Some(&scope));
            let redis_key = RedisReconstructionCache::redis_key(&key);
            assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
            assert!(redis_key.contains(hex::encode("org.name").as_str()));
            assert!(redis_key.contains(hex::encode("repo-name").as_str()));
            assert!(redis_key.contains(hex::encode("rev_sion").as_str()));
        }
    }

    #[test]
    fn redis_key_format_long_file_id() {
        let long_id = "a".repeat(1000);
        let key = ReconstructionCacheKey::latest(&long_id, None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        assert!(redis_key.starts_with("shardline:reconstruction:v1:global:latest:"));
        assert!(redis_key.ends_with(&hex::encode(&long_id)));
        assert_eq!(
            redis_key.len(),
            "shardline:reconstruction:v1:global:latest:".len() + hex::encode(&long_id).len()
        );
    }

    #[test]
    fn redis_key_format_long_content_hash() {
        let long_hash = "b".repeat(200);
        let key = ReconstructionCacheKey::version("f", &long_hash, None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        assert!(redis_key.starts_with("shardline:reconstruction:v1:global:"));
        assert!(redis_key.contains(&hex::encode(&long_hash)));
    }

    #[test]
    fn redis_key_format_with_all_providers_and_revision() {
        let providers = [
            (RepositoryProvider::GitHub, "github"),
            (RepositoryProvider::GitLab, "gitlab"),
            (RepositoryProvider::Gitea, "gitea"),
            (RepositoryProvider::Codeberg, "codeberg"),
            (RepositoryProvider::Generic, "generic"),
        ];
        for (provider, expected) in &providers {
            let scope = RepositoryScope::new(*provider, "owner", "repo", Some("main"));
            assert!(scope.is_ok());
            if let Ok(scope) = scope {
                let key = ReconstructionCacheKey::latest("f.bin", Some(&scope));
                let redis_key = RedisReconstructionCache::redis_key(&key);
                assert!(
                    redis_key.contains(expected),
                    "expected provider token {expected} in {redis_key}"
                );
            }
        }
    }

    #[test]
    fn redis_key_format_version_no_scope_all_providers() {
        let providers = [
            RepositoryProvider::GitHub,
            RepositoryProvider::GitLab,
            RepositoryProvider::Gitea,
            RepositoryProvider::Codeberg,
            RepositoryProvider::Generic,
        ];
        for provider in &providers {
            let scope = RepositoryScope::new(*provider, "ns", "proj", None);
            assert!(scope.is_ok());
            if let Ok(scope) = scope {
                let key = ReconstructionCacheKey::version("file.bin", "hash1", Some(&scope));
                let redis_key = RedisReconstructionCache::redis_key(&key);
                assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
                // Without revision, "head" is used
                assert!(
                    redis_key.contains(":head:"),
                    "expected :head: in key for {provider:?}"
                );
            }
        }
    }
}
