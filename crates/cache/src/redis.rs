use std::{fmt, num::NonZeroU64};

use redis::AsyncCommands;
use shardline_protocol::SecretBytes;

use crate::{
    AsyncReconstructionCache, ReconstructionCacheError, ReconstructionCacheFuture,
    ReconstructionCacheKey,
};

const RECONSTRUCTION_CACHE_PREFIX: &str = "shardline:reconstruction:v1";

/// TLS material for a Redis connection.
///
/// Configure a root certificate for private certificate authorities. Configure
/// a client certificate and key together when the Redis server requires mTLS.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RedisTlsConfig {
    root_cert: Option<SecretBytes>,
    client_cert: Option<SecretBytes>,
    client_key: Option<SecretBytes>,
}

impl RedisTlsConfig {
    /// Creates TLS configuration with an optional PEM-encoded root certificate.
    #[must_use]
    pub fn new(root_cert: Option<SecretBytes>) -> Self {
        Self {
            root_cert,
            client_cert: None,
            client_key: None,
        }
    }

    /// Adds a PEM-encoded client certificate and private key for mTLS.
    #[must_use]
    pub fn with_client_identity(mut self, client_cert: SecretBytes, client_key: SecretBytes) -> Self {
        self.client_cert = Some(client_cert);
        self.client_key = Some(client_key);
        self
    }

    fn is_empty(&self) -> bool {
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

impl fmt::Debug for RedisTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedisTlsConfig")
            .field("root_cert", &self.root_cert)
            .field("client_cert", &self.client_cert)
            .field("client_key", &self.client_key)
            .finish()
    }
}

/// Redis-backed reconstruction cache adapter.
pub struct RedisReconstructionCache {
    client: redis::Client,
    ttl_seconds: NonZeroU64,
}

impl Clone for RedisReconstructionCache {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            ttl_seconds: self.ttl_seconds,
        }
    }
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
        Self::new_with_tls(redis_url, ttl_seconds, RedisTlsConfig::default())
    }

    /// Creates a Redis-backed reconstruction cache adapter with TLS or mTLS material.
    ///
    /// The Redis URL must use the `rediss://` scheme whenever TLS material is
    /// supplied. An empty config still permits `rediss://` URLs that use the
    /// platform trust store.
    ///
    /// # Errors
    ///
    /// Returns [`ReconstructionCacheError`] when the URL or TLS material is invalid.
    pub fn new_with_tls(
        redis_url: &str,
        ttl_seconds: NonZeroU64,
        tls_config: RedisTlsConfig,
    ) -> Result<Self, ReconstructionCacheError> {
        if redis_url.trim().is_empty() {
            return Err(ReconstructionCacheError::EmptyRedisUrl);
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
            ttl_seconds,
        })
    }

    /// Returns a new multiplexed async connection from the client pool.
    ///
    /// The underlying `redis::Client` manages connection pooling internally,
    /// so this is cheap to call per-operation.
    async fn get_connection(
        &self,
    ) -> Result<redis::aio::MultiplexedConnection, ReconstructionCacheError> {
        Ok(self.client.get_multiplexed_async_connection().await?)
    }

    pub(crate) fn redis_key(key: &ReconstructionCacheKey) -> String {
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

fn install_rustls_crypto_provider() {
    let _already_installed = rustls::crypto::ring::default_provider()
        .install_default()
        .is_err();
}

impl AsyncReconstructionCache for RedisReconstructionCache {
    fn ready(&self) -> ReconstructionCacheFuture<'_, ()> {
        Box::pin(async move {
            let mut connection = self.get_connection().await?;
            let _pong: String = redis::cmd("PING").query_async(&mut connection).await?;
            Ok(())
        })
    }

    fn get<'operation>(
        &'operation self,
        key: &'operation ReconstructionCacheKey,
    ) -> ReconstructionCacheFuture<'operation, Option<Vec<u8>>> {
        Box::pin(async move {
            let mut connection = self.get_connection().await?;
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
            let mut connection = self.get_connection().await?;
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
            let mut connection = self.get_connection().await?;
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

    use super::{RECONSTRUCTION_CACHE_PREFIX, RedisReconstructionCache, RedisTlsConfig};
    use crate::{AsyncReconstructionCache, ReconstructionCacheKey};
    use shardline_protocol::{RepositoryProvider, RepositoryScope, SecretBytes};

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

    #[test]
    fn redis_cache_rejects_empty_url() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("", ttl_seconds);
        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::EmptyRedisUrl)
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

    // ── Repository scope key serialization ────────────────────────────────

    #[test]
    fn redis_key_format_with_scope() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "my-org",
            "my-repo",
            Some("main"),
        );
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::latest("file.bin", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        // The provider ("github") is NOT hex-encoded (comes from provider_token directly).
        // owner, repo, and revision ARE hex-encoded (via encode_component).
        // file_id and content_hash ARE also hex-encoded.
        let owner_hex = hex::encode("my-org");
        let repo_hex = hex::encode("my-repo");
        let revision_hex = hex::encode("main");
        let file_hex = hex::encode("file.bin");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(&format!(":github:{owner_hex}:{repo_hex}:{revision_hex}:")));
        assert!(redis_key.ends_with(&format!(":latest:{file_hex}")));
    }

    #[test]
    fn redis_key_format_without_scope() {
        let key = ReconstructionCacheKey::latest("file.bin", None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(":global:"));
    }

    #[test]
    fn redis_key_format_with_content_hash() {
        let key = ReconstructionCacheKey::version("file.bin", "abc123", None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let hash_hex = hex::encode("abc123");
        let file_hex = hex::encode("file.bin");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.ends_with(&format!(":{hash_hex}:{file_hex}")));
    }

    #[test]
    fn redis_key_format_with_scope_no_revision() {
        let scope = RepositoryScope::new(RepositoryProvider::GitLab, "group", "project", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::latest("doc.pdf", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let owner_hex = hex::encode("group");
        let repo_hex = hex::encode("project");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(&format!(":gitlab:{owner_hex}:{repo_hex}:")));
        assert!(redis_key.contains(":head:"));
    }

    // ── Additional provider key formats ─────────────────────────────────

    #[test]
    fn redis_key_format_gitea_provider() {
        let scope =
            RepositoryScope::new(RepositoryProvider::Gitea, "gitea-org", "gitea-repo", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::version("f1.bin", "hash1", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let owner_hex = hex::encode("gitea-org");
        let repo_hex = hex::encode("gitea-repo");
        let hash_hex = hex::encode("hash1");
        let file_hex = hex::encode("f1.bin");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(&format!(":gitea:{owner_hex}:{repo_hex}:head:")));
        assert!(redis_key.ends_with(&format!(":{hash_hex}:{file_hex}")));
    }

    #[test]
    fn redis_key_format_codeberg_provider() {
        let scope = RepositoryScope::new(RepositoryProvider::Codeberg, "user", "repo", Some("v2"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::latest("data.bin", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let owner_hex = hex::encode("user");
        let repo_hex = hex::encode("repo");
        let rev_hex = hex::encode("v2");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(&format!(":codeberg:{owner_hex}:{repo_hex}:{rev_hex}:")));
    }

    #[test]
    fn redis_key_format_generic_provider() {
        let scope = RepositoryScope::new(RepositoryProvider::Generic, "ns", "repo-name", None);
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::latest("generic.bin", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let owner_hex = hex::encode("ns");
        let repo_hex = hex::encode("repo-name");
        assert!(redis_key.starts_with("shardline:reconstruction:v1:"));
        assert!(redis_key.contains(&format!(":generic:{owner_hex}:{repo_hex}:head:")));
    }

    // ── Edge cases ──────────────────────────────────────────────────────

    #[test]
    fn redis_key_format_special_characters_in_names() {
        let scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "my-org/team",
            "asset.repo_v2",
            Some("feature/branch"),
        );
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let key = ReconstructionCacheKey::latest("my file (1).bin", Some(&scope));
        let redis_key = RedisReconstructionCache::redis_key(&key);
        let owner_hex = hex::encode("my-org/team");
        let repo_hex = hex::encode("asset.repo_v2");
        let rev_hex = hex::encode("feature/branch");
        let file_hex = hex::encode("my file (1).bin");
        assert!(redis_key.contains(&format!(":github:{owner_hex}:{repo_hex}:{rev_hex}:")));
        assert!(redis_key.ends_with(&format!(":latest:{file_hex}")));
    }

    #[test]
    fn redis_key_format_empty_file_id() {
        let key = ReconstructionCacheKey::latest("", None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        assert!(redis_key.ends_with(":latest:"));
    }

    #[test]
    fn redis_key_format_empty_content_hash() {
        let key = ReconstructionCacheKey::version("f", "", None);
        let redis_key = RedisReconstructionCache::redis_key(&key);
        // Empty content hash is still hex-encoded as an empty string.
        let hash_hex = hex::encode("");
        assert!(redis_key.contains(&format!(":{hash_hex}:")));
    }

    // ── Redis URL validation ─────────────────────────────────────────────

    #[test]
    fn redis_cache_rejects_missing_scheme() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("localhost:6379", ttl_seconds);
        assert!(result.is_err());
    }

    #[test]
    fn redis_cache_rejects_invalid_port() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("redis://localhost:abc", ttl_seconds);
        assert!(result.is_err());
    }

    #[test]
    fn redis_cache_rejects_whitespace_only_url() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("   ", ttl_seconds);
        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::EmptyRedisUrl)
        ));
    }

    #[test]
    fn redis_cache_rejects_url_with_only_newline() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("\n", ttl_seconds);
        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::EmptyRedisUrl)
        ));
    }

    #[test]
    fn redis_cache_rejects_url_with_tabs() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("\t\t", ttl_seconds);
        assert!(matches!(
            result,
            Err(crate::ReconstructionCacheError::EmptyRedisUrl)
        ));
    }

    #[test]
    fn redis_cache_accepts_valid_redis_url() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("redis://127.0.0.1:6379", ttl_seconds);
        assert!(result.is_ok());
    }

    #[test]
    fn redis_cache_accepts_unix_socket_url() {
        let ttl_seconds = NonZeroU64::new(3600).unwrap_or(NonZeroU64::MIN);
        let result = RedisReconstructionCache::new("redis+unix:///tmp/redis.sock", ttl_seconds);
        assert!(result.is_ok());
    }

    // ── encode_component ───────────────────────────────────────────────────

    #[test]
    fn encode_component_hex_encodes_input() {
        assert_eq!(super::encode_component("hello"), hex::encode("hello"));
    }

    #[test]
    fn encode_component_handles_special_characters() {
        assert_eq!(
            super::encode_component("file name!"),
            hex::encode("file name!")
        );
        assert_eq!(super::encode_component("a/b:c"), hex::encode("a/b:c"));
    }

    #[test]
    fn encode_component_handles_unicode() {
        assert_eq!(super::encode_component("héllo"), hex::encode("héllo"));
    }

    #[test]
    fn encode_component_empty_string() {
        assert_eq!(super::encode_component(""), hex::encode(""));
    }

    #[test]
    fn encode_component_handles_control_characters_in_string() {
        // encode_component takes &str so we can pass strings with escaped chars
        assert_eq!(super::encode_component("a\tb"), hex::encode("a\tb"));
        assert_eq!(
            super::encode_component("line1\nline2"),
            hex::encode("line1\nline2")
        );
    }

    // ── Clone ────────────────────────────────────────────────────────────

    #[allow(clippy::unwrap_used)]
    #[test]
    fn redis_cache_clone_produces_independent_cache() {
        let ttl = NonZeroU64::new(60).unwrap();
        let cache = RedisReconstructionCache::new("redis://localhost", ttl).unwrap();
        let cloned = cache.clone();
        // Both should have the same client URL (redacted in Debug)
        let debug_original = format!("{cache:?}");
        let debug_cloned = format!("{cloned:?}");
        assert_eq!(debug_original, debug_cloned);
        // Verify ttl is preserved
        assert_eq!(cache.ttl_seconds, cloned.ttl_seconds);
    }

    // ── RedisReconstructionCache::new edge cases ─────────────────────────

    #[allow(clippy::unwrap_used)]
    #[test]
    fn redis_cache_new_with_redis_scheme() {
        let ttl = NonZeroU64::new(60).unwrap();
        assert!(RedisReconstructionCache::new("redis://localhost", ttl).is_ok());
    }

    #[allow(clippy::unwrap_used)]
    #[test]
    fn redis_cache_new_with_unix_scheme() {
        let ttl = NonZeroU64::new(60).unwrap();
        assert!(RedisReconstructionCache::new("redis+unix:///run/redis.sock", ttl).is_ok());
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
