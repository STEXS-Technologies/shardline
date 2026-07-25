use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    thread::available_parallelism,
    time::Duration,
};

use shardline_cache::RedisTlsConfig;
use shardline_protocol::{SecretBytes, SecretString};
use shardline_storage::S3ObjectStoreConfig;
use tracing;

use super::super::secrets::ensure_secret_size_within_limit;
use super::defaults::{
    DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS,
    DEFAULT_OCI_REGISTRY_TOKEN_TTL_SECONDS, DEFAULT_OCI_UPLOAD_MAX_ACTIVE_SESSIONS,
    DEFAULT_OCI_UPLOAD_SESSION_TTL_SECONDS, DEFAULT_PARALLELISM_FALLBACK,
    MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS, MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
    MAX_ED25519_KEY_BYTES, MAX_METRICS_TOKEN_BYTES, MAX_PROVIDER_API_KEY_BYTES,
    MAX_TOKEN_SIGNING_KEY_BYTES, MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS,
    MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
};
use super::enums::{
    AuthConfig, AuthProviderKind, CacheConfig, DeploymentMode, ObjectStorageAdapter, OciConfig,
    ProviderConfig,
};
use super::error::ServerConfigError;
use crate::{
    reconstruction_cache::{
        DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES, DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
        ReconstructionCacheAdapter,
    },
    server_frontend::ServerFrontend,
    server_role::ServerRole,
};

/// Default bounded-parser limits for native Xet shard metadata.
pub use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;

/// Bounded-parser limits for native Xet shard metadata.
pub use shardline_server_core::ShardMetadataLimits;

/// Public server configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct ServerConfig {
    pub(crate) bind_addr: SocketAddr,
    pub(crate) server_role: ServerRole,
    pub(crate) server_frontends: Vec<ServerFrontend>,
    pub(crate) public_base_url: String,
    pub(crate) root_dir: PathBuf,
    pub(crate) object_storage_adapter: ObjectStorageAdapter,
    pub(crate) s3_object_store_config: Option<S3ObjectStoreConfig>,
    pub(crate) max_request_body_bytes: NonZeroUsize,
    pub(crate) shard_metadata_limits: ShardMetadataLimits,
    pub(crate) chunk_size: NonZeroUsize,
    pub(crate) upload_max_in_flight_chunks: NonZeroUsize,
    pub(crate) transfer_max_in_flight_chunks: NonZeroUsize,
    pub(crate) index_postgres_url: Option<SecretString>,
    pub(crate) metrics_token: Option<SecretBytes>,
    pub(crate) deployment_mode: DeploymentMode,
    pub(crate) auth: AuthConfig,
    pub(crate) oci: OciConfig,
    pub(crate) cache: CacheConfig,
    pub(crate) provider: ProviderConfig,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) admission_max_weight: NonZeroUsize,
}

impl ServerConfig {
    /// Creates server configuration.
    #[must_use]
    pub fn new(
        bind_addr: SocketAddr,
        public_base_url: String,
        root_dir: PathBuf,
        chunk_size: NonZeroUsize,
    ) -> Self {
        Self {
            bind_addr,
            server_role: ServerRole::All,
            server_frontends: vec![ServerFrontend::Xet],
            public_base_url,
            root_dir,
            object_storage_adapter: ObjectStorageAdapter::Local,
            s3_object_store_config: None,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            shard_metadata_limits: DEFAULT_SHARD_METADATA_LIMITS,
            chunk_size,
            upload_max_in_flight_chunks: default_upload_max_in_flight_chunks(),
            transfer_max_in_flight_chunks: default_transfer_max_in_flight_chunks(),
            index_postgres_url: None,
            metrics_token: None,
            deployment_mode: DeploymentMode::default(),
            auth: AuthConfig {
                token_signing_key: None,
                auth_provider: AuthProviderKind::Local,
                auth_oidc_issuer: None,
                auth_jwks_url: None,
                auth_jwks_issuer: None,
                ed25519_private_key: None,
                ed25519_public_key: None,
            },
            oci: OciConfig {
                upload_session_ttl_seconds: DEFAULT_OCI_UPLOAD_SESSION_TTL_SECONDS,
                upload_max_active_sessions: DEFAULT_OCI_UPLOAD_MAX_ACTIVE_SESSIONS,
                registry_token_ttl_seconds: DEFAULT_OCI_REGISTRY_TOKEN_TTL_SECONDS,
                registry_token_max_in_flight_requests:
                    DEFAULT_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS,
            },
            cache: CacheConfig {
                adapter: ReconstructionCacheAdapter::Memory,
                ttl_seconds: DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
                memory_max_entries: DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES,
                redis_url: None,
                redis_tls: None,
            },
            provider: ProviderConfig {
                config_path: None,
                api_key: None,
                token_issuer: None,
                token_ttl_seconds: None,
            },
            shutdown_timeout: None,
            admission_max_weight: NonZeroUsize::new(256).unwrap(),
        }
    }

    /// Loads server configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError`] when an environment value cannot be parsed or
    /// when the configured chunk size is zero.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        super::super::env::load_server_config_from_env()
    }

    /// Returns the socket address the server binds to.
    #[must_use]
    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// Returns the configured server role.
    #[must_use]
    pub const fn server_role(&self) -> ServerRole {
        self.server_role
    }

    /// Returns the enabled runtime protocol frontends.
    #[must_use]
    pub fn server_frontends(&self) -> &[ServerFrontend] {
        &self.server_frontends
    }

    /// Returns the public base URL used in reconstruction responses.
    #[must_use]
    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    /// Returns the local deployment root directory.
    #[must_use]
    pub fn root_dir(&self) -> &Path {
        &self.root_dir
    }

    /// Returns the selected object-storage adapter.
    #[must_use]
    pub const fn object_storage_adapter(&self) -> ObjectStorageAdapter {
        self.object_storage_adapter
    }

    /// Returns S3-compatible object-storage configuration when selected.
    #[must_use]
    pub const fn s3_object_store_config(&self) -> Option<&S3ObjectStoreConfig> {
        self.s3_object_store_config.as_ref()
    }

    /// Returns the maximum request body size accepted by body-buffering extractors.
    #[must_use]
    pub const fn max_request_body_bytes(&self) -> NonZeroUsize {
        self.max_request_body_bytes
    }

    /// Returns the bounded-parser limits for native Xet shard metadata.
    #[must_use]
    pub const fn shard_metadata_limits(&self) -> ShardMetadataLimits {
        self.shard_metadata_limits
    }

    /// Returns the content chunk size in bytes.
    #[must_use]
    pub const fn chunk_size(&self) -> NonZeroUsize {
        self.chunk_size
    }

    /// Overrides the content chunk size in bytes.
    #[must_use]
    pub const fn with_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Returns the maximum in-flight upload chunks per file upload stream.
    #[must_use]
    pub const fn upload_max_in_flight_chunks(&self) -> NonZeroUsize {
        self.upload_max_in_flight_chunks
    }

    /// Returns the maximum concurrent transfer budget measured in chunk-equivalent
    /// permits.
    #[must_use]
    pub const fn transfer_max_in_flight_chunks(&self) -> NonZeroUsize {
        self.transfer_max_in_flight_chunks
    }

    /// Returns the selected reconstruction-cache adapter.
    #[must_use]
    pub(crate) const fn reconstruction_cache_adapter(&self) -> ReconstructionCacheAdapter {
        self.cache.adapter
    }

    /// Returns the reconstruction-cache entry TTL in seconds.
    #[must_use]
    pub const fn reconstruction_cache_ttl_seconds(&self) -> NonZeroU64 {
        self.cache.ttl_seconds
    }

    /// Returns the bounded in-memory reconstruction-cache capacity.
    #[must_use]
    pub const fn reconstruction_cache_memory_max_entries(&self) -> NonZeroUsize {
        self.cache.memory_max_entries
    }

    /// Returns the maximum idle lifetime for OCI upload sessions.
    #[must_use]
    pub const fn oci_upload_session_ttl_seconds(&self) -> NonZeroU64 {
        self.oci.upload_session_ttl_seconds
    }

    /// Returns the maximum number of live OCI upload sessions allowed per server root.
    #[must_use]
    pub const fn oci_upload_max_active_sessions(&self) -> NonZeroUsize {
        self.oci.upload_max_active_sessions
    }

    #[must_use]
    pub const fn oci_registry_token_ttl_seconds(&self) -> NonZeroU64 {
        self.oci.registry_token_ttl_seconds
    }

    #[must_use]
    pub const fn oci_registry_token_max_in_flight_requests(&self) -> NonZeroUsize {
        self.oci.registry_token_max_in_flight_requests
    }

    /// Returns the optional Redis URL for the reconstruction cache.
    #[must_use]
    pub fn reconstruction_cache_redis_url(&self) -> Option<&str> {
        self.cache
            .redis_url
            .as_ref()
            .map(SecretString::expose_secret)
    }

    /// Returns optional TLS or mTLS material for the Redis reconstruction cache.
    #[must_use]
    pub const fn reconstruction_cache_redis_tls(&self) -> Option<&RedisTlsConfig> {
        self.cache.redis_tls.as_ref()
    }

    /// Overrides the local deployment root directory.
    #[must_use]
    pub fn with_root_dir(mut self, root_dir: PathBuf) -> Self {
        self.root_dir = root_dir;
        self
    }

    /// Selects object storage for immutable CAS objects.
    #[must_use]
    pub fn with_object_storage(
        mut self,
        adapter: ObjectStorageAdapter,
        s3_config: Option<S3ObjectStoreConfig>,
    ) -> Self {
        self.object_storage_adapter = adapter;
        self.s3_object_store_config = s3_config;
        self
    }

    /// Overrides the maximum request body size accepted by body-buffering extractors.
    #[must_use]
    pub const fn with_max_request_body_bytes(
        mut self,
        max_request_body_bytes: NonZeroUsize,
    ) -> Self {
        self.max_request_body_bytes = max_request_body_bytes;
        self
    }

    /// Overrides bounded-parser limits for native Xet shard metadata.
    #[must_use]
    pub const fn with_shard_metadata_limits(
        mut self,
        shard_metadata_limits: ShardMetadataLimits,
    ) -> Self {
        self.shard_metadata_limits = shard_metadata_limits;
        self
    }

    /// Selects the server role.
    #[must_use]
    pub const fn with_server_role(mut self, server_role: ServerRole) -> Self {
        self.server_role = server_role;
        self
    }

    /// Selects the enabled runtime protocol frontends.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::MissingServerFrontends`] when the supplied
    /// frontend list is empty after deduplication.
    pub fn with_server_frontends(
        mut self,
        server_frontends: impl IntoIterator<Item = ServerFrontend>,
    ) -> Result<Self, ServerConfigError> {
        let server_frontends = deduplicated_server_frontends(server_frontends);
        if server_frontends.is_empty() {
            return Err(ServerConfigError::MissingServerFrontends);
        }

        self.server_frontends = server_frontends;
        Ok(self)
    }

    /// Overrides the per-upload chunk processing window.
    #[must_use]
    pub const fn with_upload_max_in_flight_chunks(
        mut self,
        upload_max_in_flight_chunks: NonZeroUsize,
    ) -> Self {
        self.upload_max_in_flight_chunks = upload_max_in_flight_chunks;
        self
    }

    /// Overrides the transfer concurrency budget measured in chunk-equivalent permits.
    #[must_use]
    pub const fn with_transfer_max_in_flight_chunks(
        mut self,
        transfer_max_in_flight_chunks: NonZeroUsize,
    ) -> Self {
        self.transfer_max_in_flight_chunks = transfer_max_in_flight_chunks;
        self
    }

    /// Selects the disabled reconstruction-cache adapter.
    #[must_use]
    pub fn with_reconstruction_cache_disabled(mut self) -> Self {
        self.cache.adapter = ReconstructionCacheAdapter::Disabled;
        self.cache.redis_url = None;
        self.cache.redis_tls = None;
        self
    }

    /// Selects the bounded in-memory reconstruction-cache adapter.
    #[must_use]
    pub fn with_reconstruction_cache_memory(
        mut self,
        reconstruction_cache_ttl_seconds: NonZeroU64,
        reconstruction_cache_memory_max_entries: NonZeroUsize,
    ) -> Self {
        self.cache.adapter = ReconstructionCacheAdapter::Memory;
        self.cache.ttl_seconds = reconstruction_cache_ttl_seconds;
        self.cache.memory_max_entries = reconstruction_cache_memory_max_entries;
        self.cache.redis_url = None;
        self.cache.redis_tls = None;
        self
    }

    /// Overrides the maximum idle lifetime for OCI upload sessions.
    #[must_use]
    pub const fn with_oci_upload_session_ttl_seconds(
        mut self,
        oci_upload_session_ttl_seconds: NonZeroU64,
    ) -> Self {
        self.oci.upload_session_ttl_seconds = oci_upload_session_ttl_seconds;
        self
    }

    /// Overrides the maximum number of live OCI upload sessions per server root.
    #[must_use]
    pub const fn with_oci_upload_max_active_sessions(
        mut self,
        oci_upload_max_active_sessions: NonZeroUsize,
    ) -> Self {
        self.oci.upload_max_active_sessions = oci_upload_max_active_sessions;
        self
    }

    #[must_use]
    pub const fn with_oci_registry_token_ttl_seconds(
        mut self,
        oci_registry_token_ttl_seconds: NonZeroU64,
    ) -> Self {
        self.oci.registry_token_ttl_seconds = oci_registry_token_ttl_seconds;
        self
    }

    #[must_use]
    pub const fn with_oci_registry_token_max_in_flight_requests(
        mut self,
        oci_registry_token_max_in_flight_requests: NonZeroUsize,
    ) -> Self {
        self.oci.registry_token_max_in_flight_requests = oci_registry_token_max_in_flight_requests;
        self
    }

    /// Selects the Redis reconstruction-cache adapter.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyReconstructionCacheRedisUrl`] when the URL is
    /// empty.
    pub fn with_reconstruction_cache_redis(
        mut self,
        reconstruction_cache_redis_url: String,
        reconstruction_cache_ttl_seconds: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        if reconstruction_cache_redis_url.trim().is_empty() {
            return Err(ServerConfigError::EmptyReconstructionCacheRedisUrl);
        }

        self.cache.adapter = ReconstructionCacheAdapter::Redis;
        self.cache.ttl_seconds = reconstruction_cache_ttl_seconds;
        self.cache.redis_url = Some(SecretString::new(reconstruction_cache_redis_url));
        Ok(self)
    }

    /// Adds custom TLS trust material or a client identity to the Redis cache.
    #[must_use]
    pub fn with_reconstruction_cache_redis_tls(mut self, redis_tls: RedisTlsConfig) -> Self {
        self.cache.redis_tls = Some(redis_tls);
        self
    }

    /// Returns the optional Postgres metadata URL.
    #[must_use]
    pub fn index_postgres_url(&self) -> Option<&str> {
        self.index_postgres_url
            .as_ref()
            .map(SecretString::expose_secret)
    }

    /// Returns the configured auth provider kind.
    #[must_use]
    pub const fn auth_provider(&self) -> AuthProviderKind {
        self.auth.auth_provider
    }

    /// Returns the optional OIDC issuer URL.
    #[must_use]
    pub fn auth_oidc_issuer(&self) -> Option<&str> {
        self.auth.auth_oidc_issuer.as_deref()
    }

    /// Returns the optional JWKS endpoint URL.
    #[must_use]
    pub fn auth_jwks_url(&self) -> Option<&str> {
        self.auth.auth_jwks_url.as_deref()
    }

    /// Returns the optional JWKS issuer for token validation.
    #[must_use]
    pub fn auth_jwks_issuer(&self) -> Option<&str> {
        self.auth.auth_jwks_issuer.as_deref()
    }

    /// Returns the optional token signing key.
    #[must_use]
    pub fn token_signing_key(&self) -> Option<&[u8]> {
        self.auth
            .token_signing_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the optional Ed25519 private key.
    #[must_use]
    pub fn ed25519_private_key(&self) -> Option<&[u8]> {
        self.auth
            .ed25519_private_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the optional Ed25519 public key.
    #[must_use]
    pub fn ed25519_public_key(&self) -> Option<&[u8]> {
        self.auth
            .ed25519_public_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the optional metrics bearer token.
    #[must_use]
    pub fn metrics_token(&self) -> Option<&[u8]> {
        self.metrics_token.as_ref().map(SecretBytes::expose_secret)
    }

    /// Returns the deployment security mode.
    #[must_use]
    pub const fn deployment_mode(&self) -> DeploymentMode {
        self.deployment_mode
    }

    /// Overrides the deployment security mode.
    #[must_use]
    pub const fn with_deployment_mode(mut self, mode: DeploymentMode) -> Self {
        self.deployment_mode = mode;
        self
    }

    /// Returns the optional provider configuration path.
    #[must_use]
    pub fn provider_config_path(&self) -> Option<&Path> {
        self.provider.config_path.as_deref()
    }

    /// Returns the optional provider bootstrap key.
    #[must_use]
    pub fn provider_api_key(&self) -> Option<&[u8]> {
        self.provider
            .api_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the provider token issuer identity when provider issuance is enabled.
    #[must_use]
    pub fn provider_token_issuer(&self) -> Option<&str> {
        self.provider.token_issuer.as_deref()
    }

    /// Returns the provider token lifetime when provider issuance is enabled.
    #[must_use]
    pub const fn provider_token_ttl_seconds(&self) -> Option<NonZeroU64> {
        self.provider.token_ttl_seconds
    }

    /// Returns the graceful shutdown drain timeout.
    ///
    /// When `Some(duration)`, the server waits up to this long for active
    /// connections to finish after a shutdown signal before force-closing.
    /// `None` means wait indefinitely.
    #[must_use]
    pub const fn shutdown_timeout(&self) -> Option<Duration> {
        self.shutdown_timeout
    }

    /// Returns the admission control max weight.
    #[must_use]
    pub const fn admission_max_weight(&self) -> NonZeroUsize {
        self.admission_max_weight
    }

    /// Sets a maximum drain duration after the shutdown signal is received.
    ///
    /// Once the shutdown signal fires, the server stops accepting new
    /// connections and waits up to `timeout` for in-flight requests to
    /// complete.  Connections that outlive the timeout are force-closed.
    #[must_use]
    pub const fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = Some(timeout);
        self
    }

    /// Overrides the admission control max weight.
    #[must_use]
    pub const fn with_admission_max_weight(mut self, max_weight: NonZeroUsize) -> Self {
        self.admission_max_weight = max_weight;
        self
    }

    /// Sets the PostgreSQL connection URL for the index store.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyIndexPostgresUrl`] when the URL is
    /// empty.
    pub fn with_index_postgres_url(
        mut self,
        index_postgres_url: String,
    ) -> Result<Self, ServerConfigError> {
        if index_postgres_url.trim().is_empty() {
            return Err(ServerConfigError::EmptyIndexPostgresUrl);
        }

        self.index_postgres_url = Some(SecretString::new(index_postgres_url));
        Ok(self)
    }

    /// Enables local bearer-token verification with the supplied signing key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyTokenSigningKey`] when the signing key is
    /// empty.
    pub fn with_token_signing_key(
        mut self,
        token_signing_key: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let token_signing_key = token_signing_key.into();
        if token_signing_key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyTokenSigningKey);
        }
        ensure_secret_size_within_limit(
            u64::try_from(token_signing_key.len()).unwrap_or(u64::MAX),
            MAX_TOKEN_SIGNING_KEY_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::TokenSigningKeyTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;

        self.auth.token_signing_key = Some(token_signing_key);
        Ok(self)
    }

    /// Sets the Ed25519 private key for asymmetric token signing.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyEd25519PrivateKey`] when the key is empty.
    pub fn with_ed25519_private_key(
        mut self,
        private_key: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let private_key = private_key.into();
        if private_key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyEd25519PrivateKey);
        }
        ensure_secret_size_within_limit(
            u64::try_from(private_key.len()).unwrap_or(u64::MAX),
            MAX_ED25519_KEY_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::Ed25519PrivateKeyTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;
        self.auth.ed25519_private_key = Some(private_key);
        Ok(self)
    }

    /// Sets the Ed25519 public key for token verification.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyEd25519PublicKey`] when the key is empty.
    pub fn with_ed25519_public_key(
        mut self,
        public_key: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let public_key = public_key.into();
        if public_key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyEd25519PublicKey);
        }
        ensure_secret_size_within_limit(
            u64::try_from(public_key.len()).unwrap_or(u64::MAX),
            MAX_ED25519_KEY_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::Ed25519PublicKeyTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;
        self.auth.ed25519_public_key = Some(public_key);
        Ok(self)
    }

    /// Selects the authentication provider.
    #[must_use]
    pub const fn with_auth_provider(mut self, auth_provider: AuthProviderKind) -> Self {
        self.auth.auth_provider = auth_provider;
        self
    }

    /// Sets the OIDC issuer URL for the OIDC auth provider.
    #[must_use]
    pub fn with_auth_oidc_issuer(mut self, issuer: String) -> Self {
        self.auth.auth_oidc_issuer = Some(issuer);
        self
    }

    /// Sets the JWKS endpoint URL for the JWKS auth provider.
    #[must_use]
    pub fn with_auth_jwks_url(mut self, url: String) -> Self {
        self.auth.auth_jwks_url = Some(url);
        self
    }

    /// Sets the JWKS issuer for token validation.
    #[must_use]
    pub fn with_auth_jwks_issuer(mut self, issuer: String) -> Self {
        self.auth.auth_jwks_issuer = Some(issuer);
        self
    }

    /// Enables bearer-token verification for `/metrics`.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyMetricsToken`] when the metrics token is
    /// empty.
    pub fn with_metrics_token(
        mut self,
        metrics_token: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let metrics_token = metrics_token.into();
        if metrics_token.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyMetricsToken);
        }
        ensure_secret_size_within_limit(
            u64::try_from(metrics_token.len()).unwrap_or(u64::MAX),
            MAX_METRICS_TOKEN_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::MetricsTokenTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;

        self.metrics_token = Some(metrics_token);
        Ok(self)
    }

    /// Enables the provider-facing token issuance surface.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError`] when the provider bootstrap key or issuer
    /// identity is empty, or when token signing is not configured.
    pub fn with_provider_runtime(
        mut self,
        provider_config_path: PathBuf,
        provider_api_key: impl Into<SecretBytes>,
        issuer_identity: String,
        ttl_seconds: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        let provider_api_key = provider_api_key.into();
        if provider_api_key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyProviderApiKey);
        }
        ensure_secret_size_within_limit(
            u64::try_from(provider_api_key.len()).unwrap_or(u64::MAX),
            MAX_PROVIDER_API_KEY_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::ProviderApiKeyTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;
        if issuer_identity.trim().is_empty() {
            return Err(ServerConfigError::EmptyProviderTokenIssuer);
        }
        if self.auth.token_signing_key.is_none() {
            return Err(ServerConfigError::ProviderTokensRequireSigningKey);
        }

        self.provider.config_path = Some(provider_config_path);
        self.provider.api_key = Some(provider_api_key);
        self.provider.token_issuer = Some(issuer_identity);
        self.provider.token_ttl_seconds = Some(ttl_seconds);
        Ok(self)
    }

    /// Validates runtime requirements implied by the selected route surface and
    /// deployment mode.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::MissingTokenSigningKeyForServedRoutes`] when the
    /// selected role uses the local HMAC provider and would expose authenticated
    /// CAS routes without a signing key.
    ///
    /// Returns [`ServerConfigError::ConfigFileError`] when the deployment mode
    /// constraints are not satisfied.
    pub fn validate_runtime_requirements(&self) -> Result<(), ServerConfigError> {
        if self.auth.token_signing_key.is_none()
            && (self.server_role.serves_api() || self.server_role.serves_transfer())
            && matches!(self.auth.auth_provider, AuthProviderKind::Local)
            && self.deployment_mode != DeploymentMode::Insecure
        {
            return Err(ServerConfigError::MissingTokenSigningKeyForServedRoutes);
        }

        // PassthroughProvider trusts every inbound token and grants full
        // CAS write access.  It MUST NOT bind to a non-loopback interface.
        if matches!(self.auth.auth_provider, AuthProviderKind::Passthrough)
            && !self.bind_addr.ip().is_loopback()
        {
            return Err(ServerConfigError::PassthroughProviderRequiresLoopbackBind {
                bind_addr: self.bind_addr,
            });
        }

        if matches!(self.auth.auth_provider, AuthProviderKind::Ed25519) {
            match (
                self.auth.ed25519_private_key.is_some(),
                self.auth.ed25519_public_key.is_some(),
            ) {
                (false, false) => return Err(ServerConfigError::MissingEd25519Key),
                (true, true) => return Err(ServerConfigError::ConflictingEd25519Keys),
                (true, false) | (false, true) => {}
            }
        }

        self.validate_deployment_mode_requirements()?;

        Ok(())
    }

    /// Validates deployment-mode-specific constraints.
    fn validate_deployment_mode_requirements(&self) -> Result<(), ServerConfigError> {
        match self.deployment_mode {
            DeploymentMode::Strict => {
                // Passthrough auth is forbidden in strict mode
                if self.auth.auth_provider == AuthProviderKind::Passthrough {
                    return Err(ServerConfigError::ConfigFileError(
                        "strict deployment mode does not allow passthrough auth provider".into(),
                    ));
                }
                // Signing key is required for token minting
                if self.token_signing_key().is_none() {
                    return Err(ServerConfigError::ConfigFileError(
                        "strict deployment mode requires a token signing key".into(),
                    ));
                }
                // Metrics token should be configured
                if self.metrics_token().is_none() {
                    tracing::warn!(
                        "strict deployment mode recommends configuring SHARDLINE_METRICS_TOKEN_FILE"
                    );
                }
            }
            DeploymentMode::Authenticated => {
                // Some auth provider must be configured (not None)
                if self.auth.auth_provider == AuthProviderKind::Passthrough {
                    // Passthrough is allowed in authenticated mode but warn
                    tracing::warn!(
                        "authenticated mode with passthrough auth: only use behind a trusted proxy"
                    );
                }
            }
            DeploymentMode::Insecure => {
                // Allow everything — warn that this is not for production
                tracing::warn!(
                    "insecure deployment mode: all requests are allowed without authentication"
                );
            }
        }
        Ok(())
    }
}

/// Returns the adaptive default upload chunk parallelism for the current host.
#[must_use]
pub fn default_upload_max_in_flight_chunks() -> NonZeroUsize {
    adaptive_default_in_flight_chunks(
        2,
        MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
    )
}

/// Returns the adaptive default transfer budget for the current host.
#[must_use]
pub fn default_transfer_max_in_flight_chunks() -> NonZeroUsize {
    adaptive_default_in_flight_chunks(
        8,
        MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS,
        MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS,
    )
}

pub(crate) fn deduplicated_server_frontends(
    server_frontends: impl IntoIterator<Item = ServerFrontend>,
) -> Vec<ServerFrontend> {
    let mut deduplicated = Vec::new();
    for frontend in server_frontends {
        if !deduplicated.contains(&frontend) {
            deduplicated.push(frontend);
        }
    }
    deduplicated
}

fn adaptive_default_in_flight_chunks(
    multiplier: usize,
    minimum: NonZeroUsize,
    maximum: NonZeroUsize,
) -> NonZeroUsize {
    let parallelism = available_parallelism().unwrap_or(DEFAULT_PARALLELISM_FALLBACK);
    adaptive_default_in_flight_chunks_for_parallelism(
        parallelism.get(),
        multiplier,
        minimum,
        maximum,
    )
}

pub(crate) fn adaptive_default_in_flight_chunks_for_parallelism(
    parallelism: usize,
    multiplier: usize,
    minimum: NonZeroUsize,
    maximum: NonZeroUsize,
) -> NonZeroUsize {
    let scaled = parallelism.saturating_mul(multiplier);
    let bounded = scaled.clamp(minimum.get(), maximum.get());
    NonZeroUsize::new(bounded).unwrap_or(minimum)
}
