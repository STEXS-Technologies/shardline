#[cfg(test)]
use std::sync::{LazyLock, Mutex};
use std::{
    fmt,
    io::Error as IoError,
    net::{AddrParseError, SocketAddr},
    num::{NonZeroU64, NonZeroUsize, ParseIntError},
    path::{Path, PathBuf},
    thread::available_parallelism,
};

use shardline_protocol::{SecretBytes, SecretString};
use shardline_storage::S3ObjectStoreConfig;
use thiserror::Error;

use super::secrets::ensure_secret_size_within_limit;
use crate::{
    reconstruction_cache::{
        DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES, DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
        ReconstructionCacheAdapter,
    },
    server_frontend::ServerFrontend,
    server_role::ServerRole,
};

/// Authentication configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct AuthConfig {
    pub token_signing_key: Option<SecretBytes>,
    pub auth_provider: AuthProviderKind,
    pub auth_oidc_issuer: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_jwks_issuer: Option<String>,
}

/// OCI registry configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct OciConfig {
    pub upload_session_ttl_seconds: NonZeroU64,
    pub upload_max_active_sessions: NonZeroUsize,
    pub registry_token_ttl_seconds: NonZeroU64,
    pub registry_token_max_in_flight_requests: NonZeroUsize,
}

/// Reconstruction cache configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct CacheConfig {
    pub adapter: ReconstructionCacheAdapter,
    pub ttl_seconds: NonZeroU64,
    pub memory_max_entries: NonZeroUsize,
    pub redis_url: Option<SecretString>,
}

/// Provider token issuance configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderConfig {
    pub config_path: Option<PathBuf>,
    pub api_key: Option<SecretBytes>,
    pub token_issuer: Option<String>,
    pub token_ttl_seconds: Option<NonZeroU64>,
}

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
    pub(crate) auth: AuthConfig,
    pub(crate) oci: OciConfig,
    pub(crate) cache: CacheConfig,
    pub(crate) provider: ProviderConfig,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("bind_addr", &self.bind_addr)
            .field("server_role", &self.server_role)
            .field("server_frontends", &self.server_frontends)
            .field("public_base_url", &self.public_base_url)
            .field("root_dir", &self.root_dir)
            .field("object_storage_adapter", &self.object_storage_adapter)
            .field("s3_object_store_config", &self.s3_object_store_config)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .field("shard_metadata_limits", &self.shard_metadata_limits)
            .field("chunk_size", &self.chunk_size)
            .field(
                "upload_max_in_flight_chunks",
                &self.upload_max_in_flight_chunks,
            )
            .field(
                "transfer_max_in_flight_chunks",
                &self.transfer_max_in_flight_chunks,
            )
            .field(
                "index_postgres_url",
                &self.index_postgres_url.as_ref().map(|_url| "***"),
            )
            .field(
                "metrics_token",
                &self.metrics_token.as_ref().map(|_token| "***"),
            )
            .field("auth", &self.auth)
            .field("oci", &self.oci)
            .field("cache", &self.cache)
            .field("provider", &self.provider)
            .finish()
    }
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthConfig")
            .field(
                "token_signing_key",
                &self.token_signing_key.as_ref().map(|_key| "***"),
            )
            .field("auth_provider", &self.auth_provider)
            .field("auth_oidc_issuer", &self.auth_oidc_issuer)
            .field("auth_jwks_url", &self.auth_jwks_url)
            .field("auth_jwks_issuer", &self.auth_jwks_issuer)
            .finish()
    }
}

impl fmt::Debug for OciConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OciConfig")
            .field(
                "upload_session_ttl_seconds",
                &self.upload_session_ttl_seconds,
            )
            .field(
                "upload_max_active_sessions",
                &self.upload_max_active_sessions,
            )
            .field(
                "registry_token_ttl_seconds",
                &self.registry_token_ttl_seconds,
            )
            .field(
                "registry_token_max_in_flight_requests",
                &self.registry_token_max_in_flight_requests,
            )
            .finish()
    }
}

impl fmt::Debug for CacheConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CacheConfig")
            .field("adapter", &self.adapter)
            .field("ttl_seconds", &self.ttl_seconds)
            .field("memory_max_entries", &self.memory_max_entries)
            .field("redis_url", &self.redis_url.as_ref().map(|_url| "***"))
            .finish()
    }
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("config_path", &self.config_path)
            .field("api_key", &self.api_key.as_ref().map(|_key| "***"))
            .field("token_issuer", &self.token_issuer)
            .field("token_ttl_seconds", &self.token_ttl_seconds)
            .finish()
    }
}

pub(crate) const MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(64) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(1024) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(64)
{
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize =
    match NonZeroUsize::new(256) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const DEFAULT_MAX_REQUEST_BODY_BYTES: NonZeroUsize = match NonZeroUsize::new(67_108_864)
{
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_FILES: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_XORBS: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

pub(crate) const DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS: NonZeroUsize =
    match NonZeroUsize::new(65_536) {
        Some(value) => value,
        None => NonZeroUsize::MIN,
    };

pub(crate) const DEFAULT_MAX_SHARD_XORB_CHUNKS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
pub(crate) const MAX_TOKEN_SIGNING_KEY_BYTES: u64 = 1_048_576;
pub(crate) const MAX_PROVIDER_API_KEY_BYTES: u64 = 4096;
pub(crate) const MAX_METRICS_TOKEN_BYTES: u64 = 4096;
pub(crate) const MAX_S3_CREDENTIAL_BYTES: u64 = 4096;
const DEFAULT_PARALLELISM_FALLBACK: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
const DEFAULT_OCI_UPLOAD_SESSION_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(3_600) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};
const DEFAULT_OCI_UPLOAD_MAX_ACTIVE_SESSIONS: NonZeroUsize = match NonZeroUsize::new(1_024) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
const DEFAULT_OCI_REGISTRY_TOKEN_TTL_SECONDS: NonZeroU64 = match NonZeroU64::new(300) {
    Some(value) => value,
    None => NonZeroU64::MIN,
};
const DEFAULT_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS: NonZeroUsize = match NonZeroUsize::new(64)
{
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

#[cfg(test)]
pub(crate) type SecretFileReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
pub(crate) struct SecretFileReadHookRegistration {
    pub(crate) path: PathBuf,
    pub(crate) hook: SecretFileReadHook,
}

#[cfg(test)]
pub(crate) type SecretFileReadHookSlot = Vec<SecretFileReadHookRegistration>;

#[cfg(test)]
pub(crate) static BEFORE_SECRET_FILE_READ_HOOK: LazyLock<Mutex<SecretFileReadHookSlot>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Default bounded-parser limits for native Xet shard metadata.
pub use shardline_server_core::DEFAULT_SHARD_METADATA_LIMITS;

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
            auth: AuthConfig {
                token_signing_key: None,
                auth_provider: AuthProviderKind::Local,
                auth_oidc_issuer: None,
                auth_jwks_url: None,
                auth_jwks_issuer: None,
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
            },
            provider: ProviderConfig {
                config_path: None,
                api_key: None,
                token_issuer: None,
                token_ttl_seconds: None,
            },
        }
    }

    /// Loads server configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError`] when an environment value cannot be parsed or
    /// when the configured chunk size is zero.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        super::env::load_server_config_from_env()
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

    /// Returns the optional metrics bearer token.
    #[must_use]
    pub fn metrics_token(&self) -> Option<&[u8]> {
        self.metrics_token.as_ref().map(SecretBytes::expose_secret)
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
        token_signing_key: Vec<u8>,
    ) -> Result<Self, ServerConfigError> {
        if token_signing_key.is_empty() {
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

        self.auth.token_signing_key = Some(SecretBytes::new(token_signing_key));
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
    pub fn with_metrics_token(mut self, metrics_token: Vec<u8>) -> Result<Self, ServerConfigError> {
        if metrics_token.is_empty() {
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

        self.metrics_token = Some(SecretBytes::new(metrics_token));
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
        provider_api_key: Vec<u8>,
        issuer_identity: String,
        ttl_seconds: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        if provider_api_key.is_empty() {
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
        self.provider.api_key = Some(SecretBytes::new(provider_api_key));
        self.provider.token_issuer = Some(issuer_identity);
        self.provider.token_ttl_seconds = Some(ttl_seconds);
        Ok(self)
    }

    /// Validates runtime requirements implied by the selected route surface.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::MissingTokenSigningKeyForServedRoutes`] when the
    /// selected role would expose authenticated CAS routes without a signing key.
    pub const fn validate_runtime_requirements(&self) -> Result<(), ServerConfigError> {
        if self.auth.token_signing_key.is_none()
            && (self.server_role.serves_api() || self.server_role.serves_transfer())
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

fn deduplicated_server_frontends(
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

/// Bounded-parser limits for native Xet shard metadata.
pub use shardline_server_core::ShardMetadataLimits;

/// Authentication provider selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthProviderKind {
    /// Local shared-key HMAC-SHA256 signing (default).
    Local,
    /// OpenID Connect issuer validation.
    Oidc,
    /// Static JWKS endpoint validation.
    Jwks,
    /// Trust-all passthrough for development mode.
    Passthrough,
}

impl AuthProviderKind {
    /// Parses an auth provider token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::InvalidAuthProvider`] when the token is not
    /// supported.
    pub fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "local" => Ok(Self::Local),
            "oidc" => Ok(Self::Oidc),
            "jwks" => Ok(Self::Jwks),
            "passthrough" => Ok(Self::Passthrough),
            _other => Err(ServerConfigError::InvalidAuthProvider),
        }
    }
}

/// Immutable object-storage adapter selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStorageAdapter {
    /// Store immutable CAS objects on the local filesystem.
    Local,
    /// Store immutable CAS objects in an S3-compatible bucket.
    S3,
}

impl ObjectStorageAdapter {
    /// Parses an object-storage adapter token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::InvalidObjectStorageAdapter`] when the token is not
    /// supported.
    pub fn parse(value: &str) -> Result<Self, ServerConfigError> {
        match value {
            "local" => Ok(Self::Local),
            "s3" => Ok(Self::S3),
            _other => Err(ServerConfigError::InvalidObjectStorageAdapter),
        }
    }
}

/// Server configuration loading failure.
#[derive(Debug, Error)]
pub enum ServerConfigError {
    /// The bind address could not be parsed.
    #[error("invalid bind address")]
    BindAddress(#[from] AddrParseError),
    /// The local deployment root contained an invalid filesystem component.
    #[error("invalid local deployment root")]
    RootDir(#[source] IoError),
    /// The server role token was invalid.
    #[error("invalid server role")]
    InvalidServerRole,
    /// The server frontend token was invalid.
    #[error("invalid server frontend")]
    InvalidServerFrontend,
    /// The configured server frontend set was empty.
    #[error("at least one server frontend must be enabled")]
    MissingServerFrontends,
    /// The object-storage adapter token was invalid.
    #[error("invalid object storage adapter")]
    InvalidObjectStorageAdapter,
    /// The auth provider token was invalid.
    #[error("invalid auth provider")]
    InvalidAuthProvider,
    /// S3 object storage was selected without a bucket.
    #[error("s3 object storage requires SHARDLINE_S3_BUCKET")]
    MissingS3Bucket,
    /// S3 object storage was selected with an invalid allow-http flag.
    #[error("invalid s3 allow-http flag")]
    InvalidS3AllowHttp,
    /// S3 object storage was selected with an invalid virtual-hosted-style flag.
    #[error("invalid s3 virtual-hosted-style request flag")]
    InvalidS3VirtualHostedStyleRequest,
    /// An S3 credential was provided through both direct env and file indirection.
    #[error("s3 credential source conflict: both {env} and {file_env} are set")]
    S3CredentialSourceConflict {
        /// Direct environment variable name.
        env: &'static str,
        /// File-indirection environment variable name.
        file_env: &'static str,
    },
    /// An S3 credential file could not be read.
    #[error("s3 credential file {name} could not be read")]
    S3CredentialFile {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Underlying filesystem failure.
        #[source]
        source: IoError,
    },
    /// An S3 credential file exceeded the bounded parser ceiling.
    #[error("s3 credential file {name} exceeded the bounded parser ceiling")]
    S3CredentialTooLarge {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// An S3 credential file changed after validation and was rejected.
    #[error("s3 credential file {name} changed during bounded read")]
    S3CredentialLengthMismatch {
        /// Credential file-indirection environment variable name.
        name: &'static str,
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// An S3 credential file was not valid UTF-8.
    #[error("s3 credential file {name} was not valid utf-8")]
    S3CredentialUtf8 {
        /// Credential file-indirection environment variable name.
        name: &'static str,
    },
    /// The chunk size could not be parsed.
    #[error("invalid chunk size")]
    ChunkSize(#[from] ParseIntError),
    /// The maximum request body size could not be parsed.
    #[error("invalid max request body size")]
    MaxRequestBodyBytes(ParseIntError),
    /// The maximum request body size was zero.
    #[error("max request body size must be greater than zero")]
    ZeroMaxRequestBodyBytes,
    /// The maximum shard file section count could not be parsed.
    #[error("invalid max shard file section count")]
    MaxShardFiles(ParseIntError),
    /// The maximum shard file section count was zero.
    #[error("max shard file section count must be greater than zero")]
    ZeroMaxShardFiles,
    /// The maximum shard xorb section count could not be parsed.
    #[error("invalid max shard xorb section count")]
    MaxShardXorbs(ParseIntError),
    /// The maximum shard xorb section count was zero.
    #[error("max shard xorb section count must be greater than zero")]
    ZeroMaxShardXorbs,
    /// The maximum shard reconstruction term count could not be parsed.
    #[error("invalid max shard reconstruction term count")]
    MaxShardReconstructionTerms(ParseIntError),
    /// The maximum shard reconstruction term count was zero.
    #[error("max shard reconstruction term count must be greater than zero")]
    ZeroMaxShardReconstructionTerms,
    /// The maximum shard xorb chunk record count could not be parsed.
    #[error("invalid max shard xorb chunk record count")]
    MaxShardXorbChunks(ParseIntError),
    /// The maximum shard xorb chunk record count was zero.
    #[error("max shard xorb chunk record count must be greater than zero")]
    ZeroMaxShardXorbChunks,
    /// The chunk size was zero.
    #[error("chunk size must be greater than zero")]
    ZeroChunkSize,
    /// The per-upload chunk processing window could not be parsed.
    #[error("invalid upload max in-flight chunks")]
    UploadMaxInFlightChunks(ParseIntError),
    /// The per-upload chunk processing window was zero.
    #[error("upload max in-flight chunks must be greater than zero")]
    ZeroUploadMaxInFlightChunks,
    /// The transfer concurrency budget could not be parsed.
    #[error("invalid transfer max in-flight chunks")]
    TransferMaxInFlightChunks(ParseIntError),
    /// The transfer concurrency budget was zero.
    #[error("transfer max in-flight chunks must be greater than zero")]
    ZeroTransferMaxInFlightChunks,
    /// The reconstruction-cache adapter token was invalid.
    #[error("invalid reconstruction cache adapter")]
    InvalidReconstructionCacheAdapter,
    /// The reconstruction-cache TTL could not be parsed.
    #[error("invalid reconstruction cache ttl")]
    ReconstructionCacheTtl(ParseIntError),
    /// The reconstruction-cache TTL was zero.
    #[error("reconstruction cache ttl must be greater than zero")]
    ZeroReconstructionCacheTtlSeconds,
    /// The in-memory reconstruction-cache capacity could not be parsed.
    #[error("invalid reconstruction cache memory max entries")]
    ReconstructionCacheMemoryMaxEntries(ParseIntError),
    /// The in-memory reconstruction-cache capacity was zero.
    #[error("reconstruction cache memory max entries must be greater than zero")]
    ZeroReconstructionCacheMemoryMaxEntries,
    /// The OCI upload-session TTL could not be parsed.
    #[error("invalid oci upload session ttl")]
    OciUploadSessionTtl(ParseIntError),
    /// The OCI upload-session TTL was zero.
    #[error("oci upload session ttl must be greater than zero")]
    ZeroOciUploadSessionTtlSeconds,
    /// The OCI upload live-session ceiling could not be parsed.
    #[error("invalid oci upload max active sessions")]
    OciUploadMaxActiveSessions(ParseIntError),
    /// The OCI upload live-session ceiling was zero.
    #[error("oci upload max active sessions must be greater than zero")]
    ZeroOciUploadMaxActiveSessions,
    /// The OCI registry token TTL could not be parsed.
    #[error("invalid oci registry token ttl")]
    OciRegistryTokenTtl(ParseIntError),
    /// The OCI registry token TTL was zero.
    #[error("oci registry token ttl must be greater than zero")]
    ZeroOciRegistryTokenTtlSeconds,
    /// The OCI registry token in-flight request ceiling could not be parsed.
    #[error("invalid oci registry token max in-flight requests")]
    OciRegistryTokenMaxInFlightRequests(ParseIntError),
    /// The OCI registry token in-flight request ceiling was zero.
    #[error("oci registry token max in-flight requests must be greater than zero")]
    ZeroOciRegistryTokenMaxInFlightRequests,
    /// The Redis reconstruction-cache URL was empty.
    #[error("reconstruction cache redis url must not be empty")]
    EmptyReconstructionCacheRedisUrl,
    /// Redis reconstruction-cache configuration was incomplete.
    #[error("redis reconstruction cache requires SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL")]
    MissingReconstructionCacheRedisUrl,
    /// The Postgres metadata URL was empty.
    #[error("postgres metadata url must not be empty")]
    EmptyIndexPostgresUrl,
    /// The token signing key file could not be read.
    #[error("token signing key could not be read")]
    TokenSigningKey(#[source] IoError),
    /// The token signing key was provided through both direct env and file indirection.
    #[error("token signing key source conflict: both {env} and {file_env} are set")]
    TokenSigningKeySourceConflict {
        /// Direct environment variable name.
        env: &'static str,
        /// File-indirection environment variable name.
        file_env: &'static str,
    },
    /// The token signing key exceeded the bounded parser ceiling.
    #[error("token signing key exceeded the bounded parser ceiling")]
    TokenSigningKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The token signing key changed after validation and was rejected.
    #[error("token signing key changed during bounded read")]
    TokenSigningKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The provider token TTL could not be parsed.
    #[error("invalid provider token ttl")]
    ProviderTokenTtl,
    /// The token signing key was empty.
    #[error("token signing key must not be empty")]
    EmptyTokenSigningKey,
    /// The metrics token file could not be read.
    #[error("metrics token could not be read")]
    MetricsToken(#[source] IoError),
    /// The metrics bearer token was empty.
    #[error("metrics token must not be empty")]
    EmptyMetricsToken,
    /// The metrics bearer token exceeded the bounded parser ceiling.
    #[error("metrics token exceeded the bounded parser ceiling")]
    MetricsTokenTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The metrics bearer token changed after validation and was rejected.
    #[error("metrics token changed during bounded read")]
    MetricsTokenLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The selected role would expose CAS routes without bearer-token verification.
    #[error("served shardline routes require shardline token signing key configuration")]
    MissingTokenSigningKeyForServedRoutes,
    /// The provider bootstrap key was empty.
    #[error("provider bootstrap key must not be empty")]
    EmptyProviderApiKey,
    /// The provider bootstrap key file could not be read.
    #[error("provider bootstrap key could not be read")]
    ProviderApiKey(#[source] IoError),
    /// The provider bootstrap key exceeded the bounded parser ceiling.
    #[error("provider bootstrap key exceeded the bounded parser ceiling")]
    ProviderApiKeyTooLarge {
        /// Observed secret file length in bytes.
        observed_bytes: u64,
        /// Maximum accepted secret file length in bytes.
        maximum_bytes: u64,
    },
    /// The provider bootstrap key changed after validation and was rejected.
    #[error("provider bootstrap key changed during bounded read")]
    ProviderApiKeyLengthMismatch {
        /// Validated secret file length in bytes.
        expected_bytes: u64,
        /// Observed secret file length in bytes after bounded read.
        observed_bytes: u64,
    },
    /// The provider token issuer was empty.
    #[error("provider token issuer must not be empty")]
    EmptyProviderTokenIssuer,
    /// The provider token TTL was zero.
    #[error("provider token ttl must be greater than zero")]
    ZeroProviderTokenTtl,
    /// Provider token issuance was only partially configured.
    #[error("provider token issuance requires both provider config and provider api key files")]
    IncompleteProviderTokenConfig,
    /// Provider token issuance needs the CAS signing key.
    #[error("provider token issuance requires shardline token signing key configuration")]
    ProviderTokensRequireSigningKey,
    /// The chunk size exceeds the maximum allowed value.
    #[error("chunk size must not exceed 1 GB")]
    ChunkSizeTooLarge,
    /// The public base URL is not a valid URL.
    #[error("SHARDLINE_PUBLIC_BASE_URL is not a valid URL: {0}")]
    InvalidPublicBaseUrl(String),
    /// OIDC auth provider requires an issuer URL.
    #[error("oidc auth provider requires SHARDLINE_AUTH_OIDC_ISSUER")]
    MissingOidcIssuer,
    /// JWKS auth provider requires a JWKS URL.
    #[error("jwks auth provider requires SHARDLINE_AUTH_JWKS_URL")]
    MissingJwksUrl,
    /// Hub frontend requires an auth provider to be configured.
    #[error(
        "hub frontend requires auth configuration (SHARDLINE_AUTH_PROVIDER with token signing key or oidc/jwks)"
    )]
    HubRequiresAuth,
    /// Passthrough auth provider requires a loopback bind address.
    #[error("passthrough auth provider requires a loopback bind address, got {bind_addr}")]
    PassthroughProviderRequiresLoopbackBind {
        /// The rejected bind address.
        bind_addr: SocketAddr,
    },
}

#[cfg(test)]
pub fn run_before_secret_file_read_hook_for_tests(path: &Path) {
    let hook = match BEFORE_SECRET_FILE_READ_HOOK.lock() {
        Ok(mut guard) => take_secret_file_read_hook_for_path(&mut guard, path),
        Err(poisoned) => take_secret_file_read_hook_for_path(&mut poisoned.into_inner(), path),
    };

    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(test)]
fn take_secret_file_read_hook_for_path(
    slot: &mut SecretFileReadHookSlot,
    path: &Path,
) -> Option<SecretFileReadHook> {
    let index = slot
        .iter()
        .position(|registration| registration.path == path)?;
    Some(slot.remove(index).hook)
}

#[cfg(test)]
mod config_types_tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;

    #[test]
    fn auth_provider_kind_parse_local() {
        assert_eq!(
            AuthProviderKind::parse("local").unwrap(),
            AuthProviderKind::Local
        );
    }

    #[test]
    fn auth_provider_kind_parse_oidc() {
        assert_eq!(
            AuthProviderKind::parse("oidc").unwrap(),
            AuthProviderKind::Oidc
        );
    }

    #[test]
    fn auth_provider_kind_parse_jwks() {
        assert_eq!(
            AuthProviderKind::parse("jwks").unwrap(),
            AuthProviderKind::Jwks
        );
    }

    #[test]
    fn auth_provider_kind_parse_passthrough() {
        assert_eq!(
            AuthProviderKind::parse("passthrough").unwrap(),
            AuthProviderKind::Passthrough
        );
    }

    #[test]
    fn auth_provider_kind_parse_rejects_unknown() {
        assert!(matches!(
            AuthProviderKind::parse("unknown"),
            Err(ServerConfigError::InvalidAuthProvider)
        ));
    }

    #[test]
    fn object_storage_adapter_parse_local() {
        assert_eq!(
            ObjectStorageAdapter::parse("local").unwrap(),
            ObjectStorageAdapter::Local
        );
    }

    #[test]
    fn object_storage_adapter_parse_s3() {
        assert_eq!(
            ObjectStorageAdapter::parse("s3").unwrap(),
            ObjectStorageAdapter::S3
        );
    }

    #[test]
    fn object_storage_adapter_parse_rejects_unknown() {
        assert!(matches!(
            ObjectStorageAdapter::parse("unknown"),
            Err(ServerConfigError::InvalidObjectStorageAdapter)
        ));
    }

    #[test]
    fn server_config_new_constructs_with_defaults() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        assert_eq!(config.bind_addr.port(), 8080);
        assert_eq!(config.public_base_url, "http://localhost:8080");
        assert_eq!(config.server_role, ServerRole::All);
        assert_eq!(config.server_frontends, vec![ServerFrontend::Xet]);
        assert_eq!(config.object_storage_adapter, ObjectStorageAdapter::Local);
        assert!(config.s3_object_store_config.is_none());
        assert_eq!(config.chunk_size, NonZeroUsize::new(4096).unwrap());
        assert_eq!(
            config.reconstruction_cache_adapter(),
            ReconstructionCacheAdapter::Memory
        );
        assert_eq!(config.auth_provider(), AuthProviderKind::Local);
        assert!(config.token_signing_key().is_none());
    }

    #[test]
    fn server_config_builder_with_chunk_size() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(1024).unwrap(),
        )
        .with_chunk_size(NonZeroUsize::new(8192).unwrap());
        assert_eq!(config.chunk_size(), NonZeroUsize::new(8192).unwrap());
    }

    #[test]
    fn server_config_builder_with_server_role() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_server_role(ServerRole::Transfer);
        assert_eq!(config.server_role(), ServerRole::Transfer);
    }

    #[test]
    fn server_config_builder_with_root_dir() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_root_dir(PathBuf::from("/var/lib/shardline"));
        assert_eq!(config.root_dir(), Path::new("/var/lib/shardline"));
    }

    #[test]
    fn server_config_builder_with_object_storage() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_object_storage(ObjectStorageAdapter::S3, None);
        assert_eq!(
            config.object_storage_adapter(),
            ObjectStorageAdapter::S3
        );
    }

    #[test]
    fn server_config_builder_with_max_request_body_bytes() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_max_request_body_bytes(NonZeroUsize::new(1_000_000).unwrap());
        assert_eq!(
            config.max_request_body_bytes(),
            NonZeroUsize::new(1_000_000).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_shard_metadata_limits() {
        let limits = ShardMetadataLimits::new(
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(100).unwrap(),
            NonZeroUsize::new(500).unwrap(),
            NonZeroUsize::new(500).unwrap(),
        );
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_shard_metadata_limits(limits);
        assert_eq!(config.shard_metadata_limits(), limits);
    }

    #[test]
    fn server_config_builder_with_server_frontends_rejects_empty() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_server_frontends([]);
        assert!(matches!(
            result,
            Err(ServerConfigError::MissingServerFrontends)
        ));
    }

    #[test]
    fn server_config_builder_with_server_frontends_deduplicates() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_server_frontends([ServerFrontend::Xet, ServerFrontend::Xet])
        .unwrap();
        assert_eq!(config.server_frontends(), &[ServerFrontend::Xet]);
    }

    #[test]
    fn server_config_builder_with_upload_max_in_flight_chunks() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_upload_max_in_flight_chunks(NonZeroUsize::new(128).unwrap());
        assert_eq!(
            config.upload_max_in_flight_chunks(),
            NonZeroUsize::new(128).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_transfer_max_in_flight_chunks() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_transfer_max_in_flight_chunks(NonZeroUsize::new(256).unwrap());
        assert_eq!(
            config.transfer_max_in_flight_chunks(),
            NonZeroUsize::new(256).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_reconstruction_cache_disabled() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_reconstruction_cache_disabled();
        assert_eq!(
            config.reconstruction_cache_adapter(),
            ReconstructionCacheAdapter::Disabled
        );
    }

    #[test]
    fn server_config_builder_with_reconstruction_cache_memory() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_reconstruction_cache_memory(
            NonZeroU64::new(300).unwrap(),
            NonZeroUsize::new(5000).unwrap(),
        );
        assert_eq!(
            config.reconstruction_cache_adapter(),
            ReconstructionCacheAdapter::Memory
        );
        assert_eq!(
            config.reconstruction_cache_ttl_seconds(),
            NonZeroU64::new(300).unwrap()
        );
        assert_eq!(
            config.reconstruction_cache_memory_max_entries(),
            NonZeroUsize::new(5000).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_reconstruction_cache_redis_rejects_empty_url() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result =
            config.with_reconstruction_cache_redis(String::new(), NonZeroU64::new(300).unwrap());
        assert!(matches!(
            result,
            Err(ServerConfigError::EmptyReconstructionCacheRedisUrl)
        ));
    }

    #[test]
    fn server_config_builder_with_reconstruction_cache_redis_accepts_url() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_reconstruction_cache_redis(
            "redis://localhost:6379".to_owned(),
            NonZeroU64::new(300).unwrap(),
        )
        .unwrap();
        assert_eq!(
            config.reconstruction_cache_adapter(),
            ReconstructionCacheAdapter::Redis
        );
    }

    #[test]
    fn server_config_builder_with_oci_upload_session_ttl() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_oci_upload_session_ttl_seconds(NonZeroU64::new(7200).unwrap());
        assert_eq!(
            config.oci_upload_session_ttl_seconds(),
            NonZeroU64::new(7200).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_oci_upload_max_active_sessions() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_oci_upload_max_active_sessions(NonZeroUsize::new(500).unwrap());
        assert_eq!(
            config.oci_upload_max_active_sessions(),
            NonZeroUsize::new(500).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_index_postgres_url_rejects_empty() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_index_postgres_url(String::new());
        assert!(matches!(
            result,
            Err(ServerConfigError::EmptyIndexPostgresUrl)
        ));
    }

    #[test]
    fn server_config_builder_with_index_postgres_url_accepts_valid_url() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_index_postgres_url("postgres://localhost/shardline".to_owned())
        .unwrap();
        assert_eq!(
            config.index_postgres_url(),
            Some("postgres://localhost/shardline")
        );
    }

    #[test]
    fn server_config_builder_with_token_signing_key_rejects_empty() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_token_signing_key(vec![]);
        assert!(matches!(
            result,
            Err(ServerConfigError::EmptyTokenSigningKey)
        ));
    }

    #[test]
    fn server_config_builder_with_token_signing_key_accepts_valid_key() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap();
        assert!(config.token_signing_key().is_some());
        assert_eq!(
            config.token_signing_key().unwrap(),
            b"test-signing-key-32-bytes-long!!"
        );
    }

    #[test]
    fn server_config_builder_with_auth_provider() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_auth_provider(AuthProviderKind::Oidc);
        assert_eq!(config.auth_provider(), AuthProviderKind::Oidc);
    }

    #[test]
    fn server_config_builder_with_auth_oidc_issuer() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_auth_oidc_issuer("https://accounts.example.com".to_owned());
        assert_eq!(
            config.auth_oidc_issuer(),
            Some("https://accounts.example.com")
        );
    }

    #[test]
    fn server_config_builder_with_auth_jwks_url() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_auth_jwks_url("https://example.com/.well-known/jwks".to_owned());
        assert_eq!(
            config.auth_jwks_url(),
            Some("https://example.com/.well-known/jwks")
        );
    }

    #[test]
    fn server_config_builder_with_auth_jwks_issuer() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_auth_jwks_issuer("https://example.com".to_owned());
        assert_eq!(config.auth_jwks_issuer(), Some("https://example.com"));
    }

    #[test]
    fn server_config_builder_with_metrics_token_rejects_empty() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_metrics_token(vec![]);
        assert!(matches!(result, Err(ServerConfigError::EmptyMetricsToken)));
    }

    #[test]
    fn server_config_builder_with_metrics_token_accepts_valid() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_metrics_token(b"metrics-token".to_vec())
        .unwrap();
        assert!(config.metrics_token().is_some());
        assert_eq!(config.metrics_token().unwrap(), b"metrics-token");
    }

    #[test]
    fn server_config_builder_with_provider_runtime_rejects_empty_api_key() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap();
        let result = config.with_provider_runtime(
            PathBuf::from("/tmp/provider.yaml"),
            vec![],
            "issuer".to_owned(),
            NonZeroU64::new(3600).unwrap(),
        );
        assert!(matches!(
            result,
            Err(ServerConfigError::EmptyProviderApiKey)
        ));
    }

    #[test]
    fn server_config_builder_with_provider_runtime_rejects_empty_issuer() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap();
        let result = config.with_provider_runtime(
            PathBuf::from("/tmp/provider.yaml"),
            b"valid-api-key".to_vec(),
            "   ".to_owned(),
            NonZeroU64::new(3600).unwrap(),
        );
        assert!(matches!(
            result,
            Err(ServerConfigError::EmptyProviderTokenIssuer)
        ));
    }

    #[test]
    fn server_config_builder_with_provider_runtime_rejects_missing_signing_key() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_provider_runtime(
            PathBuf::from("/tmp/provider.yaml"),
            b"valid-api-key".to_vec(),
            "issuer".to_owned(),
            NonZeroU64::new(3600).unwrap(),
        );
        assert!(matches!(
            result,
            Err(ServerConfigError::ProviderTokensRequireSigningKey)
        ));
    }

    #[test]
    fn server_config_builder_with_provider_runtime_accepts_valid_config() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap()
        .with_provider_runtime(
            PathBuf::from("/tmp/provider.yaml"),
            b"valid-api-key".to_vec(),
            "shardline".to_owned(),
            NonZeroU64::new(3600).unwrap(),
        )
        .unwrap();
        assert_eq!(
            config.provider_config_path(),
            Some(Path::new("/tmp/provider.yaml"))
        );
        assert_eq!(config.provider_api_key(), Some(b"valid-api-key" as &[u8]));
        assert_eq!(config.provider_token_issuer(), Some("shardline"));
        assert_eq!(
            config.provider_token_ttl_seconds(),
            Some(NonZeroU64::new(3600).unwrap())
        );
    }

    #[test]
    fn server_config_validate_runtime_requirements_rejects_missing_signing_key_for_api_role() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.validate_runtime_requirements();
        assert!(matches!(
            result,
            Err(ServerConfigError::MissingTokenSigningKeyForServedRoutes)
        ));
    }

    #[test]
    fn server_config_validate_runtime_requirements_accepts_signing_key_for_api_role() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap();
        assert!(config.validate_runtime_requirements().is_ok());
    }

    #[test]
    fn server_config_validate_runtime_requirements_rejects_passthrough_on_non_loopback() {
        let config = ServerConfig::new(
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
                8080,
            ),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap()
        .with_auth_provider(AuthProviderKind::Passthrough);
        let result = config.validate_runtime_requirements();
        assert!(matches!(
            result,
            Err(ServerConfigError::PassthroughProviderRequiresLoopbackBind {
                ..
            })
        ));
    }

    #[test]
    fn server_config_validate_runtime_requirements_accepts_passthrough_on_loopback() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
        .unwrap()
        .with_auth_provider(AuthProviderKind::Passthrough);
        assert!(config.validate_runtime_requirements().is_ok());
    }

    #[test]
    fn server_config_debug_redacts_secrets() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_token_signing_key(b"super-secret-key-here-32-bytes!!".to_vec())
        .unwrap();
        let debug = format!("{config:?}");
        assert!(!debug.contains("super-secret-key-here-32-bytes!!"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn server_config_accessors_return_expected_values() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        assert_eq!(
            config.bind_addr(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080)
        );
        assert_eq!(config.public_base_url(), "http://localhost:8080");
        assert_eq!(config.root_dir(), Path::new("/tmp/test"));
        assert!(config.s3_object_store_config().is_none());
        assert!(config.index_postgres_url().is_none());
        assert!(config.token_signing_key().is_none());
        assert!(config.metrics_token().is_none());
        assert!(config.provider_config_path().is_none());
        assert!(config.provider_api_key().is_none());
        assert!(config.provider_token_issuer().is_none());
        assert!(config.provider_token_ttl_seconds().is_none());
    }

    #[test]
    fn deduplicated_server_frontends_removes_duplicates() {
        let result = deduplicated_server_frontends([
            ServerFrontend::Xet,
            ServerFrontend::Oci,
            ServerFrontend::Xet,
            ServerFrontend::Hub,
        ]);
        assert_eq!(
            result,
            vec![
                ServerFrontend::Xet,
                ServerFrontend::Oci,
                ServerFrontend::Hub
            ]
        );
    }

    #[test]
    fn deduplicated_server_frontends_preserves_order() {
        let result = deduplicated_server_frontends([
            ServerFrontend::Hub,
            ServerFrontend::Oci,
            ServerFrontend::Xet,
        ]);
        assert_eq!(
            result,
            vec![ServerFrontend::Hub, ServerFrontend::Oci, ServerFrontend::Xet]
        );
    }

    #[test]
    fn deduplicated_server_frontends_returns_empty_for_empty_input() {
        let result: Vec<ServerFrontend> = deduplicated_server_frontends([]);
        assert!(result.is_empty());
    }

    #[test]
    fn adaptive_default_in_flight_chunks_for_parallelism_clamps_to_minimum() {
        let result = adaptive_default_in_flight_chunks_for_parallelism(
            1,
            2,
            NonZeroUsize::new(64).unwrap(),
            NonZeroUsize::new(256).unwrap(),
        );
        assert_eq!(result, NonZeroUsize::new(64).unwrap());
    }

    #[test]
    fn adaptive_default_in_flight_chunks_for_parallelism_clamps_to_maximum() {
        let result = adaptive_default_in_flight_chunks_for_parallelism(
            1024,
            8,
            NonZeroUsize::new(64).unwrap(),
            NonZeroUsize::new(256).unwrap(),
        );
        assert_eq!(result, NonZeroUsize::new(256).unwrap());
    }

    #[test]
    fn adaptive_default_in_flight_chunks_for_parallelism_scales_within_bounds() {
        let result = adaptive_default_in_flight_chunks_for_parallelism(
            16,
            4,
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(256).unwrap(),
        );
        assert_eq!(result, NonZeroUsize::new(64).unwrap());
    }

    #[test]
    fn server_config_error_display_bind_address() {
        let err =
            ServerConfigError::BindAddress("127.0.0.1".parse::<SocketAddr>().unwrap_err());
        let display = err.to_string();
        assert_eq!(display, "invalid bind address");
    }

    #[test]
    fn server_config_error_display_invalid_server_role() {
        let err = ServerConfigError::InvalidServerRole;
        assert_eq!(err.to_string(), "invalid server role");
    }

    #[test]
    fn server_config_error_display_invalid_server_frontend() {
        let err = ServerConfigError::InvalidServerFrontend;
        assert_eq!(err.to_string(), "invalid server frontend");
    }

    #[test]
    fn server_config_error_display_missing_server_frontends() {
        let err = ServerConfigError::MissingServerFrontends;
        assert_eq!(
            err.to_string(),
            "at least one server frontend must be enabled"
        );
    }

    #[test]
    fn server_config_error_display_invalid_auth_provider() {
        let err = ServerConfigError::InvalidAuthProvider;
        assert_eq!(err.to_string(), "invalid auth provider");
    }

    #[test]
    fn server_config_error_display_invalid_object_storage_adapter() {
        let err = ServerConfigError::InvalidObjectStorageAdapter;
        assert_eq!(err.to_string(), "invalid object storage adapter");
    }

    #[test]
    fn server_config_error_display_missing_s3_bucket() {
        let err = ServerConfigError::MissingS3Bucket;
        assert_eq!(
            err.to_string(),
            "s3 object storage requires SHARDLINE_S3_BUCKET"
        );
    }

    #[test]
    fn server_config_error_display_zero_chunk_size() {
        let err = ServerConfigError::ZeroChunkSize;
        assert_eq!(err.to_string(), "chunk size must be greater than zero");
    }

    #[test]
    fn server_config_error_display_passthrough_provider_requires_loopback_bind() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 8080);
        let err = ServerConfigError::PassthroughProviderRequiresLoopbackBind {
            bind_addr: addr,
        };
        let display = err.to_string();
        assert!(display
            .contains("passthrough auth provider requires a loopback bind address"));
        assert!(display.contains("10.0.0.1"));
    }

    #[test]
    fn server_config_error_display_s3_credential_source_conflict() {
        let err = ServerConfigError::S3CredentialSourceConflict {
            env: "AWS_ACCESS_KEY_ID",
            file_env: "AWS_ACCESS_KEY_ID_FILE",
        };
        let display = err.to_string();
        assert!(display.contains("AWS_ACCESS_KEY_ID"));
        assert!(display.contains("AWS_ACCESS_KEY_ID_FILE"));
    }

    #[test]
    fn server_config_error_display_empty_token_signing_key() {
        let err = ServerConfigError::EmptyTokenSigningKey;
        assert_eq!(err.to_string(), "token signing key must not be empty");
    }

    #[test]
    fn server_config_error_display_missing_token_signing_key_for_routes() {
        let err = ServerConfigError::MissingTokenSigningKeyForServedRoutes;
        assert_eq!(
            err.to_string(),
            "served shardline routes require shardline token signing key configuration"
        );
    }

    #[test]
    fn server_config_error_display_empty_provider_api_key() {
        let err = ServerConfigError::EmptyProviderApiKey;
        assert_eq!(
            err.to_string(),
            "provider bootstrap key must not be empty"
        );
    }

    #[test]
    fn server_config_error_display_empty_provider_token_issuer() {
        let err = ServerConfigError::EmptyProviderTokenIssuer;
        assert_eq!(
            err.to_string(),
            "provider token issuer must not be empty"
        );
    }

    #[test]
    fn server_config_error_display_provider_tokens_require_signing_key() {
        let err = ServerConfigError::ProviderTokensRequireSigningKey;
        assert_eq!(
            err.to_string(),
            "provider token issuance requires shardline token signing key configuration"
        );
    }

    #[test]
    fn server_config_error_display_chunk_size_too_large() {
        let err = ServerConfigError::ChunkSizeTooLarge;
        assert_eq!(err.to_string(), "chunk size must not exceed 1 GB");
    }

    #[test]
    fn server_config_error_display_hub_requires_auth() {
        let err = ServerConfigError::HubRequiresAuth;
        assert!(err.to_string().contains("hub frontend requires auth"));
    }

    #[test]
    fn server_config_error_debug_round_trip() {
        let err = ServerConfigError::InvalidServerRole;
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());
    }

    // ── Remaining ServerConfigError Display tests ──────────────────────────

    #[test]
    fn server_config_error_display_root_dir() {
        let io_err = std::io::Error::other("permission denied");
        let err = ServerConfigError::RootDir(io_err);
        assert_eq!(err.to_string(), "invalid local deployment root");
    }

    #[test]
    fn server_config_error_display_invalid_s3_allow_http() {
        let err = ServerConfigError::InvalidS3AllowHttp;
        assert_eq!(err.to_string(), "invalid s3 allow-http flag");
    }

    #[test]
    fn server_config_error_display_invalid_s3_virtual_hosted_style_request() {
        let err = ServerConfigError::InvalidS3VirtualHostedStyleRequest;
        assert_eq!(
            err.to_string(),
            "invalid s3 virtual-hosted-style request flag"
        );
    }

    #[test]
    fn server_config_error_display_s3_credential_file() {
        let io_err = std::io::Error::other("file not found");
        let err = ServerConfigError::S3CredentialFile {
            name: "AWS_ACCESS_KEY_ID_FILE",
            source: io_err,
        };
        let display = err.to_string();
        assert!(display.contains("s3 credential file"));
        assert!(display.contains("AWS_ACCESS_KEY_ID_FILE"));
    }

    #[test]
    fn server_config_error_display_s3_credential_too_large() {
        let err = ServerConfigError::S3CredentialTooLarge {
            name: "AWS_SECRET_ACCESS_KEY_FILE",
            observed_bytes: 5000,
            maximum_bytes: 4096,
        };
        let display = err.to_string();
        assert!(display.contains("exceeded"));
        assert!(display.contains("AWS_SECRET_ACCESS_KEY_FILE"));
    }

    #[test]
    fn server_config_error_display_s3_credential_length_mismatch() {
        let err = ServerConfigError::S3CredentialLengthMismatch {
            name: "AWS_SESSION_TOKEN_FILE",
            expected_bytes: 100,
            observed_bytes: 200,
        };
        let display = err.to_string();
        assert!(display.contains("changed during bounded read"));
    }

    #[test]
    fn server_config_error_display_s3_credential_utf8() {
        let err = ServerConfigError::S3CredentialUtf8 {
            name: "AWS_SECRET_ACCESS_KEY_FILE",
        };
        let display = err.to_string();
        assert!(display.contains("not valid utf-8"));
    }

    #[test]
    fn server_config_error_display_chunk_size_parse() {
        let err = ServerConfigError::ChunkSize("not-a-number".parse::<usize>().unwrap_err());
        assert_eq!(err.to_string(), "invalid chunk size");
    }

    #[test]
    fn server_config_error_display_max_request_body_bytes() {
        let err = ServerConfigError::MaxRequestBodyBytes(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid max request body size");
    }

    #[test]
    fn server_config_error_display_zero_max_request_body_bytes() {
        let err = ServerConfigError::ZeroMaxRequestBodyBytes;
        assert_eq!(
            err.to_string(),
            "max request body size must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_max_shard_files() {
        let err = ServerConfigError::MaxShardFiles(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid max shard file section count");
    }

    #[test]
    fn server_config_error_display_zero_max_shard_files() {
        let err = ServerConfigError::ZeroMaxShardFiles;
        assert_eq!(
            err.to_string(),
            "max shard file section count must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_max_shard_xorbs() {
        let err = ServerConfigError::MaxShardXorbs(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid max shard xorb section count");
    }

    #[test]
    fn server_config_error_display_zero_max_shard_xorbs() {
        let err = ServerConfigError::ZeroMaxShardXorbs;
        assert_eq!(
            err.to_string(),
            "max shard xorb section count must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_max_shard_reconstruction_terms() {
        let err = ServerConfigError::MaxShardReconstructionTerms(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(
            err.to_string(),
            "invalid max shard reconstruction term count"
        );
    }

    #[test]
    fn server_config_error_display_zero_max_shard_reconstruction_terms() {
        let err = ServerConfigError::ZeroMaxShardReconstructionTerms;
        assert_eq!(
            err.to_string(),
            "max shard reconstruction term count must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_max_shard_xorb_chunks() {
        let err = ServerConfigError::MaxShardXorbChunks(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(
            err.to_string(),
            "invalid max shard xorb chunk record count"
        );
    }

    #[test]
    fn server_config_error_display_zero_max_shard_xorb_chunks() {
        let err = ServerConfigError::ZeroMaxShardXorbChunks;
        assert_eq!(
            err.to_string(),
            "max shard xorb chunk record count must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_upload_max_in_flight_chunks() {
        let err = ServerConfigError::UploadMaxInFlightChunks(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid upload max in-flight chunks");
    }

    #[test]
    fn server_config_error_display_zero_upload_max_in_flight_chunks() {
        let err = ServerConfigError::ZeroUploadMaxInFlightChunks;
        assert_eq!(
            err.to_string(),
            "upload max in-flight chunks must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_transfer_max_in_flight_chunks() {
        let err = ServerConfigError::TransferMaxInFlightChunks(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid transfer max in-flight chunks");
    }

    #[test]
    fn server_config_error_display_zero_transfer_max_in_flight_chunks() {
        let err = ServerConfigError::ZeroTransferMaxInFlightChunks;
        assert_eq!(
            err.to_string(),
            "transfer max in-flight chunks must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_invalid_reconstruction_cache_adapter() {
        let err = ServerConfigError::InvalidReconstructionCacheAdapter;
        assert_eq!(
            err.to_string(),
            "invalid reconstruction cache adapter"
        );
    }

    #[test]
    fn server_config_error_display_reconstruction_cache_ttl() {
        let err = ServerConfigError::ReconstructionCacheTtl(
            "not-a-number".parse::<u64>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid reconstruction cache ttl");
    }

    #[test]
    fn server_config_error_display_zero_reconstruction_cache_ttl() {
        let err = ServerConfigError::ZeroReconstructionCacheTtlSeconds;
        assert_eq!(
            err.to_string(),
            "reconstruction cache ttl must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_reconstruction_cache_memory_max_entries() {
        let err = ServerConfigError::ReconstructionCacheMemoryMaxEntries(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(
            err.to_string(),
            "invalid reconstruction cache memory max entries"
        );
    }

    #[test]
    fn server_config_error_display_zero_reconstruction_cache_memory_max_entries() {
        let err = ServerConfigError::ZeroReconstructionCacheMemoryMaxEntries;
        assert_eq!(
            err.to_string(),
            "reconstruction cache memory max entries must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_oci_upload_session_ttl() {
        let err = ServerConfigError::OciUploadSessionTtl(
            "not-a-number".parse::<u64>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid oci upload session ttl");
    }

    #[test]
    fn server_config_error_display_zero_oci_upload_session_ttl() {
        let err = ServerConfigError::ZeroOciUploadSessionTtlSeconds;
        assert_eq!(
            err.to_string(),
            "oci upload session ttl must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_oci_upload_max_active_sessions() {
        let err = ServerConfigError::OciUploadMaxActiveSessions(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(
            err.to_string(),
            "invalid oci upload max active sessions"
        );
    }

    #[test]
    fn server_config_error_display_zero_oci_upload_max_active_sessions() {
        let err = ServerConfigError::ZeroOciUploadMaxActiveSessions;
        assert_eq!(
            err.to_string(),
            "oci upload max active sessions must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_oci_registry_token_ttl() {
        let err = ServerConfigError::OciRegistryTokenTtl(
            "not-a-number".parse::<u64>().unwrap_err(),
        );
        assert_eq!(err.to_string(), "invalid oci registry token ttl");
    }

    #[test]
    fn server_config_error_display_zero_oci_registry_token_ttl() {
        let err = ServerConfigError::ZeroOciRegistryTokenTtlSeconds;
        assert_eq!(
            err.to_string(),
            "oci registry token ttl must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_oci_registry_token_max_in_flight_requests() {
        let err = ServerConfigError::OciRegistryTokenMaxInFlightRequests(
            "not-a-number".parse::<usize>().unwrap_err(),
        );
        assert_eq!(
            err.to_string(),
            "invalid oci registry token max in-flight requests"
        );
    }

    #[test]
    fn server_config_error_display_zero_oci_registry_token_max_in_flight_requests() {
        let err = ServerConfigError::ZeroOciRegistryTokenMaxInFlightRequests;
        assert_eq!(
            err.to_string(),
            "oci registry token max in-flight requests must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_empty_reconstruction_cache_redis_url() {
        let err = ServerConfigError::EmptyReconstructionCacheRedisUrl;
        assert_eq!(
            err.to_string(),
            "reconstruction cache redis url must not be empty"
        );
    }

    #[test]
    fn server_config_error_display_missing_reconstruction_cache_redis_url() {
        let err = ServerConfigError::MissingReconstructionCacheRedisUrl;
        assert_eq!(
            err.to_string(),
            "redis reconstruction cache requires SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL"
        );
    }

    #[test]
    fn server_config_error_display_empty_index_postgres_url() {
        let err = ServerConfigError::EmptyIndexPostgresUrl;
        assert_eq!(
            err.to_string(),
            "postgres metadata url must not be empty"
        );
    }

    #[test]
    fn server_config_error_display_token_signing_key() {
        let io_err = std::io::Error::other("file error");
        let err = ServerConfigError::TokenSigningKey(io_err);
        assert_eq!(err.to_string(), "token signing key could not be read");
    }

    #[test]
    fn server_config_error_display_token_signing_key_source_conflict() {
        let err = ServerConfigError::TokenSigningKeySourceConflict {
            env: "SHARDLINE_TOKEN_SIGNING_KEY",
            file_env: "SHARDLINE_TOKEN_SIGNING_KEY_FILE",
        };
        let display = err.to_string();
        assert!(display.contains("token signing key source conflict"));
    }

    #[test]
    fn server_config_error_display_token_signing_key_too_large() {
        let err = ServerConfigError::TokenSigningKeyTooLarge {
            observed_bytes: 2_000_000,
            maximum_bytes: 1_048_576,
        };
        let display = err.to_string();
        assert!(display.contains("exceeded"));
    }

    #[test]
    fn server_config_error_display_token_signing_key_length_mismatch() {
        let err = ServerConfigError::TokenSigningKeyLengthMismatch {
            expected_bytes: 100,
            observed_bytes: 200,
        };
        let display = err.to_string();
        assert!(display.contains("changed during bounded read"));
    }

    #[test]
    fn server_config_error_display_provider_token_ttl() {
        let err = ServerConfigError::ProviderTokenTtl;
        assert_eq!(err.to_string(), "invalid provider token ttl");
    }

    #[test]
    fn server_config_error_display_metrics_token() {
        let io_err = std::io::Error::other("file error");
        let err = ServerConfigError::MetricsToken(io_err);
        assert_eq!(err.to_string(), "metrics token could not be read");
    }

    #[test]
    fn server_config_error_display_empty_metrics_token() {
        let err = ServerConfigError::EmptyMetricsToken;
        assert_eq!(err.to_string(), "metrics token must not be empty");
    }

    #[test]
    fn server_config_error_display_metrics_token_too_large() {
        let err = ServerConfigError::MetricsTokenTooLarge {
            observed_bytes: 5000,
            maximum_bytes: 4096,
        };
        let display = err.to_string();
        assert!(display.contains("exceeded"));
    }

    #[test]
    fn server_config_error_display_metrics_token_length_mismatch() {
        let err = ServerConfigError::MetricsTokenLengthMismatch {
            expected_bytes: 100,
            observed_bytes: 200,
        };
        let display = err.to_string();
        assert!(display.contains("changed during bounded read"));
    }

    #[test]
    fn server_config_error_display_provider_api_key() {
        let io_err = std::io::Error::other("file error");
        let err = ServerConfigError::ProviderApiKey(io_err);
        assert_eq!(err.to_string(), "provider bootstrap key could not be read");
    }

    #[test]
    fn server_config_error_display_provider_api_key_too_large() {
        let err = ServerConfigError::ProviderApiKeyTooLarge {
            observed_bytes: 5000,
            maximum_bytes: 4096,
        };
        let display = err.to_string();
        assert!(display.contains("exceeded"));
    }

    #[test]
    fn server_config_error_display_provider_api_key_length_mismatch() {
        let err = ServerConfigError::ProviderApiKeyLengthMismatch {
            expected_bytes: 100,
            observed_bytes: 200,
        };
        let display = err.to_string();
        assert!(display.contains("changed during bounded read"));
    }

    #[test]
    fn server_config_error_display_zero_provider_token_ttl() {
        let err = ServerConfigError::ZeroProviderTokenTtl;
        assert_eq!(
            err.to_string(),
            "provider token ttl must be greater than zero"
        );
    }

    #[test]
    fn server_config_error_display_incomplete_provider_token_config() {
        let err = ServerConfigError::IncompleteProviderTokenConfig;
        assert_eq!(
            err.to_string(),
            "provider token issuance requires both provider config and provider api key files"
        );
    }

    #[test]
    fn server_config_error_display_invalid_public_base_url() {
        let err = ServerConfigError::InvalidPublicBaseUrl("not-a-url".to_owned());
        let display = err.to_string();
        assert!(display.contains("not a valid URL"));
    }

    #[test]
    fn server_config_error_display_missing_oidc_issuer() {
        let err = ServerConfigError::MissingOidcIssuer;
        assert_eq!(
            err.to_string(),
            "oidc auth provider requires SHARDLINE_AUTH_OIDC_ISSUER"
        );
    }

    #[test]
    fn server_config_error_display_missing_jwks_url() {
        let err = ServerConfigError::MissingJwksUrl;
        assert_eq!(
            err.to_string(),
            "jwks auth provider requires SHARDLINE_AUTH_JWKS_URL"
        );
    }

    // ── Remaining builder method tests ─────────────────────────────────────

    #[test]
    fn server_config_builder_with_oci_registry_token_ttl_seconds() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_oci_registry_token_ttl_seconds(NonZeroU64::new(600).unwrap());
        assert_eq!(
            config.oci_registry_token_ttl_seconds(),
            NonZeroU64::new(600).unwrap()
        );
    }

    #[test]
    fn server_config_builder_with_oci_registry_token_max_in_flight_requests() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_oci_registry_token_max_in_flight_requests(NonZeroUsize::new(128).unwrap());
        assert_eq!(
            config.oci_registry_token_max_in_flight_requests(),
            NonZeroUsize::new(128).unwrap()
        );
    }

    #[test]
    fn server_config_reconstruction_cache_redis_url_returns_none_when_not_configured() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        assert!(config.reconstruction_cache_redis_url().is_none());
    }

    #[test]
    fn server_config_builder_with_metrics_token_rejects_too_large() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        let result = config.with_metrics_token(vec![0u8; 5000]);
        assert!(matches!(
            result,
            Err(ServerConfigError::MetricsTokenTooLarge { .. })
        ));
    }

    #[test]
    fn server_config_transfer_max_in_flight_chunks_accessor() {
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        assert!(config.transfer_max_in_flight_chunks().get() > 0);
    }

    #[test]
    fn deduplicated_server_frontends_preserves_single_element() {
        let result = deduplicated_server_frontends([ServerFrontend::Xet]);
        assert_eq!(result, vec![ServerFrontend::Xet]);
    }

    #[test]
    fn adaptive_default_in_flight_chunks_for_parallelism_zero_parallelism() {
        let result = adaptive_default_in_flight_chunks_for_parallelism(
            0,
            4,
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(256).unwrap(),
        );
        assert_eq!(result, NonZeroUsize::new(8).unwrap());
    }

    #[test]
    fn server_config_upload_max_in_flight_chunks_default_is_positive() {
        // Use ServerConfig::new to verify the default is greater than zero
        let config = ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            PathBuf::from("/tmp/test"),
            NonZeroUsize::new(4096).unwrap(),
        );
        assert!(config.upload_max_in_flight_chunks().get() > 0);
    }
}

#[cfg(not(test))]
pub const fn run_before_secret_file_read_hook_for_tests(_path: &Path) {}
