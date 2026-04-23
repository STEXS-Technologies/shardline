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

mod env;
mod secrets;

use shardline_protocol::{SecretBytes, SecretString};
use shardline_storage::S3ObjectStoreConfig;
use thiserror::Error;

use crate::{
    reconstruction_cache::{
        DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES, DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
        ReconstructionCacheAdapter,
    },
    server_frontend::ServerFrontend,
    server_role::ServerRole,
};
use secrets::ensure_secret_size_within_limit;
#[cfg(test)]
use secrets::{
    PendingS3ObjectStoreConfig, configure_s3_object_store_config, optional_s3_secret_from_sources,
};
#[cfg(test)]
use secrets::{configure_provider_runtime_from_paths, read_secret_file_bytes};

/// Public server configuration.
#[derive(Clone, PartialEq, Eq)]
pub struct ServerConfig {
    bind_addr: SocketAddr,
    server_role: ServerRole,
    server_frontends: Vec<ServerFrontend>,
    public_base_url: String,
    root_dir: PathBuf,
    object_storage_adapter: ObjectStorageAdapter,
    s3_object_store_config: Option<S3ObjectStoreConfig>,
    max_request_body_bytes: NonZeroUsize,
    shard_metadata_limits: ShardMetadataLimits,
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    transfer_max_in_flight_chunks: NonZeroUsize,
    reconstruction_cache_adapter: ReconstructionCacheAdapter,
    reconstruction_cache_ttl_seconds: NonZeroU64,
    reconstruction_cache_memory_max_entries: NonZeroUsize,
    reconstruction_cache_redis_url: Option<SecretString>,
    index_postgres_url: Option<SecretString>,
    token_signing_key: Option<SecretBytes>,
    metrics_token: Option<SecretBytes>,
    provider_config_path: Option<PathBuf>,
    provider_api_key: Option<SecretBytes>,
    provider_token_issuer: Option<String>,
    provider_token_ttl_seconds: Option<NonZeroU64>,
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
                "reconstruction_cache_adapter",
                &self.reconstruction_cache_adapter,
            )
            .field(
                "reconstruction_cache_ttl_seconds",
                &self.reconstruction_cache_ttl_seconds,
            )
            .field(
                "reconstruction_cache_memory_max_entries",
                &self.reconstruction_cache_memory_max_entries,
            )
            .field(
                "reconstruction_cache_redis_url",
                &self
                    .reconstruction_cache_redis_url
                    .as_ref()
                    .map(|_url| "***"),
            )
            .field(
                "index_postgres_url",
                &self.index_postgres_url.as_ref().map(|_url| "***"),
            )
            .field(
                "token_signing_key",
                &self.token_signing_key.as_ref().map(|_key| "***"),
            )
            .field(
                "metrics_token",
                &self.metrics_token.as_ref().map(|_token| "***"),
            )
            .field("provider_config_path", &self.provider_config_path)
            .field(
                "provider_api_key",
                &self.provider_api_key.as_ref().map(|_key| "***"),
            )
            .field("provider_token_issuer", &self.provider_token_issuer)
            .field(
                "provider_token_ttl_seconds",
                &self.provider_token_ttl_seconds,
            )
            .finish()
    }
}

const MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(1024) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(64) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS: NonZeroUsize = match NonZeroUsize::new(256) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const DEFAULT_MAX_REQUEST_BODY_BYTES: NonZeroUsize = match NonZeroUsize::new(67_108_864) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const DEFAULT_MAX_SHARD_FILES: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const DEFAULT_MAX_SHARD_XORBS: NonZeroUsize = match NonZeroUsize::new(16_384) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

const DEFAULT_MAX_SHARD_XORB_CHUNKS: NonZeroUsize = match NonZeroUsize::new(65_536) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};
const MAX_TOKEN_SIGNING_KEY_BYTES: u64 = 1_048_576;
const MAX_PROVIDER_API_KEY_BYTES: u64 = 4096;
const MAX_METRICS_TOKEN_BYTES: u64 = 4096;
const MAX_S3_CREDENTIAL_BYTES: u64 = 4096;
const DEFAULT_PARALLELISM_FALLBACK: NonZeroUsize = match NonZeroUsize::new(8) {
    Some(value) => value,
    None => NonZeroUsize::MIN,
};

#[cfg(test)]
type SecretFileReadHook = Box<dyn FnOnce() + Send>;

#[cfg(test)]
struct SecretFileReadHookRegistration {
    path: PathBuf,
    hook: SecretFileReadHook,
}

#[cfg(test)]
type SecretFileReadHookSlot = Vec<SecretFileReadHookRegistration>;

#[cfg(test)]
static BEFORE_SECRET_FILE_READ_HOOK: LazyLock<Mutex<SecretFileReadHookSlot>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Default bounded-parser limits for native Xet shard metadata.
pub const DEFAULT_SHARD_METADATA_LIMITS: ShardMetadataLimits = ShardMetadataLimits::new(
    DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS,
    DEFAULT_MAX_SHARD_XORB_CHUNKS,
);

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
            reconstruction_cache_adapter: ReconstructionCacheAdapter::Memory,
            reconstruction_cache_ttl_seconds: DEFAULT_RECONSTRUCTION_CACHE_TTL_SECONDS,
            reconstruction_cache_memory_max_entries:
                DEFAULT_RECONSTRUCTION_CACHE_MEMORY_MAX_ENTRIES,
            reconstruction_cache_redis_url: None,
            index_postgres_url: None,
            token_signing_key: None,
            metrics_token: None,
            provider_config_path: None,
            provider_api_key: None,
            provider_token_issuer: None,
            provider_token_ttl_seconds: None,
        }
    }

    /// Loads server configuration from environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError`] when an environment value cannot be parsed or
    /// when the configured chunk size is zero.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        env::load_server_config_from_env()
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
    pub(crate) const fn object_storage_adapter(&self) -> ObjectStorageAdapter {
        self.object_storage_adapter
    }

    /// Returns S3-compatible object-storage configuration when selected.
    #[must_use]
    pub(crate) const fn s3_object_store_config(&self) -> Option<&S3ObjectStoreConfig> {
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
        self.reconstruction_cache_adapter
    }

    /// Returns the reconstruction-cache entry TTL in seconds.
    #[must_use]
    pub(crate) const fn reconstruction_cache_ttl_seconds(&self) -> NonZeroU64 {
        self.reconstruction_cache_ttl_seconds
    }

    /// Returns the bounded in-memory reconstruction-cache capacity.
    #[must_use]
    pub(crate) const fn reconstruction_cache_memory_max_entries(&self) -> NonZeroUsize {
        self.reconstruction_cache_memory_max_entries
    }

    /// Returns the optional Redis URL for the reconstruction cache.
    #[must_use]
    pub(crate) fn reconstruction_cache_redis_url(&self) -> Option<&str> {
        self.reconstruction_cache_redis_url
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
        self.reconstruction_cache_adapter = ReconstructionCacheAdapter::Disabled;
        self.reconstruction_cache_redis_url = None;
        self
    }

    /// Selects the bounded in-memory reconstruction-cache adapter.
    #[must_use]
    pub fn with_reconstruction_cache_memory(
        mut self,
        reconstruction_cache_ttl_seconds: NonZeroU64,
        reconstruction_cache_memory_max_entries: NonZeroUsize,
    ) -> Self {
        self.reconstruction_cache_adapter = ReconstructionCacheAdapter::Memory;
        self.reconstruction_cache_ttl_seconds = reconstruction_cache_ttl_seconds;
        self.reconstruction_cache_memory_max_entries = reconstruction_cache_memory_max_entries;
        self.reconstruction_cache_redis_url = None;
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

        self.reconstruction_cache_adapter = ReconstructionCacheAdapter::Redis;
        self.reconstruction_cache_ttl_seconds = reconstruction_cache_ttl_seconds;
        self.reconstruction_cache_redis_url =
            Some(SecretString::new(reconstruction_cache_redis_url));
        Ok(self)
    }

    /// Returns the optional Postgres metadata URL.
    #[must_use]
    pub fn index_postgres_url(&self) -> Option<&str> {
        self.index_postgres_url
            .as_ref()
            .map(SecretString::expose_secret)
    }

    /// Returns the optional token signing key.
    #[must_use]
    pub fn token_signing_key(&self) -> Option<&[u8]> {
        self.token_signing_key
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
        self.provider_config_path.as_deref()
    }

    /// Returns the optional provider bootstrap key.
    #[must_use]
    pub fn provider_api_key(&self) -> Option<&[u8]> {
        self.provider_api_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the provider token issuer identity when provider issuance is enabled.
    #[must_use]
    pub fn provider_token_issuer(&self) -> Option<&str> {
        self.provider_token_issuer.as_deref()
    }

    /// Returns the provider token lifetime when provider issuance is enabled.
    #[must_use]
    pub const fn provider_token_ttl_seconds(&self) -> Option<NonZeroU64> {
        self.provider_token_ttl_seconds
    }

    /// Enables local bearer-token verification with the supplied signing key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyTokenSigningKey`] when the signing key is
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

        self.token_signing_key = Some(SecretBytes::new(token_signing_key));
        Ok(self)
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
        if self.token_signing_key.is_none() {
            return Err(ServerConfigError::ProviderTokensRequireSigningKey);
        }

        self.provider_config_path = Some(provider_config_path);
        self.provider_api_key = Some(SecretBytes::new(provider_api_key));
        self.provider_token_issuer = Some(issuer_identity);
        self.provider_token_ttl_seconds = Some(ttl_seconds);
        Ok(self)
    }

    /// Validates runtime requirements implied by the selected route surface.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::MissingTokenSigningKeyForServedRoutes`] when the
    /// selected role would expose authenticated CAS routes without a signing key.
    pub const fn validate_runtime_requirements(&self) -> Result<(), ServerConfigError> {
        if self.token_signing_key.is_none()
            && (self.server_role.serves_api() || self.server_role.serves_transfer())
        {
            return Err(ServerConfigError::MissingTokenSigningKeyForServedRoutes);
        }

        Ok(())
    }
}

/// Returns the adaptive default upload chunk parallelism for the current host.
#[must_use]
pub(crate) fn default_upload_max_in_flight_chunks() -> NonZeroUsize {
    adaptive_default_in_flight_chunks(
        2,
        MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
        MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
    )
}

/// Returns the adaptive default transfer budget for the current host.
#[must_use]
pub(crate) fn default_transfer_max_in_flight_chunks() -> NonZeroUsize {
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

fn adaptive_default_in_flight_chunks_for_parallelism(
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardMetadataLimits {
    max_files: NonZeroUsize,
    max_xorbs: NonZeroUsize,
    max_reconstruction_terms: NonZeroUsize,
    max_xorb_chunks: NonZeroUsize,
}

impl ShardMetadataLimits {
    /// Creates native Xet shard metadata limits.
    #[must_use]
    pub const fn new(
        max_files: NonZeroUsize,
        max_xorbs: NonZeroUsize,
        max_reconstruction_terms: NonZeroUsize,
        max_xorb_chunks: NonZeroUsize,
    ) -> Self {
        Self {
            max_files,
            max_xorbs,
            max_reconstruction_terms,
            max_xorb_chunks,
        }
    }

    /// Returns the maximum file sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_files(self) -> NonZeroUsize {
        self.max_files
    }

    /// Returns the maximum xorb sections accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorbs(self) -> NonZeroUsize {
        self.max_xorbs
    }

    /// Returns the maximum file reconstruction terms accepted in one uploaded shard.
    #[must_use]
    pub const fn max_reconstruction_terms(self) -> NonZeroUsize {
        self.max_reconstruction_terms
    }

    /// Returns the maximum xorb chunk records accepted in one uploaded shard.
    #[must_use]
    pub const fn max_xorb_chunks(self) -> NonZeroUsize {
        self.max_xorb_chunks
    }
}

impl Default for ShardMetadataLimits {
    fn default() -> Self {
        DEFAULT_SHARD_METADATA_LIMITS
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
    /// The per-upload chunk processing window was zero.
    #[error("upload max in-flight chunks must be greater than zero")]
    ZeroUploadMaxInFlightChunks,
    /// The transfer concurrency budget was zero.
    #[error("transfer max in-flight chunks must be greater than zero")]
    ZeroTransferMaxInFlightChunks,
    /// The reconstruction-cache adapter token was invalid.
    #[error("invalid reconstruction cache adapter")]
    InvalidReconstructionCacheAdapter,
    /// The reconstruction-cache TTL was zero.
    #[error("reconstruction cache ttl must be greater than zero")]
    ZeroReconstructionCacheTtlSeconds,
    /// The in-memory reconstruction-cache capacity was zero.
    #[error("reconstruction cache memory max entries must be greater than zero")]
    ZeroReconstructionCacheMemoryMaxEntries,
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
}

#[cfg(test)]
fn run_before_secret_file_read_hook_for_tests(path: &Path) {
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

#[cfg(not(test))]
const fn run_before_secret_file_read_hook_for_tests(_path: &Path) {}

#[cfg(test)]
mod tests;
