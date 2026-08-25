use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    str::FromStr,
    thread::available_parallelism,
    time::Duration,
};

use shardline_cache::RedisTlsConfig;
use shardline_protocol::{SecretBytes, SecretString};
use shardline_storage::S3ObjectStoreConfig;
use tracing;

use super::super::secrets::ensure_secret_size_within_limit;
use super::defaults::{
    CONFIG_SECRET_KEY_BYTES, DEFAULT_LFS_PATCH_MAX_ACTIVE_SESSIONS,
    DEFAULT_LFS_PATCH_MAX_SEEK_AHEAD_BYTES, DEFAULT_LFS_PATCH_TOTAL_MAX_BYTES,
    DEFAULT_LFS_PATCH_TTL_SECONDS, DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_REVISIONS_PER_REPO,
    DEFAULT_MAX_TREE_ENTRIES_PER_REPO, DEFAULT_OCI_REGISTRY_TOKEN_MAX_IN_FLIGHT_REQUESTS,
    DEFAULT_OCI_REGISTRY_TOKEN_TTL_SECONDS, DEFAULT_OCI_UPLOAD_MAX_ACTIVE_SESSIONS,
    DEFAULT_OCI_UPLOAD_SESSION_TTL_SECONDS, DEFAULT_PARALLELISM_FALLBACK,
    DEFAULT_S3_MAX_PART_BYTES, DEFAULT_S3_MIN_PART_BYTES, DEFAULT_S3_UPLOAD_MAX_ACTIVE_PART_FILES,
    DEFAULT_S3_UPLOAD_MAX_ACTIVE_SESSIONS, DEFAULT_S3_UPLOAD_SESSION_MAX_BYTES,
    DEFAULT_S3_UPLOAD_SESSION_TTL_SECONDS, DEFAULT_S3_UPLOAD_TOTAL_MAX_BYTES,
    HUB_WEBHOOK_SECRET_KEY_BYTES, MAX_ADMIN_READ_TOKEN_BYTES,
    MAX_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS, MAX_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS,
    MAX_ED25519_KEY_BYTES, MAX_METRICS_TOKEN_BYTES, MAX_PROVIDER_API_KEY_BYTES,
    MAX_TOKEN_SIGNING_KEY_BYTES, MIN_DEFAULT_TRANSFER_MAX_IN_FLIGHT_CHUNKS,
    MIN_DEFAULT_UPLOAD_MAX_IN_FLIGHT_CHUNKS, MIN_S3_MAX_PART_BYTES,
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

/// Validated bearer token for the read-only administration boundary.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AdminReadToken(SecretBytes);

impl AdminReadToken {
    fn new(value: SecretBytes) -> Result<Self, ServerConfigError> {
        if value.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyAdminReadToken);
        }
        ensure_secret_size_within_limit(
            u64::try_from(value.len()).unwrap_or(u64::MAX),
            MAX_ADMIN_READ_TOKEN_BYTES,
            |observed_bytes, maximum_bytes| ServerConfigError::AdminReadTokenTooLarge {
                observed_bytes,
                maximum_bytes,
            },
        )?;
        if !value.expose_secret().iter().all(u8::is_ascii_graphic) {
            return Err(ServerConfigError::InvalidAdminReadToken);
        }
        Ok(Self(value))
    }

    fn expose_secret(&self) -> &[u8] {
        self.0.expose_secret()
    }
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
    pub(crate) admin_read_token: Option<AdminReadToken>,
    pub(crate) deployment_mode: DeploymentMode,
    /// Whether the deployment mode was explicitly selected (e.g. via
    /// `SHARDLINE_DEPLOYMENT_MODE`), as opposed to the built-in insecure
    /// default. Used by the plaintext-secret gate: an explicit Insecure choice
    /// opts out, while the implicit default still fails loud.
    pub(crate) deployment_mode_explicitly_set: bool,
    pub(crate) allow_plaintext_secrets_in_production: bool,
    pub(crate) auth: AuthConfig,
    pub(crate) oci: OciConfig,
    pub(crate) cache: CacheConfig,
    pub(crate) provider: ProviderConfig,
    pub(crate) shutdown_timeout: Option<Duration>,
    pub(crate) admission_max_weight: NonZeroUsize,
    pub(crate) s3_max_part_bytes: NonZeroU64,
    pub(crate) s3_min_part_bytes: NonZeroU64,
    pub(crate) s3_upload_session_ttl_seconds: NonZeroU64,
    pub(crate) s3_upload_max_active_sessions: NonZeroUsize,
    pub(crate) s3_upload_session_max_bytes: NonZeroU64,
    pub(crate) s3_upload_total_max_bytes: NonZeroU64,
    pub(crate) s3_upload_max_active_part_files: NonZeroUsize,
    pub(crate) lfs_patch_ttl_seconds: NonZeroU64,
    pub(crate) lfs_patch_max_active_sessions: NonZeroUsize,
    pub(crate) lfs_patch_total_max_bytes: NonZeroU64,
    pub(crate) lfs_patch_max_seek_ahead_bytes: NonZeroU64,
    pub(crate) max_revisions_per_repo: NonZeroUsize,
    pub(crate) max_tree_entries_per_repo: NonZeroUsize,
}

impl ServerConfig {
    /// Creates server configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_server::ServerConfig;
    /// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    /// use std::num::NonZeroUsize;
    ///
    /// let config = ServerConfig::new(
    ///     SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
    ///     "http://127.0.0.1:8080".to_owned(),
    ///     std::env::temp_dir(),
    ///     NonZeroUsize::new(64 * 1024).expect("64 KiB chunk size is non-zero"),
    /// );
    ///
    /// assert_eq!(config.bind_addr().port(), 8080);
    /// assert_eq!(config.public_base_url(), "http://127.0.0.1:8080");
    /// assert_eq!(config.root_dir(), std::env::temp_dir());
    /// ```
    ///
    /// The resulting config is a local-deployment default: a local filesystem
    /// metadata backend, a local object store, and the Xet protocol frontend.
    /// Point [`serve`](crate::serve) at it to start the server.
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
            admin_read_token: None,
            deployment_mode: DeploymentMode::default(),
            deployment_mode_explicitly_set: false,
            allow_plaintext_secrets_in_production: false,
            auth: AuthConfig {
                token_signing_key: None,
                hub_webhook_secret_key: None,
                config_secret_key: None,
                auth_provider: AuthProviderKind::Local,
                auth_oidc_issuer: None,
                auth_oidc_audience: None,
                auth_oidc_jwks_host_allowlist: None,
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
            admission_max_weight: NonZeroUsize::new(256).unwrap_or(NonZeroUsize::MIN),
            s3_max_part_bytes: DEFAULT_S3_MAX_PART_BYTES,
            s3_min_part_bytes: DEFAULT_S3_MIN_PART_BYTES,
            s3_upload_session_ttl_seconds: DEFAULT_S3_UPLOAD_SESSION_TTL_SECONDS,
            s3_upload_max_active_sessions: DEFAULT_S3_UPLOAD_MAX_ACTIVE_SESSIONS,
            s3_upload_session_max_bytes: DEFAULT_S3_UPLOAD_SESSION_MAX_BYTES,
            s3_upload_total_max_bytes: DEFAULT_S3_UPLOAD_TOTAL_MAX_BYTES,
            s3_upload_max_active_part_files: DEFAULT_S3_UPLOAD_MAX_ACTIVE_PART_FILES,
            lfs_patch_ttl_seconds: DEFAULT_LFS_PATCH_TTL_SECONDS,
            lfs_patch_max_active_sessions: DEFAULT_LFS_PATCH_MAX_ACTIVE_SESSIONS,
            lfs_patch_total_max_bytes: DEFAULT_LFS_PATCH_TOTAL_MAX_BYTES,
            lfs_patch_max_seek_ahead_bytes: DEFAULT_LFS_PATCH_MAX_SEEK_AHEAD_BYTES,
            max_revisions_per_repo: DEFAULT_MAX_REVISIONS_PER_REPO,
            max_tree_entries_per_repo: DEFAULT_MAX_TREE_ENTRIES_PER_REPO,
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

    /// Returns the optional OIDC audience (`aud` claim) validated for tokens.
    #[must_use]
    pub fn auth_oidc_audience(&self) -> Option<&str> {
        self.auth.auth_oidc_audience.as_deref()
    }

    /// Returns the allowlist of hosts (besides the issuer's own host) whose
    /// JWKS endpoints the OIDC discovery document may advertise via `jwks_uri`.
    ///
    /// Some IdPs legitimately cross-host their JWKS endpoint onto a different
    /// domain than the issuer (e.g. Google serves keys from
    /// `www.googleapis.com` while the issuer is `accounts.google.com`). When
    /// unset, only the issuer's own host is accepted (fail-closed).
    #[must_use]
    pub fn auth_oidc_jwks_host_allowlist(&self) -> Option<&[String]> {
        self.auth.auth_oidc_jwks_host_allowlist.as_deref()
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

    /// Returns the optional Hub webhook secret encryption key (AES-256).
    #[must_use]
    pub fn hub_webhook_secret_key(&self) -> Option<&[u8]> {
        self.auth
            .hub_webhook_secret_key
            .as_ref()
            .map(SecretBytes::expose_secret)
    }

    /// Returns the optional provider-config secret encryption key (AES-256).
    #[must_use]
    pub fn config_secret_key(&self) -> Option<&[u8]> {
        self.auth
            .config_secret_key
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

    /// Returns the optional bearer token protecting the read-only admin API.
    #[must_use]
    pub fn admin_read_token(&self) -> Option<&[u8]> {
        self.admin_read_token
            .as_ref()
            .map(AdminReadToken::expose_secret)
    }

    /// Returns the deployment security mode.
    #[must_use]
    pub const fn deployment_mode(&self) -> DeploymentMode {
        self.deployment_mode
    }

    /// Overrides the deployment security mode.
    ///
    /// Marks the mode as explicitly selected so the plaintext-secret gate can
    /// distinguish "operator chose Insecure" from "insecure default left in
    /// place" (see [`Self::validate_plaintext_secrets_in_production`]).
    #[must_use]
    pub const fn with_deployment_mode(mut self, mode: DeploymentMode) -> Self {
        self.deployment_mode = mode;
        self.deployment_mode_explicitly_set = true;
        self
    }

    /// Returns whether plaintext persistent secrets are permitted in
    /// non-insecure (production) deployment modes.
    #[must_use]
    pub const fn allow_plaintext_secrets_in_production(&self) -> bool {
        self.allow_plaintext_secrets_in_production
    }

    /// Explicitly permits plaintext persistent secrets in non-insecure
    /// deployment modes.
    ///
    /// This is an insecure override intended only for migrating an existing
    /// deployment to at-rest secret encryption.
    #[must_use]
    pub const fn with_allow_plaintext_secrets_in_production(mut self, value: bool) -> Self {
        self.allow_plaintext_secrets_in_production = value;
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

    /// Returns the maximum S3 multipart part size in bytes.
    #[must_use]
    pub const fn s3_max_part_bytes(&self) -> NonZeroU64 {
        self.s3_max_part_bytes
    }

    /// Overrides the maximum S3 multipart part size in bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3MaxPartBytesTooSmall`] when the value is
    /// smaller than 1 MiB. Zero is rejected by the [`NonZeroU64`] type.
    pub fn with_s3_max_part_bytes(
        mut self,
        s3_max_part_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        if s3_max_part_bytes.get() < MIN_S3_MAX_PART_BYTES {
            return Err(ServerConfigError::S3MaxPartBytesTooSmall {
                minimum_bytes: MIN_S3_MAX_PART_BYTES,
            });
        }
        self.s3_max_part_bytes = s3_max_part_bytes;
        Ok(self)
    }

    /// Returns the S3 multipart upload session TTL in seconds.
    #[must_use]
    pub const fn s3_upload_session_ttl_seconds(&self) -> NonZeroU64 {
        self.s3_upload_session_ttl_seconds
    }

    /// Overrides the S3 multipart upload session TTL in seconds.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3UploadSessionTtl`] when the value is zero.
    pub const fn with_s3_upload_session_ttl_seconds(
        mut self,
        s3_upload_session_ttl_seconds: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.s3_upload_session_ttl_seconds = s3_upload_session_ttl_seconds;
        Ok(self)
    }

    /// Returns the maximum number of concurrently active S3 multipart upload
    /// sessions.
    #[must_use]
    pub const fn s3_upload_max_active_sessions(&self) -> NonZeroUsize {
        self.s3_upload_max_active_sessions
    }

    /// Overrides the maximum number of concurrently active S3 multipart upload
    /// sessions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3UploadMaxActiveSessions`] when the value
    /// is zero.
    pub const fn with_s3_upload_max_active_sessions(
        mut self,
        s3_upload_max_active_sessions: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        self.s3_upload_max_active_sessions = s3_upload_max_active_sessions;
        Ok(self)
    }

    /// Returns the S3 multipart minimum part size in bytes (S3's 5 MiB rule
    /// for all but the last part, enforced at `CompleteMultipartUpload` only —
    /// `UploadPart` accepts any body size, matching S3).
    #[must_use]
    pub const fn s3_min_part_bytes(&self) -> NonZeroU64 {
        self.s3_min_part_bytes
    }

    /// Overrides the S3 multipart minimum part size in bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3MinPartBytes`] when the value is zero.
    pub const fn with_s3_min_part_bytes(
        mut self,
        s3_min_part_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.s3_min_part_bytes = s3_min_part_bytes;
        Ok(self)
    }

    /// Returns the per-session multipart byte quota.
    #[must_use]
    pub const fn s3_upload_session_max_bytes(&self) -> NonZeroU64 {
        self.s3_upload_session_max_bytes
    }

    /// Overrides the per-session multipart byte quota.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3UploadSessionMaxBytes`] when the value
    /// is zero.
    pub const fn with_s3_upload_session_max_bytes(
        mut self,
        s3_upload_session_max_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.s3_upload_session_max_bytes = s3_upload_session_max_bytes;
        Ok(self)
    }

    /// Returns the aggregate multipart byte quota across active sessions.
    #[must_use]
    pub const fn s3_upload_total_max_bytes(&self) -> NonZeroU64 {
        self.s3_upload_total_max_bytes
    }

    /// Overrides the aggregate multipart byte quota across active sessions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3UploadTotalMaxBytes`] when the value is
    /// zero.
    pub const fn with_s3_upload_total_max_bytes(
        mut self,
        s3_upload_total_max_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.s3_upload_total_max_bytes = s3_upload_total_max_bytes;
        Ok(self)
    }

    /// Returns the global cap on part files stored across all active S3
    /// multipart upload sessions.
    #[must_use]
    pub const fn s3_upload_max_active_part_files(&self) -> NonZeroUsize {
        self.s3_upload_max_active_part_files
    }

    /// Overrides the global cap on part files stored across all active S3
    /// multipart upload sessions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::S3UploadMaxActivePartFiles`] when the
    /// value is zero.
    pub const fn with_s3_upload_max_active_part_files(
        mut self,
        s3_upload_max_active_part_files: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        self.s3_upload_max_active_part_files = s3_upload_max_active_part_files;
        Ok(self)
    }

    /// Returns the LFS chunked-patch (PATCH) staging TTL in seconds.
    #[must_use]
    pub const fn lfs_patch_ttl_seconds(&self) -> NonZeroU64 {
        self.lfs_patch_ttl_seconds
    }

    /// Overrides the LFS chunked-patch (PATCH) staging TTL in seconds.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::LfsPatchTtl`] when the value is zero.
    pub const fn with_lfs_patch_ttl_seconds(
        mut self,
        lfs_patch_ttl_seconds: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.lfs_patch_ttl_seconds = lfs_patch_ttl_seconds;
        Ok(self)
    }

    /// Returns the maximum number of concurrently active LFS chunked-patch
    /// sessions.
    #[must_use]
    pub const fn lfs_patch_max_active_sessions(&self) -> NonZeroUsize {
        self.lfs_patch_max_active_sessions
    }

    /// Overrides the maximum number of concurrently active LFS chunked-patch
    /// sessions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::LfsPatchMaxActiveSessions`] when the value
    /// is zero.
    pub const fn with_lfs_patch_max_active_sessions(
        mut self,
        lfs_patch_max_active_sessions: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        self.lfs_patch_max_active_sessions = lfs_patch_max_active_sessions;
        Ok(self)
    }

    /// Returns the aggregate byte cap across active LFS chunked-patch
    /// sessions.
    #[must_use]
    pub const fn lfs_patch_total_max_bytes(&self) -> NonZeroU64 {
        self.lfs_patch_total_max_bytes
    }

    /// Overrides the aggregate byte cap across active LFS chunked-patch
    /// sessions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::LfsPatchTotalMaxBytes`] when the value is
    /// zero.
    pub const fn with_lfs_patch_total_max_bytes(
        mut self,
        lfs_patch_total_max_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.lfs_patch_total_max_bytes = lfs_patch_total_max_bytes;
        Ok(self)
    }

    /// Returns the maximum distance an LFS chunked-patch (PATCH)
    /// `Content-Range` may start ahead of the session's current high-water
    /// mark.
    #[must_use]
    pub const fn lfs_patch_max_seek_ahead_bytes(&self) -> NonZeroU64 {
        self.lfs_patch_max_seek_ahead_bytes
    }

    /// Overrides the maximum distance an LFS chunked-patch (PATCH)
    /// `Content-Range` may start ahead of the session's current high-water
    /// mark.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::LfsPatchMaxSeekAheadBytes`] when the value
    /// is zero.
    pub const fn with_lfs_patch_max_seek_ahead_bytes(
        mut self,
        lfs_patch_max_seek_ahead_bytes: NonZeroU64,
    ) -> Result<Self, ServerConfigError> {
        self.lfs_patch_max_seek_ahead_bytes = lfs_patch_max_seek_ahead_bytes;
        Ok(self)
    }

    /// Returns the per-repo revision-registry cap: `create_revision` rejects
    /// new revision names once a repository has reached this many registered
    /// revisions.
    #[must_use]
    pub const fn max_revisions_per_repo(&self) -> NonZeroUsize {
        self.max_revisions_per_repo
    }

    /// Overrides the per-repo revision-registry cap.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::ZeroMaxRevisionsPerRepo`] when the value
    /// is zero.
    pub const fn with_max_revisions_per_repo(
        mut self,
        max_revisions_per_repo: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        self.max_revisions_per_repo = max_revisions_per_repo;
        Ok(self)
    }

    /// Returns the per-repo tree-entry cap: `register_path` rejects new path
    /// mappings once a repository has reached this many tree-entry rows
    /// (across every revision).
    #[must_use]
    pub const fn max_tree_entries_per_repo(&self) -> NonZeroUsize {
        self.max_tree_entries_per_repo
    }

    /// Overrides the per-repo tree-entry cap.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::ZeroMaxTreeEntriesPerRepo`] when the value
    /// is zero.
    pub const fn with_max_tree_entries_per_repo(
        mut self,
        max_tree_entries_per_repo: NonZeroUsize,
    ) -> Result<Self, ServerConfigError> {
        self.max_tree_entries_per_repo = max_tree_entries_per_repo;
        Ok(self)
    }

    /// Sets the target xorb container size in bytes.
    ///
    /// Once accumulated chunk data reaches this threshold, the upload
    /// ingestor may pack chunks into a xorb container.
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

    /// Enables at-rest encryption for Hub webhook signing secrets with the
    /// supplied 32-byte AES-256 key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyHubWebhookSecretKey`] when the key is
    /// empty, or [`ServerConfigError::HubWebhookSecretKeyLength`] when the key
    /// is not exactly 32 bytes.
    pub fn with_hub_webhook_secret_key(
        mut self,
        key: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let key = key.into();
        if key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyHubWebhookSecretKey);
        }
        let observed = key.len();
        if observed != usize::try_from(HUB_WEBHOOK_SECRET_KEY_BYTES).unwrap_or(0) {
            return Err(ServerConfigError::HubWebhookSecretKeyLength {
                expected: usize::try_from(HUB_WEBHOOK_SECRET_KEY_BYTES).unwrap_or(0),
                observed,
            });
        }
        self.auth.hub_webhook_secret_key = Some(key);
        Ok(self)
    }

    /// Enables at-rest encryption for provider-config secrets with the supplied
    /// 32-byte AES-256 key.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyConfigSecretKey`] when the key is
    /// empty, or [`ServerConfigError::ConfigSecretKeyLength`] when the key is
    /// not exactly 32 bytes.
    pub fn with_config_secret_key(
        mut self,
        key: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        let key = key.into();
        if key.expose_secret().is_empty() {
            return Err(ServerConfigError::EmptyConfigSecretKey);
        }
        let observed = key.len();
        if observed != usize::try_from(CONFIG_SECRET_KEY_BYTES).unwrap_or(0) {
            return Err(ServerConfigError::ConfigSecretKeyLength {
                expected: usize::try_from(CONFIG_SECRET_KEY_BYTES).unwrap_or(0),
                observed,
            });
        }
        self.auth.config_secret_key = Some(key);
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

    /// Sets the OIDC audience (`aud` claim) validated for tokens issued by the
    /// OIDC auth provider.
    ///
    /// When set, tokens whose `aud` claim does not match are rejected. When
    /// unset, the `aud` claim is not validated (see the startup warning in
    /// [`crate::app::build_auth_provider`]).
    #[must_use]
    pub fn with_auth_oidc_audience(mut self, audience: String) -> Self {
        self.auth.auth_oidc_audience = Some(audience);
        self
    }

    /// Sets the allowlist of hosts (besides the issuer's own host) whose JWKS
    /// endpoints the OIDC discovery document may advertise via `jwks_uri`.
    #[must_use]
    pub fn with_auth_oidc_jwks_host_allowlist(mut self, hosts: Vec<String>) -> Self {
        self.auth.auth_oidc_jwks_host_allowlist = Some(hosts);
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

    /// Enables the read-only administrative API with a dedicated bearer token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerConfigError::EmptyAdminReadToken`] when the token is empty.
    pub fn with_admin_read_token(
        mut self,
        admin_read_token: impl Into<SecretBytes>,
    ) -> Result<Self, ServerConfigError> {
        self.admin_read_token = Some(AdminReadToken::new(admin_read_token.into())?);
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
    ///
    /// Returns [`ServerConfigError::PlaintextSecretsInProduction`] when a
    /// non-insecure deployment mode would persist secrets without at-rest
    /// encryption keys.
    pub fn validate_runtime_requirements(&self) -> Result<(), ServerConfigError> {
        // The CDC chunker requires a power-of-two chunk size; a misconfigured
        // value must fail startup with a clear error instead of panicking on
        // the first upload (see `upload_ingest::cdc::CdcChunker`).
        if !self.chunk_size.get().is_power_of_two() {
            return Err(ServerConfigError::ChunkSizeNotPowerOfTwo);
        }

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

        if matches!(self.auth.auth_provider, AuthProviderKind::Ed25519)
            && self.auth.ed25519_private_key.is_none()
            && self.auth.ed25519_public_key.is_none()
        {
            return Err(ServerConfigError::MissingEd25519Key);
        }

        self.validate_deployment_mode_requirements()?;

        self.validate_plaintext_secrets_in_production()?;

        Ok(())
    }

    /// Validates that persistent secrets are never stored unencrypted in a
    /// production (non-insecure) deployment mode unless explicitly permitted.
    ///
    /// The plaintext gate is armed whenever persistent secrets are present
    /// without an at-rest encryption key UNLESS the operator explicitly opted
    /// out: either `SHARDLINE_ALLOW_PLAINTEXT_SECRETS_IN_PRODUCTION` is set, or
    /// the deployment mode was EXPLICITLY set to Insecure. Because the default
    /// (unset) mode is Insecure, the default case — secrets configured with no
    /// encryption key and no mode override — fails loud instead of silently
    /// persisting secrets in plaintext.
    fn validate_plaintext_secrets_in_production(&self) -> Result<(), ServerConfigError> {
        if self.allow_plaintext_secrets_in_production {
            return Ok(());
        }
        let explicitly_insecure =
            self.deployment_mode == DeploymentMode::Insecure && self.deployment_mode_explicitly_set;
        if explicitly_insecure {
            return Ok(());
        }
        let surfaces = self.plaintext_secret_surfaces();
        if surfaces.is_empty() {
            return Ok(());
        }
        Err(ServerConfigError::PlaintextSecretsInProduction {
            surfaces: surfaces.join("; "),
        })
    }

    /// Returns the enabled surfaces whose persistent secrets would be stored
    /// in plaintext because no at-rest encryption key is configured.
    fn plaintext_secret_surfaces(&self) -> Vec<&'static str> {
        let mut surfaces = Vec::new();
        if self.server_frontends().contains(&ServerFrontend::Hub)
            && self.auth.hub_webhook_secret_key.is_none()
        {
            surfaces.push("hub webhook signing secrets (set SHARDLINE_HUB_WEBHOOK_SECRET_KEY)");
        }
        if (self.provider.config_path.is_some() || self.provider.api_key.is_some())
            && self.auth.config_secret_key.is_none()
        {
            surfaces.push("provider-config webhook secrets (set SHARDLINE_CONFIG_SECRET_KEY)");
        }
        surfaces
    }

    /// Returns true when this config will actually produce a REAL auth
    /// provider.
    ///
    /// Mirrors `build_auth_provider`: the Local provider with no token signing
    /// key maps to permissive mode (`None`). Passthrough is also NOT a real
    /// provider here: it trusts every inbound token (authenticated in name
    /// only — the actual control is the loopback-bind requirement enforced in
    /// [`Self::validate_runtime_requirements`]) and is handled separately as
    /// the trusted-proxy carve-out for [`DeploymentMode::Authenticated`].
    /// Every other configured provider kind (Local with a key, Oidc, Jwks,
    /// Ed25519) yields a real provider that `authorize()` enforces.
    pub(crate) fn auth_provider_is_configured(&self) -> bool {
        if self.auth.auth_provider == AuthProviderKind::Passthrough {
            return false;
        }
        !(self.auth.auth_provider == AuthProviderKind::Local
            && self.auth.token_signing_key.is_none())
    }

    /// Validates deployment-mode-specific constraints.
    pub(crate) fn validate_deployment_mode_requirements(&self) -> Result<(), ServerConfigError> {
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
                // Metrics token is required: without it /metrics serves the
                // platform's runtime state to any unauthenticated caller,
                // which the strict-mode contract forbids (enums.rs documents
                // the metrics token as required).
                if self.metrics_token().is_none() {
                    return Err(ServerConfigError::MissingMetricsToken);
                }
            }
            DeploymentMode::Authenticated => {
                if self.auth.auth_provider == AuthProviderKind::Passthrough {
                    // Passthrough trusts every inbound token (authenticated in
                    // name only); the real control is the loopback-bind
                    // requirement enforced in `validate_runtime_requirements`.
                    // This is the explicit trusted-proxy carve-out: warn but do
                    // not treat Passthrough as a configured auth provider.
                    tracing::warn!(
                        "authenticated mode with passthrough auth: only use behind a trusted proxy"
                    );
                } else if !self.auth_provider_is_configured() {
                    // Some real auth provider must be configured; without one
                    // the mode fails open to anonymous full access. Passthrough
                    // does NOT satisfy this requirement (it is handled by the
                    // carve-out above).
                    return Err(ServerConfigError::ConfigFileError(
                        "authenticated deployment mode requires a configured auth provider \
                         (set SHARDLINE_TOKEN_SIGNING_KEY_FILE or an OIDC/JWKS/Ed25519 provider; \
                         the passthrough provider does not satisfy this requirement)"
                            .into(),
                    ));
                }
            }
            DeploymentMode::Insecure => {
                // Refuse to boot a fully unauthenticated server on a
                // non-loopback address when the Insecure mode is only the
                // implicit (unset) default and no auth provider is configured:
                // an operator who did not explicitly choose the mode likely
                // intends a production deployment. Explicit
                // SHARDLINE_DEPLOYMENT_MODE=insecure, a loopback bind, or a
                // configured auth provider each make the intent unambiguous.
                if !self.deployment_mode_explicitly_set
                    && !self.auth_provider_is_configured()
                    && !self.bind_addr.ip().is_loopback()
                {
                    return Err(ServerConfigError::InsecureDefaultRequiresExplicitOptIn {
                        bind_addr: self.bind_addr,
                    });
                }
                // Warn only when no auth provider is configured: with a provider
                // present, authorize() still enforces authentication despite
                // the insecure mode.
                if !self.auth_provider_is_configured() {
                    tracing::warn!(
                        "insecure deployment mode: all requests are allowed without authentication"
                    );
                }
            }
        }
        Ok(())
    }
}

/// A human-readable byte-size unit suffix.
///
/// Supports both decimal (SI) units (`B`, `KB`, `MB`, `GB`, `TB` — powers of
/// 1000) and binary (IEC) units (`KiB`, `MiB`, `GiB`, `TiB` — powers of 1024).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteUnit {
    /// Plain bytes.
    B,
    /// Binary kibibytes (`1024` bytes).
    KiB,
    /// Binary mebibytes (`1024^2` bytes).
    MiB,
    /// Binary gibibytes (`1024^3` bytes).
    GiB,
    /// Binary tebibytes (`1024^4` bytes).
    TiB,
    /// Decimal kilobytes (`1000` bytes).
    KB,
    /// Decimal megabytes (`1000^2` bytes).
    MB,
    /// Decimal gigabytes (`1000^3` bytes).
    GB,
    /// Decimal terabytes (`1000^4` bytes).
    TB,
}

impl ByteUnit {
    /// Returns the multiplier (bytes per unit) for this unit.
    #[must_use]
    pub const fn as_multiplier(self) -> f64 {
        match self {
            Self::B => 1.0,
            Self::KiB => 1024.0,
            Self::MiB => 1_048_576.0_f64,         // 1024^2
            Self::GiB => 1_073_741_824.0_f64,     // 1024^3
            Self::TiB => 1_099_511_627_776.0_f64, // 1024^4
            Self::KB => 1_000.0,
            Self::MB => 1_000_000.0_f64,         // 1000^2
            Self::GB => 1_000_000_000.0_f64,     // 1000^3
            Self::TB => 1_000_000_000_000.0_f64, // 1000^4
        }
    }
}

impl FromStr for ByteUnit {
    type Err = ServerConfigError;

    /// Parses a byte-unit suffix, ignoring ASCII case and surrounding whitespace.
    ///
    /// An empty string is treated as plain bytes (`B`).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "b" | "" => Ok(Self::B),
            "kib" => Ok(Self::KiB),
            "mib" => Ok(Self::MiB),
            "gib" => Ok(Self::GiB),
            "tib" => Ok(Self::TiB),
            "kb" => Ok(Self::KB),
            "mb" => Ok(Self::MB),
            "gb" => Ok(Self::GB),
            "tb" => Ok(Self::TB),
            _ => Err(ServerConfigError::ChunkSizeParse(
                "unknown size unit".to_owned(),
            )),
        }
    }
}

/// Parse a human-readable byte size string like `"64KB"`, `"1GB"`, `"512b"`,
/// `"57mb"`, or a plain number interpreted as bytes.
///
/// Accepts:
/// - Decimal (SI): `b`/`B`, `kb`/`KB`, `mb`/`MB`, `gb`/`GB`, `tb`/`TB`
///   (powers of 1000)
/// - Binary (IEC): `kib`/`KiB`, `mib`/`MiB`, `gib`/`GiB`, `tib`/`TiB`
///   (powers of 1024)
///
/// Case-insensitive. Plain numbers are parsed as raw bytes.
///
/// # Errors
///
/// Returns [`ServerConfigError::ChunkSizeParse`] when the string is empty, the
/// number portion cannot be parsed, the unit is unknown, or the value is
/// negative. Returns [`ServerConfigError::ZeroChunkSize`] when the result is
/// zero.
pub fn parse_byte_size(s: &str) -> Result<usize, ServerConfigError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ServerConfigError::ChunkSizeParse(
            "empty chunk size".to_owned(),
        ));
    }

    // Try plain number first (e.g. "65536")
    if let Ok(n) = s.parse::<usize>() {
        if n == 0 {
            return Err(ServerConfigError::ZeroChunkSize);
        }
        return Ok(n);
    }

    // Split between trailing digits and the unit suffix.
    let split_pos = s
        .rfind(|c: char| c.is_ascii_digit())
        .map(|i| i.wrapping_add(1))
        .unwrap_or(0);
    if split_pos == 0 || split_pos >= s.len() {
        return Err(ServerConfigError::ChunkSizeParse(
            "invalid chunk size format".to_owned(),
        ));
    }
    let num_str = &s[..split_pos];
    let unit = &s[split_pos..];

    let num: f64 = num_str
        .parse()
        .map_err(|_e| ServerConfigError::ChunkSizeParse("invalid chunk size number".to_owned()))?;

    if num < 0.0 {
        return Err(ServerConfigError::ChunkSizeParse(
            "negative chunk size".to_owned(),
        ));
    }

    // Decimal (SI) units use powers of 1000; binary (IEC) use powers of 1024.
    let multiplier: f64 = ByteUnit::from_str(unit)?.as_multiplier();

    #[allow(clippy::float_arithmetic)]
    let bytes = (num * multiplier) as usize;
    if bytes == 0 {
        return Err(ServerConfigError::ZeroChunkSize);
    }
    Ok(bytes)
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
