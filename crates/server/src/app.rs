mod operational;
mod protocol_routes;
mod provider;
mod provider_routes;
mod reconstruction_helpers;
mod reconstruction_routes;

pub use provider::{
    extract_provider_subject, latest_lifecycle_signal_at, reconciled_provider_repository_state,
    validate_provider_name_path,
};
pub use reconstruction_helpers::{full_byte_stream_response, parse_batch_reconstruction_query};

use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::HeaderMap,
    routing::{get, head, post},
    serve as serve_http,
};
use shardline_protocol::{RepositoryScope, TokenScope};
use tokio::net::TcpListener;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::{
    ServerConfig, ServerError,
    auth::{AuthContext, ServerAuth},
    backend::ServerBackend,
    config::AuthProviderKind,
    metrics::{MetricsLayer, metrics_routes},
    provider::ProviderTokenService,
    reconstruction_cache::ReconstructionCacheService,
    server_frontend::ServerFrontend,
    server_role::ServerRole,
    transfer_limiter::TransferLimiter,
    xet_adapter::{XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE, XORB_TRANSFER_ROUTE},
};
use operational::{
    head_xorb, health, metrics, read_chunk, read_xorb_transfer, ready, stats, upload_shard,
    upload_xorb,
};
use protocol_routes::{
    bazel_get_ac, bazel_get_cas, bazel_put_ac, bazel_put_cas, lfs_batch, lfs_get_object,
    lfs_head_object, lfs_put_object, oci_api_dispatch, oci_dispatch, oci_registry_token,
    oci_transfer_dispatch, oci_v2_root,
};
pub(crate) use protocol_routes::{parse_oci_path, parse_upload_content_range};
use provider_routes::{
    git_lfs_authenticate, handle_provider_webhook, issue_provider_token, issue_xet_read_token,
    issue_xet_write_token,
};
use reconstruction_routes::{batch_reconstruction, reconstruction, reconstruction_v2};

pub const MAX_BATCH_RECONSTRUCTION_FILE_IDS: usize = 1024;
pub const MAX_BATCH_RECONSTRUCTION_QUERY_BYTES: usize = 131_072;
pub const MAX_LFS_BATCH_OBJECTS: usize = 1024;
pub const MAX_OCI_MANIFEST_TAGS: usize = 128;
pub const MAX_OCI_TAG_LIST_PAGE_SIZE: usize = 256;
pub const MAX_PROTOCOL_QUERY_BYTES: usize = 16_384;
pub const MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES: usize = 16_384;
pub const MAX_PROVIDER_WEBHOOK_BODY_BYTES: usize = 1_048_576;
pub const MAX_PROVIDER_NAME_BYTES: usize = 64;
pub const MAX_PROVIDER_SUBJECT_BYTES: usize = 512;
pub const MAX_PROVIDER_BASIC_AUTH_HEADER_BYTES: usize = 4096;

#[derive(Debug)]
pub struct AppState {
    pub config: ServerConfig,
    pub role: ServerRole,
    pub backend: ServerBackend,
    pub auth: Option<ServerAuth>,
    pub provider_tokens: Option<ProviderTokenService>,
    pub reconstruction_cache: ReconstructionCacheService,
    pub transfer_limiter: TransferLimiter,
    pub oci_registry_token_limiter: Arc<Semaphore>,
    pub protocol_metrics: ProtocolMetrics,
}

#[derive(Debug, Default)]
pub struct ProtocolMetrics {
    oci_registry_token_requests_total: AtomicU64,
    oci_registry_token_rate_limited_total: AtomicU64,
    oci_registry_token_active_requests: AtomicU64,
}

impl ProtocolMetrics {
    fn increment_oci_registry_token_requests(&self) {
        let _previous = self
            .oci_registry_token_requests_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn increment_oci_registry_token_rate_limited(&self) {
        let _previous = self
            .oci_registry_token_rate_limited_total
            .fetch_add(1, Ordering::Relaxed);
    }

    fn begin_oci_registry_token_request(&self) -> ActiveProtocolRequestGuard<'_> {
        let _previous = self
            .oci_registry_token_active_requests
            .fetch_add(1, Ordering::Relaxed);
        ActiveProtocolRequestGuard {
            gauge: &self.oci_registry_token_active_requests,
        }
    }
}

#[derive(Debug)]
struct ActiveProtocolRequestGuard<'metric> {
    gauge: &'metric AtomicU64,
}

impl Drop for ActiveProtocolRequestGuard<'_> {
    fn drop(&mut self) {
        let _previous = self.gauge.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Builds the Shardline HTTP router.
///
/// # Errors
///
/// Returns [`ServerError`] when the configured backend cannot initialize.
pub async fn router(config: ServerConfig) -> Result<Router, ServerError> {
    config.validate_runtime_requirements()?;
    let role = config.server_role();
    let max_request_body_bytes = config.max_request_body_bytes();
    let provider_token_body_limit = bounded_api_body_limit(
        max_request_body_bytes,
        MAX_PROVIDER_TOKEN_REQUEST_BODY_BYTES,
    );
    let provider_webhook_body_limit =
        bounded_api_body_limit(max_request_body_bytes, MAX_PROVIDER_WEBHOOK_BODY_BYTES);
    let backend = ServerBackend::from_config(&config).await?;
    let auth = build_auth_provider(&config).await?;
    let provider_tokens = if role.serves_api() {
        match (
            config.provider_config_path(),
            config.provider_api_key(),
            config.provider_token_issuer(),
            config.provider_token_ttl_seconds(),
            config.token_signing_key(),
        ) {
            (
                Some(config_path),
                Some(api_key),
                Some(issuer),
                Some(ttl_seconds),
                Some(signing_key),
            ) => Some(ProviderTokenService::from_file(
                config_path,
                api_key.to_vec(),
                issuer,
                ttl_seconds,
                signing_key,
            )?),
            _ => None,
        }
    } else {
        None
    };
    let reconstruction_cache = if role.uses_reconstruction_cache() {
        ReconstructionCacheService::from_config(&config)?
    } else {
        ReconstructionCacheService::disabled()
    };
    let transfer_limiter =
        TransferLimiter::new(config.chunk_size(), config.transfer_max_in_flight_chunks());
    let oci_registry_token_limiter = Arc::new(Semaphore::new(
        config.oci_registry_token_max_in_flight_requests().get(),
    ));
    let state = Arc::new(AppState {
        config,
        role,
        backend,
        auth,
        provider_tokens,
        reconstruction_cache,
        transfer_limiter,
        oci_registry_token_limiter,
        protocol_metrics: ProtocolMetrics::default(),
    });

    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .layer(MetricsLayer);
    if role.serves_api() {
        app = app
            .route(
                "/v1/providers/{provider}/tokens",
                post(issue_provider_token).layer(DefaultBodyLimit::max(provider_token_body_limit)),
            )
            .route(
                "/v1/providers/{provider}/git-lfs-authenticate",
                post(git_lfs_authenticate).layer(DefaultBodyLimit::max(provider_token_body_limit)),
            )
            .route(
                "/v1/providers/{provider}/webhooks",
                post(handle_provider_webhook)
                    .layer(DefaultBodyLimit::max(provider_webhook_body_limit)),
            )
            .route("/v1/stats", get(stats));
    }
    for frontend in state.config.server_frontends() {
        app = register_frontend_routes(app, *frontend, role, &state)?;
    }

    Ok(app
        .layer(DefaultBodyLimit::max(max_request_body_bytes.get()))
        .with_state(state))
}

/// Runs the Shardline HTTP server.
///
/// # Errors
///
/// Returns [`ServerError`] when the listener cannot bind or the server exits with an
/// IO error.
#[tracing::instrument(skip(config), fields(bind_addr = %config.bind_addr()))]
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    let listener = TcpListener::bind(config.bind_addr()).await?;
    tracing::info!("listening on {}", config.bind_addr());
    serve_with_listener(config, listener).await
}

/// Runs the Shardline HTTP server on an existing listener.
///
/// # Errors
///
/// Returns [`ServerError`] when router initialization fails or the server exits with
/// an IO error.
pub async fn serve_with_listener(
    config: ServerConfig,
    listener: TcpListener,
) -> Result<(), ServerError> {
    let app = router(config).await?;
    tracing::info!("router initialized, starting HTTP serve");
    serve_http(listener, app).await?;
    Ok(())
}

fn register_frontend_routes(
    app: Router<Arc<AppState>>,
    frontend: ServerFrontend,
    role: ServerRole,
    app_state: &AppState,
) -> Result<Router<Arc<AppState>>, ServerError> {
    match frontend {
        ServerFrontend::Xet => Ok(register_xet_routes(app, role)),
        ServerFrontend::Lfs => Ok(register_lfs_routes(app, role)),
        ServerFrontend::BazelHttp => Ok(register_bazel_routes(app, role)),
        ServerFrontend::Oci => Ok(register_oci_routes(app, role)),
        ServerFrontend::Hub => register_hub_routes(app, app_state),
        ServerFrontend::Metrics => Ok(app.merge(metrics_routes::<Arc<AppState>>())),
    }
}

fn register_xet_routes(mut app: Router<Arc<AppState>>, role: ServerRole) -> Router<Arc<AppState>> {
    if role.serves_api() {
        app = app
            .route(XET_READ_TOKEN_ROUTE, get(issue_xet_read_token))
            .route(XET_WRITE_TOKEN_ROUTE, get(issue_xet_write_token))
            .route("/reconstructions", get(batch_reconstruction))
            .route("/v1/reconstructions", get(batch_reconstruction))
            .route("/v1/reconstructions/{file_id}", get(reconstruction))
            .route("/v2/reconstructions/{file_id}", get(reconstruction_v2))
            .route("/shards", post(upload_shard))
            .route("/v1/shards", post(upload_shard));
    }
    if role.serves_transfer() {
        app = app
            .route("/v1/chunks/default/{hash}", get(read_chunk))
            .route("/v1/chunks/default-merkledb/{hash}", get(read_chunk))
            .route(
                "/v1/xorbs/default/{hash}",
                head(head_xorb).post(upload_xorb),
            )
            .route(XORB_TRANSFER_ROUTE, get(read_xorb_transfer));
    }
    app
}

fn register_lfs_routes(mut app: Router<Arc<AppState>>, role: ServerRole) -> Router<Arc<AppState>> {
    if role.serves_api() {
        app = app.route("/v1/lfs/objects/batch", post(lfs_batch));
    }
    if role.serves_transfer() {
        app = app.route(
            "/v1/lfs/objects/{oid}",
            get(lfs_get_object)
                .head(lfs_head_object)
                .put(lfs_put_object),
        );
    }
    app
}

fn register_bazel_routes(
    mut app: Router<Arc<AppState>>,
    role: ServerRole,
) -> Router<Arc<AppState>> {
    if role.serves_transfer() {
        app = app
            .route(
                "/v1/bazel/cache/ac/{hash}",
                get(bazel_get_ac).put(bazel_put_ac),
            )
            .route(
                "/v1/bazel/cache/cas/{hash}",
                get(bazel_get_cas).put(bazel_put_cas),
            );
    }
    app
}

fn register_oci_routes(mut app: Router<Arc<AppState>>, role: ServerRole) -> Router<Arc<AppState>> {
    match role {
        ServerRole::All => {
            app = app
                .route("/v2/token", get(oci_registry_token))
                .route("/v2/", get(oci_v2_root))
                .route("/v2/{*path}", axum::routing::any(oci_dispatch));
        }
        ServerRole::Api => {
            app = app
                .route("/v2/token", get(oci_registry_token))
                .route("/v2/", get(oci_v2_root))
                .route("/v2/{*path}", axum::routing::any(oci_api_dispatch));
        }
        ServerRole::Transfer => {
            app = app.route("/v2/{*path}", axum::routing::any(oci_transfer_dispatch));
        }
    }
    app
}

fn authorize(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, ServerError> {
    if let Some(auth) = &state.auth {
        return Ok(Some(auth.authorize(headers, required_scope)?));
    }

    Ok(None)
}

const fn scope_from_auth(auth: &AuthContext) -> &RepositoryScope {
    auth.claims().repository()
}

#[must_use]
pub fn bounded_api_body_limit(configured_limit: NonZeroUsize, endpoint_limit: usize) -> usize {
    configured_limit.get().min(endpoint_limit)
}

fn register_hub_routes(
    app: Router<Arc<AppState>>,
    app_state: &AppState,
) -> Result<Router<Arc<AppState>>, ServerError> {
    let hub_auth = app_state
        .auth
        .as_ref()
        .map(|sa| shardline_hub_api::auth::HubAuth::from_arc(sa.provider_arc()));

    // Create a HubStore from the configured backend.
    // When using Postgres, share the main server's pool so Hub API and the core
    // server operate on the same database connection pool (multi-process safe).
    let root_dir = app_state.config.root_dir();
    let store: shardline_index::hub::BoxedHubStore =
        app_state.config.index_postgres_url().map_or_else(
            || -> Result<shardline_index::hub::BoxedHubStore, ServerError> {
                let hub_root = root_dir.join("hub");
                std::fs::create_dir_all(&hub_root).ok();
                let sqlite_store = shardline_index::LocalIndexStore::new(hub_root)
                    .map_err(|e| ServerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
                Ok(shardline_index::hub::BoxedHubStore::from_store(sqlite_store))
            },
            |pg_url| -> Result<shardline_index::hub::BoxedHubStore, ServerError> {
                let pool = sqlx::PgPool::connect_lazy(pg_url)
                    .map_err(|e| ServerError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
                let pg_store = shardline_index::PostgresIndexStore::new(pool);
                Ok(shardline_index::hub::BoxedHubStore::from_store(pg_store))
            },
        )?;

    // Build an HTTP client for outbound webhook delivery.
    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .ok();

    let hub_state = shardline_hub_api::routes::HubState {
        store,
        auth: hub_auth,
        http_client,
    };
    shardline_hub_api::init(hub_state);
    Ok(app.merge(shardline_hub_api::hub_routes()))
}

fn endpoint_body_limit(
    configured_limit: NonZeroUsize,
    endpoint_limit: usize,
) -> Result<NonZeroUsize, ServerError> {
    NonZeroUsize::new(bounded_api_body_limit(configured_limit, endpoint_limit))
        .ok_or(ServerError::Overflow)
}

/// Acquires a semaphore permit for a chunk transfer.
///
/// # Errors
///
/// Returns [`ServerError`] if the chunk length cannot be determined or the transfer limiter
/// is closed.
pub async fn acquire_chunk_transfer_permit(
    state: &AppState,
    hash_hex: &str,
) -> Result<OwnedSemaphorePermit, ServerError> {
    let total_bytes = state.backend.chunk_length(hash_hex).await?;
    state.transfer_limiter.acquire_bytes(total_bytes).await
}

async fn build_auth_provider(config: &ServerConfig) -> Result<Option<ServerAuth>, ServerError> {
    match config.auth_provider() {
        AuthProviderKind::Local => {
            let Some(key) = config.token_signing_key() else {
                return Ok(None);
            };
            Ok(Some(ServerAuth::new(key)?))
        }
        AuthProviderKind::Passthrough => {
            let provider = Box::new(shardline_server_core::auth::PassthroughProvider);
            Ok(Some(ServerAuth::from_provider(provider)))
        }
        AuthProviderKind::Oidc => {
            let issuer = config.auth_oidc_issuer().ok_or_else(|| {
                ServerError::Config(crate::config::ServerConfigError::InvalidAuthProvider)
            })?;
            let provider = crate::oidc_provider::OidcProvider::new(issuer)
                .await
                .map_err(|_e| {
                    ServerError::Config(crate::config::ServerConfigError::InvalidAuthProvider)
                })?;
            Ok(Some(ServerAuth::from_provider(Box::new(provider))))
        }
        AuthProviderKind::Jwks => {
            let jwks_url = config.auth_jwks_url().ok_or_else(|| {
                ServerError::Config(crate::config::ServerConfigError::InvalidAuthProvider)
            })?;
            let provider = crate::jwks_provider::JwksProvider::new(jwks_url)
                .await
                .map_err(|_e| {
                    ServerError::Config(crate::config::ServerConfigError::InvalidAuthProvider)
                })?;
            Ok(Some(ServerAuth::from_provider(Box::new(provider))))
        }
    }
}

#[cfg(test)]
mod tests;
