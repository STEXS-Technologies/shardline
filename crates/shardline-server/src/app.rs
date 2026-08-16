mod metadata_routes;
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
    fs,
    future::{Future, pending},
    io::Error,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::{HeaderMap, Method, header},
    middleware::{self, Next},
    response::IntoResponse,
    routing::{get, head, post},
    serve as serve_http,
};
use shardline_protocol::{RepositoryScope, TokenScope};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use tower_http::cors::{Any, CorsLayer};

use shardline_server_core::auth::Ed25519AuthProvider;

use crate::{
    ServerConfig, ServerError,
    admission::{ExecutionPools, WeightedAdmission, timeouts},
    auth::{AuthContext, ServerAuth},
    backend::ServerBackend,
    config::{
        AuthProviderKind, DeploymentMode, ServerConfigError, env::bounded_pool_size_from_env,
    },
    jwks_provider::JwksProvider,
    metrics::MetricsLayer,
    oidc_provider::OidcProvider,
    provider::ProviderTokenService,
    reconstruction_cache::ReconstructionCacheService,
    route_policy::{RoutePolicyRegistry, register_route_policies},
    server_frontend::ServerFrontend,
    server_role::ServerRole,
    transfer_limiter::TransferLimiter,
    xet_adapter::{
        XET_PATH_ROUTE, XET_READ_TOKEN_ROUTE, XET_REVISION_ROUTE, XET_REVISIONS_ROUTE,
        XET_TREE_ROUTE, XET_WRITE_TOKEN_ROUTE, XORB_TRANSFER_ROUTE,
    },
};
use metadata_routes::{
    create_revision, delete_path, delete_revision, list_revisions, register_path, tree_lookup,
};
use operational::{
    head_xorb, health, metrics, read_chunk, read_xorb_transfer, ready, stats, upload_shard,
    upload_xorb, write_xorb_transfer,
};
use protocol_routes::{
    bazel_get, bazel_get_ac, bazel_get_cas, bazel_head, bazel_head_ac, bazel_head_cas, bazel_put,
    bazel_put_ac, bazel_put_cas, lfs_batch, lfs_delete_object, lfs_get_object, lfs_head_object,
    lfs_patch_object, lfs_put_object, lfs_verify_object, oci_api_dispatch, oci_dispatch,
    oci_registry_token, oci_transfer_dispatch, oci_v2_root, s3_create_bucket, s3_delete_bucket,
    s3_delete_object, s3_get_bucket, s3_get_object, s3_head_bucket, s3_head_object,
    s3_list_buckets, s3_post_bucket, s3_post_object, s3_put_object,
};
#[cfg(feature = "fuzzing")]
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
    pub admission: WeightedAdmission,
    pub pools: ExecutionPools,
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
/// Initializes the configured metadata backend, object store, authentication,
/// and reconstruction cache, then assembles the full Axum [`Router`] for the
/// configured server role. This is the entry point for embedders that want to
/// serve Shardline routes from their own process or test them in-process.
///
/// # Examples
///
/// ```no_run
/// use shardline_server::{ServerConfig, app::router};
/// use std::net::{IpAddr, Ipv4Addr, SocketAddr};
/// use std::num::NonZeroUsize;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ServerConfig::new(
///         SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
///         "http://127.0.0.1:8080".to_owned(),
///         std::env::temp_dir(),
///         NonZeroUsize::new(64 * 1024).expect("64 KiB chunk size is non-zero"),
///     );
///     let app = router(config).await?;
///     let listener = tokio::net::TcpListener::bind("127.0.0.1:8080").await?;
///     axum::serve(listener, app).await?;
///     Ok(())
/// }
/// ```
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
    let config_secret_cipher = config.config_secret_key().map_or_else(
        || {
            tracing::warn!(
                "SHARDLINE_CONFIG_SECRET_KEY not configured; provider-config secrets will be stored unencrypted"
            );
            None
        },
        |key| {
            let key_bytes = shardline_protocol::SecretBytes::new(key.to_vec());
            match shardline_server_core::at_rest::AtRestCipher::new(key_bytes) {
                Ok(cipher) => Some(cipher),
                Err(e) => {
                    tracing::warn!(
                        "invalid SHARDLINE_CONFIG_SECRET_KEY; provider-config secrets will be stored unencrypted: {e}"
                    );
                    None
                }
            }
        },
    );
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
                config_secret_cipher.as_ref(),
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
    let admission = WeightedAdmission::new(config.admission_max_weight());
    let pools = ExecutionPools::with_sizes(
        bounded_pool_size_from_env("SHARDLINE_HASHING_POOL_SIZE", 8),
        bounded_pool_size_from_env("SHARDLINE_PARSING_POOL_SIZE", 8),
        bounded_pool_size_from_env("SHARDLINE_BLOCKING_IO_POOL_SIZE", 16),
    );
    let state = Arc::new(AppState {
        config,
        role,
        backend,
        auth,
        provider_tokens,
        reconstruction_cache,
        transfer_limiter,
        admission,
        pools,
        oci_registry_token_limiter,
        protocol_metrics: ProtocolMetrics::default(),
    });

    // Sweep expired S3 multipart upload sessions at startup (crash recovery);
    // in-flight sweeps also run on every session creation.
    if state
        .config
        .server_frontends()
        .iter()
        .any(|frontend| matches!(frontend, ServerFrontend::S3))
    {
        match shardline_s3_adapter::sweep_expired_sessions(
            state.config.root_dir(),
            state.config.s3_upload_session_ttl_seconds(),
        )
        .await
        {
            Ok(removed) => {
                tracing::info!(
                    removed,
                    "s3 startup sweep removed expired multipart upload sessions"
                );
            }
            Err(error) => {
                tracing::warn!(error = %error, "s3 startup sweep of expired multipart upload sessions failed");
            }
        }
    }

    // Sweep expired LFS chunked-patch (PATCH) staging sessions at startup
    // (crash recovery); in-flight sweeps also run on every PATCH, mirroring
    // the S3 multipart sweep's startup + on-creation scheduling (F-20).
    if state
        .config
        .server_frontends()
        .iter()
        .any(|frontend| matches!(frontend, ServerFrontend::Lfs))
    {
        match protocol_routes::sweep_lfs_patch_sessions(
            state.config.root_dir(),
            state.config.lfs_patch_ttl_seconds(),
        ) {
            Ok(removed) => {
                tracing::info!(
                    removed,
                    "lfs startup sweep removed expired chunked-patch staging sessions"
                );
            }
            Err(error) => {
                tracing::warn!(error = %error, "lfs startup sweep of expired chunked-patch staging sessions failed");
            }
        }
    }

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
        ])
        .allow_headers(Any);

    let mut app = Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics))
        .layer(MetricsLayer)
        .layer(middleware::from_fn(request_timeout_middleware))
        .layer(middleware::from_fn(security_headers_middleware));
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

    // Build hub routes separately — they carry their own state type (HubState)
    // and must be merged at the Router<()> level after both sides are
    // converted via `.with_state()`.
    let mut hub_state: Option<shardline_hub_api::routes::HubState> = None;
    let mut xet_frontend_enabled = false;
    // S3 is mounted as the app-level FALLBACK rather than merged into the main
    // route trie: its `/{bucket}/{*key}` wildcard would otherwise conflict with
    // any other frontend's root-level parameter routes (Hub's Git Smart HTTP
    // and file-resolve routes) — matchit cannot host a wildcard and a parameter
    // with children at the same position. As a fallback, every registered route
    // (Hub/OCI/LFS/Xet/Bazel/metrics/healthz) wins, and S3 serves everything
    // else, which is exactly the S3 catch-all contract.
    let mut s3_router: Option<Router> = None;
    for frontend in state.config.server_frontends() {
        match frontend {
            ServerFrontend::Hub => {
                hub_state = Some(build_hub_state(&state)?);
            }
            ServerFrontend::Xet => {
                xet_frontend_enabled = true;
                app = register_frontend_routes(app, *frontend, role, &state);
            }
            ServerFrontend::S3 => {
                // Build the S3 router separately and apply the state at build
                // time so it can be mounted as the app-level fallback service
                // (an un-applied Router<Arc<AppState>> is not a Service).
                s3_router = Some(register_s3_routes(Router::new(), role).with_state(state.clone()));
            }
            ServerFrontend::Lfs | ServerFrontend::BazelHttp | ServerFrontend::Oci => {
                app = register_frontend_routes(app, *frontend, role, &state);
            }
        }
    }

    let app = app
        .layer(DefaultBodyLimit::max(max_request_body_bytes.get()))
        .with_state(state);

    // Merge hub routes (Router<()>) into the main app (Router<()>).
    let app = if let Some(hs) = hub_state {
        app.merge(shardline_hub_api::hub_routes(hs, !xet_frontend_enabled))
    } else {
        app
    };

    // Mount S3 as the fallback AFTER every registered route (frontends, Hub,
    // health/metrics): unmatched paths fall through to the S3 router.
    let app = if let Some(s3_router) = s3_router {
        app.fallback_service(s3_router)
    } else {
        app
    };

    // Apply CORS after every optional frontend has been registered and the Hub
    // router has been merged, so preflight and normal requests are covered by
    // the same policy regardless of which protocol owns the route.
    let app = app.layer(cors);

    // Register route auth policies for auditability and fail-closed enforcement.
    let mut policy_registry = RoutePolicyRegistry::new();
    register_route_policies(&mut policy_registry);
    tracing::debug!("registered {} route auth policies", policy_registry.len());

    Ok(app)
}

/// Runs the Shardline HTTP server.
///
/// # Examples
///
/// ```no_run
/// use shardline_server::{ServerConfig, serve};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ServerConfig::from_env()?;
///     serve(config).await?;
///     Ok(())
/// }
/// ```
///
/// This is a long-running function: it binds the configured address, initializes
/// the router (including the metadata backend and object store), and serves
/// until the process receives a shutdown signal.
///
/// # Errors
///
/// Returns [`ServerError`] when the listener cannot bind or the server exits with an
/// IO error.
#[tracing::instrument(skip(config), fields(bind_addr = %config.bind_addr()))]
pub async fn serve(config: ServerConfig) -> Result<(), ServerError> {
    shardline_metrics::metrics()
        .system
        .set_uptime(shardline_protocol::unix_now_seconds_lossy() as i64);
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
    serve_with_listener_until(config, listener, async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await
}

/// Runs the server until the supplied shutdown signal resolves.
///
/// Keeping the signal injectable lets the shutdown timeout be exercised without
/// delivering a process-wide signal during tests.
async fn serve_with_listener_until<F>(
    config: ServerConfig,
    listener: TcpListener,
    shutdown_signal: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let app = router(config.clone()).await?;
    tracing::info!("router initialized, starting HTTP serve");
    let shutdown_timeout = config.shutdown_timeout();
    let (shutdown_started_tx, shutdown_started_rx) = oneshot::channel();
    let graceful_shutdown = async move {
        shutdown_signal.await;
        tracing::info!("shutdown signal received, draining connections");
        let _ignored = shutdown_started_tx.send(());
    };
    let serve = serve_http(listener, app).with_graceful_shutdown(graceful_shutdown);
    if let Some(timeout) = shutdown_timeout {
        tokio::select! {
            result = serve => {
                result.map_err(ServerError::from)?;
            }
            () = async {
                if shutdown_started_rx.await.is_ok() {
                    tokio::time::sleep(timeout).await;
                } else {
                    pending::<()>().await;
                }
            } => {
                tracing::warn!("graceful shutdown timed out after {timeout:?}, aborting");
            }
        }
    } else {
        serve.await.map_err(ServerError::from)?;
    }
    Ok(())
}

fn register_frontend_routes(
    app: Router<Arc<AppState>>,
    frontend: ServerFrontend,
    role: ServerRole,
    app_state: &AppState,
) -> Router<Arc<AppState>> {
    // The Hub frontend already owns the `/api/{type}/{ns}/{repo}/tree/{rev}` and
    // `/api/{type}/{ns}/{repo}/revisions` path shapes, which structurally collide
    // with the M5 metadata routes. When Hub is enabled the Hub's routes win and the
    // Xet M5 routes are omitted to avoid an axum route-insertion conflict.
    let hub_enabled = app_state
        .config
        .server_frontends()
        .iter()
        .any(|candidate| matches!(candidate, ServerFrontend::Hub));
    match frontend {
        ServerFrontend::Xet => register_xet_routes(app, role, hub_enabled),
        ServerFrontend::Lfs => register_lfs_routes(app, role),
        ServerFrontend::BazelHttp => register_bazel_routes(app, role),
        ServerFrontend::Oci => register_oci_routes(app, role),
        ServerFrontend::S3 => register_s3_routes(app, role),
        ServerFrontend::Hub => app, // Hub routes are built separately
    }
}

fn register_xet_routes(
    mut app: Router<Arc<AppState>>,
    role: ServerRole,
    hub_enabled: bool,
) -> Router<Arc<AppState>> {
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
        if !hub_enabled {
            app = app
                .route(XET_TREE_ROUTE, get(tree_lookup))
                .route(
                    XET_PATH_ROUTE,
                    axum::routing::put(register_path).delete(delete_path),
                )
                .route(XET_REVISIONS_ROUTE, get(list_revisions))
                .route(
                    XET_REVISION_ROUTE,
                    axum::routing::post(create_revision).delete(delete_revision),
                );
        }
    }
    if role.serves_transfer() {
        app = app
            .route("/v1/chunks/default/{hash}", get(read_chunk))
            .route("/v1/chunks/default-merkledb/{hash}", get(read_chunk))
            .route(
                "/v1/xorbs/default/{hash}",
                head(head_xorb).post(upload_xorb),
            )
            .route(
                XORB_TRANSFER_ROUTE,
                get(read_xorb_transfer).put(write_xorb_transfer),
            );
    }
    app
}

fn register_lfs_routes(mut app: Router<Arc<AppState>>, role: ServerRole) -> Router<Arc<AppState>> {
    if role.serves_api() {
        app = app.route("/v1/lfs/objects/batch", post(lfs_batch));
    }
    if role.serves_transfer() {
        app = app
            .route(
                "/v1/lfs/objects/{oid}",
                get(lfs_get_object)
                    .head(lfs_head_object)
                    .put(lfs_put_object)
                    .patch(lfs_patch_object)
                    .delete(lfs_delete_object),
            )
            .route("/v1/lfs/objects/{oid}/verify", post(lfs_verify_object));
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
                get(bazel_get_ac).put(bazel_put_ac).head(bazel_head_ac),
            )
            .route(
                "/v1/bazel/cache/cas/{hash}",
                get(bazel_get_cas).put(bazel_put_cas).head(bazel_head_cas),
            )
            // Flat routes for Bazel client compatibility
            .route(
                "/v1/bazel/{hash}",
                get(bazel_get).put(bazel_put).head(bazel_head),
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

/// Registers the S3 frontend routes.
///
/// S3 is an API-tier frontend (reads + writes touch records/ingest), so the
/// routes are registered only when the role serves the API surface.
///
/// Bucket-level operations are registered on BOTH `/{bucket}` and `/{bucket}/`:
/// real clients (mc, the AWS SDKs, pyarrow) canonicalize bucket paths with a
/// trailing slash (`PUT /ac.assets/`, `GET /ac.assets/?location=`), and axum
/// does not match `/{bucket}` against the trailing-slash form.
fn register_s3_routes(mut app: Router<Arc<AppState>>, role: ServerRole) -> Router<Arc<AppState>> {
    if role.serves_api() {
        app = app
            // Service-level `GET /` — `ListBuckets` (the caller's single bucket).
            .route("/", axum::routing::get(s3_list_buckets))
            .route(
                "/{bucket}",
                axum::routing::put(s3_create_bucket)
                    .get(s3_get_bucket)
                    .head(s3_head_bucket)
                    .post(s3_post_bucket)
                    .delete(s3_delete_bucket),
            )
            .route(
                "/{bucket}/",
                axum::routing::put(s3_create_bucket)
                    .get(s3_get_bucket)
                    .head(s3_head_bucket)
                    .post(s3_post_bucket)
                    .delete(s3_delete_bucket),
            )
            .route(
                "/{bucket}/{*key}",
                axum::routing::get(s3_get_object)
                    .head(s3_head_object)
                    .put(s3_put_object)
                    .post(s3_post_object)
                    .delete(s3_delete_object),
            );
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

    // No auth provider configured
    if state.config.deployment_mode() == DeploymentMode::Strict {
        return Err(ServerError::Config(ServerConfigError::ConfigFileError(
            "no authentication provider configured — strict mode requires auth".into(),
        )));
    }
    Ok(None)
}

/// Kept during the authorization-capability migration; not yet wired to a
/// caller.
#[allow(dead_code)]
const fn scope_from_auth(auth: &AuthContext) -> &RepositoryScope {
    auth.claims().repository()
}

#[must_use]
pub fn bounded_api_body_limit(configured_limit: NonZeroUsize, endpoint_limit: usize) -> usize {
    configured_limit.get().min(endpoint_limit)
}

fn build_hub_state(
    app_state: &AppState,
) -> Result<shardline_hub_api::routes::HubState, ServerError> {
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
                if let Err(e) = fs::create_dir_all(&hub_root) {
                    tracing::warn!("failed to create hub directory: {e}");
                }
                let sqlite_store = shardline_index::LocalIndexStore::new(hub_root.clone())
                    .map_err(|e| ServerError::Io(Error::other(e)))?;
                // Ensure hub-specific tables exist
                if let Err(e) = shardline_index::hub::ensure_hub_tables(&hub_root) {
                    tracing::warn!("failed to create hub tables: {e}");
                }
                Ok(shardline_index::hub::BoxedHubStore::from_store(
                    sqlite_store,
                ))
            },
            |pg_url| -> Result<shardline_index::hub::BoxedHubStore, ServerError> {
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(16)
                    .connect_lazy(pg_url)
                    .map_err(|e| ServerError::Io(Error::other(e)))?;
                let pg_store = shardline_index::PostgresIndexStore::new(pool);
                Ok(shardline_index::hub::BoxedHubStore::from_store(pg_store))
            },
        )?;

    // Build an HTTP client for outbound webhook delivery.
    let http_client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(client) => Some(client),
        Err(e) => {
            tracing::warn!("failed to build HTTP client for webhook delivery: {e}");
            None
        }
    };

    // Thread an at-rest cipher for webhook signing secrets when configured.
    let webhook_secret_cipher = app_state.config.hub_webhook_secret_key().map_or_else(
        || {
            tracing::warn!(
                "SHARDLINE_HUB_WEBHOOK_SECRET_KEY not configured; webhook signing secrets will be stored unencrypted"
            );
            None
        },
        |key| {
            let key_bytes = shardline_protocol::SecretBytes::new(key.to_vec());
            match shardline_hub_api::secrets::WebhookSecretCipher::new(key_bytes) {
                Ok(cipher) => {
                    // App-level data upgrade: re-encrypt legacy plaintext rows.
                    // Run the sweep as a background task so startup latency stays
                    // bounded regardless of repository count. This is purely an
                    // accelerator — the delivery path already upgrades lazily per
                    // row — and it is idempotent vs. that lazy path: both write
                    // the same valid ciphertext under the same key, and since the
                    // nonce is random a concurrent write just replaces one valid
                    // blob with another (last-write-wins is harmless).
                    let sweep_store = store.clone();
                    let sweep_cipher = cipher.clone();
                    let _sweep_handle = tokio::task::spawn_blocking(move || {
                        tracing::info!("starting background webhook-secret upgrade sweep");
                        shardline_hub_api::secrets::upgrade_webhook_secrets(
                            &sweep_store,
                            &sweep_cipher,
                        );
                        tracing::info!("background webhook-secret upgrade sweep completed");
                    });
                    Some(cipher)
                }
                Err(e) => {
                    tracing::warn!(
                        "invalid SHARDLINE_HUB_WEBHOOK_SECRET_KEY; webhook signing secrets will be stored unencrypted: {e}"
                    );
                    None
                }
            }
        },
    );

    Ok(shardline_hub_api::routes::HubState {
        store,
        object_store: app_state.backend.object_store(),
        auth: hub_auth,
        http_client,
        webhook_secret_cipher,
    })
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
            let issuer = config
                .auth_oidc_issuer()
                .ok_or_else(|| ServerError::Config(ServerConfigError::InvalidAuthProvider))?;
            let audience = config.auth_oidc_audience();
            if audience.is_none() {
                tracing::warn!(
                    "OIDC auth provider has no SHARDLINE_AUTH_OIDC_AUDIENCE configured; the \
                     token aud claim is not validated"
                );
            }
            let provider = OidcProvider::new(issuer, audience.map(str::to_owned))
                .await
                .map_err(|_e| ServerError::Config(ServerConfigError::InvalidAuthProvider))?;
            Ok(Some(ServerAuth::from_provider(Box::new(provider))))
        }
        AuthProviderKind::Jwks => {
            let jwks_url = config
                .auth_jwks_url()
                .ok_or_else(|| ServerError::Config(ServerConfigError::InvalidAuthProvider))?;
            let issuer = config.auth_jwks_issuer().unwrap_or("jwks");
            let provider = JwksProvider::new(jwks_url, issuer)
                .await
                .map_err(|_e| ServerError::Config(ServerConfigError::InvalidAuthProvider))?;
            Ok(Some(ServerAuth::from_provider(Box::new(provider))))
        }
        AuthProviderKind::Ed25519 => {
            let provider = match (config.ed25519_private_key(), config.ed25519_public_key()) {
                (Some(private_key), None) => Ed25519AuthProvider::new(private_key),
                (None, Some(public_key)) => Ed25519AuthProvider::with_public_key(public_key),
                (None, None) => {
                    return Err(ServerError::Config(ServerConfigError::MissingEd25519Key));
                }
                (Some(_private_key), Some(_public_key)) => {
                    return Err(ServerError::Config(
                        ServerConfigError::ConflictingEd25519Keys,
                    ));
                }
            }
            .map_err(|e| {
                tracing::warn!("ed25519 auth provider initialization failed: {e}");
                ServerError::Config(ServerConfigError::InvalidAuthProvider)
            })?;
            Ok(Some(ServerAuth::from_provider(Box::new(provider))))
        }
    }
}

pub(super) async fn security_headers_middleware(
    request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    let response = next.run(request).await;
    let (mut parts, body) = response.into_parts();
    let headers = &mut parts.headers;
    if !headers.contains_key(header::X_CONTENT_TYPE_OPTIONS) {
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            header::HeaderValue::from_static("nosniff"),
        );
    }
    if !headers.contains_key(header::X_FRAME_OPTIONS) {
        headers.insert(
            header::X_FRAME_OPTIONS,
            header::HeaderValue::from_static("DENY"),
        );
    }
    if !headers.contains_key(header::STRICT_TRANSPORT_SECURITY) {
        headers.insert(
            header::STRICT_TRANSPORT_SECURITY,
            header::HeaderValue::from_static("max-age=31536000"),
        );
    }
    if !headers.contains_key(header::REFERRER_POLICY) {
        headers.insert(
            header::REFERRER_POLICY,
            header::HeaderValue::from_static("strict-origin-when-cross-origin"),
        );
    }
    axum::response::Response::from_parts(parts, body)
}

async fn request_timeout_middleware(
    request: axum::extract::Request,
    next: Next,
) -> axum::response::Response {
    tokio::time::timeout(timeouts::REQUEST_TOTAL, next.run(request))
        .await
        .unwrap_or_else(|_| ServerError::RequestTimedOut.into_response())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod e2e_tests;

#[cfg(test)]
mod metadata_routes_tests;
