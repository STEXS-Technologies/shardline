use std::{num::NonZeroUsize, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Method, header},
    routing,
};
use tokio::sync::Semaphore;

use crate::{
    AppState, ServerBackend, ServerConfig, ServerFrontend, TransferLimiter, app::ProtocolMetrics,
    app::protocol_routes::oci_dispatch, reconstruction_cache::ReconstructionCacheService,
    server_role::ServerRole,
};

/// A guard that holds a `tempfile::TempDir` and an `Arc<AppState>` backed by a
/// local filesystem backend rooted in that temporary directory. Dropping the
/// guard removes the temporary directory.
pub(crate) struct OciTestContext {
    /// The temporary directory holding all test data. Kept alive for the
    /// duration of the test so that the backend has a valid root.
    pub _temp: tempfile::TempDir,
    pub state: Arc<AppState>,
}

pub(crate) struct OciPostgresTestCluster {
    pub _temp: tempfile::TempDir,
    pub node_a: Arc<AppState>,
    pub node_b: Arc<AppState>,
}

pub(crate) async fn build_oci_postgres_test_cluster() -> Option<OciPostgresTestCluster> {
    let scoped_url = crate::test_support::postgres_fixture::isolated_postgres_url("oci").await?;
    Some(build_oci_postgres_test_cluster_for_url(&scoped_url).await)
}

pub(crate) async fn build_oci_postgres_test_cluster_for_url(
    database_url: &str,
) -> OciPostgresTestCluster {
    let temp = tempfile::tempdir().expect("create PostgreSQL OCI scratch directory");
    let shared_store = crate::ServerObjectStore::local(temp.path().join("objects"))
        .expect("create PostgreSQL OCI object store");
    let node_a = build_oci_postgres_state(
        database_url,
        temp.path().join("node-a"),
        shared_store.clone(),
    )
    .await;
    let node_b =
        build_oci_postgres_state(database_url, temp.path().join("node-b"), shared_store).await;
    OciPostgresTestCluster {
        _temp: temp,
        node_a,
        node_b,
    }
}

async fn build_oci_postgres_state(
    database_url: &str,
    root: std::path::PathBuf,
    object_store: crate::ServerObjectStore,
) -> Arc<AppState> {
    let config = oci_test_config(root.clone());
    let backend = crate::PostgresBackend::new_with_object_store(
        root,
        "http://127.0.0.1:8080".to_owned(),
        config.chunk_size(),
        database_url,
        object_store,
    )
    .await
    .expect("initialize configured PostgreSQL OCI backend");
    oci_test_state(config, ServerBackend::Postgres(backend))
}

/// Builds a minimal [`AppState`] backed by a local filesystem backend rooted
/// in a fresh temporary directory. Authentication is **disabled** so that
/// handlers can be exercised without bearer tokens.
///
/// # Panics
///
/// Panics if any required system resource (temp dir, socket address parse,
/// backend initialisation) fails.
pub(crate) async fn build_oci_test_state() -> OciTestContext {
    let temp = tempfile::tempdir().expect("failed to create temp dir");
    let config = oci_test_config(temp.path().to_path_buf());
    let backend = ServerBackend::from_config(&config)
        .await
        .expect("failed to create backend");

    let state = oci_test_state(config, backend);

    OciTestContext { _temp: temp, state }
}

fn oci_test_config(root: std::path::PathBuf) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        root,
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Oci])
    .expect("failed to set server frontends")
}

fn oci_test_state(config: ServerConfig, backend: ServerBackend) -> Arc<AppState> {
    Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend,
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(
            NonZeroUsize::new(4096).unwrap(),
            NonZeroUsize::new(16).unwrap(),
        ),
        oci_registry_token_limiter: Arc::new(Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    })
}

/// Builds an Axum [`Router`] with the OCI dispatch routes wired up for the
/// given shared state.
pub(crate) fn oci_test_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/v2/{*path}", routing::any(oci_dispatch))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(state),
            crate::app::gc_write_barrier_middleware,
        ))
        .with_state(Arc::clone(state))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

pub(crate) async fn send(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Body,
) -> axum::http::Response<Body> {
    send_with_content_type(app, method, uri, body, "application/octet-stream").await
}

pub(crate) async fn send_with_content_type(
    app: &axum::Router,
    method: Method,
    uri: &str,
    body: Body,
    content_type: &str,
) -> axum::http::Response<Body> {
    use tower::ServiceExt;
    let request = axum::http::Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, content_type)
        .body(body)
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}
