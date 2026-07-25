use std::{num::NonZeroUsize, sync::Arc};

use axum::{Router, routing};
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
    let root = temp.path().to_path_buf();
    let bind_addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let chunk_size = NonZeroUsize::new(4096).unwrap();

    let config = ServerConfig::new(
        bind_addr,
        "http://127.0.0.1:8080".to_owned(),
        root,
        chunk_size,
    )
    .with_server_frontends([ServerFrontend::Oci])
    .expect("failed to set server frontends");

    let backend = ServerBackend::from_config(&config)
        .await
        .expect("failed to create backend");

    let state = Arc::new(AppState {
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
        protocol_metrics: ProtocolMetrics::default(),
    });

    OciTestContext { _temp: temp, state }
}

/// Builds an Axum [`Router`] with the OCI dispatch routes wired up for the
/// given shared state.
pub(crate) fn oci_test_router(state: &Arc<AppState>) -> Router {
    Router::new()
        .route("/v2/{*path}", routing::any(oci_dispatch))
        .with_state(Arc::clone(state))
}
