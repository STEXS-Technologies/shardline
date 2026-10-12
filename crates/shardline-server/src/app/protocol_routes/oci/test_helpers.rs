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

pub(crate) struct OciPostgresTestCluster {
    pub _temp: tempfile::TempDir,
    pub node_a: Arc<AppState>,
    pub node_b: Arc<AppState>,
}

pub(crate) async fn build_oci_postgres_test_cluster() -> Option<OciPostgresTestCluster> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    let schema = format!(
        "oci_fixture_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_nanos()
    );
    let admin = sqlx::PgPool::connect(&database_url).await.ok()?;
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .ok()?;
    admin.close().await;
    let mut scoped_url = url::Url::parse(&database_url).ok()?;
    let old_options = scoped_url
        .query_pairs()
        .find(|(key, _)| key == "options")
        .map_or_else(String::new, |(_, value)| value.into_owned());
    let other_parameters: Vec<_> = scoped_url
        .query_pairs()
        .filter(|(key, _)| key != "options")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    scoped_url.set_query(None);
    scoped_url
        .query_pairs_mut()
        .extend_pairs(other_parameters)
        .append_pair("options", &format!("{old_options} -csearch_path={schema}"));
    let bootstrap = sqlx::PgPool::connect(scoped_url.as_str()).await.ok()?;
    crate::apply_database_migrations(&bootstrap).await.ok()?;
    bootstrap.close().await;
    build_oci_postgres_test_cluster_for_url(scoped_url.as_str()).await
}

pub(crate) async fn build_oci_postgres_test_cluster_for_url(
    database_url: &str,
) -> Option<OciPostgresTestCluster> {
    let temp = tempfile::tempdir().ok()?;
    let shared_store = crate::ServerObjectStore::local(temp.path().join("objects")).ok()?;
    let node_a = build_oci_postgres_state(
        database_url,
        temp.path().join("node-a"),
        shared_store.clone(),
    )
    .await?;
    let node_b =
        build_oci_postgres_state(database_url, temp.path().join("node-b"), shared_store).await?;
    Some(OciPostgresTestCluster {
        _temp: temp,
        node_a,
        node_b,
    })
}

async fn build_oci_postgres_state(
    database_url: &str,
    root: std::path::PathBuf,
    object_store: crate::ServerObjectStore,
) -> Option<Arc<AppState>> {
    let chunk_size = NonZeroUsize::new(65_536)?;
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().ok()?,
        "http://127.0.0.1:8080".to_owned(),
        root.clone(),
        chunk_size,
    )
    .with_server_frontends([ServerFrontend::Oci])
    .ok()?;
    let backend = crate::PostgresBackend::new_with_object_store(
        root,
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        database_url,
        object_store,
    )
    .await
    .ok()?;
    Some(Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Postgres(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(NonZeroUsize::new(4096)?, NonZeroUsize::new(16)?),
        oci_registry_token_limiter: Arc::new(Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(NonZeroUsize::new(256)?),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    }))
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
    let chunk_size = NonZeroUsize::new(65536).unwrap();

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
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    OciTestContext { _temp: temp, state }
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
