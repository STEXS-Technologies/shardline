use std::sync::Arc;

use axum::{
    Json,
    extract::State,
    http::{HeaderMap, HeaderValue, header::CACHE_CONTROL},
};
use serde::Serialize;

use crate::{
    ServerError, admission::weights, app::AppState, auth::authorize_static_bearer_token,
    clock::unix_now_seconds_checked, model::ServerStatsResponse,
};

const ADMIN_API_VERSION: &str = "v1";
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

type AdminJson<T> = (HeaderMap, Json<T>);

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OperationalState {
    Ready,
    Degraded,
    External,
    Unsupported,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminStatusResponse {
    api_version: &'static str,
    shardline_version: &'static str,
    observed_at_unix_seconds: u64,
    state: OperationalState,
    durable_storage_state: OperationalState,
    cache_state: OperationalState,
    server_role: String,
    server_frontends: Vec<String>,
    metadata_backend: String,
    object_backend: String,
    cache_backend: String,
    plugin_registry: OperationalState,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminStorageResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    authoritative: ServerStatsResponse,
    process_lifetime: AdminStorageProcessCounters,
    deduplication_ratio_state: OperationalState,
    deduplication_ratio: Option<AdminRatio>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminStorageProcessCounters {
    objects_written: u64,
    object_bytes_written: u64,
    xorbs_written: u64,
    xorb_bytes_written: u64,
    shards_written: i64,
    deduplicated_bytes: u64,
    compression_saved_bytes: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminRatio {
    numerator_bytes: u64,
    denominator_bytes: u64,
    basis_points: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminGcResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    state: OperationalState,
    execution: OperationalState,
    runs_observed_by_process: u64,
    objects_collected_by_process: u64,
    bytes_collected_by_process: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminIntegrityResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    state: OperationalState,
    execution: OperationalState,
    fsck_runs_observed_by_process: u64,
    errors_observed_by_process: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminNodesResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    discovery: OperationalState,
    nodes: Vec<AdminNode>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminNode {
    scope: &'static str,
    state: OperationalState,
    server_role: String,
    server_frontends: Vec<String>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminTasksResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    scheduler: OperationalState,
    tasks: Vec<AdminTask>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminTask {
    id: String,
    state: OperationalState,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminMetricsResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    prometheus_path: &'static str,
    active_connections: i64,
    admitted_requests: u64,
    queued_requests: u64,
    rejected_requests: u64,
    upload_requests: u64,
    upload_bytes: u64,
    download_requests: u64,
    download_bytes: u64,
    range_requests: u64,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminPluginsResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    registry: OperationalState,
    plugins: Vec<AdminPlugin>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct AdminReplicationResponse {
    api_version: &'static str,
    observed_at_unix_seconds: u64,
    state: OperationalState,
    coordinator: OperationalState,
    replicas: Vec<AdminReplica>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminReplica {
    id: String,
    state: OperationalState,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct AdminPlugin {
    id: String,
    version: String,
    state: OperationalState,
    capabilities: Vec<String>,
}

fn authorize_admin(state: &AppState, headers: &HeaderMap) -> Result<(), ServerError> {
    let token = state
        .config
        .admin_read_token()
        .ok_or(ServerError::NotFound)?;
    authorize_static_bearer_token(headers, token)
}

fn observed_at() -> Result<u64, ServerError> {
    unix_now_seconds_checked()
}

fn admin_json<T>(value: T) -> AdminJson<T> {
    let mut headers = HeaderMap::new();
    headers.insert(CACHE_CONTROL, NO_STORE);
    (headers, Json(value))
}

fn frontends(state: &AppState) -> Vec<String> {
    state
        .config
        .server_frontends()
        .iter()
        .map(|frontend| frontend.as_str().to_owned())
        .collect()
}

pub(super) async fn status(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminStatusResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let ready = state.backend.ready().await.is_ok();
    let cache_ready =
        !state.role.uses_reconstruction_cache() || state.reconstruction_cache.ready().await.is_ok();
    Ok(admin_json(AdminStatusResponse {
        api_version: ADMIN_API_VERSION,
        shardline_version: env!("CARGO_PKG_VERSION"),
        observed_at_unix_seconds: observed_at()?,
        state: if ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        durable_storage_state: if ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        cache_state: if cache_ready {
            OperationalState::Ready
        } else {
            OperationalState::Degraded
        },
        server_role: state.role.as_str().to_owned(),
        server_frontends: frontends(&state),
        metadata_backend: state.backend.backend_name().to_owned(),
        object_backend: state.backend.object_backend_name().to_owned(),
        cache_backend: state.reconstruction_cache.backend_name().to_owned(),
        plugin_registry: OperationalState::Unsupported,
    }))
}

pub(super) async fn storage(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminStorageResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let _permit = state
        .admission
        .try_acquire(weights::STATS)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let authoritative = state.backend.stats().await?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminStorageResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        authoritative,
        process_lifetime: AdminStorageProcessCounters {
            objects_written: metrics.storage.objects_total.get().try_into()?,
            object_bytes_written: metrics.storage.objects_bytes_total.get(),
            xorbs_written: metrics.storage.xorbs_total.get().try_into()?,
            xorb_bytes_written: metrics.storage.xorbs_bytes_total.get(),
            shards_written: metrics.storage.shards_total.get(),
            deduplicated_bytes: metrics.storage.dedup_saves_bytes_total.get(),
            compression_saved_bytes: metrics.storage.compression_saved_bytes_total.get(),
        },
        deduplication_ratio_state: OperationalState::Unsupported,
        // Exact logical bytes are not currently retained as an authoritative
        // aggregate. Returning null is safer than deriving a misleading ratio
        // from process-lifetime counters after restarts or across replicas.
        deduplication_ratio: None,
    }))
}

pub(super) async fn gc(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminGcResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminGcResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        state: OperationalState::External,
        execution: OperationalState::External,
        runs_observed_by_process: metrics.gc.runs.get(),
        objects_collected_by_process: metrics.gc.objects_collected.get(),
        bytes_collected_by_process: metrics.gc.bytes_collected.get(),
    }))
}

pub(super) async fn integrity(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminIntegrityResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminIntegrityResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        state: OperationalState::External,
        execution: OperationalState::External,
        fsck_runs_observed_by_process: metrics.fsck.runs.get(),
        errors_observed_by_process: metrics.fsck.errors_found.get(),
    }))
}

pub(super) async fn nodes(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminNodesResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let ready = state.backend.ready().await.is_ok();
    Ok(admin_json(AdminNodesResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        discovery: OperationalState::Unsupported,
        nodes: vec![AdminNode {
            scope: "current_process",
            state: if ready {
                OperationalState::Ready
            } else {
                OperationalState::Degraded
            },
            server_role: state.role.as_str().to_owned(),
            server_frontends: frontends(&state),
        }],
    }))
}

pub(super) async fn tasks(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminTasksResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    Ok(admin_json(AdminTasksResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        scheduler: OperationalState::External,
        tasks: Vec::new(),
    }))
}

pub(super) async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminMetricsResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    let metrics = shardline_metrics::metrics();
    Ok(admin_json(AdminMetricsResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        prometheus_path: "/metrics",
        active_connections: metrics.system.active_connections.get(),
        admitted_requests: metrics.system.admitted_total.get(),
        queued_requests: metrics.system.queued_total.get(),
        rejected_requests: metrics.system.rejected_total.get(),
        upload_requests: metrics.transfer.upload_requests.get(),
        upload_bytes: metrics.transfer.upload_bytes.get(),
        download_requests: metrics.transfer.download_requests.get(),
        download_bytes: metrics.transfer.download_bytes.get(),
        range_requests: metrics.transfer.range_requests.get(),
    }))
}

pub(super) async fn plugins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminPluginsResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    Ok(admin_json(AdminPluginsResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        registry: OperationalState::Unsupported,
        plugins: Vec::new(),
    }))
}

pub(super) async fn replication(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<AdminJson<AdminReplicationResponse>, ServerError> {
    authorize_admin(&state, &headers)?;
    Ok(admin_json(AdminReplicationResponse {
        api_version: ADMIN_API_VERSION,
        observed_at_unix_seconds: observed_at()?,
        // Shardline coordinates writers over shared durable state. It does not
        // own an asynchronous replication controller whose lag could be
        // reported authoritatively, so keep this surface explicit and empty.
        state: OperationalState::External,
        coordinator: OperationalState::External,
        replicas: Vec::new(),
    }))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::{
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode, header::AUTHORIZATION},
    };
    use serde_json::Value;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::*;
    use crate::{DeploymentMode, ServerConfig, ServerFrontend, ServerRole, app::router};

    const ADMIN_TOKEN: &str = "admin-read-secret";
    const ADMIN_PATHS: [&str; 9] = [
        "/api/v1/status",
        "/api/v1/storage",
        "/api/v1/gc",
        "/api/v1/integrity",
        "/api/v1/nodes",
        "/api/v1/tasks",
        "/api/v1/metrics",
        "/api/v1/plugins",
        "/api/v1/replication",
    ];

    async fn app_with_config(
        admin_token: Option<&str>,
        metrics_token: Option<&str>,
        role: ServerRole,
    ) -> (axum::Router, TempDir) {
        let temp = TempDir::new().expect("temp dir");
        let mut config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind address"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("frontends")
        .with_server_role(role);
        if let Some(token) = metrics_token {
            config = config
                .with_metrics_token(token.as_bytes().to_vec())
                .expect("metrics token");
        }
        if let Some(token) = admin_token {
            config = config
                .with_admin_read_token(token.as_bytes().to_vec())
                .expect("admin token");
        }
        (router(config).await.expect("router"), temp)
    }

    async fn app(admin_token: Option<&str>) -> (axum::Router, TempDir) {
        app_with_config(admin_token, None, ServerRole::All).await
    }

    fn request(method: Method, path: &str, token: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(path);
        if let Some(token) = token {
            builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        builder.body(Body::empty()).expect("request")
    }

    async fn json_body(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), 1_048_576)
            .await
            .expect("response body");
        serde_json::from_slice(&bytes).expect("JSON response")
    }

    #[tokio::test]
    async fn disabled_admin_api_hides_every_endpoint() {
        let (app, _temp) = app(None).await;
        for path in ADMIN_PATHS {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    async fn admin_api_rejects_missing_and_wrong_credentials() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for token in [None, Some("wrong-token")] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, ADMIN_PATHS[0], token))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn every_admin_endpoint_is_read_only_and_returns_versioned_json() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for path in ADMIN_PATHS {
            let get_response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("GET response");
            assert_eq!(get_response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                get_response.headers().get(CACHE_CONTROL),
                Some(&NO_STORE),
                "{path} must not be cached"
            );
            let body = json_body(get_response).await;
            assert_eq!(body["api_version"], ADMIN_API_VERSION, "{path}");

            for method in [Method::POST, Method::PUT, Method::PATCH, Method::DELETE] {
                let mutation_response = app
                    .clone()
                    .oneshot(request(method.clone(), path, Some(ADMIN_TOKEN)))
                    .await
                    .expect("mutation response");
                assert_eq!(
                    mutation_response.status(),
                    StatusCode::METHOD_NOT_ALLOWED,
                    "{path} must not expose {method}"
                );
            }
        }
    }

    #[tokio::test]
    async fn admin_api_rejects_ambiguous_authorization_headers() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let request = Request::builder()
            .method(Method::GET)
            .uri("/api/v1/status")
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .header(AUTHORIZATION, format!("Bearer {ADMIN_TOKEN}"))
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_and_metrics_tokens_are_not_interchangeable() {
        let metrics_token = "metrics-only-secret";
        let (app, _temp) =
            app_with_config(Some(ADMIN_TOKEN), Some(metrics_token), ServerRole::All).await;

        let admin_with_metrics_token = app
            .clone()
            .oneshot(request(Method::GET, "/api/v1/status", Some(metrics_token)))
            .await
            .expect("admin response");
        assert_eq!(admin_with_metrics_token.status(), StatusCode::UNAUTHORIZED);

        let metrics_with_admin_token = app
            .oneshot(request(Method::GET, "/metrics", Some(ADMIN_TOKEN)))
            .await
            .expect("metrics response");
        assert_eq!(metrics_with_admin_token.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn admin_api_is_available_on_each_runtime_role() {
        for role in [ServerRole::All, ServerRole::Api, ServerRole::Transfer] {
            let (app, _temp) = app_with_config(Some(ADMIN_TOKEN), None, role).await;
            let response = app
                .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "role={role:?}");
            let body = json_body(response).await;
            assert_eq!(body["server_role"], role.as_str());
        }
    }

    #[tokio::test]
    async fn readiness_failure_is_reported_as_degraded_without_internal_details() {
        let (app, temp) = app(Some(ADMIN_TOKEN)).await;
        tokio::fs::remove_file(temp.path().join("metadata.sqlite3"))
            .await
            .expect("remove metadata database");
        tokio::fs::create_dir(temp.path().join("metadata.sqlite3"))
            .await
            .expect("replace metadata database with directory");

        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["state"], "degraded");
        assert_eq!(body["durable_storage_state"], "degraded");
        let encoded = body.to_string();
        assert!(!encoded.contains("sqlite"));
        assert!(!encoded.contains("metadata.sqlite3"));
    }

    #[tokio::test]
    async fn admin_status_reports_current_runtime_without_secret_material() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["state"], "ready");
        assert_eq!(body["durable_storage_state"], "ready");
        assert_eq!(body["cache_state"], "ready");
        assert_eq!(body["server_role"], "all");
        assert_eq!(body["server_frontends"], serde_json::json!(["xet"]));
        assert_eq!(body["plugin_registry"], "unsupported");
        assert!(!body.to_string().contains(ADMIN_TOKEN));
    }

    #[tokio::test]
    async fn storage_reports_authoritative_physical_usage_and_honest_ratio_availability() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let response = app
            .oneshot(request(Method::GET, "/api/v1/storage", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = json_body(response).await;
        assert_eq!(body["authoritative"]["objects"], 0);
        assert_eq!(body["authoritative"]["object_bytes"], 0);
        assert_eq!(body["authoritative"]["chunks"], 0);
        assert_eq!(body["authoritative"]["files"], 0);
        assert_eq!(body["deduplication_ratio_state"], "unsupported");
        assert!(body["deduplication_ratio"].is_null());
    }

    #[tokio::test]
    async fn externally_owned_operations_never_claim_in_process_progress() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        for path in ["/api/v1/gc", "/api/v1/integrity"] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            let body = json_body(response).await;
            assert_eq!(body["state"], "external", "{path}");
            assert_eq!(body["execution"], "external", "{path}");
        }
        for path in ["/api/v1/tasks", "/api/v1/replication"] {
            let response = app
                .clone()
                .oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                .await
                .expect("response");
            let body = json_body(response).await;
            assert_eq!(
                body.get("scheduler").or_else(|| body.get("coordinator")),
                Some(&Value::String("external".to_owned())),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn admin_routes_coexist_with_hub_catch_all_routes() {
        let temp = TempDir::new().expect("temp dir");
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().expect("bind address"),
            "http://127.0.0.1:8080".to_owned(),
            temp.path().to_path_buf(),
            NonZeroUsize::new(65_536).expect("chunk size"),
        )
        .with_server_frontends([ServerFrontend::Hub])
        .expect("frontends")
        .with_deployment_mode(DeploymentMode::Insecure)
        .with_admin_read_token(ADMIN_TOKEN.as_bytes().to_vec())
        .expect("admin token");
        let app = router(config).await.expect("router");

        let response = app
            .oneshot(request(Method::GET, "/api/v1/status", Some(ADMIN_TOKEN)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json_body(response).await["api_version"], "v1");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_admin_polling_remains_bounded_and_successful() {
        let (app, _temp) = app(Some(ADMIN_TOKEN)).await;
        let mut requests = tokio::task::JoinSet::new();
        for index in 0..128 {
            let app = app.clone();
            let path = ADMIN_PATHS[index % ADMIN_PATHS.len()];
            requests.spawn(async move {
                app.oneshot(request(Method::GET, path, Some(ADMIN_TOKEN)))
                    .await
                    .map(|response| response.status())
            });
        }
        while let Some(response) = requests.join_next().await {
            let status = response.expect("poll task").expect("poll response");
            assert!(
                status == StatusCode::OK || status == StatusCode::SERVICE_UNAVAILABLE,
                "unexpected bounded-poll status {status}"
            );
        }
    }
}
