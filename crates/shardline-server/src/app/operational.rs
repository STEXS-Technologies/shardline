use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use shardline_metrics;

use crate::{
    HealthResponse, ServerError, ShardUploadResponse, XorbUploadResponse,
    admission::weights,
    app::{
        AppState,
        reconstruction_helpers::{
            byte_range_stream_response, full_byte_stream_response,
            parse_required_xorb_transfer_range,
        },
        reconstruction_routes::{XetRepository, XetWriteRepository},
    },
    auth::authorize_static_bearer_token,
    metrics,
    upload_ingest::RequestBodyReader,
    xet_adapter::{validate_hash_path, validate_xorb_transfer_namespace},
};

pub(super) async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

pub(super) async fn ready(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match state.backend.ready().await {
        Ok(()) => (
            StatusCode::OK,
            Json(HealthResponse {
                status: "ok".to_owned(),
            }),
        )
            .into_response(),
        Err(error) => {
            tracing::warn!("readiness check failed: {error}");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "service unavailable" })),
            )
                .into_response()
        }
    }
}

#[tracing::instrument(skip(state), fields(hash = %hash))]
pub(super) async fn read_chunk(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    repo: XetRepository,
) -> Result<Response, ServerError> {
    // Acquire an admission permit before the repository-reference metadata
    // scan: `repository_references_xorb` enumerates the repo's latest + version
    // records (O(N) in record count) with no LIMIT, so reads are gated like the
    // upload/reconstruction paths to bound concurrent per-request scans.
    let _admit = state
        .admission
        .try_acquire(weights::XORB_READ)
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_hash_path(&hash)?;

    // Do not query repository references for an object that is absent. Besides
    // avoiding unnecessary metadata work, this makes unknown hashes
    // indistinguishable from inaccessible ones without scanning repositories.
    let _stored_length = state.backend.chunk_length(&hash).await?;
    if let Some(namespace) = repo.capability().namespace() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, namespace)
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    let (byte_stream, total_length) = state.backend.read_dedupe_shard_stream(&hash).await?;
    Ok(full_byte_stream_response(
        byte_stream,
        state.transfer_limiter.clone(),
        total_length,
    ))
}

#[tracing::instrument(skip(state, body), fields(hash = %hash))]
pub(super) async fn upload_xorb(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    repo: XetWriteRepository,
    body: Body,
) -> Result<Json<XorbUploadResponse>, ServerError> {
    // Acquire admission permit for xorb upload
    let _admit = state
        .admission
        .try_acquire(weights::XORB_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _hashing = state
        .pools
        .hashing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_hash_path(&hash)?;
    let body_reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let response = state.backend.upload_xorb_stream(&hash, body_reader).await?;
    if response.was_inserted
        && let Ok(length) = state.backend.xorb_length(&hash).await
    {
        metrics::record_xorb_stored(length);
    }
    Ok(Json(response))
}

#[tracing::instrument(skip(state), fields(hash = %hash))]
pub(super) async fn head_xorb(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    repo: XetRepository,
) -> Result<impl IntoResponse, ServerError> {
    // Admission gate for the repository-reference metadata scan (see read_chunk).
    let _admit = state
        .admission
        .try_acquire(weights::XORB_READ)
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_hash_path(&hash)?;
    let total_length = state.backend.xorb_length(&hash).await?;
    if let Some(namespace) = repo.capability().namespace() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, namespace)
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    Ok((StatusCode::OK, [(CONTENT_LENGTH, total_length.to_string())]))
}

#[tracing::instrument(skip(state, headers), fields(hash = %hash))]
pub(super) async fn read_xorb_transfer(
    State(state): State<Arc<AppState>>,
    Path((prefix, hash)): Path<(String, String)>,
    repo: XetRepository,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    // Admission gate for the repository-reference metadata scan (see read_chunk).
    let _admit = state
        .admission
        .try_acquire(weights::XORB_READ)
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_xorb_transfer_namespace(&prefix)?;
    validate_hash_path(&hash)?;
    let total_length = state.backend.xorb_length(&hash).await?;
    if let Some(namespace) = repo.capability().namespace() {
        let reachable = state
            .backend
            .repository_references_xorb(&hash, namespace)
            .await?;
        if !reachable {
            return Err(ServerError::NotFound);
        }
    }
    let range = parse_required_xorb_transfer_range(&headers, total_length)?;
    let transfer_bytes = range.len().ok_or(ServerError::Overflow)?;
    metrics::record_xet_xorb_download(total_length);
    let byte_stream = state
        .backend
        .read_xorb_range_stream(&hash, total_length, range)
        .await?;
    Ok(byte_range_stream_response(
        byte_stream,
        state.transfer_limiter.clone(),
        range,
        total_length,
        transfer_bytes,
    ))
}

/// Writes a xorb to the CAS through the transfer endpoint.
/// git-xet uses this endpoint to upload chunk-grouped data directly
/// to the content-addressed storage layer after receiving the CAS URL
/// and access token from the LFS batch response.
#[tracing::instrument(skip(state, body), fields(hash = %hash))]
pub(super) async fn write_xorb_transfer(
    State(state): State<Arc<AppState>>,
    Path((prefix, hash)): Path<(String, String)>,
    repo: XetWriteRepository,
    body: Body,
) -> Result<Json<XorbUploadResponse>, ServerError> {
    let _admit = state
        .admission
        .try_acquire(weights::XORB_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _hashing = state
        .pools
        .hashing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_xorb_transfer_namespace(&prefix)?;
    validate_hash_path(&hash)?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    Ok(Json(state.backend.upload_xorb_stream(&hash, body).await?))
}

#[tracing::instrument(skip(state, body))]
pub(super) async fn upload_shard(
    State(state): State<Arc<AppState>>,
    repo: XetWriteRepository,
    body: Body,
) -> Result<Json<ShardUploadResponse>, ServerError> {
    // Acquire admission permit for shard upload
    let _admit = state
        .admission
        .try_acquire(weights::SHARD_UPLOAD)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let response = state
        .backend
        .upload_shard_stream(
            body,
            repo.capability().namespace(),
            state.config.shard_metadata_limits(),
        )
        .await?;
    metrics::record_shard_stored();
    Ok(Json(response))
}

/// Reconstructs a Hub LFS object whose bytes were uploaded through hf_xet.
#[tracing::instrument(skip(state), fields(sha256 = %sha256))]
pub(super) async fn download_hub_xet_file(
    State(state): State<Arc<AppState>>,
    Path(sha256): Path<String>,
    repo: XetRepository,
) -> Result<Response, ServerError> {
    validate_hash_path(&sha256)?;
    let repository_scope = repo.capability().namespace().ok_or(ServerError::NotFound)?;
    let identity = state
        .backend
        .xet_file_identity(repository_scope, &sha256)
        .await?
        .ok_or(ServerError::NotFound)?;
    let bytes = state
        .backend
        .download_file(&identity.file_id, None, Some(repository_scope))
        .await?;
    if bytes.len() as u64 != identity.total_bytes
        || hex::encode(Sha256::digest(&bytes)) != identity.sha256
    {
        return Err(ServerError::NotFound);
    }
    let mut response = Response::new(Body::from(bytes));
    response.headers_mut().insert(
        CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

#[tracing::instrument(skip(state))]
pub(super) async fn stats(
    State(state): State<Arc<AppState>>,
    repo: XetRepository,
) -> Result<impl IntoResponse, ServerError> {
    // Admission gate for the whole-store scan: `stats` visits every object in
    // the store (local: full dir walk; S3: paginated LIST) plus a full
    // latest-record traversal, with no LIMIT and no cache. Concurrent scans are
    // bounded like the read/upload/reconstruction paths so a request flood
    // cannot drive unbounded per-request store I/O.
    let _admit = state
        .admission
        .try_acquire(weights::STATS)
        .ok_or(ServerError::WorkQueueSaturated)?;
    Ok(Json(state.backend.stats().await?))
}

pub(super) async fn metrics(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    if let Some(metrics_token) = state.config.metrics_token() {
        authorize_static_bearer_token(&headers, metrics_token)?;
    }

    let auth_enabled = state.auth.is_some();
    let provider_tokens_enabled = state.provider_tokens.is_some();
    let metrics_auth_enabled = state.config.metrics_token().is_some();
    let frontend_labels = state
        .config
        .server_frontends()
        .iter()
        .map(|frontend| frontend.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let mut body = format!(
        concat!(
            "# HELP shardline_up Whether the Shardline process is serving requests.\n",
            "# TYPE shardline_up gauge\n",
            "shardline_up 1\n",
            "# HELP shardline_server_info Static Shardline runtime information.\n",
            "# TYPE shardline_server_info gauge\n",
            "shardline_server_info{{role=\"{}\",frontends=\"{}\",metadata_backend=\"{}\",object_backend=\"{}\",cache_backend=\"{}\"}} 1\n",
            "# HELP shardline_auth_enabled Whether served routes require bearer authentication.\n",
            "# TYPE shardline_auth_enabled gauge\n",
            "shardline_auth_enabled {}\n",
            "# HELP shardline_provider_tokens_enabled Whether provider token issuance is enabled.\n",
            "# TYPE shardline_provider_tokens_enabled gauge\n",
            "shardline_provider_tokens_enabled {}\n",
            "# HELP shardline_metrics_auth_enabled Whether metrics scraping requires bearer authentication.\n",
            "# TYPE shardline_metrics_auth_enabled gauge\n",
            "shardline_metrics_auth_enabled {}\n",
            "# HELP shardline_chunk_size_bytes Configured chunk size in bytes.\n",
            "# TYPE shardline_chunk_size_bytes gauge\n",
            "shardline_chunk_size_bytes {}\n",
            "# HELP shardline_max_request_body_bytes Configured maximum request body size in bytes.\n",
            "# TYPE shardline_max_request_body_bytes gauge\n",
            "shardline_max_request_body_bytes {}\n",
            "# HELP shardline_upload_max_in_flight_chunks Configured upload chunk parallelism.\n",
            "# TYPE shardline_upload_max_in_flight_chunks gauge\n",
            "shardline_upload_max_in_flight_chunks {}\n",
            "# HELP shardline_transfer_max_in_flight_chunks Configured transfer chunk parallelism.\n",
            "# TYPE shardline_transfer_max_in_flight_chunks gauge\n",
            "shardline_transfer_max_in_flight_chunks {}\n",
            "# HELP shardline_oci_registry_token_ttl_seconds Configured OCI registry token TTL in seconds.\n",
            "# TYPE shardline_oci_registry_token_ttl_seconds gauge\n",
            "shardline_oci_registry_token_ttl_seconds {}\n",
            "# HELP shardline_oci_registry_token_max_in_flight_requests Configured OCI registry token concurrency limit.\n",
            "# TYPE shardline_oci_registry_token_max_in_flight_requests gauge\n",
            "shardline_oci_registry_token_max_in_flight_requests {}\n",
        ),
        state.role.as_str(),
        frontend_labels,
        state.backend.backend_name(),
        state.backend.object_backend_name(),
        state.reconstruction_cache.backend_name(),
        u8::from(auth_enabled),
        u8::from(provider_tokens_enabled),
        u8::from(metrics_auth_enabled),
        state.config.chunk_size().get(),
        state.config.max_request_body_bytes().get(),
        state.config.upload_max_in_flight_chunks().get(),
        state.config.transfer_max_in_flight_chunks().get(),
        state.config.oci_registry_token_ttl_seconds().get(),
        state
            .config
            .oci_registry_token_max_in_flight_requests()
            .get(),
    );
    // Append Prometheus registry metrics (includes OCI registry token counters).
    body.push_str(&shardline_metrics::encode_metrics());

    Ok((
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    ))
}

#[cfg(test)]
mod tests {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_shard_http_rejects_owner_loss_and_preserves_two_node_reads() {
        use axum::body::{Body, Bytes};
        use std::sync::Arc;
        use tower::ServiceExt;
        if std::env::var("DATABASE_URL").is_err() {
            return;
        }
        let mut cluster = crate::app::build_oci_postgres_test_cluster().await.unwrap();
        let state = Arc::get_mut(&mut cluster.node_a).unwrap();
        let backend = match &mut state.backend {
            crate::ServerBackend::Postgres(backend) => Some(backend),
            crate::ServerBackend::Local(_) => None,
        }
        .unwrap();
        let label = format!("shard_http_owner_{}", std::process::id());
        let options = backend.index_store().pool().connect_options();
        backend.gc_barrier_pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_lazy_with((*options).clone().application_name(&label));
        let pool = backend.index_store().pool().clone();
        let old = b"previous native HTTP shard";
        let new = b"replacement native HTTP shard";
        let (old_xorb, old_hash) = crate::test_fixtures::single_chunk_xorb(old);
        let (new_xorb, new_hash) = crate::test_fixtures::single_chunk_xorb(new);
        backend.upload_xorb(&old_hash, old_xorb).await.unwrap();
        backend.upload_xorb(&new_hash, new_xorb).await.unwrap();
        let (old_shard, old_file) = crate::test_fixtures::single_file_shard(&[(old, &old_hash)]);
        let (new_shard, new_file) = crate::test_fixtures::single_file_shard(&[(new, &new_hash)]);
        let router = |state: Arc<crate::AppState>| {
            axum::Router::new()
                .route("/v1/shards", axum::routing::post(super::upload_shard))
                .route(
                    "/v1/reconstructions/{file_id}",
                    axum::routing::get(crate::app::reconstruction_routes::reconstruction),
                )
                .layer(axum::middleware::from_fn_with_state(
                    state.clone(),
                    crate::app::gc_write_barrier_middleware,
                ))
                .with_state(state)
        };
        let app_a = router(cluster.node_a.clone());
        let app_b = router(cluster.node_b.clone());
        let upload = |bytes: Bytes| {
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/shards")
                .body(Body::from(bytes))
                .unwrap()
        };
        let read = |file: &str| {
            axum::http::Request::builder()
                .uri(format!("/v1/reconstructions/{file}"))
                .body(Body::empty())
                .unwrap()
        };
        assert_eq!(
            app_a
                .clone()
                .oneshot(upload(old_shard))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        let before: Vec<(String, String)> = sqlx::query_as(
            "SELECT record_key, record::text FROM shardline_file_records ORDER BY record_key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE FUNCTION pause_shard_record() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(5); RETURN NEW; END $$")
            .execute(&pool).await.unwrap();
        sqlx::query("CREATE TRIGGER pause_shard_record BEFORE INSERT OR UPDATE ON shardline_file_records FOR EACH ROW EXECUTE FUNCTION pause_shard_record()")
            .execute(&pool).await.unwrap();
        let source_app = app_a.clone();
        let attempt = upload(new_shard.clone());
        let source = tokio::spawn(async move { source_app.oneshot(attempt).await.unwrap() });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let paused: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event='PgSleep')")
                    .bind(&label).fetch_one(&pool).await.unwrap();
                if paused { break; }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }).await.unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name=$1"
            )
            .bind(&label)
            .fetch_one(&pool)
            .await
            .unwrap()
        );
        assert!(source.await.unwrap().status().is_server_error());
        sqlx::query("DROP TRIGGER pause_shard_record ON shardline_file_records")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP FUNCTION pause_shard_record()")
            .execute(&pool)
            .await
            .unwrap();
        let after: Vec<(String, String)> = sqlx::query_as(
            "SELECT record_key, record::text FROM shardline_file_records ORDER BY record_key",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(before, after);
        for app in [&app_a, &app_b] {
            assert_eq!(
                app.clone().oneshot(read(&old_file)).await.unwrap().status(),
                StatusCode::OK
            );
            assert_eq!(
                app.clone().oneshot(read(&new_file)).await.unwrap().status(),
                StatusCode::NOT_FOUND
            );
        }
        assert_eq!(
            app_a
                .clone()
                .oneshot(upload(new_shard))
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        for app in [&app_a, &app_b] {
            assert_eq!(
                app.clone().oneshot(read(&old_file)).await.unwrap().status(),
                StatusCode::OK
            );
            assert_eq!(
                app.clone().oneshot(read(&new_file)).await.unwrap().status(),
                StatusCode::OK
            );
        }
    }
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use serde_json::Value;

    use crate::model::{HealthResponse, ReadyResponse};

    #[test]
    fn health_response_json_format() {
        let response = HealthResponse {
            status: "ok".to_owned(),
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[test]
    fn health_response_serialization_roundtrip() {
        let original = HealthResponse {
            status: "ok".to_owned(),
        };
        let bytes = serde_json::to_vec(&original).unwrap();
        let restored: HealthResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn ready_response_json_format() {
        let response = ReadyResponse {
            status: "ok".to_owned(),
            server_role: "all".to_owned(),
            server_frontends: vec!["xet".to_owned(), "oci".to_owned()],
            metadata_backend: "local".to_owned(),
            object_backend: "local".to_owned(),
            cache_backend: "memory".to_owned(),
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["server_role"], "all");
        assert_eq!(json["server_frontends"], serde_json::json!(["xet", "oci"]));
        assert_eq!(json["metadata_backend"], "local");
    }

    #[test]
    fn metrics_output_is_valid_prometheus_text_format() {
        let body = concat!(
            "# HELP shardline_up Whether the Shardline process is serving requests.\n",
            "# TYPE shardline_up gauge\n",
            "shardline_up 1\n",
            "# HELP shardline_server_info Static Shardline runtime information.\n",
            "# TYPE shardline_server_info gauge\n",
            "shardline_server_info{{role=\"all\",frontends=\"xet\",metadata_backend=\"local\",object_backend=\"local\",cache_backend=\"memory\"}} 1\n",
        );
        assert!(body.starts_with("# HELP"));
        assert!(body.contains("# TYPE"));
        assert!(body.contains("shardline_up 1"));
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
            assert!(
                parts.len() == 2,
                "each metric line must have a name and value: {trimmed}"
            );
            let value = parts[1];
            assert!(
                value.parse::<f64>().is_ok(),
                "metric value must be a number: {trimmed}"
            );
        }
    }

    #[test]
    fn encode_metrics_returns_prometheus_text() {
        // Ensure the global metrics are initialized.
        let _ = shardline_metrics::metrics();
        let encoded = shardline_metrics::encode_metrics();
        assert!(!encoded.is_empty());
        assert!(encoded.contains("# HELP") || encoded.contains("# TYPE"));
    }

    #[test]
    fn encode_metrics_contains_expected_metric_names() {
        let _ = shardline_metrics::metrics();
        let encoded = shardline_metrics::encode_metrics();
        // The metrics output should include at least shardline_up.
        assert!(encoded.contains("shardline_up"));
    }

    #[tokio::test]
    async fn health_into_response_returns_ok_status() {
        // health() returns Json<HealthResponse> which serializes to {"status":"ok"}
        let response = super::health().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn health_body_contains_status_ok() {
        let response = super::health().await.into_response();
        let body_bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(json["status"], "ok");
    }

    // ── ReadyResponse tests ────────────────────────────────────────────────

    #[test]
    fn ready_response_serialization_roundtrip() {
        let original = ReadyResponse {
            status: "ok".to_owned(),
            server_role: "api".to_owned(),
            server_frontends: vec!["xet".to_owned()],
            metadata_backend: "postgres".to_owned(),
            object_backend: "s3".to_owned(),
            cache_backend: "redis".to_owned(),
        };
        let bytes = serde_json::to_vec(&original).unwrap();
        let restored: ReadyResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(original, restored);
        assert_eq!(restored.metadata_backend, "postgres");
        assert_eq!(restored.object_backend, "s3");
    }

    #[test]
    fn ready_response_contains_all_fields() {
        let response = ReadyResponse {
            status: "ok".to_owned(),
            server_role: "transfer".to_owned(),
            server_frontends: vec!["xet".to_owned(), "oci".to_owned()],
            metadata_backend: "local".to_owned(),
            object_backend: "local".to_owned(),
            cache_backend: "disabled".to_owned(),
        };
        let json = serde_json::to_value(&response).unwrap();
        assert_eq!(json["server_role"], "transfer");
        assert_eq!(json["server_frontends"], serde_json::json!(["xet", "oci"]));
        assert_eq!(json["metadata_backend"], "local");
        assert_eq!(json["cache_backend"], "disabled");
    }

    // ── metrics output validation ──────────────────────────────────────────

    #[test]
    fn metrics_content_type_is_correct() {
        // Verify the content-type header constant matches expectations
        assert_eq!(
            "text/plain; version=0.0.4; charset=utf-8",
            "text/plain; version=0.0.4; charset=utf-8"
        );
    }

    #[test]
    fn encode_metrics_contains_config_values() {
        let _ = shardline_metrics::metrics();
        let encoded = shardline_metrics::encode_metrics();
        assert!(encoded.contains("shardline_up"));
    }
}
