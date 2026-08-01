use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, Uri},
    response::IntoResponse,
};
use serde::Deserialize;
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    admission::weights,
    xet_adapter::{
        BatchReconstructionResponse, build_batch_reconstruction_response, validate_hash_path,
        validate_optional_content_hash,
    },
};

use super::{
    AppState, authorize,
    reconstruction_helpers::{
        load_reconstruction_response, load_reconstruction_v2_response,
        parse_batch_reconstruction_file_ids, parse_reconstruction_request_range,
    },
    scope_from_auth,
};

#[derive(Debug, Deserialize)]
pub(super) struct FileVersionQuery {
    content_hash: Option<String>,
}

#[tracing::instrument(skip(state, headers), fields(file_id))]
pub(super) async fn reconstruction(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<FileVersionQuery>,
) -> Result<impl IntoResponse, ServerError> {
    // Acquire admission permit for reconstruction
    let _admit = state
        .admission
        .try_acquire(weights::RECONSTRUCTION)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    let start = Instant::now();
    let result = load_reconstruction_response(
        &state,
        &file_id,
        query.content_hash.as_deref(),
        requested_range,
        auth.as_ref().map(scope_from_auth),
    )
    .await;
    let elapsed = start.elapsed();
    match &result {
        Ok(response) => {
            let chunks = u64::try_from(response.terms.len()).unwrap_or(0);
            shardline_metrics::record_reconstruction(true, elapsed, chunks);
        }
        Err(_) => {
            shardline_metrics::record_reconstruction(false, elapsed, 0);
        }
    }
    Ok(Json(result?))
}

#[tracing::instrument(skip(state, headers), fields(file_id))]
pub(super) async fn reconstruction_v2(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<FileVersionQuery>,
) -> Result<impl IntoResponse, ServerError> {
    // Acquire admission permit for reconstruction
    let _admit = state
        .admission
        .try_acquire(weights::RECONSTRUCTION)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    let start = Instant::now();
    let result = load_reconstruction_v2_response(
        &state,
        &file_id,
        query.content_hash.as_deref(),
        requested_range,
        auth.as_ref().map(scope_from_auth),
    )
    .await;
    let elapsed = start.elapsed();
    match &result {
        Ok(response) => {
            let chunks = u64::try_from(response.terms.len()).unwrap_or(0);
            shardline_metrics::record_reconstruction(true, elapsed, chunks);
        }
        Err(_) => {
            shardline_metrics::record_reconstruction(false, elapsed, 0);
        }
    }
    Ok(Json(result?))
}

#[tracing::instrument(skip(state, headers, uri))]
pub(super) async fn batch_reconstruction(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<BatchReconstructionResponse>, ServerError> {
    // Acquire admission permit for batch reconstruction
    let _admit = state
        .admission
        .try_acquire(weights::BATCH_OPERATION)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let repository_scope = auth.as_ref().map(scope_from_auth);
    let file_ids = parse_batch_reconstruction_file_ids(&uri)?;
    let start = Instant::now();
    let mut responses = Vec::new();

    for file_id in file_ids {
        match load_reconstruction_response(&state, &file_id, None, None, repository_scope).await {
            Ok(response) => responses.push((file_id, response)),
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }

    let elapsed = start.elapsed();
    let total_chunks: u64 = responses
        .iter()
        .map(|(_, r)| u64::try_from(r.terms.len()).unwrap_or(0))
        .sum();
    shardline_metrics::record_reconstruction(true, elapsed, total_chunks);

    Ok(Json(build_batch_reconstruction_response(responses)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Router,
        body::Body,
        extract::Query,
        http::{Request, StatusCode},
    };
    use tempfile::TempDir;
    use tower::ServiceExt;

    use super::FileVersionQuery;
    use crate::{
        ProtocolMetrics, ReconstructionCacheService, ServerBackend, ServerConfig, ServerFrontend,
        ServerRole, TransferLimiter, app::AppState,
    };

    #[test]
    fn file_version_query_debug_format() {
        let query = FileVersionQuery {
            content_hash: Some("hash".to_owned()),
        };
        let debug = format!("{query:?}");
        assert!(debug.contains("content_hash"));
        assert!(debug.contains("hash"));
    }

    #[test]
    fn file_version_query_content_hash_none() {
        let query = FileVersionQuery { content_hash: None };
        assert!(query.content_hash.is_none());
    }

    #[test]
    fn file_version_query_with_content_hash() {
        let query = FileVersionQuery {
            content_hash: Some("abc123".to_owned()),
        };
        assert_eq!(query.content_hash.as_deref(), Some("abc123"));
    }

    #[test]
    fn file_version_query_deserialize_from_json() {
        // Verify deserialization from JSON representation
        let json = r#"{"content_hash": "abc123"}"#;
        let deserialized: FileVersionQuery = serde_json::from_str(json).unwrap();
        assert_eq!(deserialized.content_hash, Some("abc123".to_owned()));
    }

    #[test]
    fn file_version_query_deserialize_empty_json() {
        let json = r#"{}"#;
        let deserialized: FileVersionQuery = serde_json::from_str(json).unwrap();
        assert!(deserialized.content_hash.is_none());
    }

    #[test]
    fn file_version_query_deserialize_from_url_query() {
        // Verify deserialization from URL query string via axum::extract::Query
        let query: Query<FileVersionQuery> = Query::try_from_uri(
            &"http://example.com/path?content_hash=abc123"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(query.content_hash.as_deref(), Some("abc123"));
    }

    #[test]
    fn file_version_query_deserialize_from_url_query_without_hash() {
        let query: Query<FileVersionQuery> =
            Query::try_from_uri(&"http://example.com/path".parse().unwrap()).unwrap();
        assert!(query.content_hash.is_none());
    }

    #[test]
    fn file_version_query_deserialize_from_url_query_with_empty_hash() {
        let query: Query<FileVersionQuery> =
            Query::try_from_uri(&"http://example.com/path?content_hash=".parse().unwrap()).unwrap();
        assert_eq!(query.content_hash.as_deref(), Some(""));
    }

    // =====================================================================
    // Handler-level integration tests
    // =====================================================================

    /// Builds a minimal AppState for testing reconstruction handlers.
    async fn build_reconstruction_state() -> (Arc<AppState>, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let chunk_size = std::num::NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:0".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_frontends([ServerFrontend::Xet])
        .expect("server frontends");

        let backend = ServerBackend::from_config(&config)
            .await
            .expect("backend from config");

        let transfer_limiter = TransferLimiter::new(chunk_size, chunk_size);

        let state = Arc::new(AppState {
            config,
            role: ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: ReconstructionCacheService::disabled(),
            transfer_limiter,
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            admission: crate::admission::WeightedAdmission::new(
                std::num::NonZeroUsize::new(256).unwrap(),
            ),
            pools: crate::admission::ExecutionPools::default_sizes(),
            protocol_metrics: ProtocolMetrics::default(),
        });

        (state, tmp)
    }

    fn reconstruction_router(state: Arc<AppState>) -> Router {
        use super::{batch_reconstruction, reconstruction, reconstruction_v2};
        use axum::routing::get;

        Router::new()
            .route("/reconstruction/{file_id}", get(reconstruction))
            .route("/reconstruction/v2/{file_id}", get(reconstruction_v2))
            .route("/reconstruction/batch", get(batch_reconstruction))
            .with_state(state)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_reconstruction_invalid_file_id_returns_error() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Invalid file_id (too short, not 64-char hex)
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/reconstruction/short")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_reconstruction_v2_invalid_file_id_returns_error() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Invalid file_id with uppercase hex
        let hash = "A".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/reconstruction/v2/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_reconstruction_with_content_hash_param() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Valid 64-char hex hash, but content_hash is not valid hex
        let file_id = "a".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/reconstruction/{file_id}?content_hash=not-a-hash"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should fail because content_hash is not valid hex
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_batch_reconstruction_without_query() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Batch with no query params should return empty list
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/reconstruction/batch")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_batch_reconstruction_with_missing_file() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Batch with a valid hash but file doesn't exist
        let file_id = "a".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/reconstruction/batch?file_id={file_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_batch_reconstruction_invalid_file_id_returns_error() {
        let (state, _tmp) = build_reconstruction_state().await;
        let app = reconstruction_router(state);

        // Batch with invalid file_id (non-hex)
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/reconstruction/batch?file_id=not-a-valid-hash")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
