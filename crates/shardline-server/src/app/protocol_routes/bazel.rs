use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use shardline_protocol::TokenScope;

use crate::{
    BazelCacheKind, ServerError, bazel_cache_object_key,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{AppState, authorize, direct_object_response, scope_from_auth};

// ---------------------------------------------------------------------------
// AC (Action Cache) handlers — /v1/bazel/cache/ac/{hash}
// ---------------------------------------------------------------------------

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    // Action Cache keys identify actions, not the serialized action result.
    // Unlike CAS, the action key is therefore not expected to hash the body.
    let _stored = state
        .backend
        .put_object_bytes_if_absent(&object_key, bytes)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// CAS (Content-Addressable Storage) handlers — /v1/bazel/cache/cas/{hash}
// ---------------------------------------------------------------------------

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Flat routes — /v1/bazel/{hash}
// Bazel remote cache spec uses a flat URL namespace: GET/PUT/HEAD /{hash}.
// These routes try both AC and CAS for backward compatibility with stock
// Bazel clients that configure --remote_cache=http://host:port/v1/bazel.
// ---------------------------------------------------------------------------

/// GET /v1/bazel/{hash} — Download blob (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) && state.backend.object_length(&ac_key).await.is_ok()
    {
        return direct_object_response(
            &state,
            &headers,
            &ac_key,
            "application/octet-stream",
            None,
            "bazel",
        )
        .await;
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &cas_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

/// PUT /v1/bazel/{hash} — Upload blob to CAS.
#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// HEAD /v1/bazel/{hash} — Check existence (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) && let Ok(total_length) = state.backend.object_length(&ac_key).await
    {
        return Ok((
            StatusCode::OK,
            [
                (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
                (
                    axum::http::header::CONTENT_TYPE,
                    "application/octet-stream".to_owned(),
                ),
            ],
        )
            .into_response());
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&cas_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (
                axum::http::header::CONTENT_TYPE,
                "application/octet-stream".to_owned(),
            ),
        ],
    )
        .into_response())
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, sync::Arc};

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use sha2::{Digest, Sha256};
    use shardline_protocol_adapters::ProtocolError;
    use tempfile::TempDir;
    use tower::ServiceExt;

    use crate::{
        BazelCacheKind, ProtocolMetrics, ReconstructionCacheService, ServerBackend, ServerConfig,
        ServerFrontend, ServerRole, TransferLimiter, app::AppState, bazel_cache_object_key,
    };

    use super::{
        bazel_get, bazel_get_ac, bazel_get_cas, bazel_head, bazel_head_ac, bazel_head_cas,
        bazel_put, bazel_put_ac, bazel_put_cas,
    };

    /// Builds a minimal [`AppState`] backed by a fresh temp directory.
    async fn build_test_state() -> (Arc<AppState>, TempDir) {
        let tmp = TempDir::new().expect("tempdir");
        let chunk_size = NonZeroUsize::new(4096).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:0".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_frontends([ServerFrontend::BazelHttp])
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
            protocol_metrics: ProtocolMetrics::default(),
        });

        (state, tmp)
    }

    fn bazel_router(state: Arc<AppState>) -> Router {
        Router::new()
            // AC routes
            .route(
                "/v1/bazel/cache/ac/{hash}",
                get(bazel_get_ac).head(bazel_head_ac).put(bazel_put_ac),
            )
            // CAS routes
            .route(
                "/v1/bazel/cache/cas/{hash}",
                get(bazel_get_cas).head(bazel_head_cas).put(bazel_put_cas),
            )
            // Flat routes (try AC first, fall back to CAS)
            .route(
                "/v1/bazel/{hash}",
                get(bazel_get).head(bazel_head).put(bazel_put),
            )
            .with_state(state)
    }

    fn test_hash() -> String {
        "a".repeat(64)
    }

    fn test_content() -> Vec<u8> {
        b"bazel-test-content".to_vec()
    }

    fn test_content_hash() -> String {
        hex::encode(Sha256::digest(b"bazel-test-content"))
    }

    // =========================================================================
    // AC (Action Cache) integration tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_get_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = test_hash();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_head_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = test_hash();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_put_and_get_happy_path() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state.clone());
        let hash = "a".repeat(64); // An action digest is independent of the action result.
        let content = test_content();

        // PUT
        let put_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::NO_CONTENT);

        // HEAD
        let head_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_resp.status(), StatusCode::OK);
        let content_length: u64 = head_resp
            .headers()
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(content_length, content.len() as u64);

        // GET
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content.as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_put_is_idempotent() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = "a".repeat(64);
        let content = test_content();

        // PUT twice
        let r1 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r1.status(), StatusCode::NO_CONTENT);

        let r2 = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(r2.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ac_accepts_an_action_digest_that_does_not_match_the_result_body() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);

        // An Action Cache digest names an action rather than its result body.
        let action_hash = "b".repeat(64);
        let content = test_content();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/ac/{action_hash}"))
                    .body(Body::from(content))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    // =========================================================================
    // CAS (Content-Addressable Storage) integration tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_get_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = test_hash();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/cache/cas/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_put_and_get_happy_path() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state.clone());
        let content = test_content();
        let hash = test_content_hash(); // CAS uses SHA-256 hash matching content

        // PUT
        let put_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/cas/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::NO_CONTENT);

        // HEAD
        let head_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/bazel/cache/cas/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_resp.status(), StatusCode::OK);

        // GET
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/cache/cas/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content.as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cas_rejects_hash_mismatch_on_put() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        // Hash that does NOT match the content -> should fail with hash mismatch error
        let wrong_hash = "b".repeat(64);
        let content = test_content();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/cas/{wrong_hash}"))
                    .body(Body::from(content))
                    .unwrap(),
            )
            .await
            .unwrap();

        // CAS verifies SHA-256 hash of content matches URL hash, so wrong hash
        // should produce an error response
        assert!(response.status().is_client_error());
    }

    // =========================================================================
    // Flat routes (try AC first, fall back to CAS)
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flat_get_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = test_hash();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flat_head_missing_returns_not_found() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state);
        let hash = test_hash();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flat_put_to_cas_and_get_from_flat() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state.clone());
        let content = test_content();
        let hash = test_content_hash(); // CAS hash matching content

        // PUT to flat route stores in CAS
        let put_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::NO_CONTENT);

        // HEAD from flat route
        let head_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_resp.status(), StatusCode::OK);

        // GET from flat route
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content.as_slice());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flat_route_tries_ac_before_cas() {
        let (state, _tmp) = build_test_state().await;
        let app = bazel_router(state.clone());
        let content = b"ac-content".to_vec();
        let hash = "c".repeat(64);

        // PUT to AC route
        let put_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/bazel/cache/ac/{hash}"))
                    .body(Body::from(content.clone()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::NO_CONTENT);

        // GET from flat route should find it in AC first
        let get_resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/bazel/{hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(get_resp.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(body.as_ref(), content.as_slice());
    }

    // =========================================================================
    // bazel_cache_object_key (existing tests kept)
    // =========================================================================

    #[test]
    fn ac_hash_produces_key_with_ac_prefix() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        assert!(
            key.as_str().contains("/ac/"),
            "expected /ac/ in key: {}",
            key.as_str()
        );
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn cas_hash_produces_key_with_cas_prefix() {
        let hash = "b".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert!(
            key.as_str().contains("/cas/"),
            "expected /cas/ in key: {}",
            key.as_str()
        );
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn invalid_hash_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, "short", None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn invalid_hash_not_hex_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(
                BazelCacheKind::Cas,
                "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
                None,
            ),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn empty_hash_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, "", None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn uppercase_hex_returns_error() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, &"A".repeat(64), None),
            Err(ProtocolError::InvalidContentHash)
        ));
    }
}
