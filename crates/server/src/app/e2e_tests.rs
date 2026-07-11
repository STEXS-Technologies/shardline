//! End-to-end tests that exercise every protocol endpoint through the full Axum stack.
//!
//! Each test group builds a minimal [`Router`] with only the routes needed for that
//! protocol and sends real HTTP requests via [`tower::ServiceExt::oneshot`].

use std::{num::NonZeroUsize, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware,
    routing::{get, head, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    AppState, ServerConfig, ServerFrontend, TransferLimiter,
    app::ProtocolMetrics,
    backend::ServerBackend,
    local_backend::LocalBackend,
    object_store::ServerObjectStore,
    reconstruction_cache::ReconstructionCacheService,
    server_role::ServerRole,
    test_fixtures,
};

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

/// Builds a minimal Axum [`Router`] wired to the given `frontends` backed by a
/// fresh [`TempDir`].  Authentication is **disabled** so every route handler
/// skips token validation.
///
/// The returned [`TempDir`] must be kept alive for the lifetime of the Router.
async fn test_app(frontends: &[ServerFrontend]) -> (Router, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).expect("chunk size");
    let object_store =
        ServerObjectStore::local(tmp.path().join("chunks")).expect("local object store");
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).expect("upload parallelism"),
        object_store,
        frontends,
    )
    .await
    .expect("local backend");

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().expect("bind addr"),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(frontends.to_vec())
    .expect("server frontends")
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .expect("token signing key");

    config
        .validate_runtime_requirements()
        .expect("runtime requirements");

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).expect("limiter")),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let mut app = Router::new()
        .route("/healthz", get(super::operational::health))
        .route("/readyz", get(super::operational::ready))
        .layer(middleware::from_fn(super::security_headers_middleware));

    // Stats route (registered when role serves API, outside per-frontend loop).
    if state.role.serves_api() {
        app = app.route("/v1/stats", get(super::operational::stats));
    }

    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if state.role.serves_api() {
                    app = app
                        .route(
                            "/reconstructions",
                            get(super::reconstruction_routes::batch_reconstruction),
                        )
                        .route(
                            "/v1/reconstructions",
                            get(super::reconstruction_routes::batch_reconstruction),
                        )
                        .route(
                            "/v1/reconstructions/{file_id}",
                            get(super::reconstruction_routes::reconstruction),
                        )
                        .route(
                            "/v2/reconstructions/{file_id}",
                            get(super::reconstruction_routes::reconstruction_v2),
                        )
                        .route("/shards", post(super::operational::upload_shard))
                        .route("/v1/shards", post(super::operational::upload_shard));
                }
                if state.role.serves_transfer() {
                    app = app
                        .route(
                            "/v1/chunks/default/{hash}",
                            get(super::operational::read_chunk),
                        )
                        .route(
                            "/v1/chunks/default-merkledb/{hash}",
                            get(super::operational::read_chunk),
                        )
                        .route(
                            "/v1/xorbs/default/{hash}",
                            head(super::operational::head_xorb)
                                .post(super::operational::upload_xorb),
                        )
                        .route(
                            "/transfer/xorb/{prefix}/{hash}",
                            get(super::operational::read_xorb_transfer),
                        );
                }
            }
            ServerFrontend::Lfs => {
                if state.role.serves_api() {
                    app = app.route(
                        "/v1/lfs/objects/batch",
                        post(super::protocol_routes::lfs_batch),
                    );
                }
                if state.role.serves_transfer() {
                    app = app
                        .route(
                            "/v1/lfs/objects/{oid}",
                            get(super::protocol_routes::lfs_get_object)
                                .head(super::protocol_routes::lfs_head_object)
                                .put(super::protocol_routes::lfs_put_object)
                                .patch(super::protocol_routes::lfs_patch_object)
                                .delete(super::protocol_routes::lfs_delete_object),
                        )
                        .route(
                            "/v1/lfs/objects/{oid}/verify",
                            post(super::protocol_routes::lfs_verify_object),
                        );
                }
            }
            ServerFrontend::BazelHttp => {
                if state.role.serves_transfer() {
                    app = app
                        .route(
                            "/v1/bazel/cache/ac/{hash}",
                            get(super::protocol_routes::bazel_get_ac)
                                .put(super::protocol_routes::bazel_put_ac)
                                .head(super::protocol_routes::bazel_head_ac),
                        )
                        .route(
                            "/v1/bazel/cache/cas/{hash}",
                            get(super::protocol_routes::bazel_get_cas)
                                .put(super::protocol_routes::bazel_put_cas)
                                .head(super::protocol_routes::bazel_head_cas),
                        )
                        .route(
                            "/v1/bazel/{hash}",
                            get(super::protocol_routes::bazel_get)
                                .put(super::protocol_routes::bazel_put)
                                .head(super::protocol_routes::bazel_head),
                        );
                }
            }
            ServerFrontend::Oci => {
                app = app
                    .route(
                        "/v2/token",
                        get(super::protocol_routes::oci_registry_token),
                    )
                    .route("/v2/", get(super::protocol_routes::oci_v2_root))
                    .route(
                        "/v2/{*path}",
                        axum::routing::any(super::protocol_routes::oci_dispatch),
                    );
            }
            ServerFrontend::Hub => {} // Hub not tested in E2E
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));
    (app, tmp)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Computes a SHA-256 hex digest of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Creates a valid 64-character lowercase hex OID for LFS tests.
fn test_oid(content: &[u8]) -> String {
    sha256_hex(content)
}

/// Creates a valid 64-character lowercase hex hash for Bazel tests.
fn test_hash(content: &[u8]) -> String {
    sha256_hex(content)
}

/// Sends a request and collects the response body as bytes.
async fn body_bytes(response: axum::http::Response<Body>) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
               .await
        .expect("body bytes")
        .to_vec()
}

/// Sends a request and collects the response body as a JSON value.
async fn body_json(response: axum::http::Response<Body>) -> Value {
    let bytes = body_bytes(response).await;
    serde_json::from_slice(&bytes).expect("json body")
}

// ============================================================================
// Xet Protocol Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_endpoint_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["status"], "ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_endpoint_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(Request::builder().uri("/readyz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["status"], "ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_endpoint_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    // Fresh backend has zero chunks and zero files.
    assert_eq!(json["chunks"], 0);
    assert_eq!(json["files"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_upload_and_read() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"hello-xorb-content-for-e2e-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);

    // Upload the xorb via POST /v1/xorbs/default/{hash}
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/xorbs/default/{xorb_hash}"))
                .body(Body::from(xorb_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        upload.status(),
        StatusCode::OK,
        "xorb upload failed: {}",
        String::from_utf8_lossy(&body_bytes(upload).await)
    );

    // Verify HEAD returns 200
    let head_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/xorbs/default/{xorb_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_resp.status(), StatusCode::OK);

    // Download the xorb via GET /transfer/xorb/default/{hash}
    let download = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/transfer/xorb/default/{xorb_hash}"))
                .header(header::RANGE, "bytes=0-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The transfer route requires a Range header; it should return 200 with content.
    assert!(download.status().is_success(), "download status: {}", download.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_read_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let nonexistent_hash = "a".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/transfer/xorb/default/{nonexistent_hash}"))
                .header(header::RANGE, "bytes=0-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_read_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let nonexistent_hash = "b".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/chunks/default/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let nonexistent_id = "c".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/reconstructions/{nonexistent_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_reconstruction_empty() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/reconstructions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    // Empty reconstruction batch should return an empty files map.
    assert!(json["files"].is_object());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_read_token_without_auth_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Without auth configured, token endpoints should return 401 or 500
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/repo/xet-read-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status().is_server_error() || response.status().is_client_error(),
        "expected error status without auth, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_write_token_without_auth_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app.oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/repo/xet-write-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status().is_server_error() || response.status().is_client_error(),
        "expected error status without auth, got {}",
        response.status()
    );
}

// ============================================================================
// LFS Protocol Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_valid() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = json!({
        "operation": "download",
        "objects": []
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["transfer"], "basic");
    assert_eq!(json["hash_algo"], "sha256");
    assert!(json["objects"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_invalid_json() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from("not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();

    // Invalid JSON should result in 400 or 422
    assert!(
        response.status().is_client_error(),
        "expected client error, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_put_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-put-test-content-42";
    let oid = test_oid(content);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_object_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-get-present-content";
    let oid = test_oid(content);

    // Upload first
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // GET the object
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_object_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"never-uploaded-lfs-object");
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_head_object_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-head-present";
    let oid = test_oid(content);

    // Upload
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // HEAD
    let head_resp = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_resp.status(), StatusCode::OK);
    let content_length = head_resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("content-length")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_head_object_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"never-existed-lfs-head");
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_delete_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-delete-content";
    let oid = test_oid(content);

    // Upload
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Delete
    let delete_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // 202 Accepted on successful deletion
    assert!(delete_resp.status().is_success(), "delete status: {}", delete_resp.status());

    // Confirm deleted
    let head_resp = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_invalid_oid_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    // Test GET with invalid OID
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/lfs/objects/not-a-valid-oid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(get.status().is_client_error(), "GET invalid OID: {}", get.status());

    // Test PUT with invalid OID
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/lfs/objects/not-a-valid-oid")
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(put.status().is_client_error(), "PUT invalid OID: {}", put.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let chunk1 = b"lfs-patch-initial-";
    let chunk2 = b"content";
    let full_content = [chunk1.to_vec(), chunk2.to_vec()].concat();
    let oid = test_oid(&full_content);
    let total = full_content.len() as u64;

    // PATCH chunk 1 (offset 0)
    let range1 = format!("bytes 0-{}/{}", chunk1.len() as u64 - 1, total);
    let patch1 = app.clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, chunk1.len().to_string())
                .header("Content-Range", &range1)
                .body(Body::from(chunk1.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch1.status(), StatusCode::OK, "PATCH chunk1 failed: {}", String::from_utf8_lossy(&body_bytes(patch1).await));

    // PATCH chunk 2 (final chunk)
    let range2 = format!("bytes {}-{}/{}", chunk1.len(), total - 1, total);
    let patch2 = app.clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, chunk2.len().to_string())
                .header("Content-Range", &range2)
                .body(Body::from(chunk2.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch2.status(), StatusCode::OK, "PATCH chunk2 failed: {}", String::from_utf8_lossy(&body_bytes(patch2).await));

    // GET the final object and verify it contains both parts
    let get = app.oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, full_content, "PATCH result should contain both chunks");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_invalid_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-patch-invalid-range";
    let oid = test_oid(content);

    // Upload
    let put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // PATCH with invalid Content-Range (start > end — triggers the underflow bug we found)
    let patch = app.oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "bytes 100-0/*")  // intentionally invalid
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    
    // Should return a client error, not panic
    assert!(
        patch.status().is_client_error(),
        "invalid Content-Range should return 4xx, got {}",
        patch.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_verify_valid() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-verify-valid-content";
    let oid = test_oid(content);

    // Upload
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Verify
    let verify = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(verify.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_verify_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"never-uploaded-verify");
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// Bazel Protocol Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_put_and_get() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-cas-test-content";
    let hash = test_hash(content);

    // PUT to CAS
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // GET from CAS
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_get_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let nonexistent_hash = "d".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_head_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-cas-head-content";
    let hash = test_hash(content);

    // PUT
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // HEAD
    let head_resp = app
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
    let content_length = head_resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("content-length")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_head_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let nonexistent_hash = "e".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/cache/cas/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_put_and_get() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-ac-test-content";
    let hash = test_hash(content);

    // PUT to AC
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/ac/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // GET from AC
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/ac/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_get_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let nonexistent_hash = "f".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/ac/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_head_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-ac-head-content";
    let hash = test_hash(content);

    // PUT to AC
    let put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/ac/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // HEAD
    let head_resp = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/cache/ac/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_resp.status(), StatusCode::OK);
    let content_length = head_resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("content-length")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_head_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let nonexistent_hash = "0".repeat(64);
    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/cache/ac/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_flat_put_and_get() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-flat-test-content";
    let hash = test_hash(content);

    // PUT to flat route
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // GET from flat route
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_flat_head_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-flat-head-content";
    let hash = test_hash(content);

    // PUT
    let put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/{hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT);

    // HEAD
    let head_resp = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(head_resp.status(), StatusCode::OK);
    let content_length = head_resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("content-length")
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, content.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_flat_head_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let nonexistent_hash = "0".repeat(64);
    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/{nonexistent_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_invalid_hash_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    // Try PUT with a hash that is too short (must be 64 lowercase hex chars)
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/bazel/cache/cas/short")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status().is_client_error(),
        "expected client error, got {}",
        response.status()
    );
}

// ============================================================================
// OCI Protocol Tests
// ============================================================================

const OCI_TEST_REPO: &str = "team/assets";

/// A minimal OCI image manifest JSON for testing.
fn test_manifest_json(config_digest: &str, layer_digest: &str) -> String {
    json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": 0,
            "digest": format!("sha256:{config_digest}")
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "size": 0,
                "digest": format!("sha256:{layer_digest}")
            }
        ]
    })
    .to_string()
}

/// Uploads a blob directly via POST with digest query parameter.
async fn oci_upload_blob(app: &Router, repository: &str, data: &[u8]) -> String {
    let digest = sha256_hex(data);
    let uri = format!("/v2/{repository}/blobs/uploads/?digest=sha256:{digest}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(&uri)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(data.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "blob upload failed: uri={uri} body={}",
        String::from_utf8_lossy(&body_bytes(response).await)
    );
    digest
}

/// Uploads a config blob and a layer blob, then PUTs a manifest referencing
/// both. Returns the manifest digest hex.
async fn oci_setup_manifest(app: &Router, repository: &str, tag: &str) -> String {
    let config_data = b"{}";
    let layer_data = b"\x1f\x8b\x08\x00";

    let config_digest = oci_upload_blob(app, repository, config_data).await;
    let layer_digest = oci_upload_blob(app, repository, layer_data).await;

    let manifest_json = test_manifest_json(&config_digest, &layer_digest);
    let manifest_bytes = manifest_json.as_bytes();
    let manifest_digest = sha256_hex(manifest_bytes);

    let uri = format!("/v2/{repository}/manifests/{tag}");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(manifest_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "manifest PUT failed: {}",
        String::from_utf8_lossy(&body_bytes(response).await)
    );

    manifest_digest
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_v2_root_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v2/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("Docker-Distribution-API-Version")
            .unwrap()
            .to_str()
            .unwrap(),
        "registry/2.0"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_token_endpoint_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v2/token?scope=repository:team/assets:pull&service=shardline")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Token endpoint may return 401 when no auth provider is configured in
    // the AppState. That is expected for test simplicity.
    assert!(response.status().is_success() || response.status() == StatusCode::UNAUTHORIZED,
        "unexpected status: {} body: {}",
        response.status(),
        String::from_utf8_lossy(&body_bytes(response).await));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_upload_monolithic() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let data = b"hello-oci-blob";
    let digest = oci_upload_blob(&app, OCI_TEST_REPO, data).await;

    // Now GET the blob back
    let get_response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);
    let body = body_bytes(get_response).await;
    assert_eq!(body, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_get_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let nonexistent_digest = "0".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{nonexistent_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_head_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let data = b"oci-blob-head-test";
    let digest = oci_upload_blob(&app, OCI_TEST_REPO, data).await;

    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(header::CONTENT_LENGTH).is_some());
    assert!(response.headers().get(header::CONTENT_TYPE).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_head_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let nonexistent_digest = "1".repeat(64);
    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{nonexistent_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_delete() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let data = b"oci-blob-delete-test";
    let digest = oci_upload_blob(&app, OCI_TEST_REPO, data).await;

    // Delete
    let delete = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "delete status: {}", delete.status());

    // Confirm deleted
    let get = app.oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_put_and_get() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let manifest_digest = oci_setup_manifest(&app, OCI_TEST_REPO, "latest").await;

    // GET the manifest by tag
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/latest"))
                .header(
                    header::ACCEPT,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    let manifest: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        manifest["schemaVersion"],
        2,
        "unexpected manifest: {}",
        String::from_utf8_lossy(&body)
    );

    // GET the manifest by digest
    let get_by_digest = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/manifests/sha256:{manifest_digest}"
                ))
                .header(
                    header::ACCEPT,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_by_digest.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_get_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/nonexistent"))
                .header(
                    header::ACCEPT,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_head_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    oci_setup_manifest(&app, OCI_TEST_REPO, "head-test").await;

    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/head-test"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(header::CONTENT_LENGTH).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_head_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app.oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/nonexistent-manifest"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_delete() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    oci_setup_manifest(&app, OCI_TEST_REPO, "delete-me").await;

    // Delete the manifest
    let delete = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/delete-me"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // DELETE may return 202 Accepted or 204 No Content
    assert!(delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "delete status: {}", delete.status());

    // Confirm deleted
    let get = app.oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/delete-me"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tags_list() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // First, create a manifest so that there's a tag to list
    oci_setup_manifest(&app, OCI_TEST_REPO, "latest").await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/tags/list"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["name"], OCI_TEST_REPO);
    let tags = json["tags"].as_array().expect("tags array");
    assert!(
        tags.iter().any(|t| t.as_str() == Some("latest")),
        "expected 'latest' tag in {tags:?}"
    );
}

// ============================================================================
// Security Headers
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn security_headers_present() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let headers = response.headers();
    assert_eq!(
        headers
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .unwrap()
            .to_str()
            .unwrap(),
        "nosniff"
    );
    assert_eq!(
        headers
            .get(header::X_FRAME_OPTIONS)
            .unwrap()
            .to_str()
            .unwrap(),
        "DENY"
    );
    assert_eq!(
        headers
            .get(header::STRICT_TRANSPORT_SECURITY)
            .unwrap()
            .to_str()
            .unwrap(),
        "max-age=31536000"
    );
    assert_eq!(
        headers
            .get(header::REFERRER_POLICY)
            .unwrap()
            .to_str()
            .unwrap(),
        "strict-origin-when-cross-origin"
    );
}

// ============================================================================
// Cross-Protocol Happy Path
// ============================================================================

/// Verifies that Xet and LFS frontends coexist in a single app instance.
/// Uploads xorb data, creates a shard, and verifies reconstruction metadata.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_via_lfs_read_metadata_via_reconstruction() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet, ServerFrontend::Lfs]).await;

    // 1. Create a xorb and upload it
    let content = b"cross-protocol-test-data";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);

    let xorb_upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/xorbs/default/{xorb_hash}"))
                .body(Body::from(xorb_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(xorb_upload.status(), StatusCode::OK);

    // 2. Create a shard referencing that xorb
    let (shard_bytes, file_id) =
        test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);

    let shard_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/shards")
                .body(Body::from(shard_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(shard_resp.status(), StatusCode::OK);
    assert!(!file_id.is_empty());

    // 3. Verify reconstruction returns metadata for the uploaded file
    let recon = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/reconstructions/{file_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recon.status(), StatusCode::OK);

    // 4. Also verify LFS upload works in same app
    let lfs_content = b"lfs-side-content";
    let oid = test_oid(lfs_content);

    let lfs_put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, lfs_content.len().to_string())
                .body(Body::from(lfs_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_put.status(), StatusCode::OK);

    // 5. Verify LFS GET works
    let lfs_get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(lfs_get).await, lfs_content);
}
