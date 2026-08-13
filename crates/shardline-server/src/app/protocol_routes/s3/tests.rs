//! In-process e2e tests for the S3 object-data path (Lane 3).
//!
//! Exercises the full Axum stack: `PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`
//! plus the `/{bucket}` stubs, with SigV4-style access keys (the token) and
//! bucket-scope binding.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::arithmetic_side_effects,
    clippy::option_if_let_else,
    clippy::unreachable,
    clippy::shadow_unrelated,
    clippy::let_underscore_must_use,
    clippy::string_add
)]

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    routing::{get, put},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    ServerConfig, ServerFrontend, ServerRole,
    app::{AppState, ProtocolMetrics},
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
};

use super::{
    bucket::{s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket},
    format_http_date,
    object::{s3_delete_object, s3_get_object, s3_head_object, s3_post_object, s3_put_object},
};

/// Test signing key matching the one used in e2e tests.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const OWNER: &str = "acme";
const NAME: &str = "models";
const BUCKET: &str = "acme.models";
const KEY: &str = "data/model.pt";

/// Builds an auth-enabled [`AppState`] whose token scope is `acme.models`.
async fn build_test_state() -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_frontends([ServerFrontend::S3])
    .expect("server frontends")
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key")
    .with_s3_max_part_bytes(NonZeroU64::new(1_048_576).unwrap())
    .expect("s3 max part bytes");

    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");

    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend,
        auth: Some(auth),
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    (state, tmp)
}

/// Mints a token scoped to `owner.name` with the test signing key.
fn mint_token(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

/// Builds a SigV4-style `Authorization` header whose access key **is** the
/// bearer token.
fn sigv4_auth(token: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=deadbeef"
    )
}

/// Builds the S3 router with the test state attached.
fn s3_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/{bucket}",
            put(s3_create_bucket)
                .get(s3_get_bucket)
                .head(s3_head_bucket)
                .delete(s3_delete_bucket),
        )
        .route(
            "/{bucket}/{*key}",
            get(s3_get_object)
                .head(s3_head_object)
                .put(s3_put_object)
                .post(s3_post_object)
                .delete(s3_delete_object),
        )
        .with_state(state)
}

/// Collects a response body into bytes.
async fn body_bytes(response: axum::http::Response<Body>) -> Vec<u8> {
    axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
}

/// A default write-token request builder for the `acme.models` bucket.
fn put_request(uri: String, body: Vec<u8>) -> axum::http::Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap()
}

// =========================================================================
// Object data path
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_get_head_delete_roundtrip() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));
    let content = b"s3-roundtrip-content";

    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), content.to_vec()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        etag.starts_with('"') && etag.ends_with('"'),
        "etag must be served quoted: {etag}"
    );

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, content);

    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert_eq!(
        head.headers()
            .get(header::CONTENT_LENGTH)
            .unwrap()
            .to_str()
            .unwrap(),
        content.len().to_string()
    );
    assert_eq!(
        head.headers().get(header::ETAG).unwrap().to_str().unwrap(),
        etag
    );
    assert!(head.headers().contains_key(header::LAST_MODIFIED));

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    let get_after = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_after.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_bearer_auth_is_accepted() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let token = mint_token(TokenScope::Write, OWNER, NAME);

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(b"bearer-authed".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_range_serves_206_with_content_range() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let content = b"0123456789abcdef";
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), content.to_vec()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=2-5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        get.headers()
            .get(header::CONTENT_RANGE)
            .unwrap()
            .to_str()
            .unwrap(),
        format!("bytes 2-5/{}", content.len())
    );
    assert_eq!(body_bytes(get).await, b"2345");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_open_ended_and_suffix_ranges() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let content = b"0123456789abcdef";
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), content.to_vec()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let open_ended = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=14-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(open_ended.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_bytes(open_ended).await, b"ef");

    let suffix = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(suffix.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_bytes(suffix).await, b"def");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_unsatisfiable_range_returns_416_invalid_range() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let content = b"0123456789abcdef";
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), content.to_vec()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=100-200")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let body = String::from_utf8(body_bytes(get).await).unwrap();
    assert!(body.contains("<Code>InvalidRange</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_missing_key_returns_404_no_such_key_xml() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/missing.pt"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(get).await).unwrap();
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
    assert!(body.contains("missing.pt"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_bucket_token_mismatch_returns_403_access_denied() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // Token scoped to a different owner than the bucket.
    let wrong_token = mint_token(TokenScope::Write, "other", NAME);
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, sigv4_auth(&wrong_token))
                .body(Body::from(b"nope".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_undecodable_bucket_returns_404_no_such_bucket() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/notabucket/some-key")
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::from(b"nope".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>NoSuchBucket</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_missing_auth_returns_403_access_denied() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::FORBIDDEN);
    let body = String::from_utf8(body_bytes(get).await).unwrap();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_overwrite_returns_new_content() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let first = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{KEY}"),
            b"first-version-bytes".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first_etag = first
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let second = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{KEY}"),
            b"second-version-content".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(second.status(), StatusCode::OK);
    let second_etag = second
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(
        first_etag, second_etag,
        "different content must produce a different etag"
    );

    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, b"second-version-content");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_oversized_body_returns_413_entity_too_large() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // SHARDLINE_S3_MAX_PART_BYTES is 1 MiB in the test config; one byte more
    // must be rejected with 413 before any ingestion starts.
    let oversized = vec![0xAB_u8; 1_048_577];
    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), oversized))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>EntityTooLarge</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_index_row_present_after_put_absent_after_delete() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{KEY}"),
            b"indexed-content".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let repo_scope = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repo_scope));
    let rows = state
        .backend
        .scan_s3_objects(&namespace, "", None, 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "one index row after put");
    assert_eq!(rows[0].object_key, KEY);
    assert_eq!(rows[0].size_bytes, "indexed-content".len() as u64);

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    let rows = state
        .backend
        .scan_s3_objects(&namespace, "", None, 100)
        .await
        .unwrap();
    assert!(rows.is_empty(), "index row removed after delete");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_metrics_counters_move() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let uploads_before = shardline_metrics::metrics().transfer.upload_requests.get();
    let downloads_before = shardline_metrics::metrics()
        .transfer
        .download_requests
        .get();
    let ranges_before = shardline_metrics::metrics().transfer.range_requests.get();

    let put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{KEY}"),
            b"metrics-content".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);

    let uploads_after = shardline_metrics::metrics().transfer.upload_requests.get();
    let downloads_after = shardline_metrics::metrics()
        .transfer
        .download_requests
        .get();
    let ranges_after = shardline_metrics::metrics().transfer.range_requests.get();
    assert!(
        uploads_after > uploads_before,
        "s3 upload counter must move ({uploads_before} -> {uploads_after})"
    );
    assert!(
        downloads_after > downloads_before,
        "s3 download counter must move ({downloads_before} -> {downloads_after})"
    );
    assert!(
        ranges_after > ranges_before,
        "s3 range counter must move ({ranges_before} -> {ranges_after})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_object_is_idempotent() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    // Deleting a key that never existed still returns 204.
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}/never-existed.pt"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_post_object_returns_501_not_implemented() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}/{KEY}?uploads"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::NOT_IMPLEMENTED);
    let body = String::from_utf8(body_bytes(post).await).unwrap();
    assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");
}

// =========================================================================
// Bucket stubs
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_bucket_stubs_create_head_location() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let write_auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));
    let read_auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    // CreateBucket → 200 no-op.
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}"))
                .header(header::AUTHORIZATION, &write_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);

    // HeadBucket → 200.
    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/{BUCKET}"))
                .header(header::AUTHORIZATION, &read_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);

    // GetBucketLocation → XML stub.
    let location = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}?location"))
                .header(header::AUTHORIZATION, &read_auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(location.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(location).await).unwrap();
    assert!(
        body.contains("<LocationConstraint") && body.contains("us-east-1"),
        "{body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_head_bucket_rejects_mismatched_token() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let head = app
        .clone()
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/{BUCKET}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, "other", NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_returns_501_not_implemented_xml() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}?list-type=2"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::NOT_IMPLEMENTED);
    let body = String::from_utf8(body_bytes(list).await).unwrap();
    assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_bucket_returns_501_not_implemented() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NOT_IMPLEMENTED);
}

// =========================================================================
// Unit helpers
// =========================================================================

#[test]
fn format_http_date_epoch_is_imf_fixdate() {
    assert_eq!(format_http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
}

#[test]
fn format_http_date_known_timestamp() {
    // 1785110400 = 2026-07-27T00:00:00Z.
    assert_eq!(
        format_http_date(1_785_110_400),
        "Mon, 27 Jul 2026 00:00:00 GMT"
    );
}
