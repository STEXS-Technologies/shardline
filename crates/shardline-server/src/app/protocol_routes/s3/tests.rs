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
async fn s3_post_without_sub_resource_returns_501_not_implemented() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // POST without `?uploads` or `?uploadId` is PostObject (form upload), which
    // is out of scope → 501.
    let post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}/{KEY}"))
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
async fn s3_list_objects_v1_returns_501_not_implemented_xml() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // `?list-type=1` (ListObjectsV1) is out of scope → 501.
    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}?list-type=1"))
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

// =========================================================================
// Multipart upload (Lane 4)
// =========================================================================

/// Creates a multipart upload and extracts the upload id from the XML.
async fn create_upload_id(app: &Router) -> String {
    let response = app
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
    assert_eq!(response.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(response).await).unwrap();
    extract_tag(&xml, "UploadId")
}

/// Extracts the text content of the first `<tag>…</tag>` occurrence.
fn extract_tag(xml: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open).unwrap() + open.len();
    let end = xml.find(&close).unwrap();
    xml[start..end].to_owned()
}

/// Uploads one part and returns the response.
async fn upload_part(
    app: &Router,
    upload_id: &str,
    part_number: u32,
    content: &[u8],
) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/{BUCKET}/{KEY}?partNumber={part_number}&uploadId={upload_id}"
                ))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Builds a minimal `CompleteMultipartUpload` request body.
fn complete_body(upload_id: &str, part_numbers: &[u32]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for part in part_numbers {
        xml.push_str(&format!(
            "  <Part><PartNumber>{part}</PartNumber><ETag>\"{upload_id}-{part}\"</ETag></Part>\n"
        ));
    }
    xml.push_str("</CompleteMultipartUpload>\n");
    xml
}

/// Sends a `CompleteMultipartUpload` request.
async fn complete_upload(
    app: &Router,
    upload_id: &str,
    body: String,
) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/{BUCKET}/{KEY}?uploadId={upload_id}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_roundtrip_assembles_parts_and_ranges() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    let upload_id = create_upload_id(&app).await;

    // Three parts of varying sizes (all under the 1 MiB test cap).
    let part1 = vec![0x11_u8; 256 * 1024];
    let part2 = vec![0x22_u8; 512 * 1024];
    let part3 = vec![0x33_u8; 64 * 1024];
    let assembled = [part1.clone(), part2.clone(), part3.clone()].concat();

    let put1 = upload_part(&app, &upload_id, 1, &part1).await;
    assert_eq!(put1.status(), StatusCode::OK);
    assert!(
        put1.headers().get(header::ETAG).is_some(),
        "UploadPart must return a per-part etag"
    );
    assert_eq!(
        upload_part(&app, &upload_id, 2, &part2).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        upload_part(&app, &upload_id, 3, &part3).await.status(),
        StatusCode::OK
    );

    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1, 2, 3])).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(xml.contains("<CompleteMultipartUploadResult"), "{xml}");
    assert!(xml.contains("<Bucket>acme.models</Bucket>"), "{xml}");
    assert!(xml.contains("<Key>data/model.pt</Key>"), "{xml}");
    assert!(xml.contains("<ETag>\""), "{xml}");

    // Full GET returns the assembled bytes.
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
    assert_eq!(body_bytes(get).await, assembled);

    // Range over a multipart object (spanning into part 2).
    let range = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::RANGE, "bytes=262144-262147")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(range.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(body_bytes(range).await, &part2[0..4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_etag_matches_single_put_of_same_bytes() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // Multipart upload of the assembled bytes.
    let upload_id = create_upload_id(&app).await;
    let part1 = b"multipart-identity-".to_vec();
    let part2 = b"part-two".to_vec();
    let assembled = [part1.clone(), part2.clone()].concat();
    assert_eq!(
        upload_part(&app, &upload_id, 1, &part1).await.status(),
        StatusCode::OK
    );
    assert_eq!(
        upload_part(&app, &upload_id, 2, &part2).await.status(),
        StatusCode::OK
    );
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1, 2])).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(complete).await).unwrap();
    let multipart_etag = extract_tag(&xml, "ETag");

    // Single PUT of the same bytes on a different key.
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/single.pt"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(assembled))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let single_etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    assert_eq!(
        multipart_etag, single_etag,
        "multipart and single-put of the same bytes must share the BLAKE3 etag"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_with_missing_part_returns_invalid_part() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    assert_eq!(
        upload_part(&app, &upload_id, 1, b"one").await.status(),
        StatusCode::OK
    );
    // Part 2 is never uploaded.
    assert_eq!(
        upload_part(&app, &upload_id, 3, b"three").await.status(),
        StatusCode::OK
    );

    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1, 3])).await;
    assert_eq!(complete.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(body.contains("<Code>InvalidPart</Code>"), "{body}");

    // The session is still alive: uploading the missing part and retrying works.
    assert_eq!(
        upload_part(&app, &upload_id, 2, b"two").await.status(),
        StatusCode::OK
    );
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1, 2, 3])).await;
    assert_eq!(complete.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_unknown_upload_id_returns_no_such_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = "00".repeat(16); // valid hex, never created
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(body.contains("<Code>NoSuchUpload</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_abort_multipart_leaves_no_object_and_no_index_row() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    let upload_id = create_upload_id(&app).await;
    assert_eq!(
        upload_part(&app, &upload_id, 1, b"partial").await.status(),
        StatusCode::OK
    );

    let abort = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}/{KEY}?uploadId={upload_id}"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);

    // No object…
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
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    // …and no index row.
    let repo_scope = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repo_scope));
    let rows = state
        .backend
        .scan_s3_objects(&namespace, "", None, 100)
        .await
        .unwrap();
    assert!(rows.is_empty(), "no index row after abort");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_upload_part_oversized_returns_413_entity_too_large() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    let oversized = vec![0xAB_u8; 1_048_577]; // 1 MiB + 1
    let put = upload_part(&app, &upload_id, 1, &oversized).await;
    assert_eq!(put.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>EntityTooLarge</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_upload_part_out_of_range_returns_400() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    let zero = upload_part(&app, &upload_id, 0, b"x").await;
    assert_eq!(zero.status(), StatusCode::BAD_REQUEST);
    let over = upload_part(&app, &upload_id, 10_001, b"x").await;
    assert_eq!(over.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_session_ttl_eviction_and_handler_rejection() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    async fn backdate_session(root: &std::path::Path, upload_id: &str, ttl: NonZeroU64) {
        let metadata_path = shardline_s3_adapter::session_metadata_path(root, upload_id).unwrap();
        let mut session = shardline_s3_adapter::read_session(root, upload_id, ttl)
            .await
            .unwrap();
        session.last_touched_unix_seconds = 1;
        let json = serde_json::to_vec(&session).unwrap();
        tokio::fs::write(&metadata_path, json).await.unwrap();
    }

    // Startup sweep removes a backdated (expired) session.
    let upload_id = create_upload_id(&app).await;
    backdate_session(root, &upload_id, ttl).await;
    let removed = shardline_s3_adapter::sweep_expired_sessions(root, ttl)
        .await
        .unwrap();
    assert_eq!(removed, 1);

    // A backdated session is rejected by the handlers as NoSuchUpload.
    let upload_id = create_upload_id(&app).await;
    backdate_session(root, &upload_id, ttl).await;
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(body.contains("<Code>NoSuchUpload</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_create_multipart_upload_returns_initiate_xml() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let create = app
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
    assert_eq!(create.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(create).await).unwrap();
    assert!(xml.contains("<InitiateMultipartUploadResult"), "{xml}");
    assert!(xml.contains("<Bucket>acme.models</Bucket>"), "{xml}");
    assert!(xml.contains("<Key>data/model.pt</Key>"), "{xml}");
    assert!(xml.contains("<UploadId>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_parts_and_list_multipart_uploads_return_501() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    let list_parts = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}?uploadId=abc"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_parts.status(), StatusCode::NOT_IMPLEMENTED);
    let body = String::from_utf8(body_bytes(list_parts).await).unwrap();
    assert!(body.contains("<Code>NotImplemented</Code>"), "{body}");

    let list_uploads = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}?uploads"))
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list_uploads.status(), StatusCode::NOT_IMPLEMENTED);
}

// =========================================================================
// ListObjectsV2 (Lane 5)
// =========================================================================

/// Sends a `ListObjectsV2` request and returns the XML body.
async fn list_objects(app: &Router, query: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}?{query}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "list {query:?}");
    String::from_utf8(body_bytes(response).await).unwrap()
}

/// PUTs one object under the bucket.
async fn put_object(app: &Router, key: &str, content: &[u8]) {
    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{key}"), content.to_vec()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK, "put {key}");
}

/// Counts occurrences of a substring (used for `<Contents>`/`<CommonPrefixes>`).
fn count_occurrences(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_prefix_shape_duckdb_glob() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "dir/a.txt", b"a").await;
    put_object(&app, "dir/sub/b.txt", b"b").await;
    put_object(&app, "other.txt", b"c").await;

    // `prefix=dir/` returns only keys under dir/.
    let xml = list_objects(&app, "list-type=2&prefix=dir%2F").await;
    assert!(xml.contains("<Key>dir/a.txt</Key>"), "{xml}");
    assert!(xml.contains("<Key>dir/sub/b.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>other.txt</Key>"), "{xml}");
    assert!(!xml.contains("<CommonPrefixes>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_delimiter_shape_s3a() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "a.txt", b"1").await;
    put_object(&app, "dir/b.txt", b"2").await;
    put_object(&app, "dir/sub/c.txt", b"3").await;
    put_object(&app, "zz.txt", b"4").await;

    // `prefix=` + `delimiter=/` → top-level Contents + CommonPrefixes; nested
    // keys collapse into the `dir/` rollup.
    let xml = list_objects(&app, "list-type=2&delimiter=%2F").await;
    assert!(xml.contains("<Key>a.txt</Key>"), "{xml}");
    assert!(xml.contains("<Key>zz.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>dir/b.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>dir/sub/c.txt</Key>"), "{xml}");
    assert!(
        xml.contains("<CommonPrefixes>") && xml.contains("<Prefix>dir/</Prefix>"),
        "{xml}"
    );
    assert_eq!(count_occurrences(&xml, "<Contents>"), 2);
    assert_eq!(count_occurrences(&xml, "<CommonPrefixes>"), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_pagination_with_continuation_token() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "a.txt", b"1").await;
    put_object(&app, "b.txt", b"2").await;
    put_object(&app, "c.txt", b"3").await;

    let first = list_objects(&app, "list-type=2&max-keys=1").await;
    assert!(first.contains("<Key>a.txt</Key>"), "{first}");
    assert!(first.contains("<IsTruncated>true</IsTruncated>"), "{first}");
    assert!(first.contains("<NextContinuationToken>"), "{first}");
    let token1 = extract_tag(&first, "NextContinuationToken");

    let second = list_objects(
        &app,
        &format!("list-type=2&max-keys=1&continuation-token={token1}"),
    )
    .await;
    assert!(second.contains("<Key>b.txt</Key>"), "{second}");
    assert!(
        second.contains("<IsTruncated>true</IsTruncated>"),
        "{second}"
    );
    let token2 = extract_tag(&second, "NextContinuationToken");

    let third = list_objects(
        &app,
        &format!("list-type=2&max-keys=1&continuation-token={token2}"),
    )
    .await;
    assert!(third.contains("<Key>c.txt</Key>"), "{third}");
    assert!(
        third.contains("<IsTruncated>false</IsTruncated>"),
        "{third}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_size_and_etag_match_index() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let content = b"list-me-please";
    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/size.pt"), content.to_vec()))
        .await
        .unwrap();
    let put_etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let xml = list_objects(&app, "list-type=2&prefix=size.pt").await;
    assert!(xml.contains("<Key>size.pt</Key>"), "{xml}");
    assert!(
        xml.contains(&format!("<Size>{}</Size>", content.len())),
        "{xml}"
    );
    assert!(xml.contains(&format!("<ETag>{put_etag}</ETag>")), "{xml}");
    assert!(xml.contains("<LastModified>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_empty_bucket() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let xml = list_objects(&app, "list-type=2").await;
    assert!(xml.contains("<ListBucketResult"), "{xml}");
    assert_eq!(count_occurrences(&xml, "<Contents>"), 0);
    assert_eq!(count_occurrences(&xml, "<CommonPrefixes>"), 0);
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
    assert!(!xml.contains("<NextContinuationToken>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_key_equal_to_prefix_is_contents() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "dir", b"object").await;
    put_object(&app, "dir/a.txt", b"1").await;

    // prefix=dir lists the object `dir` itself plus dir/a.txt.
    let xml = list_objects(&app, "list-type=2&prefix=dir").await;
    assert!(xml.contains("<Key>dir</Key>"), "{xml}");
    assert!(xml.contains("<Key>dir/a.txt</Key>"), "{xml}");

    // prefix=dir/ lists dir/ contents; `dir/` as a key would be a Contents row.
    let xml = list_objects(&app, "list-type=2&prefix=dir%2F").await;
    assert!(xml.contains("<Key>dir/a.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>dir</Key>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_delimiter_at_prefix_boundary() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "dir/a.txt", b"1").await;
    put_object(&app, "dirx/y.txt", b"2").await;

    // prefix=dir + delimiter=/ → `dir/a.txt` groups into CommonPrefixes `dir/`
    // (the delimiter sits exactly at the prefix boundary); `dirx/` groups too.
    let xml = list_objects(&app, "list-type=2&prefix=dir&delimiter=%2F").await;
    assert!(!xml.contains("<Key>dir/a.txt</Key>"), "{xml}");
    assert!(xml.contains("<Prefix>dir/</Prefix>"), "{xml}");
    assert!(xml.contains("<Prefix>dirx/</Prefix>"), "{xml}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_start_after_skips() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    put_object(&app, "a.txt", b"1").await;
    put_object(&app, "b.txt", b"2").await;
    put_object(&app, "c.txt", b"3").await;

    let xml = list_objects(&app, "list-type=2&start-after=b.txt").await;
    assert!(!xml.contains("<Key>a.txt</Key>"), "{xml}");
    assert!(!xml.contains("<Key>b.txt</Key>"), "{xml}");
    assert!(xml.contains("<Key>c.txt</Key>"), "{xml}");
}
