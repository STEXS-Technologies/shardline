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
    bucket::{
        s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket, s3_list_buckets,
        s3_post_bucket,
    },
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
    build_test_state_with_options(
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(1 << 40).unwrap(),
    )
    .await
}

/// Like [`build_test_state`] but with an explicit S3 minimum part size and
/// session byte quota (used by the quota/min-size hardening tests).
async fn build_test_state_with_options(
    min_part_bytes: NonZeroU64,
    session_max_bytes: NonZeroU64,
) -> (Arc<AppState>, TempDir) {
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
    .expect("s3 max part bytes")
    .with_s3_min_part_bytes(min_part_bytes)
    .expect("s3 min part bytes")
    .with_s3_upload_session_max_bytes(session_max_bytes)
    .expect("s3 session max bytes");

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

/// Like [`build_test_state_with_options`] but with an explicit global
/// active-part-file cap (used by the F-19 part-file-cap hardening test).
async fn build_test_state_with_s3_part_file_cap(
    max_active_part_files: NonZeroUsize,
) -> (Arc<AppState>, TempDir) {
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
    .with_s3_min_part_bytes(NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes")
    .with_s3_upload_session_max_bytes(NonZeroU64::new(1 << 40).unwrap())
    .expect("s3 session max bytes")
    .with_s3_upload_max_active_part_files(max_active_part_files)
    .expect("s3 max active part files");

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

/// Like [`build_test_state_with_options`] but with explicit S3 minimum AND
/// maximum part sizes (used by the Complete-time min-size tests, whose
/// conforming parts must exceed the 5 MiB minimum while staying under the
/// max part cap).
async fn build_test_state_with_part_sizes(
    min_part_bytes: NonZeroU64,
    max_part_bytes: NonZeroU64,
    session_max_bytes: NonZeroU64,
) -> (Arc<AppState>, TempDir) {
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
    .with_s3_max_part_bytes(max_part_bytes)
    .expect("s3 max part bytes")
    .with_s3_min_part_bytes(min_part_bytes)
    .expect("s3 min part bytes")
    .with_s3_upload_session_max_bytes(session_max_bytes)
    .expect("s3 session max bytes");

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
        .route("/", get(s3_list_buckets))
        .route(
            "/{bucket}",
            put(s3_create_bucket)
                .get(s3_get_bucket)
                .head(s3_head_bucket)
                .delete(s3_delete_bucket)
                .post(s3_post_bucket),
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
        // Expiry is anchored to session creation (keep-alive parts cannot
        // extend the lifetime), so backdating created_at makes the session
        // expire regardless of last_touched.
        session.created_at_unix_seconds = 1;
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

// =========================================================================
// Hardening: multipart lifecycle / auth / key edge cases
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_abort_then_complete_returns_no_such_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    assert_eq!(
        upload_part(&app, &upload_id, 1, b"x").await.status(),
        StatusCode::OK
    );

    // Abort deletes the session (and its part files).
    let abort = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/{BUCKET}/{KEY}?uploadId={upload_id}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(abort.status(), StatusCode::NO_CONTENT);

    // Completing a dead session → NoSuchUpload.
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(body.contains("<Code>NoSuchUpload</Code>"), "{body}");

    // Uploading a part to a dead session → NoSuchUpload.
    let part = upload_part(&app, &upload_id, 2, b"y").await;
    assert_eq!(part.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_concurrent_upload_part_last_writer_wins() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    // Two concurrent UploadPart calls for the SAME part number: the per-session
    // write lock serializes them, so the part file is one of the two bodies.
    let (first, second) = tokio::join!(
        async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"))
                        .header(
                            header::AUTHORIZATION,
                            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                        )
                        .body(Body::from(b"first-writer".to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap()
        },
        async {
            app.clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(format!("/{BUCKET}/{KEY}?partNumber=1&uploadId={upload_id}"))
                        .header(
                            header::AUTHORIZATION,
                            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                        )
                        .body(Body::from(b"second-writer".to_vec()))
                        .unwrap(),
                )
                .await
                .unwrap()
        },
    );
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);

    // Completion succeeds and the assembled object is exactly one of the two
    // bodies (never an interleaved mix).
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::OK);
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert!(
        body == b"first-writer" || body == b"second-writer",
        "part must be exactly one of the two writers, got {body:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sigv4_with_malformed_credential_scope_still_authenticates() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // The access key is the first Credential path segment; malformed date
    // segments, extra commas, and extra slashes are ignored (the signature is
    // never verified).
    let token = mint_token(TokenScope::Write, OWNER, NAME);
    for malformed in [
        format!("AWS4-HMAC-SHA256 Credential={token}/not-a-date/,,/s3/aws4_request"),
        format!("AWS4-HMAC-SHA256 Credential={token}/20260813//s3///aws4_request"),
        format!("AWS4-HMAC-SHA256 Credential={token},,,"),
    ] {
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/{BUCKET}/{KEY}"))
                    .header(header::AUTHORIZATION, &malformed)
                    .body(Body::from(b"malformed-sig".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK, "header {malformed:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_bearer_with_trailing_space_is_rejected() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // The adapter returns the raw value; the server-side token verifier
    // rejects tokens containing whitespace.
    let token = mint_token(TokenScope::Write, OWNER, NAME);
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, format!("Bearer {token} "))
                .body(Body::from(b"x".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_query_credentials_are_ignored_header_wins() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let header_token = mint_token(TokenScope::Write, OWNER, NAME);
    let query_token = mint_token(TokenScope::Write, "other", NAME);
    let query = format!("X-Amz-Credential={query_token}/20260813/us-east-1/s3/aws4_request");

    // Conflicting query credential + valid header → the header wins.
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}?{query}"))
                .header(header::AUTHORIZATION, format!("Bearer {header_token}"))
                .body(Body::from(b"header-wins".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK, "header credential must win");

    // Query credential only → denied (the handlers read the header only).
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}?{query}"))
                .body(Body::from(b"no-header".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_unsafe_key_returns_no_such_key() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // Path-traversal and absolute keys are rejected at key construction.
    for key in ["../evil", "%2e%2e%2fevil", "/abs", "a/../evil"] {
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/{BUCKET}/{key}"))
                    .header(
                        header::AUTHORIZATION,
                        sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                    )
                    .body(Body::from(b"x".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::NOT_FOUND, "key {key:?}");
        let body = String::from_utf8(body_bytes(put).await).unwrap();
        assert!(
            body.contains("<Code>NoSuchKey</Code>"),
            "key {key:?}: {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_unicode_key_roundtrip() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let key = "ünïcode/🦆.txt";
    let content = b"unicode-key-content";
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{key}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
                )
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{key}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, content);
}

// =========================================================================
// Audit fixes: atomic overwrite (F-s3-1), quotas/min-part (F-s3-2),
// listing params (F-s3-3), sweep/upload race (F-s3-4)
// =========================================================================

/// A chunked request body with no size hint (streams at runtime).
fn streamed_body(chunk_size: usize, chunk_count: usize) -> Body {
    use axum::body::Bytes;
    use futures_util::stream;
    let chunks: Vec<Result<Bytes, axum::Error>> = (0..chunk_count)
        .map(|_| Ok(Bytes::from(vec![0x42_u8; chunk_size])))
        .collect();
    Body::from_stream(stream::iter(chunks))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_failed_overwrite_keeps_old_object_and_index() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));

    // Seed the key with a small object.
    let old_content = b"old-object-content";
    let put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{KEY}"),
            old_content.to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let old_etag = put
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // A chunked overwrite whose body exceeds SHARDLINE_S3_MAX_PART_BYTES (1 MiB
    // in the test config) mid-stream must fail WITHOUT touching the old object.
    let oversized = streamed_body(600_000, 3); // 1.8 MiB total, no size hint
    let failed = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &auth)
                .body(oversized)
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        failed.status(),
        StatusCode::PAYLOAD_TOO_LARGE,
        "mid-stream oversize → 413"
    );

    // The old object is still readable and the index still points at it.
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
    assert_eq!(body_bytes(get).await, old_content);

    let repo_scope = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repo_scope));
    let rows = state
        .backend
        .scan_s3_objects(&namespace, "", None, 100)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        shardline_s3_adapter::etag_header(&rows[0].etag),
        old_etag,
        "index must still point at the old etag"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_overwrite_is_atomic_no_transient_404_for_readers() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let write_auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));
    let read_auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    let old_content = vec![0xAA_u8; 16];
    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), old_content.clone()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Overwrite with a large-enough body that the ingest takes a moment, while
    // a concurrent reader hammers the key. Every read must be 200 (old or new
    // bytes) — never a transient 404.
    let new_content = vec![0xBB_u8; 300_000];
    let (overwrite, reads) = tokio::join!(
        app.clone().oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/{BUCKET}/{KEY}"))
                .header(header::AUTHORIZATION, &write_auth)
                .body(Body::from(new_content.clone()))
                .unwrap(),
        ),
        async {
            let mut results = Vec::new();
            for _ in 0..12 {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .method("GET")
                            .uri(format!("/{BUCKET}/{KEY}"))
                            .header(header::AUTHORIZATION, &read_auth)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let body = body_bytes(response).await;
                results.push(body);
            }
            results
        },
    );
    assert_eq!(overwrite.unwrap().status(), StatusCode::OK);
    for body in reads {
        assert!(
            body == old_content || body == new_content,
            "concurrent read saw a torn/absent object: {} bytes",
            body.len()
        );
    }
}

/// Stress test for the overwrite race: N back-to-back overwrites alternating
/// two contents while M concurrent GETs hammer the key.
///
/// Every GET must return exactly one of the two contents — never a 404
/// (`NoSuchKey`), never a 500 (`StoredLengthMismatch` from resolving the
/// length from one record version and the stream from another), never a torn
/// body. The pre-fix code failed this within a handful of rounds; with the
/// length+stream resolved from one immutable record snapshot it is stable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_overwrite_stress_readers_never_see_torn_or_absent_object() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let write_auth = sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME));
    let read_auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));
    let contents = [vec![0xAA_u8; 16], vec![0xBB_u8; 300_000]];

    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/{KEY}"), contents[0].clone()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    for round in 0..20 {
        let content = contents[round % 2].clone();
        let (overwrite, reads) = tokio::join!(
            app.clone().oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/{BUCKET}/{KEY}"))
                    .header(header::AUTHORIZATION, &write_auth)
                    .body(Body::from(content))
                    .unwrap(),
            ),
            async {
                let mut tasks = Vec::new();
                for _ in 0..50 {
                    let app = app.clone();
                    let read_auth = read_auth.clone();
                    tasks.push(tokio::spawn(async move {
                        let response = app
                            .oneshot(
                                Request::builder()
                                    .method("GET")
                                    .uri(format!("/{BUCKET}/{KEY}"))
                                    .header(header::AUTHORIZATION, &read_auth)
                                    .body(Body::empty())
                                    .unwrap(),
                            )
                            .await
                            .unwrap();
                        let status = response.status();
                        let body = body_bytes(response).await;
                        (status, body)
                    }));
                }
                let mut results = Vec::with_capacity(tasks.len());
                for task in tasks {
                    results.push(task.await.unwrap());
                }
                results
            },
        );
        assert_eq!(
            overwrite.unwrap().status(),
            StatusCode::OK,
            "overwrite round {round}"
        );
        for (index, (status, body)) in reads.into_iter().enumerate() {
            assert_eq!(status, StatusCode::OK, "round {round} read {index}");
            assert!(
                body == contents[0] || body == contents[1],
                "round {round} read {index}: torn/absent read of {} bytes",
                body.len()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_non_final_small_part_returns_entity_too_small() {
    // F-19 rework: UploadPart accepts ANY body size (matching real S3, which
    // validates the 5 MiB minimum only at CompleteMultipartUpload). A small
    // NON-final part is accepted at UploadPart but rejected at Complete with
    // EntityTooSmall.
    let (state, _tmp) = build_test_state_with_options(
        NonZeroU64::new(5_242_880).unwrap(), // 5 MiB minimum
        NonZeroU64::new(1 << 40).unwrap(),
    )
    .await;
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    // Both parts are tiny (way below the 5 MiB minimum); UploadPart must
    // accept them (a real 10 MiB / 8 MiB + 2 MiB client shape).
    assert_eq!(
        upload_part(&app, &upload_id, 1, b"tiny-non-final")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        upload_part(&app, &upload_id, 2, b"final").await.status(),
        StatusCode::OK
    );

    // Part 1 is NOT the last part (part 2 is), so Complete rejects it.
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1, 2])).await;
    assert_eq!(complete.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body_bytes(complete).await).unwrap();
    assert!(body.contains("<Code>EntityTooSmall</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_complete_accepts_small_last_part() {
    // F-19 rework: the real-world multipart shape — e.g. a 10 MiB file with
    // 8 MiB parts: part 1 = 8 MiB, part 2 = 2 MiB (final) — must complete.
    // A small LAST part (the highest part number) is accepted at Complete,
    // whatever its number.
    let (state, _tmp) = build_test_state_with_part_sizes(
        NonZeroU64::new(5_242_880).unwrap(), // 5 MiB minimum
        NonZeroU64::new(1 << 30).unwrap(),   // 1 GiB max part
        NonZeroU64::new(1 << 40).unwrap(),
    )
    .await;
    let app = s3_router(state);
    let auth = sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME));

    let upload_id = create_upload_id(&app).await;
    let part1 = vec![0x11_u8; 5_242_880]; // ≥ 5 MiB minimum
    let part2 = vec![0x22_u8; 2 * 1024 * 1024]; // final part: small is fine
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

    // The assembled object is byte-identical to the parts.
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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_upload_part_file_cap_rejects_before_any_file_materializes() {
    // F-19: the global active-part-file cap bounds the part-FILE count
    // directly (UploadPart accepts any size, so the byte quotas cannot bound
    // it). A new part beyond the cap is rejected BEFORE its file is written —
    // no file materializes on disk — and the rejection is a clean 429.
    let (state, _tmp) = build_test_state_with_s3_part_file_cap(NonZeroUsize::new(2).unwrap()).await;
    let root = state.config.root_dir().to_path_buf();
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    for part in [1_u32, 2] {
        assert_eq!(
            upload_part(&app, &upload_id, part, b"in-cap-part")
                .await
                .status(),
            StatusCode::OK
        );
    }

    // The third part file would exceed the cap of 2.
    let over = upload_part(&app, &upload_id, 3, b"over-cap-part").await;
    assert_eq!(over.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = String::from_utf8(body_bytes(over).await).unwrap();
    assert!(body.contains("<Code>TooManyParts</Code>"), "{body}");

    // The rejected part never materialized a file on disk.
    assert!(
        !shardline_s3_adapter::part_file_path(&root, &upload_id, 3)
            .unwrap()
            .exists(),
        "an over-cap part must not leave a part file behind"
    );

    // The session stays usable: deleting one part's slot frees the cap, and
    // overwriting an existing part number does not consume a new slot.
    assert_eq!(
        upload_part(&app, &upload_id, 1, b"overwrite-ok")
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_upload_part_exceeding_session_quota_returns_error() {
    let (state, _tmp) = build_test_state_with_options(
        NonZeroU64::new(1).unwrap(),
        NonZeroU64::new(200).unwrap(), // 200-byte per-session quota
    )
    .await;
    let root = state.config.root_dir().to_path_buf();
    let app = s3_router(state);

    let upload_id = create_upload_id(&app).await;
    let part1 = vec![0x01_u8; 150];
    let part2 = vec![0x02_u8; 100];
    assert_eq!(
        upload_part(&app, &upload_id, 1, &part1).await.status(),
        StatusCode::OK
    );
    // 150 + 100 > 200 → quota exceeded.
    let over = upload_part(&app, &upload_id, 2, &part2).await;
    assert_eq!(over.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body_bytes(over).await).unwrap();
    assert!(body.contains("<Code>EntityTooLarge</Code>"), "{body}");

    // F-19: the quota is enforced BEFORE the write, so the rejected part's
    // file never materializes on disk.
    assert!(
        !shardline_s3_adapter::part_file_path(&root, &upload_id, 2)
            .unwrap()
            .exists(),
        "an over-quota part must not leave a part file behind"
    );

    // The rejected part must not have orphaned a part file: completing with
    // only part 1 works.
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_listing_max_keys_zero_and_multi_char_delimiter_are_400() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    for query in ["list-type=2&max-keys=0", "list-type=2&delimiter=%2F%2F"] {
        let list = app
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
        assert_eq!(list.status(), StatusCode::BAD_REQUEST, "query {query}");
        let body = String::from_utf8(body_bytes(list).await).unwrap();
        assert!(
            body.contains("<Code>InvalidArgument</Code>"),
            "query {query}: {body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_sweep_during_upload_part_is_safe() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state.clone());
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    let upload_id = create_upload_id(&app).await;

    // A sweep (which the store runs under the global session lock) racing an
    // UploadPart (which holds the same lock) cannot delete the session dir
    // mid-write or 500 the upload: they serialize.
    let (sweep, part) = tokio::join!(
        shardline_s3_adapter::sweep_expired_sessions(root, ttl),
        upload_part(&app, &upload_id, 1, b"sweep-race-part"),
    );
    sweep.unwrap();
    assert_eq!(
        part.status(),
        StatusCode::OK,
        "UploadPart must not 500 under a sweep"
    );

    // The session survives and completes normally.
    let complete = complete_upload(&app, &upload_id, complete_body(&upload_id, &[1])).await;
    assert_eq!(complete.status(), StatusCode::OK);
}

// =========================================================================
// DeleteObjects (POST /{bucket}?delete=)
// =========================================================================

/// Builds a `DeleteObjects` XML request body from the given keys.
fn delete_objects_body(keys: &[&str]) -> String {
    let mut xml = String::from("<?xml version=\"1.0\"?><Delete>");
    for key in keys {
        xml.push_str("<Object><Key>");
        xml.push_str(key);
        xml.push_str("</Key></Object>");
    }
    xml.push_str("</Delete>");
    xml
}

/// A write-token `POST /{bucket}?delete=` request with an XML body.
fn delete_objects_request(uri: String, body: String) -> axum::http::Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header(header::CONTENT_TYPE, "application/xml")
        .body(Body::from(body))
        .unwrap()
}

/// Seeds an object with the given key and asserts the PUT succeeded.
async fn seed_object(app: &Router, key: &str) {
    let put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{key}"),
            b"seed-content".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK, "seeding {key}");
}

/// GETs an object and returns its status (used to prove deletion).
async fn object_status(app: &Router, key: &str) -> StatusCode {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{key}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_objects_empty_keys_are_400_malformed_xml() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // Both a body with no keys at all and a body whose only `<Key>` is empty
    // must be `400 MalformedXML` (never a 500) per S3's published schema.
    for body in [
        "<Delete></Delete>",
        "<Delete><Object><Key></Key></Object></Delete>",
    ] {
        let post = app
            .clone()
            .oneshot(delete_objects_request(
                format!("/{BUCKET}?delete="),
                body.to_owned(),
            ))
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::BAD_REQUEST, "body {body}");
        let response_body = String::from_utf8(body_bytes(post).await).unwrap();
        assert!(
            response_body.contains("<Code>MalformedXML</Code>"),
            "body {body}: {response_body}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_objects_over_1000_keys_rejected_before_any_deletion() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // A sentinel that is NOT in the batch: it must survive, proving no
    // deletion happened when the request was rejected.
    seed_object(&app, "survivor.txt").await;

    let keys: Vec<String> = (0..=shardline_s3_adapter::MAX_S3_DELETE_KEYS)
        .map(|index| format!("key-{index:04}.txt"))
        .collect();
    let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    let post = app
        .clone()
        .oneshot(delete_objects_request(
            format!("/{BUCKET}?delete="),
            delete_objects_body(&key_refs),
        ))
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::BAD_REQUEST);
    let body = String::from_utf8(body_bytes(post).await).unwrap();
    assert!(body.contains("<Code>MalformedXML</Code>"), "{body}");

    // Nothing was deleted: the sentinel is still readable, and a batch key
    // that never existed is still absent (404, not 204).
    assert_eq!(object_status(&app, "survivor.txt").await, StatusCode::OK);
    assert_eq!(
        object_status(&app, "key-0000.txt").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_objects_duplicate_keys_collapse_to_one_delete() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    for key in ["dup/a.txt", "dup/b.txt", "dup/c.txt"] {
        seed_object(&app, key).await;
    }

    // Three distinct keys, five `<Object>` entries: the duplicates must
    // collapse into a single delete (and a single `<Deleted>` row) each.
    let post = app
        .clone()
        .oneshot(delete_objects_request(
            format!("/{BUCKET}?delete="),
            delete_objects_body(&[
                "dup/a.txt",
                "dup/a.txt",
                "dup/b.txt",
                "dup/a.txt",
                "dup/c.txt",
            ]),
        ))
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(post).await).unwrap();
    assert_eq!(body.matches("<Deleted>").count(), 3, "{body}");
    assert_eq!(body.matches("<Error>").count(), 0, "{body}");

    // All three objects are actually gone.
    for key in ["dup/a.txt", "dup/b.txt", "dup/c.txt"] {
        assert_eq!(
            object_status(&app, key).await,
            StatusCode::NOT_FOUND,
            "key {key} must be deleted"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_objects_cap_enforced_on_distinct_keys_during_dedupe() {
    // F-23: the MAX_S3_DELETE_KEYS cap is enforced DURING the dedupe loop, on
    // the DISTINCT key count — duplicate `<Key>` entries never count toward
    // the cap, and the handler never materializes a key list beyond it. A
    // body with exactly 1000 distinct keys plus many duplicates of them is
    // accepted (the duplicates collapse and the request deletes 1000 keys).
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // 1000 distinct keys, each repeated twice (2000 `<Object>` entries).
    let distinct: Vec<String> = (0..shardline_s3_adapter::MAX_S3_DELETE_KEYS)
        .map(|index| format!("cap-dedupe/key-{index:04}.txt"))
        .collect();
    let mut body = String::from("<?xml version=\"1.0\"?><Delete>");
    for key in &distinct {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("</Delete>");
    let post = app
        .clone()
        .oneshot(delete_objects_request(format!("/{BUCKET}?delete="), body))
        .await
        .unwrap();
    assert_eq!(
        post.status(),
        StatusCode::OK,
        "duplicates must collapse and never trip the distinct-key cap"
    );
    let response_body = String::from_utf8(body_bytes(post).await).unwrap();
    assert_eq!(
        response_body.matches("<Deleted>").count(),
        shardline_s3_adapter::MAX_S3_DELETE_KEYS,
        "exactly the 1000 distinct keys are deleted"
    );

    // 1000 distinct keys + ONE extra distinct key → rejected (the 1001st
    // distinct key trips the cap as soon as it is seen, never beyond it).
    let mut body = String::from("<?xml version=\"1.0\"?><Delete>");
    for key in &distinct {
        body.push_str(&format!("<Object><Key>{key}</Key></Object>"));
    }
    body.push_str("<Object><Key>cap-dedupe/overflow.txt</Key></Object>");
    body.push_str("</Delete>");
    let post = app
        .clone()
        .oneshot(delete_objects_request(format!("/{BUCKET}?delete="), body))
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::BAD_REQUEST);
    let response_body = String::from_utf8(body_bytes(post).await).unwrap();
    assert!(
        response_body.contains("<Code>MalformedXML</Code>"),
        "{response_body}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_objects_invalid_keys_yield_per_key_errors_and_valid_keys_deleted() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    for key in ["good-a.txt", "good-b.txt"] {
        seed_object(&app, key).await;
    }

    // A batch interleaving valid and invalid keys: the valid keys must be
    // deleted, the invalid ones reported as per-key `<Error>` rows, and the
    // request must return 200 (never abort mid-batch after mutating state).
    let post = app
        .clone()
        .oneshot(delete_objects_request(
            format!("/{BUCKET}?delete="),
            delete_objects_body(&[
                "good-a.txt",
                "/leading-slash.txt",
                "good-b.txt",
                "../escape.txt",
            ]),
        ))
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(post).await).unwrap();

    assert_eq!(body.matches("<Deleted>").count(), 2, "{body}");
    assert_eq!(body.matches("<Error>").count(), 2, "{body}");
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
    assert!(
        body.contains("<Key>/leading-slash.txt</Key>"),
        "leading-slash key must have an Error row: {body}"
    );
    assert!(
        body.contains("<Key>../escape.txt</Key>"),
        "traversal key must have an Error row: {body}"
    );

    // Rows are emitted in request order: Deleted(a), Error(slash), Deleted(b),
    // Error(traversal).
    let deleted_a = body.find("<Deleted><Key>good-a.txt</Key>").unwrap();
    let error_slash = body.find("<Error><Key>/leading-slash.txt</Key>").unwrap();
    let deleted_b = body.find("<Deleted><Key>good-b.txt</Key>").unwrap();
    let error_traversal = body.find("<Error><Key>../escape.txt</Key>").unwrap();
    assert!(
        deleted_a < error_slash && error_slash < deleted_b && deleted_b < error_traversal,
        "rows must be in request order: {body}"
    );

    // The valid keys are gone; the invalid keys were never stored (404).
    assert_eq!(
        object_status(&app, "good-a.txt").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        object_status(&app, "good-b.txt").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        object_status(&app, "/leading-slash.txt").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        object_status(&app, "../escape.txt").await,
        StatusCode::NOT_FOUND
    );
}

// =========================================================================
// ListBuckets (GET /)
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_buckets_returns_the_callers_bucket() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(list).await).unwrap();
    assert!(body.contains("<ListAllMyBucketsResult"), "{body}");
    assert!(
        body.contains("<Bucket><Name>acme.models</Name></Bucket>"),
        "ListBuckets must list the caller's single bucket: {body}"
    );
    // Exactly one bucket (the token is bound to exactly one scope).
    assert_eq!(body.matches("<Bucket>").count(), 1, "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_buckets_missing_auth_returns_403() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::FORBIDDEN);
    let body = String::from_utf8(body_bytes(list).await).unwrap();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
}

// =========================================================================
// Conditional requests (If-Match / If-None-Match)
// =========================================================================

/// GETs an object and returns its quoted ETag (asserting the read succeeded).
async fn get_etag(app: &Router, key: &str) -> String {
    let response = get_etag_request(app, key).await;
    assert_eq!(response.status(), StatusCode::OK);
    response
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned()
}

/// GETs an object and returns the full response (body readable).
async fn get_etag_request(app: &Router, key: &str) -> axum::http::Response<Body> {
    app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/{BUCKET}/{key}"))
                .header(
                    header::AUTHORIZATION,
                    sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap()
}

/// Builds a method request with an optional conditional header.
fn conditional_request(
    method: &str,
    uri: String,
    auth: &str,
    condition: Option<(&str, &str)>,
) -> axum::http::Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, auth);
    if let Some((name, value)) = condition {
        builder = builder.header(name, value);
    }
    builder.body(Body::empty()).unwrap()
}

/// PUTs a body with an optional conditional header.
fn conditional_put_request(
    uri: String,
    body: Vec<u8>,
    condition: Option<(&str, &str)>,
) -> axum::http::Request<Body> {
    let mut builder = Request::builder()
        .method("PUT")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header(header::CONTENT_TYPE, "application/octet-stream");
    if let Some((name, value)) = condition {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(body)).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_if_match_ok_when_etag_matches() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let content = b"if-match-content".to_vec();

    seed_object(&app, "cond.txt").await;
    let put = app
        .clone()
        .oneshot(put_request(format!("/{BUCKET}/cond.txt"), content.clone()))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let etag = get_etag(&app, "cond.txt").await;

    let get = app
        .clone()
        .oneshot(conditional_request(
            "GET",
            format!("/{BUCKET}/cond.txt"),
            &sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
            Some(("if-match", &etag)),
        ))
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_get_if_match_mismatch_returns_412() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "cond.txt").await;

    let get = app
        .clone()
        .oneshot(conditional_request(
            "GET",
            format!("/{BUCKET}/cond.txt"),
            &sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
            Some(("if-match", "\"deadbeef\"")),
        ))
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PRECONDITION_FAILED);
    let body = String::from_utf8(body_bytes(get).await).unwrap();
    assert!(body.contains("<Code>PreconditionFailed</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_if_none_match_on_missing_proceeds() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // Create-if-absent: a fresh key with If-None-Match must succeed.
    let put = app
        .clone()
        .oneshot(conditional_put_request(
            format!("/{BUCKET}/fresh.txt"),
            b"created".to_vec(),
            Some(("if-none-match", "\"anything\"")),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(get_etag(&app, "fresh.txt").await.len(), 34); // quoted 32-hex MD5
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_if_none_match_on_existing_returns_412() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "cond.txt").await;
    let etag = get_etag(&app, "cond.txt").await;

    let put = app
        .clone()
        .oneshot(conditional_put_request(
            format!("/{BUCKET}/cond.txt"),
            b"should-not-land".to_vec(),
            Some(("if-none-match", &etag)),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::PRECONDITION_FAILED);
    // The write did not happen: the stored content is unchanged.
    assert_eq!(
        body_bytes(get_etag_request(&app, "cond.txt").await).await,
        b"seed-content".to_vec()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_if_none_match_on_absent_key_with_prefix_sibling_proceeds() {
    // F-33: the exact key `a` is absent but a sibling key `a/b` has it as a
    // string prefix. The entry lookup must resolve the exact row only — NOT
    // the lexicographically-smallest sibling — so the create-if-absent PUT
    // (If-None-Match: *) proceeds instead of a spurious 412 against the
    // sibling's ETag.
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    seed_object(&app, "a/b").await;
    assert_eq!(
        object_status(&app, "a").await,
        StatusCode::NOT_FOUND,
        "the exact key must be absent despite the prefix sibling"
    );

    let put = app
        .clone()
        .oneshot(conditional_put_request(
            format!("/{BUCKET}/a"),
            b"created".to_vec(),
            Some(("if-none-match", "*")),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_eq!(get_etag(&app, "a").await.len(), 34); // quoted 32-hex MD5
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_if_match_mismatch_checked_before_write() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "cond.txt").await;

    let put = app
        .clone()
        .oneshot(conditional_put_request(
            format!("/{BUCKET}/cond.txt"),
            b"should-not-land".to_vec(),
            Some(("if-match", "\"deadbeef\"")),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::PRECONDITION_FAILED);
    // The write was rejected BEFORE mutating anything.
    assert_eq!(
        body_bytes(get_etag_request(&app, "cond.txt").await).await,
        b"seed-content".to_vec()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_if_match_on_missing_returns_404() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    // RFC 9110: a missing resource fails If-Match.
    let put = app
        .clone()
        .oneshot(conditional_put_request(
            format!("/{BUCKET}/never-existed.txt"),
            b"x".to_vec(),
            Some(("if-match", "\"deadbeef\"")),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(put).await).unwrap();
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
    // Nothing was created.
    assert_eq!(
        object_status(&app, "never-existed.txt").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_head_if_none_match_star_on_existing_returns_412() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "cond.txt").await;

    let head = app
        .clone()
        .oneshot(conditional_request(
            "HEAD",
            format!("/{BUCKET}/cond.txt"),
            &sigv4_auth(&mint_token(TokenScope::Read, OWNER, NAME)),
            Some(("if-none-match", "*")),
        ))
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::PRECONDITION_FAILED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_if_match_mismatch_returns_412_and_object_survives() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "cond.txt").await;

    let delete = app
        .clone()
        .oneshot(conditional_request(
            "DELETE",
            format!("/{BUCKET}/cond.txt"),
            &sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
            Some(("if-match", "\"deadbeef\"")),
        ))
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::PRECONDITION_FAILED);
    assert_eq!(object_status(&app, "cond.txt").await, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_delete_if_match_on_missing_returns_404() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let delete = app
        .clone()
        .oneshot(conditional_request(
            "DELETE",
            format!("/{BUCKET}/never-existed.txt"),
            &sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
            Some(("if-match", "\"deadbeef\"")),
        ))
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NOT_FOUND);
}

// =========================================================================
// CopyObject (PUT with x-amz-copy-source)
// =========================================================================

/// A write-token `PUT` request carrying `x-amz-copy-source`.
fn copy_request(uri: String, copy_source: &str) -> axum::http::Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header("x-amz-copy-source", copy_source)
        .body(Body::empty())
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_copy_object_same_bucket_roundtrip() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    let source_content = b"copyable-content".to_vec();

    // Seed the source.
    let put = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/src/file.bin"),
            source_content.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    let source_etag = get_etag(&app, "src/file.bin").await;

    // Copy to a destination key.
    let copy = app
        .clone()
        .oneshot(copy_request(
            format!("/{BUCKET}/dst/file.bin"),
            &format!("/{BUCKET}/src/file.bin"),
        ))
        .await
        .unwrap();
    assert_eq!(copy.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(copy).await).unwrap();
    assert!(body.contains("<CopyObjectResult"), "{body}");
    assert!(
        body.contains(&format!("<ETag>{source_etag}</ETag>")),
        "identical content must yield the identical ETag: {body}"
    );
    assert!(body.contains("<LastModified>"), "{body}");

    // The destination holds the source's bytes with the same ETag.
    let dest_get = get_etag_request(&app, "dst/file.bin").await;
    assert_eq!(dest_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(dest_get).await, source_content);
    let dest_etag = get_etag(&app, "dst/file.bin").await;
    assert_eq!(dest_etag, source_etag);
    // The source is untouched.
    assert_eq!(object_status(&app, "src/file.bin").await, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_copy_object_missing_source_returns_no_such_key() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);

    let copy = app
        .clone()
        .oneshot(copy_request(
            format!("/{BUCKET}/dst/missing.bin"),
            "/acme.models/never-wrote.bin",
        ))
        .await
        .unwrap();
    assert_eq!(copy.status(), StatusCode::NOT_FOUND);
    let body = String::from_utf8(body_bytes(copy).await).unwrap();
    assert!(body.contains("<Code>NoSuchKey</Code>"), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_copy_object_cross_bucket_returns_access_denied() {
    let (state, _tmp) = build_test_state().await;
    let app = s3_router(state);
    seed_object(&app, "src.txt").await;

    // The source bucket does not match the caller's bound bucket.
    let copy = app
        .clone()
        .oneshot(copy_request(
            format!("/{BUCKET}/dst.txt"),
            "/other.owner/secret.bin",
        ))
        .await
        .unwrap();
    assert_eq!(copy.status(), StatusCode::FORBIDDEN);
    let body = String::from_utf8(body_bytes(copy).await).unwrap();
    assert!(body.contains("<Code>AccessDenied</Code>"), "{body}");
    // Nothing was written to the destination.
    assert_eq!(object_status(&app, "dst.txt").await, StatusCode::NOT_FOUND);
}
