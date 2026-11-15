use std::{
    fs,
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    sync::Arc,
};

use axum::{
    Router,
    body::Body,
    http::{
        Request, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_RANGE},
    },
    routing::{get, post},
};
use proptest::prelude::*;
use serde_json::{Value, json};
use sha2::Digest;
use shardline_index::{ResumablePartRange, ResumableSessionPart};
use shardline_protocol::TokenScope;
use shardline_server_core::AuthProvider;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    ServerBackend, ServerConfig, ServerError, ServerFrontend, app::AppState, lfs_object_key,
};
use shardline_server_core::AuthorizedRepository;

use super::{
    LFS_PATCH_LOCKS, acquire_lfs_patch_lock, acquire_lfs_patch_lock_for_dir,
    evict_lfs_patch_ranges, inspect_lfs_patch_ranges, lfs_batch, lfs_delete_object, lfs_get_object,
    lfs_head_object, lfs_patch_dir, lfs_patch_meta_path, lfs_patch_now_seconds, lfs_patch_object,
    lfs_patch_ranges_compactions, lfs_put_object, lfs_ranges_cover_total, lfs_validation_response,
    lfs_verify_object, live_lfs_patch_lock_count, lock_lfs_patch_oid, parse_content_range,
    patch_store_usage, record_lfs_patch_range, sweep_lfs_patch_sessions, touch_patch_session,
};

/// Test signing key matching the one used in e2e tests.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// A valid 64-character lowercase hex OID suitable for LFS tests.
fn test_oid(content: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(content))
}

async fn assert_multichunk_object(state: &AppState, oid: &str) {
    let key = lfs_object_key(oid, &AuthorizedRepository::anonymous_full_access()).unwrap();
    let file_id = shardline_server_core::protocol_support::protocol_object_file_id(&key);
    let record = state.backend.protocol_file_record(&file_id).await.unwrap();
    assert!(
        record.chunks.len() > 1,
        "fixture must exercise multiple chunks"
    );
}

fn test_oid_constant() -> String {
    test_oid(b"test-lfs-object")
}

fn assert_lfs_patch_session_absent(dir: &std::path::Path, oid: &str, operation: &str) {
    for path in [
        dir.join(oid),
        dir.join(format!("{oid}.ranges")),
        lfs_patch_meta_path(dir, oid),
    ] {
        assert!(
            !path.exists(),
            "{operation} must consume session artifact {}",
            path.display()
        );
    }
}

fn lfs_test_config(root: PathBuf, base_url: &str) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:0".parse().expect("fixture address"),
        base_url.to_owned(),
        root,
        NonZeroUsize::new(65_536).expect("nonzero fixture chunk size"),
    )
    .with_server_frontends([ServerFrontend::Lfs])
    .expect("LFS fixture frontend")
}

fn lfs_test_state(
    config: ServerConfig,
    backend: ServerBackend,
    auth: Option<crate::auth::ServerAuth>,
) -> Arc<AppState> {
    let chunk_size = config.chunk_size();
    crate::test_support::protocol_state(
        config,
        backend,
        auth,
        crate::TransferLimiter::new(chunk_size, chunk_size),
    )
}

/// Builds a minimal [`AppState`] backed by a fresh temp directory.
///
/// `auth` is left as `None` so that route handlers skip authorization checks,
/// which keeps each test self-contained without token minting.
async fn build_test_state() -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = lfs_test_config(tmp.path().to_path_buf(), "http://127.0.0.1:0");

    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");

    let state = lfs_test_state(config, backend, None);

    (state, tmp)
}

async fn build_postgres_lfs_state(
    database_url: &str,
    root: PathBuf,
    object_store: crate::ServerObjectStore,
) -> Arc<AppState> {
    let config = lfs_test_config(root.clone(), "http://127.0.0.1:8080");
    let backend = crate::PostgresBackend::new_with_object_store(
        root,
        "http://127.0.0.1:8080".to_owned(),
        config.chunk_size(),
        database_url,
        object_store,
    )
    .await
    .expect("configured PostgreSQL LFS fixture must initialize");
    crate::apply_database_migrations(&backend.gc_barrier_pool)
        .await
        .expect("configured PostgreSQL LFS fixture migrations must apply");
    lfs_test_state(config, ServerBackend::Postgres(backend), None)
}

/// Builds a minimal [`AppState`] with explicit LFS chunked-patch store
/// limits (active-session cap, aggregate byte cap, staging TTL).
async fn build_test_state_with_lfs_options(
    max_active_sessions: std::num::NonZeroUsize,
    total_max_bytes: NonZeroU64,
    ttl_seconds: NonZeroU64,
) -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = lfs_test_config(tmp.path().to_path_buf(), "http://127.0.0.1:0")
        .with_lfs_patch_max_active_sessions(max_active_sessions)
        .expect("lfs patch max active sessions")
        .with_lfs_patch_total_max_bytes(total_max_bytes)
        .expect("lfs patch total max bytes")
        .with_lfs_patch_ttl_seconds(ttl_seconds)
        .expect("lfs patch ttl");

    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");

    let state = lfs_test_state(config, backend, None);

    (state, tmp)
}

/// Builds a minimal [`AppState`] with an auth provider for xet transfer tests.
async fn build_test_state_with_auth() -> (Arc<AppState>, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let config = lfs_test_config(tmp.path().to_path_buf(), "http://127.0.0.1:8080");

    let backend = crate::ServerBackend::from_config(&config)
        .await
        .expect("backend from config");

    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    let state = lfs_test_state(config, backend, Some(auth));

    (state, tmp)
}

/// Mints a test token for use with the auth-enabled test state.
fn mint_test_token(scope: TokenScope) -> String {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims};
    use shardline_server_core::auth::LocalHmacProvider;

    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

/// Registers only the LFS routes on a fresh [`Router`] and attaches state.
fn lfs_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/lfs/objects/batch", post(lfs_batch))
        .route(
            "/v1/lfs/objects/{oid}",
            get(lfs_get_object)
                .head(lfs_head_object)
                .put(lfs_put_object)
                .patch(lfs_patch_object)
                .delete(lfs_delete_object),
        )
        .route("/v1/lfs/objects/{oid}/verify", post(lfs_verify_object))
        .with_state(state)
}

// =========================================================================
// parse_content_range tests
// =========================================================================

#[test]
fn parse_content_range_accepts_supported_formats() {
    for (value, expected) in [
        ("bytes 0-99/200", (0, 99, 200)),
        ("  bytes 0-99/200 ", (0, 99, 200)),
        ("bytes  0 - 99 /200", (0, 99, 200)),
        ("bytes 1048576-2097151/4194304", (1048576, 2097151, 4194304)),
        ("bytes 5-5/200", (5, 5, 200)),
    ] {
        assert_eq!(parse_content_range(value), Ok(expected), "{value:?}");
    }
}

#[test]
fn parse_content_range_rejects_malformed_values() {
    for value in [
        "0-99/200",
        "bytes 0-99",
        "bytes abc-99/200",
        "bytes 0-xyz/200",
        "bytes 0-99/abc",
        "",
        "bytes /200",
        "bytes -1-99/200",
        "bytes 100-50/200",
        "bytes 0-99-100/200",
        "bytes 0-99-/200",
        "bytes 0-99/ 200",
    ] {
        assert_eq!(parse_content_range(value), Err(()), "{value:?}");
    }
}

proptest! {
    #[test]
    #[allow(clippy::arithmetic_side_effects)]
    fn durable_range_coverage_matches_byte_oracle(
        total in 1_u64..128,
        raw_ranges in proptest::collection::vec((0_u64..128, 1_u64..129), 0..64),
    ) {
        let mut parts = Vec::new();
        let mut oracle = vec![false; usize::try_from(total).expect("small total")];
        for (generation, (raw_start, raw_end)) in raw_ranges.into_iter().enumerate() {
            let start = raw_start.min(total - 1);
            let end = raw_end.max(start + 1).min(total);
            let range = ResumablePartRange::new(start, end).expect("normalized range");
            for covered in oracle
                .get_mut(usize::try_from(start).expect("small start")..
                    usize::try_from(end).expect("small end"))
                .expect("range inside oracle")
            {
                *covered = true;
            }
            parts.push(
                ResumableSessionPart::new(
                    NonZeroU64::new(u64::try_from(generation + 1).expect("small generation"))
                        .expect("nonzero part"),
                    NonZeroU64::new(u64::try_from(generation + 1).expect("small generation"))
                        .expect("nonzero generation"),
                    format!("staging/{generation}"),
                    end - start,
                    None,
                )
                .with_range(range),
            );
        }
        let expected = oracle.iter().all(|covered| *covered);
        prop_assert_eq!(lfs_ranges_cover_total(&parts, total).expect("valid ranges"), expected);
    }
}

// ── lfs_validation_response ────────────────────────────────────────────

#[test]
fn lfs_validation_response_returns_unprocessable_entity() {
    let response = lfs_validation_response("test error");
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_content_range_preserves_local_staging_and_retry() {
    for fields in [
        ["bytes 4-8/9", "invalid"],
        ["invalid", "bytes 4-8/9"],
        ["bytes 4-8/9", "bytes 5-9/10"],
        ["bytes 5-9/10", "bytes 4-8/9"],
        ["bytes 4-8/9", "bytes 4-8/9"],
    ] {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(Arc::clone(&state));
        let oid = test_oid(b"seedfixed");
        let uri = format!("/v1/lfs/objects/{oid}");
        let seeded = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&uri)
                    .header(CONTENT_RANGE, "bytes 0-3/9")
                    .header(CONTENT_LENGTH, 4)
                    .body(Body::from("seed"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(seeded.status().is_success(), "{}", seeded.status());
        let staging_dir = lfs_patch_dir(state.config.root_dir());
        let prior_bytes = fs::read(staging_dir.join(&oid)).unwrap();
        let ranges_path = staging_dir.join(format!("{oid}.ranges"));
        let prior_ranges = fs::read(&ranges_path).unwrap();
        let rejected = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&uri)
                    .header(CONTENT_RANGE, fields[0])
                    .header(CONTENT_RANGE, fields[1])
                    .header(CONTENT_LENGTH, 5)
                    .body(Body::from("bad!!"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST, "{fields:?}");
        assert_eq!(fs::read(staging_dir.join(&oid)).unwrap(), prior_bytes);
        assert_eq!(fs::read(&ranges_path).unwrap(), prior_ranges);
        let unpublished = app
            .clone()
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(unpublished.status(), StatusCode::NOT_FOUND);
        let retry = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(&uri)
                    .header(CONTENT_RANGE, "bytes 4-8/9")
                    .header(CONTENT_LENGTH, 5)
                    .body(Body::from("fixed"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(retry.status(), StatusCode::OK);
        let downloaded = app
            .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(downloaded.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(downloaded.into_body(), 1024)
                .await
                .unwrap()
                .as_ref(),
            b"seedfixed"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_validation_response_includes_json_body() {
    let response = lfs_validation_response("invalid oid");
    let body = response.into_body();
    let bytes = axum::body::to_bytes(body, 1024).await.unwrap();
    let parsed: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parsed["message"], "invalid oid");
}

#[test]
fn lfs_validation_response_sets_lfs_content_type() {
    let response = lfs_validation_response("too many objects");
    assert_eq!(response.headers()["content-type"], crate::LFS_CONTENT_TYPE);
}

fn batch_request(body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/lfs/objects/batch")
        .header("content-type", crate::LFS_CONTENT_TYPE)
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

// =========================================================================
// lfs_batch tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_download_missing_object_returns_404_error_in_objects() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let request = json!({
        "operation": "download",
        "objects": [{ "oid": oid, "size": 1024 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["transfer"], "basic");
    assert_eq!(parsed["hash_algo"], "sha256");
    let objects = parsed["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["oid"], oid);
    assert_eq!(objects[0]["error"]["code"], 404);
    assert_eq!(objects[0]["error"]["message"], "Object does not exist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_upload_missing_object_returns_upload_action() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let request = json!({
        "operation": "upload",
        "objects": [{ "oid": oid, "size": 512 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    let objects = parsed["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["oid"], oid);
    assert_eq!(objects[0]["size"], 512);
    assert!(
        objects[0]["actions"]["upload"]["href"]
            .as_str()
            .unwrap()
            .contains(&oid)
    );
    assert!(objects[0]["error"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_rejects_invalid_requests() {
    for (request, expected_message) in [
        (
            json!({
                "operation": "verify",
                "objects": []
            }),
            "unsupported operation",
        ),
        (
            json!({
                "operation": "download",
                "objects": [{ "oid": "not-a-valid-hash", "size": 100 }]
            }),
            "invalid oid",
        ),
        (
            json!({
                "operation": "download",
                "hash_algo": "sha512",
                "objects": [{ "oid": test_oid_constant(), "size": 100 }]
            }),
            "unsupported hash algorithm",
        ),
        (
            json!({
                "operation": "download",
                "transfers": ["custom"],
                "objects": [{ "oid": test_oid_constant(), "size": 100 }]
            }),
            "unsupported transfer adapter",
        ),
    ] {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let response = app.oneshot(batch_request(&request)).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{request}"
        );
        let parsed = crate::test_support::json_body(response).await;
        assert_eq!(parsed["message"], expected_message, "{request}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_accepts_basic_transfer_adapter_explicitly() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);

    let request = json!({
        "operation": "download",
        "transfers": ["basic"],
        "objects": [{ "oid": test_oid_constant(), "size": 100 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["transfer"], "basic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_accepts_empty_objects_list() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);

    let request = json!({
        "operation": "download",
        "objects": []
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    let objects = parsed["objects"].as_array().unwrap();
    assert!(objects.is_empty());
}

// =========================================================================
// lfs_batch xet transfer tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_xet_transfer_without_auth_falls_back_to_basic() {
    // When no auth provider is configured, xet transfer is not available.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);

    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{ "oid": test_oid_constant(), "size": 100 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    // Without auth, falls back to basic (no CAS token to return).
    assert_eq!(parsed["transfer"], "basic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_xet_transfer_without_auth_rejects_xet_only() {
    // When no auth provider, "xet" alone is unsupported (no fallback).
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);

    let request = json!({
        "operation": "download",
        "transfers": ["xet"],
        "objects": [{ "oid": test_oid_constant(), "size": 100 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_xet_transfer_with_auth_returns_xet_upload_actions() {
    let (state, _tmp) = build_test_state_with_auth().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();
    let token = mint_test_token(TokenScope::Write);

    let request = json!({
        "operation": "upload",
        "transfers": ["xet", "basic"],
        "objects": [{ "oid": oid, "size": 512 }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;

    // Must use xet transfer
    assert_eq!(parsed["transfer"], "xet");

    // Each object must have the CAS action headers
    let obj = &parsed["objects"][0];
    assert_eq!(obj["oid"], oid);
    let upload = &obj["actions"]["upload"];
    assert!(upload["href"].as_str().unwrap().contains(&oid));

    let header = &upload["header"];
    assert!(
        header["X-Xet-Cas-Url"]
            .as_str()
            .unwrap()
            .contains("http://127.0.0.1:8080"),
        "CAS URL should point to the server"
    );
    assert!(
        header["X-Xet-Access-Token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "Access token should be present and non-empty"
    );
    assert!(
        header["X-Xet-Token-Expiration"].as_str().is_some(),
        "Token expiration should be present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_xet_transfer_download_existing_object_includes_headers() {
    let (state, _tmp) = build_test_state_with_auth().await;
    let app = lfs_router(state);
    let content = b"xet-download-test-content";
    let oid = test_oid(content);
    let token = mint_test_token(TokenScope::Write);

    // Upload first (requires auth)
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/octet-stream")
                .header("content-length", content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Batch download with xet transfer
    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{ "oid": oid, "size": content.len() as u64 }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;

    assert_eq!(parsed["transfer"], "xet");
    let obj = &parsed["objects"][0];
    let download = &obj["actions"]["download"];

    let header = &download["header"];
    assert!(
        header["X-Xet-Cas-Url"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "download actions should include X-Xet-Cas-Url"
    );
    assert!(
        header["X-Xet-Access-Token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "download actions should include X-Xet-Access-Token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_xet_transfer_authenticated_with_read_token() {
    let (state, _tmp) = build_test_state_with_auth().await;
    let app = lfs_router(state);
    let token = mint_test_token(TokenScope::Read);

    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{ "oid": test_oid_constant(), "size": 100 }]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;

    // Read scope should still get xet transfer for downloads
    assert_eq!(parsed["transfer"], "xet");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_falls_back_only_on_internal_cas_issuance_failure() {
    use shardline_server_core::auth::{AuthError, AuthProvider};
    struct VerifyOnly(Arc<dyn AuthProvider>, fn() -> AuthError);
    impl AuthProvider for VerifyOnly {
        fn verify_token(&self, token: &str) -> Result<shardline_protocol::TokenClaims, AuthError> {
            self.0.verify_token(token)
        }
        fn mint_token(
            &self,
            _claims: &shardline_protocol::TokenClaims,
        ) -> Result<String, AuthError> {
            Err((self.1)())
        }
    }
    let cases = [
        (
            (|| AuthError::ProviderError("private signing diagnostic".to_owned()))
                as fn() -> AuthError,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::OK,
        ),
        (
            || AuthError::InsufficientScope,
            StatusCode::FORBIDDEN,
            StatusCode::FORBIDDEN,
        ),
        (
            || AuthError::InvalidToken,
            StatusCode::UNAUTHORIZED,
            StatusCode::UNAUTHORIZED,
        ),
        (
            || AuthError::ExpiredToken,
            StatusCode::UNAUTHORIZED,
            StatusCode::UNAUTHORIZED,
        ),
    ];
    for (failure, xet_status, mixed_status) in cases {
        let (mut state, _tmp) = build_test_state_with_auth().await;
        let verifier = state.auth.as_ref().unwrap().provider_arc();
        Arc::get_mut(&mut state).unwrap().auth = Some(crate::auth::ServerAuth::from_provider(
            Box::new(VerifyOnly(verifier, failure)),
        ));
        let app = lfs_router(state);
        for (transfers, status) in [
            (json!(["xet"]), xet_status),
            (json!(["xet", "basic"]), mixed_status),
            (json!(["basic"]), StatusCode::OK),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/v1/lfs/objects/batch")
                        .header("content-type", "application/vnd.git-lfs+json")
                        .header(
                            "authorization",
                            format!("Bearer {}", mint_test_token(TokenScope::Read)),
                        )
                        .body(Body::from(
                            serde_json::to_vec(&json!({
                                "operation": "download", "transfers": transfers,
                                "objects": [{"oid": test_oid_constant(), "size": 100}]
                            }))
                            .unwrap(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(!String::from_utf8_lossy(&bytes).contains("private signing diagnostic"));
            let parsed: Value = serde_json::from_slice(&bytes).unwrap();
            assert_ne!(parsed["transfer"], "xet");
            if status == StatusCode::OK {
                assert_eq!(parsed["transfer"], "basic");
            }
        }
    }
}

// =========================================================================
// lfs_get_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_object_missing_returns_not_found() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_handlers_reject_invalid_oid() {
    for (method, path, header, body) in [
        ("GET", "/v1/lfs/objects/not-a-valid-oid", None, ""),
        ("HEAD", "/v1/lfs/objects/not-a-valid-oid", None, ""),
        (
            "PUT",
            "/v1/lfs/objects/not-a-valid-oid",
            Some(("content-type", "application/octet-stream")),
            "hello",
        ),
        ("DELETE", "/v1/lfs/objects/not-a-valid-oid", None, ""),
        (
            "PATCH",
            "/v1/lfs/objects/not-a-valid-oid",
            Some(("content-range", "bytes 0-4/8")),
            "hello",
        ),
        ("POST", "/v1/lfs/objects/not-a-valid-oid/verify", None, ""),
    ] {
        let (state, _tmp) = build_test_state().await;
        let app = lfs_router(state);
        let mut request = Request::builder().method(method).uri(path);
        if let Some((name, value)) = header {
            request = request.header(name, value);
        }
        let response = app
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNPROCESSABLE_ENTITY,
            "{method} {path}"
        );
        if method != "HEAD" {
            let parsed = crate::test_support::json_body(response).await;
            assert_eq!(parsed["message"], "invalid oid", "{method} {path}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_object_happy_path_after_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    // Exceed the 128 KiB maximum CDC chunk size of the 64 KiB fixture.
    let content: Vec<u8> = (0u32..131_073).map(|i| (i as u8) ^ 0xAA).collect();
    let oid = test_oid(&content);

    // Upload first
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .header("content-length", content.len())
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_response.status(), StatusCode::OK);
    assert_multichunk_object(&state, &oid).await;

    // Download
    let get_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get_response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), content);
}

// =========================================================================
// lfs_head_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn head_object_missing_returns_not_found() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

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
async fn head_object_happy_path_after_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"head-test-content";
    let oid = test_oid(content);

    // Upload first
    let put_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .header("content-length", content.len())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_response.status(), StatusCode::OK);

    // HEAD
    let head_response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head_response.status(), StatusCode::OK);
    let content_length = head_response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, content.len() as u64);
}

// =========================================================================
// lfs_put_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_happy_path() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"put-test-content";
    let oid = test_oid(content);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .header("content-length", content.len())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_accepts_wrong_content_type() {
    // The Content-Type check was relaxed for git-lfs compatibility.
    // Non-octet-stream Content-Types are accepted; the body is validated
    // by its SHA-256 digest regardless of Content-Type.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"test-lfs-object";
    let oid = test_oid(content);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "text/plain")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_accepts_missing_content_type() {
    // git-lfs does not always send Content-Type; the handler accepts
    // requests without it.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"test-lfs-object";
    let oid = test_oid(content);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_is_idempotent() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"idempotent-content";
    let oid = test_oid(content);

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    let second = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_same_oid_puts_are_idempotent() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"concurrent-same-oid-content".to_vec();
    let oid = test_oid(&content);
    let barrier = Arc::new(tokio::sync::Barrier::new(16));
    let mut uploads = Vec::new();

    for _ in 0..16 {
        let app = app.clone();
        let content = content.clone();
        let oid = oid.clone();
        let barrier = barrier.clone();
        uploads.push(tokio::spawn(async move {
            barrier.wait().await;
            app.oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-length", content.len())
                    .body(Body::from(content))
                    .unwrap(),
            )
            .await
            .unwrap()
        }));
    }

    for upload in uploads {
        assert_eq!(upload.await.unwrap().status(), StatusCode::OK);
    }

    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let stored = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(stored.as_ref(), content.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_object_rejects_content_length_mismatch_without_publishing() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let content = b"content-length-mismatch".to_vec();
    let oid = test_oid(&content);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-length", content.len() + 1)
                .body(Body::from(content))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

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

// =========================================================================
// lfs_delete_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_object_missing_returns_not_found() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_object_happy_path_after_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"delete-me";
    let oid = test_oid(content);

    // Upload
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Delete
    let del = app
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
    assert_eq!(del.status(), StatusCode::ACCEPTED);

    // Confirm deleted
    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::NOT_FOUND);
}

// =========================================================================
// lfs_patch_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_single_chunk_happy_path() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"patch-content";
    let oid = test_oid(content);
    let total = content.len() as u64;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                .header("content-length", content.len())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Verify object was stored
    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let content_length = head
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, total);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::arithmetic_side_effects)]
async fn patch_object_resumes_across_postgres_nodes_without_shared_rwx() {
    let Some(database_url) = shardline_test_support::postgres_fixture_url("DATABASE_URL") else {
        return;
    };
    let tmp = TempDir::new().expect("tempdir");
    let shared_store =
        crate::ServerObjectStore::local(tmp.path().join("objects")).expect("shared object store");
    let node_a_root = tmp.path().join("node-a");
    let node_b_root = tmp.path().join("node-b");
    let node_a =
        build_postgres_lfs_state(&database_url, node_a_root.clone(), shared_store.clone()).await;
    let node_b = build_postgres_lfs_state(&database_url, node_b_root.clone(), shared_store).await;
    let app_a = lfs_router(node_a);
    let app_b = lfs_router(node_b);
    let content = format!("cross-replica:{}", tmp.path().display()).into_bytes();
    let split = content.len() / 2;
    let oid = test_oid(&content);
    let total = content.len();
    let suffix_start = split + 3;

    let second = app_b
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(
                    CONTENT_RANGE,
                    format!("bytes {suffix_start}-{}/{}", total - 1, total),
                )
                .header(CONTENT_LENGTH, total - suffix_start)
                .body(Body::from(content[suffix_start..].to_vec()))
                .expect("request"),
        )
        .await
        .expect("second-node patch");
    assert_eq!(second.status(), StatusCode::OK);

    let mut damaged_prefix = content[..split].to_vec();
    damaged_prefix[split - 2..].fill(b'X');
    let first = app_a
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(CONTENT_RANGE, format!("bytes 0-{}/{}", split - 1, total))
                .header(CONTENT_LENGTH, split)
                .body(Body::from(damaged_prefix))
                .expect("request"),
        )
        .await
        .expect("first-node patch");
    assert_eq!(first.status(), StatusCode::OK);

    let overlap_start = split - 2;
    let overlap = app_b
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(
                    CONTENT_RANGE,
                    format!("bytes {overlap_start}-{}/{}", suffix_start - 1, total),
                )
                .header(CONTENT_LENGTH, suffix_start - overlap_start)
                .body(Body::from(content[overlap_start..suffix_start].to_vec()))
                .expect("request"),
        )
        .await
        .expect("overlapping completion patch");
    assert_eq!(overlap.status(), StatusCode::OK);
    assert!(!node_a_root.join("lfs-patch").exists());
    assert!(!node_b_root.join("lfs-patch").exists());

    let get = app_b
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("cross-node get");
    assert_eq!(get.status(), StatusCode::OK);
    let observed = axum::body::to_bytes(get.into_body(), usize::MAX)
        .await
        .expect("response bytes");
    assert_eq!(observed.as_ref(), content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn patch_object_hash_mismatch_can_be_repaired_durably() {
    let Some(database_url) = shardline_test_support::postgres_fixture_url("DATABASE_URL") else {
        return;
    };
    let tmp = TempDir::new().expect("tempdir");
    let shared_store =
        crate::ServerObjectStore::local(tmp.path().join("objects")).expect("shared object store");
    let state =
        build_postgres_lfs_state(&database_url, tmp.path().join("node"), shared_store).await;
    let app = lfs_router(Arc::clone(&state));
    let expected = format!("expected-lfs-content:{}", tmp.path().display()).into_bytes();
    let mut damaged = expected.clone();
    let damaged_byte = damaged
        .first_mut()
        .expect("deterministic test content is non-empty");
    *damaged_byte ^= 0x01;
    let oid = test_oid(&expected);
    let total = expected.len();
    let uri = format!("/v1/lfs/objects/{oid}");

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header(CONTENT_RANGE, format!("bytes 0-{}/{}", total - 1, total))
                .header(CONTENT_LENGTH, total)
                .body(Body::from(damaged.clone()))
                .expect("request"),
        )
        .await
        .expect("damaged patch");
    assert_eq!(first.status(), StatusCode::BAD_REQUEST);

    let session_id = super::durable_lfs_session_id("global", &oid);
    let claimed = state
        .backend
        .resumable_session_by_id(&session_id)
        .await
        .expect("session lookup")
        .expect("failed completion leaves session");
    assert_eq!(
        claimed.state(),
        shardline_index::ResumableSessionState::Active
    );

    // Hash validation failed after the durable completion fence was
    // claimed. A corrected PATCH must still be able to repair the session.
    let retry = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&uri)
                .header(CONTENT_RANGE, format!("bytes 0-{}/{}", total - 1, total))
                .header(CONTENT_LENGTH, total)
                .body(Body::from(expected.clone()))
                .expect("request"),
        )
        .await
        .expect("corrected patch");
    assert_eq!(retry.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_missing_content_range_returns_416() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::from(b"chunk".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["message"], "missing Content-Range header");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_invalid_content_range_returns_416() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", "invalid-format")
                .body(Body::from(b"chunk".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["message"], "invalid Content-Range header");
}

// =========================================================================
// lfs_patch_object concurrency / lock tests
// =========================================================================

#[test]
fn acquire_lfs_patch_lock_returns_same_lock_for_same_oid() {
    let lock1 = acquire_lfs_patch_lock("abc123");
    let lock2 = acquire_lfs_patch_lock("abc123");
    assert!(Arc::ptr_eq(&lock1, &lock2));
}

#[test]
fn acquire_lfs_patch_lock_returns_different_lock_for_different_oid() {
    let lock1 = acquire_lfs_patch_lock("abc123");
    let lock2 = acquire_lfs_patch_lock("def456");
    assert!(!Arc::ptr_eq(&lock1, &lock2));
}

#[test]
fn acquire_lfs_patch_lock_for_dir_isolated_by_root() {
    let first_root = TempDir::new().unwrap();
    let second_root = TempDir::new().unwrap();
    let first_dir = lfs_patch_dir(first_root.path());
    let second_dir = lfs_patch_dir(second_root.path());
    let first = acquire_lfs_patch_lock_for_dir(&first_dir, "same-oid");
    let second = acquire_lfs_patch_lock_for_dir(&second_dir, "same-oid");
    assert!(!Arc::ptr_eq(&first, &second));
}

#[test]
fn lfs_patch_oid_file_lock_excludes_independent_openers() {
    let root = TempDir::new().unwrap();
    let dir = lfs_patch_dir(root.path());
    fs::create_dir_all(&dir).unwrap();
    let first = lock_lfs_patch_oid(&dir, "abc123").unwrap();
    let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
    let waiter_dir = dir;
    let waiter = std::thread::spawn(move || {
        let _second = lock_lfs_patch_oid(&waiter_dir, "abc123").unwrap();
        acquired_tx.send(()).unwrap();
    });

    assert!(
        acquired_rx
            .recv_timeout(std::time::Duration::from_millis(50))
            .is_err()
    );
    drop(first);
    acquired_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    waiter.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_concurrent_chunks_assembles_correctly() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());

    let chunk1 = b"hello-world-part-AAAA"; // 20 bytes
    let chunk2 = b"BBBB-part-two-last!!"; // 20 bytes
    let full_content = [chunk1.as_slice(), chunk2.as_slice()].concat();
    let oid = test_oid(&full_content);
    let total = full_content.len() as u64;

    let app1 = app.clone();
    let oid1 = oid.clone();
    let h1 = tokio::spawn(async move {
        app1.oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid1}"))
                .header(
                    "content-range",
                    format!("bytes 0-{}/{}", chunk1.len() as u64 - 1, total),
                )
                .header("content-length", chunk1.len())
                .body(Body::from(chunk1.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap()
    });

    let app2 = app.clone();
    let oid2 = oid.clone();
    let h2 = tokio::spawn(async move {
        app2.oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid2}"))
                .header(
                    "content-range",
                    format!("bytes {}-{}/{}", chunk1.len(), total - 1, total),
                )
                .header("content-length", chunk2.len())
                .body(Body::from(chunk2.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap()
    });

    let (r1, r2) = tokio::join!(h1, h2);
    let r1 = r1.unwrap();
    let r2 = r2.unwrap();
    assert_eq!(r1.status(), StatusCode::OK);
    assert_eq!(r2.status(), StatusCode::OK);

    // Verify the assembled object is correct.
    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let content_length = head
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(content_length, total);
}

// =========================================================================
// Large / overflow size field tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_accepts_u64_max_size() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid(b"test");

    let request = json!({
        "operation": "download",
        "objects": [{ "oid": oid, "size": 18446744073709551615u64 }]
    });

    let response = app.oneshot(batch_request(&request)).await.unwrap();

    // u64::MAX is a valid u64 value → should deserialize and return 200.
    assert_eq!(response.status(), StatusCode::OK);
    let parsed = crate::test_support::json_body(response).await;
    let objects = parsed["objects"].as_array().unwrap();
    assert!(
        objects[0].get("error").is_some(),
        "u64::MAX size on missing object should give an error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batch_rejects_overflow_size() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);

    // A JSON number larger than u64::MAX — serde_json will reject it at
    // deserialization time, so axum returns 400 before the handler runs.
    // We build the JSON as a raw string because serde_json::Value cannot
    // represent numbers beyond f64 precision.
    let overflow_body = format!(
        r#"{{"operation":"download","objects":[{{"oid":"{}","size":999999999999999999999999999999999999}}]}}"#,
        "a".repeat(64)
    );

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header("content-type", "application/vnd.git-lfs+json")
                .body(Body::from(overflow_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // serde_json deserialization failure results in a 422 Unprocessable Entity
    // (axum's default Json rejection status).
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

// =========================================================================
// lfs_verify_object tests
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_object_missing_returns_not_found() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid_constant();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_object_happy_path_after_upload() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    // Exceed the 128 KiB maximum CDC chunk size of the 64 KiB fixture.
    let content: Vec<u8> = (0u32..131_073).map(|i| (i as u8) ^ 0xBB).collect();
    let oid = test_oid(&content);

    // Upload
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);
    assert_multichunk_object(&state, &oid).await;

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
async fn verify_object_hash_mismatch_returns_422() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"mismatch-content";
    let correct_oid = test_oid(content);

    // Upload with correct OID
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{correct_oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Insert data under a *different* OID key with mismatched content.
    // We use a second valid OID (of different bytes) and store `content`
    // under it.  The verify endpoint will read the bytes, re-hash them,
    // and find that sha256(content) != second_oid, triggering a 422.
    let second_oid = test_oid(b"different-content-only-for-key");
    let object_key = lfs_object_key(&second_oid, &AuthorizedRepository::anonymous_full_access())
        .expect("object key");
    state
        .backend
        .put_object_bytes_if_absent(&object_key, content.to_vec())
        .await
        .expect("insert mismatched data");

    // Verify with second_oid — content hash won't match
    let verify = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{second_oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(verify.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = axum::body::to_bytes(verify.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["message"], "SHA-256 hash mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_rejects_body_length_mismatch() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let oid = test_oid_constant();
    let object_key =
        lfs_object_key(&oid, &AuthorizedRepository::anonymous_full_access()).expect("object key");

    // Store an initial object with known size
    let content = b"0123456789abcdef";
    state
        .backend
        .put_object_bytes_if_absent(&object_key, content.to_vec())
        .await
        .expect("store initial object");

    // Send PATCH with Content-Range claiming 10 bytes but body only has 5
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("Content-Range", "bytes 0-4/20") // claim 5 bytes
                .header("Content-Length", "10") // but say 10
                .header("Content-Type", "application/octet-stream")
                .body(Body::from(b"short-body".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Content-Length != expected_chunk_size → RangeNotSatisfiable
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_object_too_large_returns_413() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"small-object-for-size-inflation";
    let oid = test_oid(content);

    // Store a small object under the correct OID key.
    let object_key =
        lfs_object_key(&oid, &AuthorizedRepository::anonymous_full_access()).expect("object key");
    state
        .backend
        .put_object_bytes_if_absent(&object_key, content.to_vec())
        .await
        .expect("insert object");

    // Inflate the file size on disk beyond MAX_LFS_VERIFY_BYTES.
    // The local backend stores objects at root_dir()/chunks/<key>.
    let object_path = state
        .config
        .root_dir()
        .join("chunks")
        .join(object_key.as_str());
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&object_path)
        .expect("open object file for size inflation");
    file.set_len(super::MAX_LFS_VERIFY_BYTES + 1)
        .expect("inflate file size");
    drop(file);

    // Verify should be rejected with 413 Payload Too Large.
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
    assert_eq!(verify.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let body = axum::body::to_bytes(verify.into_body(), usize::MAX)
        .await
        .unwrap();
    let parsed: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        parsed["message"],
        "object too large for server-side verification"
    );
}

// =========================================================================
// F-20 — LFS patch store: bounded sessions, aggregate byte cap, TTL sweep
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_sweep_removes_stale_staging_sessions() {
    let (state, _tmp) = build_test_state().await;
    let dir = lfs_patch_dir(state.config.root_dir());
    std::fs::create_dir_all(&dir).unwrap();

    let stale_oid = "a".repeat(64);
    let fresh_oid = "b".repeat(64);
    // A stale session: data + ranges + a backdated last-touched sidecar.
    std::fs::write(dir.join(&stale_oid), b"stale-data").unwrap();
    std::fs::write(dir.join(format!("{stale_oid}.ranges")), "10\n0 10\n").unwrap();
    touch_patch_session(&dir, &stale_oid, 1).unwrap();
    // A fresh session, touched just now.
    std::fs::write(dir.join(&fresh_oid), b"fresh-data").unwrap();
    std::fs::write(dir.join(format!("{fresh_oid}.ranges")), "10\n0 10\n").unwrap();
    let now = lfs_patch_now_seconds().unwrap();
    touch_patch_session(&dir, &fresh_oid, now).unwrap();

    let ttl = state.config.lfs_patch_ttl_seconds();
    let removed = sweep_lfs_patch_sessions(state.config.root_dir(), ttl).unwrap();
    assert_eq!(removed, 1, "only the stale session is swept");

    // The stale session's data, ranges, and sidecar are all gone.
    assert!(!dir.join(&stale_oid).exists());
    assert!(!dir.join(format!("{stale_oid}.ranges")).exists());
    assert!(!lfs_patch_meta_path(&dir, &stale_oid).exists());
    // The fresh session survives.
    assert!(dir.join(&fresh_oid).exists());
    assert!(lfs_patch_meta_path(&dir, &fresh_oid).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_rejects_when_active_session_cap_reached() {
    let (state, _tmp) = build_test_state_with_lfs_options(
        std::num::NonZeroUsize::new(2).unwrap(), // max 2 active sessions
        NonZeroU64::new(1 << 40).unwrap(),
        NonZeroU64::new(3600).unwrap(),
    )
    .await;
    let root_dir = state.config.root_dir().to_path_buf();
    let app = lfs_router(state);

    // Two partial (in-flight) sessions consume both slots.
    let oid1 = test_oid(b"cap-session-one-content");
    let oid2 = test_oid(b"cap-session-two-content");
    for oid in [&oid1, &oid2] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", "bytes 0-9/20")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // The third session hits the active-session cap (F-20).
    let oid3 = test_oid(b"cap-session-three-content");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid3}"))
                .header("content-range", "bytes 0-9/20")
                .header("content-length", 10)
                .body(Body::from(b"0123456789".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a PATCH beyond the active-session cap must be rejected"
    );
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["error"], "too many active lfs patch sessions");

    // The rejected PATCH never wrote a staging file.
    let dir = lfs_patch_dir(root_dir.as_path());
    assert!(
        !dir.join(&oid3).exists(),
        "an over-cap PATCH must not write its chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_rejects_over_aggregate_byte_cap_before_write() {
    let (state, _tmp) = build_test_state_with_lfs_options(
        std::num::NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(100).unwrap(), // 100-byte aggregate staging cap
        NonZeroU64::new(3600).unwrap(),
    )
    .await;
    let root_dir = state.config.root_dir().to_path_buf();
    let app = lfs_router(state);

    let oid1 = test_oid(b"aggregate-cap-first-session");
    let chunk1 = vec![0x01_u8; 80];
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid1}"))
                .header("content-range", format!("bytes 0-{}/200", chunk1.len() - 1))
                .header("content-length", chunk1.len())
                .body(Body::from(chunk1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // A second session whose chunk would push the aggregate over the cap
    // is rejected BEFORE the chunk is written (F-20).
    let oid2 = test_oid(b"aggregate-cap-second-session");
    let chunk2 = vec![0x02_u8; 30];
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid2}"))
                .header("content-range", format!("bytes 0-{}/200", chunk2.len() - 1))
                .header("content-length", chunk2.len())
                .body(Body::from(chunk2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["error"], "lfs patch staging byte quota exceeded");

    // The rejected chunk never materialized a staging file.
    let dir = lfs_patch_dir(root_dir.as_path());
    assert!(
        !dir.join(&oid2).exists(),
        "an over-cap PATCH must not write its chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_continuations_exempt_from_active_session_cap() {
    // F-60: the active-session cap must only charge NEW sessions. With the
    // cap full, a continuation of an existing session still succeeds while
    // a brand-new session is rejected — otherwise one writer could hold all
    // slots with tiny sessions and permanently block the chunked-PATCH
    // feature (every continuation of those sessions would be rejected with
    // 429 mid-stream). The aggregate byte cap below still binds
    // continuations (see patch_continuation_still_binds_aggregate_byte_cap).
    let (state, _tmp) = build_test_state_with_lfs_options(
        std::num::NonZeroUsize::new(2).unwrap(), // max 2 active sessions
        NonZeroU64::new(1 << 40).unwrap(),
        NonZeroU64::new(3600).unwrap(),
    )
    .await;
    let app = lfs_router(state);

    // Two partial sessions consume both slots.
    let content_a = b"0123456789abcdefghij"; // 20 bytes
    let oid_a = test_oid(content_a);
    let oid_b = test_oid(b"continuation-cap-session-b");
    for oid in [&oid_a, &oid_b] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", "bytes 0-9/20")
                    .header("content-length", 10)
                    .body(Body::from(b"0123456789".to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // A brand-new session still hits the active-session cap.
    let oid_new = test_oid(b"continuation-cap-new-session");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid_new}"))
                .header("content-range", "bytes 0-9/20")
                .header("content-length", 10)
                .body(Body::from(b"0123456789".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "a new session beyond the active-session cap must be rejected"
    );

    // A continuation of an EXISTING session succeeds despite the cap; it
    // completes [0,20) and promotes, so the OID must match the assembled
    // bytes.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid_a}"))
                .header("content-range", "bytes 10-19/20")
                .header("content-length", 10)
                .body(Body::from(b"abcdefghij".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a continuation of an existing session must not be rejected by the active-session cap"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_continuation_still_binds_aggregate_byte_cap() {
    // F-60: exempting continuations from the active-session cap must not
    // weaken the aggregate staging-BYTE cap — a writer cannot bypass the
    // byte limits by reusing a single OID. A continuation that would push
    // the aggregate over the cap is still rejected BEFORE the chunk is
    // written (F-20).
    let (state, _tmp) = build_test_state_with_lfs_options(
        std::num::NonZeroUsize::new(4).unwrap(),
        NonZeroU64::new(100).unwrap(), // 100-byte aggregate staging cap
        NonZeroU64::new(3600).unwrap(),
    )
    .await;
    let root_dir = state.config.root_dir().to_path_buf();
    let app = lfs_router(state);

    let oid = test_oid(b"continuation-byte-cap-content");
    let chunk1 = vec![0x01_u8; 80];
    let chunk1_len = chunk1.len() as u64;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes 0-{}/200", chunk1.len() - 1))
                .header("content-length", chunk1.len())
                .body(Body::from(chunk1))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // A continuation of the SAME session whose chunk would push the
    // aggregate over the cap is rejected.
    let chunk2 = vec![0x02_u8; 30];
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(
                    "content-range",
                    format!("bytes 80-{}/200", 80 + chunk2.len() - 1),
                )
                .header("content-length", chunk2.len())
                .body(Body::from(chunk2))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let parsed = crate::test_support::json_body(response).await;
    assert_eq!(parsed["error"], "lfs patch staging byte quota exceeded");

    // The rejected continuation never extended the staging file.
    let dir = lfs_patch_dir(root_dir.as_path());
    let data_len = std::fs::metadata(dir.join(&oid)).unwrap().len();
    assert_eq!(
        data_len, chunk1_len,
        "an over-cap continuation must not write its chunk"
    );
}

// =========================================================================
// F-21 — LFS promotion streams in bounded chunks and cleans its temp
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_promotion_leaves_no_staging_files() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());

    let chunk1 = b"streaming-promotion-part-AAAA"; // 27 bytes
    let chunk2 = b"BBBB-part-two-last-final"; // 22 bytes
    let full_content = [chunk1.as_slice(), chunk2.as_slice()].concat();
    let oid = test_oid(&full_content);
    let total = full_content.len() as u64;

    // Two sequential PATCHes (order preserved) assemble and promote the
    // object through the bounded streaming path (F-21).
    for (offset, chunk) in [
        (0_u64, chunk1.as_slice()),
        (chunk1.len() as u64, chunk2.as_slice()),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header(
                        "content-range",
                        format!(
                            "bytes {offset}-{}/{}",
                            offset + chunk.len() as u64 - 1,
                            total
                        ),
                    )
                    .header("content-length", chunk.len())
                    .body(Body::from(chunk.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    // Byte-equivalence through the streamed promotion.
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = axum::body::to_bytes(get.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(body.as_ref(), full_content);

    // The promotion consumed the staging files.
    let dir = lfs_patch_dir(state.config.root_dir());
    assert_lfs_patch_session_absent(&dir, &oid, "a completed promotion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_promotion_failure_cleans_staging_files() {
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());

    // The OID claims one content, but the assembled bytes differ: the
    // promotion's SHA-256 verification fails, and the failed promotion
    // must still clean its own staging files (F-21).
    let oid = test_oid(b"expected-promotion-content");
    let wrong = b"wrong-promotion-content";
    let total = wrong.len() as u64;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                .header("content-length", wrong.len())
                .body(Body::from(wrong.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "a hash-mismatched promotion must fail"
    );

    let dir = lfs_patch_dir(state.config.root_dir());
    assert_lfs_patch_session_absent(&dir, &oid, "a failed promotion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_retry_re_promotes_crash_completed_session() {
    // F-59: a crash between the promotion trigger and the store commit
    // leaves {oid} data + {oid}.ranges showing [0,total) + a fresh
    // {oid}.meta on disk with the object ABSENT from the store. A retried
    // PATCH must re-arm the promotion instead of early-returning 200
    // forever (the old behavior left PATCH 200 / HEAD 404 until the TTL
    // sweep): the retry returns 200, the object is now present (HEAD 200),
    // and the staging files are consumed.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state.clone());
    let content = b"crash-reloaded-promotion-content";
    let oid = test_oid(content);
    let total = content.len() as u64;

    // Seed the crash-left state: data + complete ranges + fresh sidecar,
    // with NO object in the store.
    let dir = lfs_patch_dir(state.config.root_dir());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(&oid), content).unwrap();
    std::fs::write(
        dir.join(format!("{oid}.ranges")),
        format!("{total}\n0 {total}\n"),
    )
    .unwrap();
    let now = lfs_patch_now_seconds().unwrap();
    touch_patch_session(&dir, &oid, now).unwrap();

    // The client retries the chunk; the server must re-promote the
    // crash-completed session.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes 0-{}/{}", total - 1, total))
                .header("content-length", content.len())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a retried PATCH on a crash-completed session must report success"
    );

    // The re-promotion stored the object and consumed the staging files.
    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        head.status(),
        StatusCode::OK,
        "the object must be present after the re-promotion"
    );
    assert_lfs_patch_session_absent(&dir, &oid, "the re-promotion");
}

// =========================================================================
// F-30 — sparse-file accounting, sequential-growth seek bound, incremental
// range bookkeeping
// =========================================================================

#[cfg(unix)]
#[test]
fn patch_store_usage_counts_allocated_not_logical_bytes() {
    // A 1-byte write at a huge offset creates a SPARSE file: logical size
    // 1 GiB+, allocated footprint a few KiB. The aggregate staging cap
    // must count the ALLOCATED size so a handful of such writes can never
    // exhaust it (F-30).
    let tmp = TempDir::new().expect("tempdir");
    let dir = tmp.path().join("lfs-patch");
    std::fs::create_dir_all(&dir).unwrap();
    let oid = "a".repeat(64);
    std::fs::write(dir.join(format!("{oid}.meta")), "1").unwrap();
    {
        use std::io::{Seek, SeekFrom, Write as _};
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(&oid))
            .unwrap();
        file.seek(SeekFrom::Start(1 << 30)).unwrap();
        file.write_all(b"x").unwrap();
    }
    let (_sessions, bytes) = patch_store_usage(&dir).unwrap();
    let allocated = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(dir.join(&oid)).unwrap().blocks() * 512
    };
    assert!(
        bytes < (1 << 30),
        "a sparse staging file must count allocated bytes, not its logical size: {bytes}"
    );
    assert_eq!(
        bytes, allocated,
        "usage must equal the file's allocated block footprint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_rejects_out_of_bounds_seek() {
    // F-30: a PATCH whose Content-Range start jumps far beyond the session
    // high-water mark (here: offset 2^40-1, i.e. the sparse-file attack
    // against a fresh session) is rejected instead of creating a
    // multi-TiB sparse staging file.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid(b"seek-out-of-bounds-content");
    let total = 1_u64 << 40;
    let offset = total - 1;
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                .header("content-length", 1)
                .body(Body::from(vec![0x01]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::RANGE_NOT_SATISFIABLE,
        "a PATCH seeking to 2^40-1 on a fresh session must be rejected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_object_rejects_seek_far_beyond_high_water_mark() {
    // F-30: even after a session has grown sequentially, a seek far beyond
    // its high-water mark + slack is rejected.
    let (state, _tmp) = build_test_state().await;
    let app = lfs_router(state);
    let oid = test_oid(b"seek-past-high-water-content");
    let total = 1_u64 << 40;

    let first = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", "bytes 0-9/1099511627776")
                .header("content-length", 10)
                .body(Body::from(b"0123456789".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);

    let offset = total - 1;
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                .header("content-length", 1)
                .body(Body::from(vec![0x01]))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::RANGE_NOT_SATISFIABLE,
        "a PATCH seeking past the session high-water mark + slack must be rejected"
    );
}

#[test]
fn patch_ranges_disjoint_set_is_incremental() {
    // F-30: recording a large disjoint range set must not rewrite the
    // whole ranges file per PATCH. The compaction counter proves the
    // journal + periodic-compaction design keeps amortized work bounded:
    // 20k appends trigger only ~20 compactions (every 1024 appends), never
    // a full read-sort-merge-rewrite per PATCH.
    let tmp = TempDir::new().expect("tempdir");
    let ranges_path = tmp.path().join("disjoint.ranges");
    let total = 2_000_000_u64;
    let compactions_before = lfs_patch_ranges_compactions();
    let mut offset = 0_u64;
    for _ in 0..20_000 {
        let start = offset;
        let end = start + 10;
        let promote = record_lfs_patch_range(&ranges_path, start, end, total).unwrap();
        assert!(!promote, "disjoint ranges with gaps must never promote");
        offset = end + 10; // leave a 10-byte gap between ranges
    }
    let compactions = lfs_patch_ranges_compactions() - compactions_before;
    assert!(
        compactions < 64,
        "a 20k disjoint-range set must compact rarely (observed {compactions})"
    );

    // The in-memory bookkeeping still exposes the exact merged state.
    let (high_water, complete) = inspect_lfs_patch_ranges(&ranges_path, total).unwrap();
    assert_eq!(
        high_water,
        offset - 10,
        "high-water must track the last end"
    );
    assert!(!complete, "gaps must never read as complete coverage");

    // Crash recovery: evicting the entry and re-loading from the on-disk
    // file must reproduce the same high-water mark.
    evict_lfs_patch_ranges(&ranges_path);
    let (high_water_after_reload, _) = inspect_lfs_patch_ranges(&ranges_path, total).unwrap();
    assert_eq!(high_water_after_reload, offset - 10);
}

#[test]
fn patch_ranges_sequential_growth_promotes_incrementally() {
    // The merged-[0,total) promotion trigger must survive the incremental
    // format: sequential growth promotes exactly when the last range
    // reaches total, and out-of-order arrival still merges to full
    // coverage.
    let tmp = TempDir::new().expect("tempdir");
    let ranges_path = tmp.path().join("sequential.ranges");
    let total = 100_u64;
    for (start, end) in [(0, 40), (40, 70)] {
        let promote = record_lfs_patch_range(&ranges_path, start, end, total).unwrap();
        assert!(!promote, "partial coverage must not promote");
    }
    let promote = record_lfs_patch_range(&ranges_path, 70, 100, total).unwrap();
    assert!(promote, "sequential growth to total must trigger promotion");

    // A second session receiving the tail before the head.
    let backfill = tmp.path().join("backfill.ranges");
    assert!(!record_lfs_patch_range(&backfill, 40, 100, 100).unwrap());
    let promote = record_lfs_patch_range(&backfill, 0, 40, 100).unwrap();
    assert!(
        promote,
        "out-of-order ranges must still merge to full coverage"
    );
}

#[test]
fn patch_ranges_rejects_inconsistent_total() {
    // A client changing the declared total mid-session is rejected.
    let tmp = TempDir::new().expect("tempdir");
    let ranges_path = tmp.path().join("mismatch.ranges");
    record_lfs_patch_range(&ranges_path, 0, 10, 100).unwrap();
    let error = record_lfs_patch_range(&ranges_path, 10, 20, 200).unwrap_err();
    assert!(
        matches!(error, ServerError::Io(ref io_error) if io_error.kind() == std::io::ErrorKind::InvalidData),
        "inconsistent totals must be rejected with InvalidData, got: {error}"
    );
}

// =========================================================================
// F-77 — a rejected seek must not leave a ghost patch session
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn patch_rejected_seek_leaves_no_ghost_session() {
    // F-77: a PATCH whose Content-Range start jumps beyond the
    // sequential-growth seek bound returns 416 but must NOT leave a
    // zero-byte "ghost" session behind — no {oid}.meta, no staging files.
    // Every .meta counts as a session against the active-session cap, so a
    // ghost alone would exhaust a slot while costing zero bytes against the
    // aggregate byte cap, and each repeated 416 re-touches its last-touched
    // time, keeping the ghost refreshable (immune to the TTL sweep) for the
    // whole cap lifetime.
    //
    // With the active-session cap at 1, a rejected 416 must not occupy the
    // slot: a subsequent NEW-session PATCH still succeeds (200).
    let (state, _tmp) = build_test_state_with_lfs_options(
        std::num::NonZeroUsize::new(1).unwrap(), // cap: 1 active session
        NonZeroU64::new(1 << 40).unwrap(),
        NonZeroU64::new(3600).unwrap(),
    )
    .await;
    let root_dir = state.config.root_dir().to_path_buf();
    let app = lfs_router(state);
    let dir = lfs_patch_dir(root_dir.as_path());

    let oid = test_oid(b"ghost-session-content");
    let total = 1_u64 << 30; // 1 GiB declared
    // Just past the default 64 MiB seek bound (a fresh session's high-water
    // mark is 0), so the PATCH is rejected as range-not-satisfiable.
    let offset = (1_u64 << 26) + 1;

    // Repeated 416s: none of them may materialize a session.
    for _ in 0..3 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header("content-range", format!("bytes {offset}-{offset}/{total}"))
                    .header("content-length", 1)
                    .body(Body::from(vec![0x01]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "a PATCH seeking past the session high-water mark + slack must be rejected"
        );
        assert!(
            !lfs_patch_meta_path(&dir, &oid).exists(),
            "a rejected seek must not create the {oid}.meta sidecar"
        );
        let (active_sessions, used_bytes) = patch_store_usage(&dir).unwrap();
        assert_eq!(
            active_sessions, 0,
            "no ghost session may be charged against the active-session cap"
        );
        assert_eq!(used_bytes, 0, "no ghost session may hold staging bytes");
    }

    // The slot is still free: a brand-new session succeeds under the cap
    // of 1, which a ghost would otherwise have consumed.
    let oid_ok = test_oid(b"ghost-slot-still-free-content");
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid_ok}"))
                .header("content-range", "bytes 0-9/20")
                .header("content-length", 10)
                .body(Body::from(b"0123456789".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "a rejected seek must not consume the active-session slot"
    );
}

// =========================================================================
// F-31 — promotion cleanup and the sweep share a consistent lock order
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn patch_promotion_and_sweep_complete_without_deadlock() {
    // F-31: the promotion cleanup must never re-acquire the store lock
    // while holding a per-OID lock, and the sweep must never be starved by
    // a live session. Drive a sweep and a promotion-triggering PATCH
    // concurrently while the test holds the stale session's per-OID lock
    // (mimicking a PATCH that is mid-promotion): the sweep blocks on the
    // per-OID lock while holding the store lock, and the PATCH parks
    // behind the store lock. Both must finish within a hard bound — an
    // inverted lock order would hang them forever.
    let (state, _tmp) = build_test_state().await;
    let dir = lfs_patch_dir(state.config.root_dir());
    std::fs::create_dir_all(&dir).unwrap();
    let content = b"lock-order-promotion-content";
    let oid = test_oid(content);
    // A stale session for the same OID the PATCH targets.
    std::fs::write(dir.join(&oid), b"stale-data").unwrap();
    std::fs::write(dir.join(format!("{oid}.ranges")), "10\n0 10\n").unwrap();
    touch_patch_session(&dir, &oid, 1).unwrap();

    // Mimic a mid-promotion PATCH: hold the target session's per-OID lock
    // so the sweep must wait on it while holding the store lock.
    let oid_lock = acquire_lfs_patch_lock_for_dir(&dir, &oid);
    let guard = oid_lock.lock().unwrap();

    let root = state.config.root_dir().to_path_buf();
    let ttl = state.config.lfs_patch_ttl_seconds();
    let app = lfs_router(state);

    let sweep_task = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            sweep_lfs_patch_sessions(&root, ttl).unwrap()
        })
        .await
        .expect("sweep must complete within the deadline")
    });

    let patch_task = tokio::spawn(async move {
        tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            app.oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri(format!("/v1/lfs/objects/{oid}"))
                    .header(
                        "content-range",
                        format!("bytes 0-{}/{}", content.len() - 1, content.len()),
                    )
                    .header("content-length", content.len())
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap()
        })
        .await
        .expect("PATCH must complete within the deadline")
    });

    // Let both tasks reach their parked state (sweep on the per-OID lock,
    // PATCH on the store lock) without awaiting while the guard is held,
    // then release the per-OID guard so both can drain.
    std::thread::sleep(std::time::Duration::from_millis(200));
    drop(guard);

    let removed = sweep_task.await.unwrap();
    let patch = patch_task.await.unwrap();
    assert_eq!(removed, 1, "the stale session must be swept");
    assert_eq!(
        patch.status(),
        StatusCode::OK,
        "the PATCH must not be starved by the sweep"
    );
}

// =========================================================================
// F-22 — LFS per-OID lock map stays bounded (weak values + lazy eviction)
// =========================================================================

#[test]
fn acquire_lfs_patch_lock_evicts_dead_entries() {
    let baseline_live = live_lfs_patch_lock_count();
    let baseline_map = LFS_PATCH_LOCKS.lock().unwrap().len();

    {
        let _lock1 = acquire_lfs_patch_lock("evict-lock-a");
        let _lock2 = acquire_lfs_patch_lock("evict-lock-b");
        // The map is process-global and tests run in parallel, so a
        // sibling test may legitimately hold an extra live lock (e.g.
        // patch_promotion_and_sweep_complete_without_deadlock sleeps
        // while holding one). Assert the invariant rather than an exact
        // count: both guards held here must keep their entries alive.
        let live = live_lfs_patch_lock_count();
        assert!(
            live >= baseline_live + 2,
            "held guards keep their entries alive: baseline {baseline_live} -> {live}"
        );
    }

    // Both guards dropped: the strong Arcs are gone. The next acquire
    // evicts the dead entries, so the live count returns toward baseline
    // instead of growing 1:1 with the distinct OIDs (F-22). Slack for
    // concurrently-running tests, mirroring the S3 lock-map test.
    let _lock3 = acquire_lfs_patch_lock("evict-lock-c");
    assert!(
        live_lfs_patch_lock_count() <= baseline_live + 1,
        "live locks must return toward baseline once guards drop: \
         baseline {baseline_live} -> {}",
        live_lfs_patch_lock_count()
    );
    assert!(
        LFS_PATCH_LOCKS.lock().unwrap().len() <= baseline_map + 1,
        "map must not retain dead per-OID entries: baseline {baseline_map} -> {}",
        LFS_PATCH_LOCKS.lock().unwrap().len()
    );
}
