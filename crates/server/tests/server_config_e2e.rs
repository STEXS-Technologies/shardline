//! Server configuration end-to-end tests.
//!
//! These tests do NOT require Docker or a Postgres instance. They validate
//! server-level configuration handling: body-limit enforcement, invalid
//! bind addresses, and empty frontend lists.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::let_underscore_must_use,
    clippy::panic
)]

use std::num::NonZeroUsize;

use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerConfigError, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tower::ServiceExt;

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------
// 1. Body limit exceeded — 413 on oversized payload
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_body_limit_exceeded() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Lfs])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_max_request_body_bytes(NonZeroUsize::new(1024).unwrap())
    .with_reconstruction_cache_disabled();
    config.validate_runtime_requirements().unwrap();

    let app = app::router(config).await.unwrap();

    // Mint a token
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap();
    let token = provider.mint_token(&claims).unwrap();

    let oid = "a".repeat(64);
    let oversized_body = vec![0u8; 2048]; // exceeds 1024 limit

    let req = axum::http::Request::builder()
        .method("PUT")
        .uri(format!("/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(oversized_body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        413,
        "oversized body should return 413 Payload Too Large"
    );
}

// ---------------------------------------------------------------------------
// 2. Startup: invalid bind address
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_server_startup_invalid_bind() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();

    // Try binding to port 0 on an invalid/unbindable address.
    // "1.2.3.4:9999" should fail because it's not a local address.
    let bind_addr: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();

    let config = ServerConfig::new(
        bind_addr,
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Xet])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled();
    config.validate_runtime_requirements().unwrap();

    // Binding to port 1 requires root on most systems → should fail.
    let result = tokio::net::TcpListener::bind(bind_addr).await;
    assert!(
        result.is_err(),
        "binding to port 1 should fail with EACCES (permission denied)"
    );
}

// ---------------------------------------------------------------------------
// 3. Empty frontends list is rejected by config validation
// ---------------------------------------------------------------------------

#[test]
fn test_server_with_no_frontends() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();

    let result = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_frontends([]);

    assert!(
        result.is_err(),
        "empty frontends list should produce an error"
    );
    let err = result.unwrap_err();
    assert!(
        matches!(err, ServerConfigError::MissingServerFrontends),
        "expected MissingServerFrontends error, got: {err:?}"
    );
}
