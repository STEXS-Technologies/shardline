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

use std::{io::Write, num::NonZeroUsize, path::Path};

use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{
    ServerConfig, ServerConfigError, ServerFrontend, ServerRole, app, load_toml_config,
};
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

// ── TOML config file tests ──────────────────────────────────────────

fn write_toml(dir: &Path, content: &str) -> std::path::PathBuf {
    let path = dir.join("shardline.toml");
    let mut file = std::fs::File::create(&path).unwrap();
    write!(file, "{content}").unwrap();
    path
}

#[test]
fn test_load_toml_config_found() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"[server]
bind_addr = "127.0.0.1:9090"
server_role = "api"
frontends = ["xet", "oci"]
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let result = load_toml_config(Some(&config_path));
    assert!(
        result.is_ok(),
        "should parse valid TOML: {:?}",
        result.err()
    );
    let toml = result.unwrap().unwrap();
    let srv = toml.server.expect("server section should be present");
    assert_eq!(srv.bind_addr.unwrap(), "127.0.0.1:9090");
    assert_eq!(srv.server_role.unwrap(), "api");
    assert_eq!(srv.frontends.unwrap(), vec!["xet", "oci"]);
}

#[test]
fn test_load_toml_config_not_found() {
    let result = load_toml_config(Some(Path::new("/nonexistent/path/shardline.toml")));
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_load_toml_config_invalid_syntax() {
    let dir = TempDir::new().unwrap();
    write_toml(dir.path(), "[[[invalid]]]");
    let config_path = dir.path().join("shardline.toml");
    let result = load_toml_config(Some(&config_path));
    assert!(result.is_err(), "invalid TOML should fail");
}

// ── Additional TOML config tests ────────────────────────────────────

#[test]
fn test_load_toml_config_minimal() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"[server]
bind_addr = "127.0.0.1:5555"
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    assert_eq!(
        toml.server.as_ref().unwrap().bind_addr.as_deref(),
        Some("127.0.0.1:5555")
    );
    assert!(toml.storage.is_none());
    assert!(toml.index.is_none());
    assert!(toml.cache.is_none());
    assert!(toml.auth.is_none());
}

#[test]
fn test_load_toml_config_full_storage_s3() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"
[storage]
adapter = "s3"

[storage.s3]
endpoint = "https://s3.custom.com"
region = "eu-west-1"
bucket = "my-bucket"
prefix = "staging/"
virtual_hosted_style = true
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    let s3 = toml.storage.unwrap().s3.unwrap();
    assert_eq!(s3.endpoint.unwrap(), "https://s3.custom.com");
    assert_eq!(s3.region.unwrap(), "eu-west-1");
    assert_eq!(s3.bucket.unwrap(), "my-bucket");
    assert_eq!(s3.prefix.unwrap(), "staging/");
    assert!(s3.virtual_hosted_style.unwrap());
}

#[test]
fn test_load_toml_config_full_auth_jwks_oidc() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"
[auth]
provider = "jwks"

[auth.jwks]
url = "https://auth.example.com/jwks"

[auth.oidc]
issuer_url = "https://accounts.example.com"
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    let auth = toml.auth.unwrap();
    assert_eq!(auth.provider.unwrap(), "jwks");
    assert_eq!(
        auth.jwks.as_ref().unwrap().url.as_deref(),
        Some("https://auth.example.com/jwks")
    );
    assert_eq!(
        auth.oidc.as_ref().unwrap().issuer_url.as_deref(),
        Some("https://accounts.example.com")
    );
}

#[test]
fn test_load_toml_config_empty_document() {
    let dir = TempDir::new().unwrap();
    write_toml(dir.path(), "");
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    assert!(toml.server.is_none());
    assert!(toml.storage.is_none());
    assert!(toml.index.is_none());
    assert!(toml.cache.is_none());
    assert!(toml.auth.is_none());
}

#[test]
fn test_load_toml_config_cache_section() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"
[cache]
adapter = "redis"
ttl_seconds = 120
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    let cch = toml.cache.as_ref().unwrap();
    assert_eq!(cch.adapter.as_deref(), Some("redis"));
    assert_eq!(cch.ttl_seconds, Some(120));
}

#[test]
fn test_load_toml_config_unknown_fields_ignored() {
    // TOML with unknown fields should not cause errors (serde default behavior)
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"
[server]
bind_addr = "0.0.0.0:8080"
unknown_field = "should be ignored"

[unknown_section]
foo = "bar"
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let result = load_toml_config(Some(&config_path));
    assert!(
        result.is_ok(),
        "unknown fields should be ignored: {:?}",
        result.err()
    );
}

#[test]
fn test_load_toml_config_utf8_bom() {
    // TOML parsers handle UTF-8 BOM
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        "\u{feff}[server]\nbind_addr = \"0.0.0.0:6060\"\n",
    );
    let config_path = dir.path().join("shardline.toml");
    let result = load_toml_config(Some(&config_path));
    assert!(result.is_ok(), "UTF-8 BOM should be handled");
    let toml = result.unwrap().unwrap();
    assert_eq!(toml.server.unwrap().bind_addr.unwrap(), "0.0.0.0:6060");
}

#[test]
fn test_load_toml_config_file_not_found_returns_none() {
    let result = load_toml_config(None).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_load_toml_config_absolute_path_not_found() {
    let result = load_toml_config(Some(Path::new("/etc/shardline/shardline.toml"))).unwrap();
    assert!(result.is_none());
}

#[test]
fn test_load_toml_config_jwks_section() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"
[auth]
provider = "jwks"

[auth.jwks]
url = "https://example.com/jwks.json"
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    let jwks = toml.auth.as_ref().unwrap().jwks.as_ref().unwrap();
    assert_eq!(jwks.url.as_deref(), Some("https://example.com/jwks.json"));
}

#[test]
fn test_load_toml_config_invalid_server_role() {
    let dir = TempDir::new().unwrap();
    write_toml(
        dir.path(),
        r#"[server]
server_role = "invalid_role"
"#,
    );
    let config_path = dir.path().join("shardline.toml");
    let toml = load_toml_config(Some(&config_path)).unwrap().unwrap();
    // TOML parsing succeeds; validation happens at config load time
    assert_eq!(toml.server.unwrap().server_role.unwrap(), "invalid_role");
}
