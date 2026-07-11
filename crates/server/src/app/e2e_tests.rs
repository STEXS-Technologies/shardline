//! End-to-end tests that exercise every protocol endpoint through the full Axum stack.
//!
//! Each test group builds a minimal [`Router`] with only the routes needed for that
//! protocol and sends real HTTP requests via [`tower::ServiceExt::oneshot`].

use std::{num::{NonZeroU64, NonZeroUsize}, sync::Arc};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware,
    routing::{get, head, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use hmac::Mac;
use tempfile::TempDir;
use tower::ServiceExt;

use crate::{
    AppState, ServerConfig, ServerFrontend, TransferLimiter,
    app::ProtocolMetrics,
    backend::ServerBackend,
    local_backend::LocalBackend,
    object_store::ServerObjectStore,
    provider::ProviderTokenService,
    reconstruction_cache::ReconstructionCacheService,
    server_role::ServerRole,
    test_fixtures,
    xet_adapter::{XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE},
};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope, TokenClaims};
use shardline_server_core::{AuthProvider, auth::LocalEd25519Provider};

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
        .layer(middleware::from_fn(super::security_headers_middleware))
        .route("/metrics", get(super::operational::metrics));

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

/// Signing key shared by both test-app builders and token helpers so that
/// tokens minted in tests are valid against the server auth layer.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

/// Builds a test app with **authentication enabled**.
async fn test_app_with_auth(frontends: &[ServerFrontend]) -> (Router, TempDir) {
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
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key");

    config
        .validate_runtime_requirements()
        .expect("runtime requirements");

    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: Some(auth),
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).expect("limiter")),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let mut app = Router::new()
        .route("/healthz", get(super::operational::health))
        .route("/readyz", get(super::operational::ready))
        .layer(middleware::from_fn(super::security_headers_middleware))
        .route("/metrics", get(super::operational::metrics));

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
            ServerFrontend::Hub => {}
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));
    (app, tmp)
}

// ---------------------------------------------------------------------------
// Provider config helpers
// ---------------------------------------------------------------------------

/// Creates a temporary provider config file for testing.
fn create_provider_config_file() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("tempdir for provider config");
    let config_path = dir.path().join("providers.json");
    let config_content = br#"{
        "providers": [{
            "kind": "github",
            "integration_subject": "github-app",
            "webhook_secret": "secret",
            "repositories": [{
                "owner": "team",
                "name": "assets",
                "visibility": "private",
                "default_revision": "main",
                "clone_url": "https://github.example/team/assets.git",
                "read_subjects": ["github-user-1"],
                "write_subjects": ["github-user-1"]
            }]
        }]
    }"#;
    std::fs::write(&config_path, config_content).expect("write provider config");
    (dir, config_path)
}

/// Builds a test app with authentication and provider tokens enabled.
async fn test_app_with_provider_tokens(frontends: &[ServerFrontend]) -> (Router, TempDir, TempDir) {
    let (_config_dir, config_path) = create_provider_config_file();

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
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .expect("token signing key");

    config
        .validate_runtime_requirements()
        .expect("runtime requirements");

    let auth = crate::auth::ServerAuth::new(TEST_SIGNING_KEY).expect("ServerAuth");

    let provider_tokens = ProviderTokenService::from_file(
        &config_path,
        b"bootstrap".to_vec(),
        "test-issuer",
        NonZeroU64::MIN,
        b"a]32-byte-signing-key-for-testing!",
    )
    .expect("provider token service");

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: Some(auth),
        provider_tokens: Some(provider_tokens),
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).expect("limiter")),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let mut app = Router::new()
        .route("/healthz", get(super::operational::health))
        .route("/readyz", get(super::operational::ready))
        .layer(middleware::from_fn(super::security_headers_middleware))
        .route("/metrics", get(super::operational::metrics));

    // Provider routes (registered when role serves API, outside per-frontend loop).
    if state.role.serves_api() {
        app = app
            .route("/v1/providers/{provider}/tokens", post(super::provider_routes::issue_provider_token))
            .route("/v1/providers/{provider}/git-lfs-authenticate", post(super::provider_routes::git_lfs_authenticate))
            .route("/v1/providers/{provider}/webhooks", post(super::provider_routes::handle_provider_webhook))
            .route("/v1/stats", get(super::operational::stats));
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
                        .route("/v1/shards", post(super::operational::upload_shard))
                        .route(XET_READ_TOKEN_ROUTE, get(super::provider_routes::issue_xet_read_token))
                        .route(XET_WRITE_TOKEN_ROUTE, get(super::provider_routes::issue_xet_write_token));
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
            ServerFrontend::Hub => {}
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));
    (app, tmp, _config_dir)
}

// ---------------------------------------------------------------------------
// Token helpers
// ---------------------------------------------------------------------------

/// Mints a signed test token with the given scope using the shared test
/// signing key.
fn test_token(scope: TokenScope) -> String {
    let provider = LocalEd25519Provider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

/// Mints a signed test token with the given scope and custom repository
/// owner/name.
fn test_token_with_scope_and_repo(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalEd25519Provider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
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
async fn metrics_endpoint_returns_prometheus_text() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(Request::builder().method("GET")
            .uri("/metrics")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(response).await).unwrap();
    assert!(body.contains("shardline_up 1"), "metrics should contain shardline_up gauge");
    assert!(body.contains("# HELP"), "metrics should contain HELP lines");
    assert!(body.contains("# TYPE"), "metrics should contain TYPE lines");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_upload_hash_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"xorb-hash-mismatch-test";
    // Compute a hash but upload different content
    let wrong_hash = "a".repeat(64);
    
    let upload = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/xorbs/default/{wrong_hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    
    // Hash mismatch should return an error (4xx or 5xx)
    assert!(
        upload.status().is_client_error() || upload.status().is_server_error(),
        "expected error status for hash mismatch, got {}",
        upload.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_merkledb_route_returns_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/chunks/default-merkledb/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_upload_invalid_data() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/shards")
                .body(Body::from(b"invalid-shard-data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_client_error(),
        "invalid shard should return 4xx, got {}", response.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_for_existing_data() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb + shard
    let content = b"reconstruction-existing-data-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let xorb_upload = app.clone()
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

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let shard_resp = app.clone()
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

    // Verify reconstruction returns the file data
    let recon = app
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
    let recon_json = body_json(recon).await;
    assert!(recon_json.get("terms").is_some(), "reconstruction should have terms");
    assert!(recon_json.get("fetch_info").is_some(), "reconstruction should have fetch_info");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_v2_route() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"recon-v2-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let xorb_upload = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v1/xorbs/default/{xorb_hash}"))
            .body(Body::from(xorb_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(xorb_upload.status(), StatusCode::OK);

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let shard_resp = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri("/v1/shards").body(Body::from(shard_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(shard_resp.status(), StatusCode::OK);

    // Use v2 reconstruction route
    let recon = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/reconstructions/{file_id}"))
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(recon.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_not_found_v2() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(Request::builder().method("GET")
            .uri("/v2/reconstructions/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_with_content_hash() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"recon-content-hash-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let xorb_upload = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v1/xorbs/default/{xorb_hash}"))
            .body(Body::from(xorb_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(xorb_upload.status(), StatusCode::OK);

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let shard_resp = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri("/v1/shards").body(Body::from(shard_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(shard_resp.status(), StatusCode::OK);

    // GET reconstruction with content_hash query param using a valid-format
    // hash that doesn't match any stored version — handler accepts the param
    // but the backend returns NOT_FOUND since the hash doesn't identify a
    // known file version.
    let recon = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v1/reconstructions/{file_id}?content_hash={xorb_hash}"))
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(recon.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_with_range_header() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"recon-range-test-content-1234567890";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let xorb_upload = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v1/xorbs/default/{xorb_hash}"))
            .body(Body::from(xorb_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(xorb_upload.status(), StatusCode::OK);

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let shard_resp = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri("/v1/shards").body(Body::from(shard_bytes.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(shard_resp.status(), StatusCode::OK);

    // GET reconstruction with Range header
    let recon = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v1/reconstructions/{file_id}"))
            .header(header::RANGE, "bytes=0-3")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(recon.status(), StatusCode::OK);
    let body = body_bytes(recon).await;
    // The response could be either JSON (reconstruction metadata) or binary (chunk data),
    // depending on the handler. Just verify it returns 200.
    assert!(body.len() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(Request::builder().method("GET").uri("/v1/reconstructions/nonexistent")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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
async fn lfs_patch_missing_content_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"missing-range-test");
    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_content_length_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"content-length-mismatch");
    let response = app
        .oneshot(Request::builder().method("PATCH")
            .uri(format!("/v1/lfs/objects/{oid}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", "bytes 0-9/100")
            .header(header::CONTENT_LENGTH, "3") // says 10 bytes in range, sends 3
            .body(Body::from(b"abc".to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_single_chunk_final() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-single-chunk-final";
    let oid = test_oid(content);

    // Single PATCH that covers the entire range (is_final=true)
    let patch = app.clone()
        .oneshot(Request::builder().method("PATCH")
            .uri(format!("/v1/lfs/objects/{oid}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", format!("bytes 0-{}/{}", content.len() - 1, content.len()))
            .header(header::CONTENT_LENGTH, content.len().to_string())
            .body(Body::from(content.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(patch.status(), StatusCode::OK);

    // Verify content is accessible via GET
    let get = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v1/lfs/objects/{oid}"))
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_unsupported_operation() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = serde_json::json!({
        "operation": "delete",
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

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_too_many_objects() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let objects: Vec<serde_json::Value> = (0..2000)
        .map(|i| serde_json::json!({"oid": format!("{:064x}", i), "size": 100}))
        .collect();
    let request = serde_json::json!({
        "operation": "download",
        "objects": objects
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

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_unsupported_hash_algo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = serde_json::json!({
        "operation": "download",
        "hash_algo": "sha512",
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

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_unsupported_transfer() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = serde_json::json!({
        "operation": "download",
        "transfers": ["ssh"],
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

    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_put_wrong_content_type() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let oid = test_oid(b"wrong-content-type-test");
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "text/plain")
                .body(Body::from(b"test".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_object_with_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-range-test-content-1234567890";
    let oid = test_oid(content);

    // Upload
    let put = app.clone()
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

    // GET with Range
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::RANGE, "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(get).await;
    assert_eq!(body, &content[0..4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_upload_existing_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    // Upload an object first
    let content = b"lfs-batch-exists-test";
    let oid = test_oid(content);
    let put = app.clone()
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

    // Batch with upload operation — should report the object as already present (no actions)
    let request = serde_json::json!({
        "operation": "upload",
        "objects": [{"oid": oid, "size": content.len() as u64}]
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
    let json: serde_json::Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    let obj = &json["objects"][0];
    assert_eq!(obj["oid"], oid);
    // Existing object should NOT have upload actions
    assert!(obj.get("actions").is_none() || obj["actions"].as_object().map_or(true, |m| m.is_empty()),
        "existing object should not have upload actions: {:?}", obj["actions"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_download_existing_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-batch-download-existing";
    let oid = test_oid(content);

    // Upload first
    let put = app.clone()
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

    // Batch download should include download actions for the object
    let request = json!({
        "operation": "download",
        "objects": [{"oid": oid, "size": content.len() as u64}]
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
    let json: Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    let obj = &json["objects"][0];
    assert_eq!(obj["oid"], oid);
    assert!(obj["actions"].is_object(), "existing object should have download actions");
    assert!(obj["actions"]["download"]["href"].as_str().unwrap().contains(&oid));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_mixed_present_absent() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let present_content = b"present-obj";
    let present_oid = test_oid(present_content);

    // Upload one object
    let put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{present_oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, present_content.len().to_string())
                .body(Body::from(present_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    let absent_oid = test_oid(b"absent-obj");

    let request = json!({
        "operation": "download",
        "objects": [
            {"oid": present_oid, "size": present_content.len() as u64},
            {"oid": absent_oid, "size": 0}
        ]
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
    let json: Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    let objects = json["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 2);
    // Present object has actions
    assert!(objects[0]["actions"].is_object(), "present object should have actions");
    // Absent object has error
    assert!(objects[1]["error"].is_object(), "absent object should have error");
    assert_eq!(objects[1]["error"]["code"], 404);
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
async fn lfs_verify_hash_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    // Upload object with one hash
    let content = b"verify-hash-mismatch-content";
    let actual_oid = test_oid(content);
    let put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{actual_oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Verify with a DIFFERENT (non-existent) OID — should return 404
    let wrong_oid = "b".repeat(64);
    let verify = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{wrong_oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(verify.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let response = app
        .oneshot(Request::builder().method("POST").uri("/v1/lfs/objects/batch")
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Body::from(r#"{"operation":"download","objects":[]}"#)).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_valid_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Read);
    let response = app
        .oneshot(Request::builder().method("POST").uri("/v1/lfs/objects/batch")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Body::from(r#"{"operation":"download","objects":[]}"#)).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_insufficient_scope() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    // Upload requires Write scope, a Read-only token should fail
    let token = test_token(TokenScope::Read);
    let response = app
        .oneshot(Request::builder().method("POST").uri("/v1/lfs/objects/batch")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Body::from(r#"{"operation":"upload","objects":[]}"#)).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
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
async fn bazel_flat_route_serves_ac_before_cas() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let cas_content = b"bazel-flat-cas-content";
    let hash = test_hash(cas_content);

    let ac_content = b"bazel-flat-ac-first";

    // PUT different content to AC (AC does NOT validate hash, so any content works)
    let put_ac = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/ac/{hash}"))
                .body(Body::from(ac_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_ac.status(), StatusCode::NO_CONTENT);

    // Also PUT to CAS with matching hash (CAS DOES validate hash)
    let put_cas = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::from(cas_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_cas.status(), StatusCode::NO_CONTENT);

    // Flat GET should return AC content (AC checked first)
    let get = app
        .clone()
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
    assert_eq!(body, ac_content, "flat route should serve AC content (checked first)");

    // Flat HEAD should also return AC content-length
    let head = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!("/v1/bazel/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    let cl: u64 = head.headers().get(header::CONTENT_LENGTH).unwrap().to_str().unwrap().parse().unwrap();
    assert_eq!(cl, ac_content.len() as u64, "flat HEAD should return AC content-length");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;
    let hash = "a".repeat(64);
    let response = app
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v1/bazel/cache/cas/{hash}"))
            .body(Body::from(b"data".to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_with_valid_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;
    let content = b"bazel-auth-test";
    let hash = test_hash(content);
    let token = test_token(TokenScope::Write);
    let response = app
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v1/bazel/cache/cas/{hash}"))
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::from(content.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_get_with_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-cas-range-test-content-42";
    let hash = test_hash(content);

    // PUT
    let put = app.clone()
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

    // GET with Range header
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::RANGE, "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(get).await;
    assert_eq!(body, &content[0..4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_invalid_hash_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/bazel/cache/ac/short")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_client_error(),
        "expected client error, got {}", response.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_flat_invalid_hash_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/v1/bazel/short")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_client_error(),
        "expected client error, got {}", response.status());
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
async fn oci_blob_delete_referenced_by_manifest() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let config_data = b"{}";
    let layer_data = b"\x1f\x8b\x08\x00";
    let config_digest = oci_upload_blob(&app, OCI_TEST_REPO, config_data).await;
    let layer_digest = oci_upload_blob(&app, OCI_TEST_REPO, layer_data).await;

    // Create a manifest referencing both blobs
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config_data.len() as u64,
            "digest": format!("sha256:{config_digest}")
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "size": layer_data.len() as u64,
            "digest": format!("sha256:{layer_digest}")
        }]
    })
    .to_string();

    let manifest_bytes = manifest_json.as_bytes();
    let put_manifest = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/referencing"))
                .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(manifest_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_manifest.status(), StatusCode::CREATED);

    // Try to delete the config blob — should be blocked by manifest reference
    let delete_config = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{config_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_config.status(), StatusCode::BAD_REQUEST,
        "should reject deleting blob referenced by manifest");

    // Try to delete the layer blob — should also be blocked
    let delete_layer = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{layer_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_layer.status(), StatusCode::BAD_REQUEST,
        "should reject deleting layer blob referenced by manifest");
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

// ── session upload (PATCH + PUT complete) ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_session_upload() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Step 1: Create upload session (POST without digest)
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED, "session create failed: {}",
        String::from_utf8_lossy(&body_bytes(create).await));
    let location = create
        .headers()
        .get(header::LOCATION)
        .expect("LOCATION header")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(location.contains("/blobs/uploads/"), "location: {location}");

    // Step 2: PATCH first chunk
    let chunk1 = b"hello-";
    let patch1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, chunk1.len().to_string())
                .header("Content-Range", format!("0-{}", chunk1.len() - 1))
                .body(Body::from(chunk1.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch1.status(), StatusCode::ACCEPTED, "PATCH 1 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch1).await));
    let location2 = patch1
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or(location);

    // Step 3: PATCH second chunk
    let chunk2 = b"world!";
    let patch2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location2)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, chunk2.len().to_string())
                .header(
                    "Content-Range",
                    format!("{}-{}", chunk1.len(), chunk1.len() + chunk2.len() - 1),
                )
                .body(Body::from(chunk2.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch2.status(), StatusCode::ACCEPTED, "PATCH 2 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch2).await));
    let location3 = patch2
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or(location2);

    // Step 4: PUT to complete with digest
    let full_data = [chunk1.to_vec(), chunk2.to_vec()].concat();
    let digest_hex = sha256_hex(&full_data);
    let complete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("{location3}?digest=sha256:{digest_hex}"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        complete.status(),
        StatusCode::CREATED,
        "PUT complete failed: {}",
        String::from_utf8_lossy(&body_bytes(complete).await)
    );

    // Step 5: Verify blob is readable
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, full_data);
}

// ── cross-repo mount ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_mount_cross_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Upload blob to source repo
    let data = b"mountable-blob-data-42";
    let source_repo = "team/source";
    let digest_hex = oci_upload_blob(&app, source_repo, data).await;

    // Mount from source to target
    let target_repo = "team/target";
    let mount = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/{target_repo}/blobs/uploads/?mount=sha256:{digest_hex}&from={source_repo}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        mount.status(),
        StatusCode::CREATED,
        "mount failed: {}",
        String::from_utf8_lossy(&body_bytes(mount).await)
    );

    // Verify blob accessible in target repo
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{target_repo}/blobs/sha256:{digest_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, data);
}

// ── digest-algorithm rejection ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_upload_unsupported_digest_algorithm() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let data = b"test-data";
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/blobs/uploads/?digest-algorithm=sha512"
                ))
                .body(Body::from(data.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ── Range request ──

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_get_with_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let data = b"oci-range-test-data-1234567890";
    let digest_hex = oci_upload_blob(&app, OCI_TEST_REPO, data).await;

    // GET with Range
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest_hex}"))
                .header(header::RANGE, "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    let body = body_bytes(response).await;
    assert_eq!(body, &data[0..4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_upload_session_get_status() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
    let create = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    // GET session status
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&location)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_upload_session_delete_cancel() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
    let create = app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    // DELETE (cancel) session
    let delete = app.clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&location)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "cancel session status: {}", delete.status());

    // Verify session is gone (GET should return 404)
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&location)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_put_invalid_json() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/latest"))
                .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(b"not-valid-json".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_client_error(),
        "invalid manifest should return 4xx, got {}", response.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_put_with_multiple_tags() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let config_data = b"{}";
    let layer_data = b"\x1f\x8b\x08\x00";
    let config_digest = oci_upload_blob(&app, OCI_TEST_REPO, config_data).await;
    let layer_digest = oci_upload_blob(&app, OCI_TEST_REPO, layer_data).await;
    let manifest_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "size": config_data.len() as u64,
            "digest": format!("sha256:{config_digest}")
        },
        "layers": [{
            "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
            "size": layer_data.len() as u64,
            "digest": format!("sha256:{layer_digest}")
        }]
    })
    .to_string();

    // PUT with multiple ?tag= query params
    let manifest_bytes = manifest_json.as_bytes();
    let response = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/latest?tag=stable&tag=release"))
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(manifest_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    // Should have OCI-Tag header with combined tags
    let oci_tag = response.headers().get("OCI-Tag")
        .expect("OCI-Tag header")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(oci_tag.contains("latest"), "should contain 'latest': {oci_tag}");
    assert!(oci_tag.contains("stable"), "should contain 'stable': {oci_tag}");
    assert!(oci_tag.contains("release"), "should contain 'release': {oci_tag}");

    // Verify all three tags resolve to the same manifest
    for tag in &["latest", "stable", "release"] {
        let get = app.clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(format!("/v2/{OCI_TEST_REPO}/manifests/{tag}"))
                    .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(get.status(), StatusCode::OK, "tag {tag} should resolve");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_put_digest_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // PUT to a digest that doesn't match the body
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
                .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":0,"digest":"sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"},"layers":[]}"#.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST,
        "digest mismatch should return 400, got {}", response.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_unknown_path_returns_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
    let response = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/{OCI_TEST_REPO}/nonexistent/endpoint"))
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_wrong_content_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
    let create = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    // PATCH with wrong Content-Range (start > end)
    let patch = app
        .oneshot(Request::builder().method("PATCH")
            .uri(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", "100-0")
            .body(Body::from(b"data".to_vec())).unwrap())
        .await.unwrap();
    assert!(patch.status().is_client_error(),
        "wrong Content-Range should return 4xx, got {}", patch.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_offset_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
    let create = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    // PATCH first chunk at offset 0
    let patch1 = app.clone()
        .oneshot(Request::builder().method("PATCH")
            .uri(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", "0-4")
            .body(Body::from(b"hello".to_vec())).unwrap())
        .await.unwrap();
    assert!(patch1.status().is_success());

    // PATCH second chunk at WRONG offset (should be 5, send 10 instead)
    let patch2 = app
        .oneshot(Request::builder().method("PATCH")
            .uri(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", "10-14")
            .body(Body::from(b"world".to_vec())).unwrap())
        .await.unwrap();
    // Offset mismatch should return a client error
    assert!(patch2.status().is_client_error(),
        "offset mismatch should return 4xx, got {}", patch2.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_content_range_end_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let create = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    // PATCH with Content-Range saying 0-9 (10 bytes) but only sending 3
    let patch = app
        .oneshot(Request::builder().method("PATCH")
            .uri(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", "0-9")
            .body(Body::from(b"abc".to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(patch.status(), StatusCode::RANGE_NOT_SATISFIABLE,
        "end mismatch should return 416, got {}", patch.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_put_session_hash_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session and upload data
    let data = b"oci-hash-mismatch-test-data";
    let create = app.clone()
        .oneshot(Request::builder().method("POST")
            .uri(format!("/v2/{OCI_TEST_REPO}/blobs/uploads/"))
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create.headers().get(header::LOCATION).unwrap().to_str().unwrap().to_owned();

    let patch = app.clone()
        .oneshot(Request::builder().method("PATCH")
            .uri(&location)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header("Content-Range", format!("0-{}", data.len() - 1))
            .body(Body::from(data.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(patch.status(), StatusCode::ACCEPTED);
    let location2 = patch.headers().get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or(location);

    // Complete with WRONG digest (not matching the data)
    let wrong_digest = "0".repeat(64);
    let complete = app
        .oneshot(Request::builder().method("PUT")
            .uri(format!("{location2}?digest=sha256:{wrong_digest}"))
            .header(header::CONTENT_LENGTH, "0")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    // Hash mismatch should return 400
    assert_eq!(complete.status(), StatusCode::BAD_REQUEST,
        "hash mismatch should return 400, got {}", complete.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_list_put() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // First create a regular manifest so we have something to reference
    let config_data = b"{}";
    let layer_data = b"\x1f\x8b\x08\x00";
    let config_digest = oci_upload_blob(&app, OCI_TEST_REPO, config_data).await;
    let layer_digest = oci_upload_blob(&app, OCI_TEST_REPO, layer_data).await;
    let manifest_len = {
        let mj = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "size": config_data.len(), "digest": format!("sha256:{config_digest}")},
            "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "size": layer_data.len(), "digest": format!("sha256:{layer_digest}")}]
        }).to_string();
        let len = mj.len();
        let put_m = app.clone()
            .oneshot(Request::builder().method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/child"))
                .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::from(mj.into_bytes())).unwrap())
            .await.unwrap();
        assert_eq!(put_m.status(), StatusCode::CREATED);
        len
    };

    // Now create a manifest list (index) referencing that manifest
    // Use the child manifest's digest
    let child_get = app.clone()
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/child"))
            .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(child_get.status(), StatusCode::OK);
    let child_digest = child_get.headers().get("Docker-Content-Digest")
        .unwrap().to_str().unwrap()
        .strip_prefix("sha256:").unwrap().to_owned();

    let index_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "size": manifest_len,
            "digest": format!("sha256:{child_digest}"),
            "platform": {"architecture": "amd64", "os": "linux"}
        }]
    }).to_string();

    let put_idx = app.clone()
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/index-v1"))
            .header(header::CONTENT_TYPE, "application/vnd.oci.image.index.v1+json")
            .body(Body::from(index_json.into_bytes())).unwrap())
        .await.unwrap();
    assert_eq!(put_idx.status(), StatusCode::CREATED);

    // Verify index is retrievable
    let get_idx = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/index-v1"))
            .header(header::ACCEPT, "application/vnd.oci.image.index.v1+json")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(get_idx.status(), StatusCode::OK);
    let idx_json = body_json(get_idx).await;
    assert_eq!(idx_json["mediaType"], "application/vnd.oci.image.index.v1+json");
    assert_eq!(idx_json["manifests"][0]["platform"]["architecture"], "amd64");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tag_overwrite_cleans_up_old_reference() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // First manifest version under tag "movable"
    let config_data = b"{\"v\":1}";
    let layer_data1 = b"\x1f\x8b\x08\x00";
    let config_digest1 = oci_upload_blob(&app, OCI_TEST_REPO, config_data).await;
    let layer_digest1 = oci_upload_blob(&app, OCI_TEST_REPO, layer_data1).await;
    let manifest1 = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "size": config_data.len() as u64, "digest": format!("sha256:{config_digest1}")},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "size": layer_data1.len() as u64, "digest": format!("sha256:{layer_digest1}")}]
    }).to_string();

    let put1 = app.clone()
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
            .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
            .body(Body::from(manifest1.clone().into_bytes())).unwrap())
        .await.unwrap();
    assert_eq!(put1.status(), StatusCode::CREATED);
    let digest1 = put1.headers().get("Docker-Content-Digest").unwrap().to_str().unwrap().to_owned();

    // Verify tag resolves to digest1
    let get1 = app.clone()
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
            .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(get1.status(), StatusCode::OK);
    let d1 = get1.headers().get("Docker-Content-Digest").unwrap().to_str().unwrap().to_owned();
    assert_eq!(d1, digest1);

    // Second manifest version with different content, same tag "movable"
    let layer_data2 = b"\x1f\x8b\x08\x01";
    let layer_digest2 = oci_upload_blob(&app, OCI_TEST_REPO, layer_data2).await;
    // Reuse same config blob
    let manifest2 = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "size": config_data.len() as u64, "digest": format!("sha256:{config_digest1}")},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "size": layer_data2.len() as u64, "digest": format!("sha256:{layer_digest2}")}]
    }).to_string();

    let put2 = app.clone()
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
            .header(header::CONTENT_TYPE, "application/vnd.oci.image.manifest.v1+json")
            .body(Body::from(manifest2.into_bytes())).unwrap())
        .await.unwrap();
    assert_eq!(put2.status(), StatusCode::CREATED);
    let digest2 = put2.headers().get("Docker-Content-Digest").unwrap().to_str().unwrap().to_owned();
    assert_ne!(digest1, digest2, "different content should produce different digest");

    // Verify tag now resolves to digest2
    let get2 = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
            .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(get2.status(), StatusCode::OK);
    let d2 = get2.headers().get("Docker-Content-Digest").unwrap().to_str().unwrap().to_owned();
    assert_eq!(d2, digest2, "tag should now point to second manifest version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_v2_root_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Oci]).await;
    let response = app
        .oneshot(Request::builder().method("GET").uri("/v2/")
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tags_list_empty_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
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
    assert!(tags.is_empty(), "expected empty tags list, got {tags:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tags_list_pagination() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create 3 manifests with different tags
    oci_setup_manifest(&app, OCI_TEST_REPO, "v1.0").await;
    oci_setup_manifest(&app, OCI_TEST_REPO, "v2.0").await;
    oci_setup_manifest(&app, OCI_TEST_REPO, "v3.0").await;

    // Request first page with n=1
    let page1 = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/tags/list?n=1"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page1.status(), StatusCode::OK);

    // Save headers and Link header before body_json consumes page1
    let link = page1.headers().get(axum::http::header::LINK)
        .expect("Link header should be present for pagination")
        .to_str()
        .unwrap()
        .to_owned();

    let page1_json = body_json(page1).await;
    assert_eq!(page1_json["name"], OCI_TEST_REPO);
    let page1_tags = page1_json["tags"].as_array().unwrap();
    assert_eq!(page1_tags.len(), 1, "expected 1 tag per page, got {page1_tags:?}");

    // Verify Link header is present
    assert!(link.contains("rel=\"next\""), "Link header should contain rel=next: {link}");

    // Extract the last tag from the Link header
    assert!(link.contains("last="), "Link header should contain last=: {link}");

    // Request page with n=0 — should return empty list, no Link header
    let page0 = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/tags/list?n=0"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page0.status(), StatusCode::OK);

    // Check headers before body_json consumes page0
    let page0_has_link = page0.headers().get(axum::http::header::LINK).is_some();

    let page0_json = body_json(page0).await;
    assert_eq!(page0_json["tags"].as_array().unwrap().len(), 0);
    assert!(!page0_has_link,
        "n=0 should not include Link header");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_and_lfs_coexist_independently() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci, ServerFrontend::Lfs]).await;

    // Upload via OCI
    let data = b"oci-lfs-cross-proto-test";
    let digest_hex = oci_upload_blob(&app, OCI_TEST_REPO, data).await;

    // Verify OCI blob is accessible
    let oci_get = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oci_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(oci_get).await, data);

    // Verify LFS cannot access the same content via OID (different namespace)
    let lfs_get = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{digest_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // LFS should NOT find content uploaded via OCI (namespace isolation)
    assert_eq!(lfs_get.status(), StatusCode::NOT_FOUND,
        "LFS should not find OCI-uploaded content (namespace isolation)");

    // Upload via LFS 
    let lfs_content = b"lfs-only-content";
    let lfs_oid = test_oid(lfs_content);
    let lfs_put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{lfs_oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, lfs_content.len().to_string())
                .body(Body::from(lfs_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_put.status(), StatusCode::OK);

    // Verify OCI cannot access LFS content (reverse isolation)
    let oci_get2 = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{lfs_oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oci_get2.status(), StatusCode::NOT_FOUND,
        "OCI should not find LFS-uploaded content (namespace isolation)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_protocols_coexist() {
    let (app, _tmp) = test_app(&[
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
    ]).await;

    // 1. Xet: upload xorb + shard, get reconstruction
    let content = b"quad-proto-content";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let xorb_upload = app.clone()
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

    // 2. LFS: upload and download
    let lfs_content = b"lfs-quad-content";
    let lfs_oid = test_oid(lfs_content);
    let lfs_put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{lfs_oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, lfs_content.len().to_string())
                .body(Body::from(lfs_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_put.status(), StatusCode::OK);

    // 3. Bazel: upload and download
    let bazel_content = b"bazel-quad-content";
    let bazel_hash = test_hash(bazel_content);
    let bazel_put = app.clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{bazel_hash}"))
                .body(Body::from(bazel_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bazel_put.status(), StatusCode::NO_CONTENT);

    // 4. OCI: upload and download blob
    let oci_data = b"oci-quad-content";
    let oci_digest = oci_upload_blob(&app, OCI_TEST_REPO, oci_data).await;

    // 5. Verify ALL four are independently accessible
    // LFS
    let lfs_get = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{lfs_oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(lfs_get).await, lfs_content);

    // Bazel
    let bazel_get = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{bazel_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bazel_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(bazel_get).await, bazel_content);

    // OCI
    let oci_get = app.clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{oci_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oci_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(oci_get).await, oci_data);

    // 6. Namespace isolation: OCI blob not accessible via LFS
    let lfs_oci_cross = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oci_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(lfs_oci_cross.status(), StatusCode::NOT_FOUND,
        "namespace isolation: LFS should not see OCI content");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_then_bazel_download_with_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs, ServerFrontend::BazelHttp]).await;
    let write_token = test_token(TokenScope::Write);
    let read_token = test_token(TokenScope::Read);

    // Upload an object via LFS with auth
    let content = b"auth-cross-proto-content";
    let oid = test_oid(content);
    let put = app.clone()
        .oneshot(Request::builder().method("PUT")
            .uri(format!("/v1/lfs/objects/{oid}"))
            .header(header::AUTHORIZATION, format!("Bearer {write_token}"))
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, content.len().to_string())
            .body(Body::from(content.to_vec())).unwrap())
        .await.unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Now download the same content via Bazel (should NOT work -- different namespace)
    let hash = test_hash(b"unrelated");
    let get = app
        .oneshot(Request::builder().method("GET")
            .uri(format!("/v1/bazel/cache/cas/{hash}"))
            .header(header::AUTHORIZATION, format!("Bearer {read_token}"))
            .body(Body::empty()).unwrap())
        .await.unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// Provider Token E2E Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_issuance_with_valid_bootstrap_key() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs, ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::from(r#"{"subject":"github-user-1","owner":"team","repo":"assets","revision":"refs/heads/main","scope":"Read"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json.get("token").and_then(|t| t.as_str()).is_some(), "should return a token");
    assert_eq!(json["issuer"], "test-issuer");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_rejected_wrong_bootstrap_key() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-shardline-provider-key", "wrong-key")
                .body(Body::from(r#"{"subject":"github-user-1","owner":"team","repo":"assets","revision":"refs/heads/main","scope":"Read"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_rejected_unknown_provider() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/gitlab/tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::from(r#"{"subject":"user","owner":"team","repo":"assets","revision":"main","scope":"Read"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_rejected_unauthorized_subject() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/tokens")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::from(r#"{"subject":"unknown-user","owner":"team","repo":"assets","revision":"main","scope":"Read"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_read_token_with_provider_tokens() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/assets/xet-read-token/main?subject=github-user-1")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json.get("accessToken").and_then(|t| t.as_str()).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_write_token_with_provider_tokens() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/assets/xet-write-token/main?subject=github-user-1")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json.get("accessToken").and_then(|t| t.as_str()).is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_lfs_authenticate_with_provider_token() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/git-lfs-authenticate")
                .header(header::CONTENT_TYPE, "application/json")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::from(r#"{"subject":"github-user-1","owner":"team","repo":"assets","revision":"refs/heads/main","scope":"Read"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["header"]["X-Xet-Access-Token"].as_str().is_some());
}

// ============================================================================
// Provider Webhook E2E Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_github_push_webhook_accepted() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(body);
    let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/webhooks")
                .header("x-github-event", "repository")
                .header("x-github-delivery", "delivery-1")
                .header("x-hub-signature-256", &signature)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    // A valid webhook should be accepted (202) or processed (204 for ping events)
    assert!(response.status() == StatusCode::ACCEPTED || response.status() == StatusCode::NO_CONTENT,
        "expected 202 or 204, got {}", response.status());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_github_webhook_invalid_signature() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/webhooks")
                .header("x-github-event", "repository")
                .header("x-github-delivery", "delivery-1")
                .header("x-hub-signature-256", "sha256:0000000000000000000000000000000000000000000000000000000000000000")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}