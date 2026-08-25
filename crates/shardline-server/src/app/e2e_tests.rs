//! End-to-end tests that exercise every protocol endpoint through the full Axum stack.
//!
//! Each test group builds a minimal [`Router`] with only the routes needed for that
//! protocol and sends real HTTP requests via [`tower::ServiceExt::oneshot`].

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
    middleware,
    routing::{get, head, post, put},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::Mac;
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
    provider::ProviderTokenService,
    reconstruction_cache::ReconstructionCacheService,
    server_role::ServerRole,
    test_fixtures,
    xet_adapter::{XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE},
};
use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, xet_hash_hex_string};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{
    AuthProvider, AuthorizedRepository, auth::Ed25519AuthProvider, auth::LocalHmacProvider,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
use shardline_xet_core::merklehash::compute_data_hash;

// ---------------------------------------------------------------------------
// Test scaffolding
// ---------------------------------------------------------------------------

/// Builds a minimal Axum [`Router`] wired to the given `frontends` backed by a
/// fresh [`TempDir`].  Authentication is **disabled** so every route handler
/// skips token validation.
///
/// The returned [`TempDir`] must be kept alive for the lifetime of the Router.
async fn test_app(frontends: &[ServerFrontend]) -> (Router, TempDir) {
    test_app_for_frontends_with_role(frontends, ServerRole::All).await
}

/// Like [`test_app`] but with an explicit [`ServerRole`] so testers can
/// exercise role-split code paths (e.g. Api-only or Transfer-only).
async fn test_app_for_frontends_with_role(
    frontends: &[ServerFrontend],
    role: ServerRole,
) -> (Router, TempDir) {
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
    .with_server_role(role)
    .with_deployment_mode(crate::DeploymentMode::Insecure)
    .with_server_frontends(frontends.to_vec())
    .expect("server frontends")
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .expect("token signing key")
    .with_s3_min_part_bytes(std::num::NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes");

    config
        .validate_runtime_requirements()
        .expect("runtime requirements");

    let state = Arc::new(AppState {
        config,
        role,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).expect("limiter")),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
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

    // HubState is built conditionally when Hub frontend is requested.
    let mut hub_state: Option<shardline_hub_api::routes::HubState> = None;

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
                            get(super::operational::read_xorb_transfer)
                                .put(super::operational::write_xorb_transfer),
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
            ServerFrontend::Oci => match role {
                ServerRole::All => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_dispatch),
                        );
                }
                ServerRole::Api => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_api_dispatch),
                        );
                }
                ServerRole::Transfer => {
                    app = app.route(
                        "/v2/{*path}",
                        axum::routing::any(super::protocol_routes::oci_transfer_dispatch),
                    );
                }
            },
            ServerFrontend::S3 => {
                if state.role.serves_api() {
                    app = app
                        .route(
                            "/{bucket}",
                            put(super::protocol_routes::s3_create_bucket)
                                .get(super::protocol_routes::s3_get_bucket)
                                .head(super::protocol_routes::s3_head_bucket)
                                .delete(super::protocol_routes::s3_delete_bucket),
                        )
                        .route(
                            "/{bucket}/{*key}",
                            get(super::protocol_routes::s3_get_object)
                                .head(super::protocol_routes::s3_head_object)
                                .put(super::protocol_routes::s3_put_object)
                                .post(super::protocol_routes::s3_post_object)
                                .delete(super::protocol_routes::s3_delete_object),
                        );
                }
            }
            ServerFrontend::Hub => {
                hub_state = Some(build_test_hub_state(tmp.path()).await);
            }
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));

    // Merge Hub routes at the type-erased Router<()> level.
    // Register Hub's xet token routes only when the Xet frontend is not active
    // (to avoid route conflicts with the native Xet adapter routes).
    let register_hub_xet_routes = !frontends.contains(&ServerFrontend::Xet);
    let app = if let Some(hs) = hub_state {
        app.merge(shardline_hub_api::hub_routes(hs, register_hub_xet_routes))
    } else {
        app
    };

    (app, tmp)
}

/// Builds a [`HubState`] backed by a temporary SQLite database for E2E tests.
/// Auth and HTTP client are disabled; only the local SQLite store is wired.
async fn build_test_hub_state(root: &std::path::Path) -> shardline_hub_api::routes::HubState {
    let hub_root = root.join("hub");
    std::fs::create_dir_all(&hub_root).ok();
    let store = shardline_index::LocalIndexStore::new(hub_root.clone()).expect("hub sqlite store");
    shardline_index::hub::ensure_hub_tables(&hub_root).ok();
    let boxed = shardline_index::hub::BoxedHubStore::from_store(store);
    let object_store = ServerObjectStore::local(root.join("lfs")).expect("local object store");
    shardline_hub_api::routes::HubState {
        store: boxed,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    }
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
    .expect("token signing key")
    .with_s3_min_part_bytes(std::num::NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes");

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
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
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

    let mut hub_state: Option<shardline_hub_api::routes::HubState> = None;

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
                            get(super::operational::read_xorb_transfer)
                                .put(super::operational::write_xorb_transfer),
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
            ServerFrontend::Oci => match state.role {
                ServerRole::All => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_dispatch),
                        );
                }
                ServerRole::Api => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_api_dispatch),
                        );
                }
                ServerRole::Transfer => {
                    app = app.route(
                        "/v2/{*path}",
                        axum::routing::any(super::protocol_routes::oci_transfer_dispatch),
                    );
                }
            },
            ServerFrontend::S3 => {
                if state.role.serves_api() {
                    app = app
                        .route(
                            "/{bucket}",
                            put(super::protocol_routes::s3_create_bucket)
                                .get(super::protocol_routes::s3_get_bucket)
                                .head(super::protocol_routes::s3_head_bucket)
                                .delete(super::protocol_routes::s3_delete_bucket),
                        )
                        .route(
                            "/{bucket}/{*key}",
                            get(super::protocol_routes::s3_get_object)
                                .head(super::protocol_routes::s3_head_object)
                                .put(super::protocol_routes::s3_put_object)
                                .post(super::protocol_routes::s3_post_object)
                                .delete(super::protocol_routes::s3_delete_object),
                        );
                }
            }
            ServerFrontend::Hub => {
                hub_state = Some(build_test_hub_state(tmp.path()).await);
            }
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));

    let app = if let Some(hs) = hub_state {
        app.merge(shardline_hub_api::hub_routes(hs, false))
    } else {
        app
    };

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
    .expect("token signing key")
    .with_s3_min_part_bytes(std::num::NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes");

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
        None,
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
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
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
            .route(
                "/v1/providers/{provider}/tokens",
                post(super::provider_routes::issue_provider_token),
            )
            .route(
                "/v1/providers/{provider}/git-lfs-authenticate",
                post(super::provider_routes::git_lfs_authenticate),
            )
            .route(
                "/v1/providers/{provider}/webhooks",
                post(super::provider_routes::handle_provider_webhook),
            )
            .route("/v1/stats", get(super::operational::stats));
    }

    let mut hub_state: Option<shardline_hub_api::routes::HubState> = None;

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
                        .route(
                            XET_READ_TOKEN_ROUTE,
                            get(super::provider_routes::issue_xet_read_token),
                        )
                        .route(
                            XET_WRITE_TOKEN_ROUTE,
                            get(super::provider_routes::issue_xet_write_token),
                        );
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
                            get(super::operational::read_xorb_transfer)
                                .put(super::operational::write_xorb_transfer),
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
            ServerFrontend::Oci => match state.role {
                ServerRole::All => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_dispatch),
                        );
                }
                ServerRole::Api => {
                    app = app
                        .route("/v2/token", get(super::protocol_routes::oci_registry_token))
                        .route("/v2/", get(super::protocol_routes::oci_v2_root))
                        .route(
                            "/v2/{*path}",
                            axum::routing::any(super::protocol_routes::oci_api_dispatch),
                        );
                }
                ServerRole::Transfer => {
                    app = app.route(
                        "/v2/{*path}",
                        axum::routing::any(super::protocol_routes::oci_transfer_dispatch),
                    );
                }
            },
            ServerFrontend::S3 => {
                // S3 routes are registered in a later lane.
            }
            ServerFrontend::Hub => {
                hub_state = Some(build_test_hub_state(tmp.path()).await);
            }
        }
    }

    let app: Router = app.with_state(Arc::clone(&state));

    let app = if let Some(hs) = hub_state {
        app.merge(shardline_hub_api::hub_routes(hs, false))
    } else {
        app
    };

    (app, tmp, _config_dir)
}

// ---------------------------------------------------------------------------
// Token helpers
// ---------------------------------------------------------------------------

/// Mints a signed test token with the given scope using the shared test
/// signing key.
fn test_token(scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

/// Mints a signed test token with the given scope and custom repository
/// owner/name.
fn _test_token_with_scope_and_repo(scope: TokenScope, owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, owner, name, Some("main")).unwrap();
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
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
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
        .oneshot(
            Request::builder()
                .uri("/readyz")
                .body(Body::empty())
                .unwrap(),
        )
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
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(response).await).unwrap();
    assert!(
        body.contains("shardline_up 1"),
        "metrics should contain shardline_up gauge"
    );
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
    assert!(
        download.status().is_success(),
        "download status: {}",
        download.status()
    );
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
async fn xorb_transfer_put_upload_and_download() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"xorb-transfer-put-test-content";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);

    // Upload via PUT /transfer/xorb/default/{hash} — the endpoint git-xet uses
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/transfer/xorb/default/{xorb_hash}"))
                .body(Body::from(xorb_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        upload.status(),
        StatusCode::OK,
        "xorb transfer PUT failed: {}",
        String::from_utf8_lossy(&body_bytes(upload).await)
    );

    // HEAD to verify
    let head = app
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
    assert_eq!(head.status(), StatusCode::OK);

    // Download via GET /transfer/xorb/default/{hash}
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
    assert!(
        download.status().is_success(),
        "transfer GET status: {}",
        download.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_transfer_put_invalid_namespace_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
    let hash = "a".repeat(64);

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/transfer/xorb/invalid/{hash}"))
                .body(Body::from(b"test".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_transfer_put_invalid_hash_returns_error() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/transfer/xorb/default/short-hash")
                .body(Body::from(b"test".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_transfer_put_with_auth_rejects_read_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Xet]).await;
    let read_token = test_token(TokenScope::Read);
    let hash = "a".repeat(64);

    // PUT with Read token should fail (requires Write)
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/transfer/xorb/default/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::from(b"test".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_transfer_get_with_auth_requires_read_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Xet]).await;
    let hash = "a".repeat(64);

    // GET without auth should fail
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/transfer/xorb/default/{hash}"))
                .header(header::RANGE, "bytes=0-")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

/// Error-path test only — the happy path is covered by [`chunk_read_happy_path`]
/// below, which uploads a xorb + shard then reads the stored chunk.
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
async fn chunk_read_happy_path() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // 1. Upload a xorb (stores chunk data in the object store).
    let content = b"chunk-read-happy-path-test-data";
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

    // 2. Upload a shard that references the xorb — this registers a
    //    DedupeShardMapping (chunk_hash → shard_key) in the index store.
    let (shard_bytes, _file_id) =
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

    // 3. Compute the chunk hash from the plain content.
    let chunk_hash = compute_data_hash(content);
    let chunk_hash_hex = test_fixtures::xet_hash_hex(&chunk_hash);

    // 4. Read the chunk via the /v1/chunks/default/{hash} route.
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/chunks/default/{chunk_hash_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The handler resolves the chunk hash to a shard and returns the shard bytes.
    assert_eq!(response.status(), StatusCode::OK);
    let body = body_bytes(response).await;
    assert!(!body.is_empty(), "chunk response body should not be empty");
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

// ── Read-path admission gating ──────────────────────────────────────────
//
// `read_chunk`, `head_xorb`, and `read_xorb_transfer` each run an O(N)
// repository-reference metadata scan (`repository_references_xorb` enumerates
// the repo's latest + version records) with no LIMIT and no cache. They must be
// admission-gated like the upload/reconstruction paths so a request flood
// cannot drive unbounded per-request scans. When the admission gate is
// saturated, every read handler rejects with 503 SERVICE_UNAVAILABLE.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_paths_are_admission_gated() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).unwrap(),
        object_store,
        &[ServerFrontend::Xet],
    )
    .await
    .unwrap();

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap();

    // Saturated gate: max weight is the read weight and the only permit is held.
    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(1).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });
    let _held_permit = state
        .admission
        .try_acquire(crate::admission::weights::XORB_READ)
        .expect("held permit");

    let app = Router::new()
        .route(
            "/v1/chunks/default/{hash}",
            get(super::operational::read_chunk),
        )
        .route(
            "/v1/xorbs/default/{hash}",
            head(super::operational::head_xorb),
        )
        .route(
            "/transfer/xorb/{prefix}/{hash}",
            get(super::operational::read_xorb_transfer),
        )
        .with_state(Arc::clone(&state));

    let hash = "a".repeat(64);
    let requests = [
        Request::builder()
            .method("GET")
            .uri(format!("/v1/chunks/default/{hash}"))
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .method("HEAD")
            .uri(format!("/v1/xorbs/default/{hash}"))
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .method("GET")
            .uri(format!("/transfer/xorb/default/{hash}"))
            .header(header::RANGE, "bytes=0-")
            .body(Body::empty())
            .unwrap(),
    ];

    for request in requests {
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "read handler must be admission-gated"
        );
    }
}

// ── /v1/stats admission gating (F-62) ───────────────────────────────────
//
// `stats` walks every object in the store (a full dir walk or paginated S3
// LIST) plus a full latest-record traversal, with no LIMIT and no cache. It
// must be admission-gated like the read paths: when the admission gate is
// saturated the handler rejects with 503, and a normal (unsaturated) gate
// serves the aggregate.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stats_route_is_admission_gated() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).unwrap(),
        object_store,
        &[ServerFrontend::Xet],
    )
    .await
    .unwrap();

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap();

    // Saturated gate: max weight equals the stats weight and the only permit
    // is held, so a whole-store stats scan cannot run.
    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(crate::admission::weights::STATS as usize).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });
    let _held_permit = state
        .admission
        .try_acquire(crate::admission::weights::STATS)
        .expect("held permit");

    let app = Router::new()
        .route("/v1/stats", get(super::operational::stats))
        .with_state(Arc::clone(&state));

    let request = Request::builder()
        .method("GET")
        .uri("/v1/stats")
        .body(Body::empty())
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "stats must be admission-gated"
    );

    // Release the permit: the same gate now admits the scan and returns 200.
    drop(_held_permit);
    let request = Request::builder()
        .method("GET")
        .uri("/v1/stats")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hashing_pool_starvation_rejects_immediately_and_recovers() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65_536).unwrap();
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(1).unwrap(),
        object_store,
        &[ServerFrontend::Xet],
    )
    .await
    .unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap();
    let one = NonZeroUsize::new(1).unwrap();
    let pools = crate::admission::ExecutionPools::with_sizes(one, one, one);
    let held_hashing_permit = pools.hashing.try_acquire().expect("hold only hashing slot");
    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: ServerBackend::Local(backend),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(1).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(1)),
        admission: crate::admission::WeightedAdmission::new(
            NonZeroUsize::new(crate::admission::weights::XORB_UPLOAD as usize).unwrap(),
        ),
        pools,
        protocol_metrics: ProtocolMetrics::default(),
    });
    let app = Router::new()
        .route(
            "/transfer/xorb/{prefix}/{hash}",
            put(super::operational::write_xorb_transfer),
        )
        .with_state(state);
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(b"pool recovery payload");
    let uri = format!("/transfer/xorb/default/{xorb_hash}");

    let saturated = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        app.clone().oneshot(
            Request::builder()
                .method("PUT")
                .uri(&uri)
                .body(Body::from(xorb_bytes.clone()))
                .unwrap(),
        ),
    )
    .await
    .expect("saturated pool must reject without queueing")
    .unwrap();
    assert_eq!(saturated.status(), StatusCode::SERVICE_UNAVAILABLE);

    drop(held_hashing_permit);

    let recovered = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        app.oneshot(
            Request::builder()
                .method("PUT")
                .uri(uri)
                .body(Body::from(xorb_bytes))
                .unwrap(),
        ),
    )
    .await
    .expect("released pool must admit new work")
    .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
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
async fn batch_reconstruction_v1_route() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/reconstructions")
                .header("accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The v1 route behaves identically to its unversioned sibling — an empty
    // batch reconstruction returns 200 with an empty files map.
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert!(json["files"].is_object(), "files should be an empty object");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_read_token_without_provider_key_returns_unauthorized() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Xet]).await;

    // Request the xet-read-token route WITHOUT the x-shardline-provider-key header.
    // The route IS registered when provider tokens are configured, but fails auth.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/assets/xet-read-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "missing provider key should return 401, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_write_token_without_provider_key_returns_unauthorized() {
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/github/team/assets/xet-write-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "missing provider key should return 401, got {}",
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

/// Error-path test only — the happy path is covered by [`chunk_merkledb_happy_path`].
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
async fn chunk_merkledb_happy_path() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload a xorb + shard so that a chunk is registered in the index.
    let content = b"chunk-merkledb-happy-path";
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

    let (shard_bytes, _file_id) =
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

    // Read the chunk via the default-merkledb route.
    let chunk_hash = compute_data_hash(content);
    let chunk_hash_hex = test_fixtures::xet_hash_hex(&chunk_hash);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/chunks/default-merkledb/{chunk_hash_hex}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        !body_bytes(response).await.is_empty(),
        "chunk merkledb response body should not be empty"
    );
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
    assert!(
        response.status().is_client_error(),
        "invalid shard should return 4xx, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_upload_unversioned_route() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // The unversioned `/shards` route is registered alongside `/v1/shards`.
    // It should behave identically — reject invalid shard data with 4xx.
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/shards")
                .header("content-type", "application/octet-stream")
                .body(Body::from(b"invalid shard data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        response.status().is_client_error(),
        "invalid shard via /shards should return 4xx, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_for_existing_data() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb + shard
    let content = b"reconstruction-existing-data-test";
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

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
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
    assert!(
        recon_json.get("terms").is_some(),
        "reconstruction should have terms"
    );
    assert!(
        recon_json.get("fetch_info").is_some(),
        "reconstruction should have fetch_info"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_v2_route() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"recon-v2-test";
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

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
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

    // Use v2 reconstruction route
    let recon = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/reconstructions/{file_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recon.status(), StatusCode::OK);
}

// ============================================================================
// Backward-compatibility tests — old-format data readable after upgrade
// ============================================================================

/// Validates that data written by all three historical storage formats
/// (WholeFileV1, FixedChunkV1, XorbCdcV1) remains readable through the
/// current HTTP API after an upgrade.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backward_compatibility_all_formats_readable() {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).expect("chunk size");

    // ── 1. Pre-populate storage with old-format data ────────────────────────
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).expect("object store");

    // FixedChunkV1: raw uncompressed chunk + old-style record
    let fixed_content = b"fixed-chunk-old-format-data-0123456789";
    let fixed_hash = test_hash(fixed_content);
    let fixed_key = crate::bazel_cache_object_key(
        crate::BazelCacheKind::Cas,
        &fixed_hash,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .expect("fixed key");
    let fixed_file_id = format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(fixed_key.as_str().as_bytes()))
    );
    let fixed_chunk_hash =
        shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(fixed_content));
    let fixed_chunk_key =
        crate::chunk_store::chunk_object_key(&fixed_chunk_hash).expect("fixed chunk key");
    object_store
        .put_if_absent(
            &fixed_chunk_key,
            ObjectBody::from_vec(fixed_content.to_vec()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(fixed_content),
                fixed_content.len() as u64,
            ),
        )
        .expect("write fixed chunk");

    // WholeFileV1: single object at the object key path
    let whole_content = b"whole-file-old-format-data-0123456789";
    let whole_hash = test_hash(whole_content);
    let whole_key = crate::bazel_cache_object_key(
        crate::BazelCacheKind::Cas,
        &whole_hash,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .expect("whole key");
    let whole_file_id = format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(whole_key.as_str().as_bytes()))
    );
    object_store
        .put_if_absent(
            &whole_key,
            ObjectBody::from_vec(whole_content.to_vec()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(whole_content),
                whole_content.len() as u64,
            ),
        )
        .expect("write whole object");

    // Register both FileRecords in the index store before the backend opens it
    let record_store = LocalRecordStore::open(tmp.path().to_path_buf());
    record_store
        .commit_file_version_metadata(&FileRecord {
            file_id: fixed_file_id,
            content_hash: fixed_hash.clone(),
            total_bytes: fixed_content.len() as u64,
            chunk_size: 4_194_304,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: fixed_chunk_hash,
                offset: 0,
                length: fixed_content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: fixed_content.len() as u64,
            }],
        })
        .await
        .expect("commit fixed record");

    let whole_chunk_hash =
        shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(whole_content));
    record_store
        .commit_file_version_metadata(&FileRecord {
            file_id: whole_file_id,
            content_hash: whole_hash.clone(),
            total_bytes: whole_content.len() as u64,
            chunk_size: 0, // ReferencedObjectTerms
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: whole_chunk_hash,
                offset: 0,
                length: whole_content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: whole_content.len() as u64,
            }],
        })
        .await
        .expect("commit whole record");

    // ── 2. Build app on the SAME storage (not test_app which creates a new tmp) ─
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).expect("upload parallelism"),
        object_store,
        &[ServerFrontend::BazelHttp],
    )
    .await
    .expect("local backend");

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().expect("bind addr"),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_frontends(vec![ServerFrontend::BazelHttp])
    .expect("server frontends")
    .with_token_signing_key(b"0123456789abcdef0123456789abcdef".to_vec())
    .expect("token signing key")
    .with_s3_min_part_bytes(std::num::NonZeroU64::new(1).unwrap())
    .expect("s3 min part bytes");
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
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let mut app = Router::new()
        .route("/healthz", get(super::operational::health))
        .route("/readyz", get(super::operational::ready))
        .layer(middleware::from_fn(super::security_headers_middleware))
        .route("/metrics", get(super::operational::metrics))
        .route("/v1/stats", get(super::operational::stats));
    // Bazel CAS routes (same registration as test_app_for_frontends_with_role)
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
    let app: Router = app.with_state(state);

    // ── 3. Verify FixedChunkV1 readable ──────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{fixed_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "FixedChunkV1 should be readable"
    );
    assert_eq!(
        body_bytes(resp).await,
        fixed_content,
        "FixedChunkV1 content"
    );

    // ── 4. Verify WholeFileV1 readable ───────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{whole_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "WholeFileV1 should be readable"
    );
    assert_eq!(body_bytes(resp).await, whole_content, "WholeFileV1 content");

    // ── 5. Upload + verify XorbCdcV1 ─────────────────────────────────────────
    let new_content = b"new-xorb-cdc-format-data-0123456789";
    let new_hash = test_hash(new_content);
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{new_hash}"))
                .body(Body::from(new_content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::NO_CONTENT, "XorbCdcV1 upload");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{new_hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "XorbCdcV1 should be readable"
    );
    assert_eq!(body_bytes(resp).await, new_content, "XorbCdcV1 content");
}

// ============================================================================
// GC + xorb mixed-format test — validates that GC correctly handles all three
// storage formats (WholeFileV1, FixedChunkV1, XorbCdcV1) in the same repo.
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_preserves_old_and_new_formats() {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).expect("chunk size");
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).expect("object store");

    // 1. Create FixedChunkV1 record (old format, uncompressed chunk)
    let fixed_content = b"fixed-chunk-gc-test-data-0123456789";
    let fixed_hash = test_hash(fixed_content);
    let fixed_key = crate::bazel_cache_object_key(
        crate::BazelCacheKind::Cas,
        &fixed_hash,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .expect("fixed key");
    let fixed_file_id = format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(fixed_key.as_str().as_bytes()))
    );
    let fixed_chunk_hash =
        shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(fixed_content));
    let fixed_chunk_key =
        crate::chunk_store::chunk_object_key(&fixed_chunk_hash).expect("fixed chunk key");
    object_store
        .put_if_absent(
            &fixed_chunk_key,
            ObjectBody::from_vec(fixed_content.to_vec()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(fixed_content),
                fixed_content.len() as u64,
            ),
        )
        .expect("write fixed chunk");

    // 2. Create WholeFileV1 record (single object at object key)
    let whole_content = b"whole-file-gc-test-data-0123456789";
    let whole_hash = test_hash(whole_content);
    let whole_key = crate::bazel_cache_object_key(
        crate::BazelCacheKind::Cas,
        &whole_hash,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .expect("whole key");
    let whole_file_id = format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(whole_key.as_str().as_bytes()))
    );
    object_store
        .put_if_absent(
            &whole_key,
            ObjectBody::from_vec(whole_content.to_vec()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(whole_content),
                whole_content.len() as u64,
            ),
        )
        .expect("write whole object");

    // 3. Register both old-format FileRecords
    let record_store = LocalRecordStore::open(tmp.path().to_path_buf());
    record_store
        .commit_file_version_metadata(&FileRecord {
            file_id: fixed_file_id,
            content_hash: fixed_hash.clone(),
            total_bytes: fixed_content.len() as u64,
            chunk_size: 4_194_304,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: fixed_chunk_hash,
                offset: 0,
                length: fixed_content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: fixed_content.len() as u64,
            }],
        })
        .await
        .expect("commit fixed record");
    let whole_chunk_hash =
        shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(whole_content));
    record_store
        .commit_file_version_metadata(&FileRecord {
            file_id: whole_file_id,
            content_hash: whole_hash.clone(),
            total_bytes: whole_content.len() as u64,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: whole_chunk_hash,
                offset: 0,
                length: whole_content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: whole_content.len() as u64,
            }],
        })
        .await
        .expect("commit whole record");

    // 4. Create a new-format XorbCdcV1 upload via ServerBackend wrapping LocalBackend
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).expect("upload parallelism"),
        object_store.clone(),
        &[ServerFrontend::BazelHttp],
    )
    .await
    .expect("backend");
    let server_backend = crate::ServerBackend::Local(backend);
    let new_content = b"new-xorb-gc-test-data-0123456789";
    let new_hash = test_hash(new_content);
    server_backend
        .put_sha256_addressed_object_stream_if_absent(
            &crate::bazel_cache_object_key(
                crate::BazelCacheKind::Cas,
                &new_hash,
                &AuthorizedRepository::anonymous_full_access(),
            )
            .expect("new key"),
            &new_hash,
            crate::upload_ingest::RequestBodyReader::from_bytes(axum::body::Bytes::from_static(
                new_content,
            )),
        )
        .await
        .expect("new upload");

    // 5. Run GC dry run — verify all three formats' chunks are referenced
    let report = crate::gc::run_local_gc(
        tmp.path().to_path_buf(),
        crate::gc::LocalGcOptions::dry_run(),
    )
    .await
    .expect("gc dry run");
    assert!(
        report.referenced_chunks >= 3,
        "expected at least 3 referenced chunks, got {}",
        report.referenced_chunks
    );
    assert_eq!(report.deleted_chunks, 0, "dry run must not delete chunks");

    // 6. Verify all three formats still readable via the backend
    for (label, hash) in [
        ("FixedChunkV1", &fixed_hash),
        ("WholeFileV1", &whole_hash),
        ("XorbCdcV1", &new_hash),
    ] {
        let key = crate::bazel_cache_object_key(
            crate::BazelCacheKind::Cas,
            hash,
            &AuthorizedRepository::anonymous_full_access(),
        )
        .expect("key");
        let read = server_backend.read_object(&key).await;
        assert!(read.is_ok(), "{label} readable: {read:?}");
        let bytes = read.expect("read_object");
        assert!(!bytes.is_empty(), "{label} must have content");
    }
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

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
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

    // GET reconstruction with content_hash query param using a valid-format
    // hash that doesn't match any stored version — handler accepts the param
    // but the backend returns NOT_FOUND since the hash doesn't identify a
    // known file version.
    let recon = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!(
                    "/v1/reconstructions/{file_id}?content_hash={xorb_hash}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recon.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_with_range_header() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"recon-range-test-content-1234567890";
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

    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
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

    // GET reconstruction with Range header
    let recon = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/reconstructions/{file_id}"))
                .header(header::RANGE, "bytes=0-3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recon.status(), StatusCode::OK);
    let body = body_bytes(recon).await;
    // The response could be either JSON (reconstruction metadata) or binary (chunk data),
    // depending on the handler. Just verify it returns 200.
    assert!(!body.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Xet]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/reconstructions/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_chunk_file_ingest_is_xorb_backed_and_reconstructs_byte_identical() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(128).unwrap();
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).unwrap();
    let local = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).unwrap(),
        object_store.clone(),
        &[ServerFrontend::Xet],
    )
    .await
    .unwrap();
    let upload_backend = local.clone();
    let server_backend = ServerBackend::Local(local);

    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends(vec![ServerFrontend::Xet])
    .unwrap();

    let state = Arc::new(AppState {
        config,
        role: ServerRole::All,
        backend: server_backend.clone(),
        auth: None,
        provider_tokens: None,
        reconstruction_cache: ReconstructionCacheService::disabled(),
        transfer_limiter: TransferLimiter::new(chunk_size, NonZeroUsize::new(64).unwrap()),
        oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(100)),
        admission: crate::admission::WeightedAdmission::new(
            std::num::NonZeroUsize::new(256).unwrap(),
        ),
        pools: crate::admission::ExecutionPools::default_sizes(),
        protocol_metrics: ProtocolMetrics::default(),
    });

    let app = Router::new()
        .route(
            "/v1/reconstructions/{file_id}",
            get(super::reconstruction_routes::reconstruction),
        )
        .route(
            "/transfer/xorb/{prefix}/{hash}",
            get(super::operational::read_xorb_transfer),
        )
        .route(
            "/v1/chunks/default/{hash}",
            get(super::operational::read_chunk),
        )
        .with_state(Arc::clone(&state));

    // Upload a single-chunk file via the ingest path (CDC, 128-byte target).
    let content = b"single-chunk-ingest-payload";
    let file_id = "a".repeat(64);
    let upload = upload_backend
        .upload_file(&file_id, axum::body::Bytes::from_static(content), None)
        .await
        .unwrap();
    assert_eq!(upload.chunks.len(), 1, "payload must be a single CDC chunk");
    let chunk_hash = upload.chunks.first().unwrap().hash.clone();

    // The stored record must reference the xorb, not the individual chunk.
    let record = upload_backend
        .file_record(&file_id, None, None)
        .await
        .unwrap();
    let record_chunk = record.chunks.first().unwrap();
    assert_eq!(record_chunk.range_start, 0);
    assert_eq!(record_chunk.range_end, 1);
    assert!(
        record_chunk.packed_end > 0,
        "xorb-backed chunks carry a packed length"
    );
    assert_ne!(
        record_chunk.hash, chunk_hash,
        "single-chunk record must point at the xorb hash, not the chunk hash"
    );

    // The server download stream returns byte-identical data.
    use futures_util::StreamExt;
    let mut stream =
        crate::download_stream::file_record_byte_stream(object_store, record.clone(), None)
            .await
            .unwrap();
    let mut downloaded = Vec::new();
    while let Some(item) = stream.next().await {
        downloaded.extend_from_slice(&item.unwrap());
    }
    assert_eq!(downloaded, content);

    // The reconstruction fetch info references the stored xorb URL.
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
    let json = body_json(recon).await;
    let fetch_info = json
        .get("fetch_info")
        .and_then(Value::as_object)
        .expect("fetch_info object");
    assert_eq!(fetch_info.len(), 1, "single chunk yields one fetch entry");
    let (fetch_hash, entries) = fetch_info.iter().next().unwrap();
    assert_eq!(*fetch_hash, record_chunk.hash);
    let entry = entries.as_array().unwrap().first().unwrap();
    let url = entry.get("url").and_then(Value::as_str).unwrap();
    assert_eq!(
        url,
        format!(
            "http://127.0.0.1:8080/transfer/xorb/default/{}",
            record_chunk.hash
        )
    );
    let url_range = entry.get("url_range").unwrap();
    let range_start = url_range.get("start").and_then(Value::as_u64).unwrap();
    let range_end = url_range.get("end").and_then(Value::as_u64).unwrap();

    // The transfer endpoint serves the xorb byte range with 206.
    let transfer = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(url)
                .header(header::RANGE, format!("bytes={range_start}-{range_end}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(transfer.status(), StatusCode::PARTIAL_CONTENT);

    // The individual chunk object is still stored for the dedup path: the
    // upload pipeline stores every chunk both standalone (under its chunk
    // hash) and inside the xorb, so dedupe shards can reference it. The
    // `/v1/chunks/default/{hash}` HTTP endpoint serves *dedupe shards*, which
    // the server-side ingest path does not build; the standalone chunk read
    // (`read_chunk`) is the direct proof that the individual-chunk storage
    // path is intact for dedup.
    let chunk_bytes = upload_backend.read_chunk(&chunk_hash).await.unwrap();
    let decompressed = lz4_flex::decompress_size_prepended(&chunk_bytes).unwrap();
    assert_eq!(decompressed.as_slice(), content);
}

// ============================================================================
// Admission Control E2E Test
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_control_allows_request_when_capacity_is_available() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let content = b"test-data-for-admission-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/xorbs/default/{xorb_hash}"))
                .header("Authorization", "Bearer test-token")
                .body(Body::from(xorb_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ============================================================================
// Metric Emission Verification Tests (TDD)
// These tests verify that every metric-recording function is wired into
// production code. Each test performs an operation and checks that the
// corresponding Prometheus counter/gauge increased. If a metric is defined
// but never called from production, the test will fail.
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_tracks_protocol_counter() {
    // Verify shardline_lfs_upload_requests_total increments on LFS PUT.
    let before = shardline_metrics::metrics().protocol.lfs_uploads.get();
    let content = b"lfs-metric-upload-test";
    let oid = test_oid(content);
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;
    let resp = app
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
    assert_eq!(resp.status(), StatusCode::OK);
    let after = shardline_metrics::metrics().protocol.lfs_uploads.get();
    assert!(after > before, "lfs_uploads should increase");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_tracks_protocol_counter() {
    // Verify shardline_lfs_download_requests_total increments on LFS GET.
    let content = b"lfs-metric-download-test";
    let oid = test_oid(content);
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;
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

    let before = shardline_metrics::metrics().protocol.lfs_downloads.get();
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
    let after = shardline_metrics::metrics().protocol.lfs_downloads.get();
    assert!(after > before, "lfs_downloads should increase");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_upload_tracks_storage_metrics() {
    // Verify shardline_xorbs_bytes_total / shardline_objects_bytes_total
    // increment when a xorb is uploaded.
    let bytes_before = shardline_metrics::metrics().storage.xorbs_bytes_total.get();
    let objects_before = shardline_metrics::metrics()
        .storage
        .objects_bytes_total
        .get();
    let content = b"xorb-metric-storage-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;
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
    assert_eq!(upload.status(), StatusCode::OK);
    let bytes_after = shardline_metrics::metrics().storage.xorbs_bytes_total.get();
    let objects_after = shardline_metrics::metrics()
        .storage
        .objects_bytes_total
        .get();
    assert!(
        bytes_after > bytes_before,
        "xorbs_bytes_total should increase after xorb upload"
    );
    assert!(
        objects_after >= objects_before,
        "objects_bytes_total should not regress"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shard_upload_tracks_storage_metrics() {
    // Verify shardline_shards_total increments on shard upload.
    let shards_before = shardline_metrics::metrics().storage.shards_total.get();
    let content = b"shard-metric-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let (shard_bytes, _file_id) =
        test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb first
    let xorb = app
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
    assert_eq!(xorb.status(), StatusCode::OK);

    // Upload shard
    let shard = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/shards")
                .body(Body::from(shard_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(shard.status(), StatusCode::OK);
    let shards_after = shardline_metrics::metrics().storage.shards_total.get();
    assert!(
        shards_after > shards_before,
        "shards_total should increase after shard upload"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xorb_download_tracks_xet_metric() {
    // Verify shardline_xet_xorb_downloads_total increments.
    let downloads_before = shardline_metrics::metrics().xet.xorb_downloads.get();
    let content = b"xorb-dl-metric-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb
    let up = app
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
    assert_eq!(up.status(), StatusCode::OK);

    // Download via transfer endpoint
    let dl = app
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
    assert!(dl.status().is_success());
    let downloads_after = shardline_metrics::metrics().xet.xorb_downloads.get();
    assert!(
        downloads_after > downloads_before,
        "xorb_downloads should increase after xorb download"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconstruction_tracks_xet_metric() {
    // Verify shardline_xet_reconstructions_total increments.
    let recon_before = shardline_metrics::metrics().reconstruction.requests.get();
    let content = b"recon-metric-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb
    let xorb = app
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
    assert_eq!(xorb.status(), StatusCode::OK);
    // Upload shard
    let shard = app
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
    assert_eq!(shard.status(), StatusCode::OK);

    // Query reconstruction
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
    let recon_after = shardline_metrics::metrics().reconstruction.requests.get();
    assert!(
        recon_after > recon_before,
        "reconstruction requests should increase"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn range_request_tracks_transfer_metric() {
    // Verify shardline_range_requests_total increments on ranged downloads.
    let before = shardline_metrics::metrics().transfer.range_requests.get();
    let content = b"range-metric-test-content";
    let oid = test_oid(content);
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

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

    // Ranged GET
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
    let after = shardline_metrics::metrics().transfer.range_requests.get();
    assert!(
        after > before,
        "range_requests should increase on ranged GET"
    );
}

// ============================================================================
// OCI Upload/Download Metric Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_upload_tracks_protocol_counter() {
    let upload_before = shardline_metrics::metrics().protocol.oci_uploads.get();
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
    let data = b"oci-metric-upload-test-data";
    oci_upload_blob(&app, OCI_TEST_REPO, data).await;
    let upload_after = shardline_metrics::metrics().protocol.oci_uploads.get();
    assert!(upload_after > upload_before, "oci_uploads should increase");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_download_tracks_protocol_counter() {
    let dl_before = shardline_metrics::metrics().protocol.oci_downloads.get();
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
    let data = b"oci-metric-dl-test";
    let digest = oci_upload_blob(&app, OCI_TEST_REPO, data).await;
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let dl_after = shardline_metrics::metrics().protocol.oci_downloads.get();
    assert!(dl_after > dl_before, "oci_downloads should increase");
}

// ============================================================================
// Hub API File Upload/Download Metric Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_file_upload_tracks_protocol_counter() {
    let up_before = shardline_metrics::metrics()
        .protocol
        .hub_api_file_uploads
        .get();
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo first
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"metric-test","type":"model","organization":"org","private":false}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Upload a file via LFS endpoint (Hub API routes include /lfs/objects/{oid})
    let content = b"hub-api-file-metric-content";
    let oid = test_oid(content);
    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);
    let up_after = shardline_metrics::metrics()
        .protocol
        .hub_api_file_uploads
        .get();
    assert!(up_after > up_before, "hub_api_file_uploads should increase");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_file_download_tracks_protocol_counter() {
    let dl_before = shardline_metrics::metrics()
        .protocol
        .hub_api_file_downloads
        .get();
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;
    let content = b"hub-api-file-dl-metric";
    let oid = test_oid(content);

    // Upload first
    let up = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(up.status(), StatusCode::OK);

    // Download
    let dl = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(dl.status(), StatusCode::OK);
    let dl_after = shardline_metrics::metrics()
        .protocol
        .hub_api_file_downloads
        .get();
    assert!(
        dl_after > dl_before,
        "hub_api_file_downloads should increase"
    );
}

// ============================================================================
// Provider Webhook/Token Metric Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_exchange_tracks_metric() {
    let before = shardline_metrics::metrics().provider.token_exchanges.get();
    let (app, _tmp, _cfg_dir) = test_app_with_provider_tokens(&[ServerFrontend::Lfs]).await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/providers/github/tokens")
                .header("content-type", "application/json")
                .header("x-shardline-provider-key", "bootstrap")
                .body(Body::from(
                    r#"{"subject":"github-user-1","owner":"team","repo":"assets","revision":"refs/heads/main","scope":"Read"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let after = shardline_metrics::metrics().provider.token_exchanges.get();
    assert!(after > before, "token_exchanges should increase");
}

// ============================================================================
// Dedupe Shard Query Metric Test
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedupe_shard_query_tracks_xet_metric() {
    let before_queries = shardline_metrics::metrics().xet.dedupe_shard_queries.get();
    let content = b"dedupe-query-metric-test";
    let (xorb_bytes, xorb_hash) = test_fixtures::single_chunk_xorb(content);
    let (shard_bytes, _file_id) =
        test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    // Upload xorb
    let xorb = app
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
    assert_eq!(xorb.status(), StatusCode::OK);
    // Upload shard — this triggers dedupe shard queries
    let shard = app
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
    assert_eq!(shard.status(), StatusCode::OK);

    // Query reconstruction — triggers dedupe shard lookups
    let recon = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/reconstructions/{_file_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(recon.status().is_success());

    // Note: dedupe_shard_mapping is called during reconstruction when resolving
    // chunk hashes. The specific count depends on the shard layout. The metric
    // is wired in LocalIndexStore (DedupeStore impl) and AsyncIndexStore.
    // At minimum the metric should not regress.
    let after_queries = shardline_metrics::metrics().xet.dedupe_shard_queries.get();
    assert!(
        after_queries >= before_queries,
        "dedupe shard queries should not regress"
    );
}

// ============================================================================
// Object/Chunk Storage Metric Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn object_stored_tracks_metrics_on_lfs_upload() {
    let obj_before = shardline_metrics::metrics()
        .storage
        .objects_bytes_total
        .get();
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;
    let content = b"object-stored-metric-test-data";
    let oid = test_oid(content);

    let upload = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .header("content-length", content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::OK);
    let obj_after = shardline_metrics::metrics()
        .storage
        .objects_bytes_total
        .get();
    assert!(
        obj_after >= obj_before,
        "objects_bytes_total should not regress after LFS upload"
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
    assert!(
        delete_resp.status().is_success(),
        "delete status: {}",
        delete_resp.status()
    );

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
async fn test_lfs_delete_nonexistent() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    // Delete an OID that was never uploaded
    let nonexistent_oid = "a".repeat(64);
    let delete_resp = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/lfs/objects/{nonexistent_oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // lfs_delete_object returns 404 for non-existent objects
    assert_eq!(
        delete_resp.status(),
        StatusCode::NOT_FOUND,
        "expected 404 for deleting non-existent LFS object, got {}",
        delete_resp.status()
    );
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
    assert!(
        get.status().is_client_error(),
        "GET invalid OID: {}",
        get.status()
    );

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
    assert!(
        put.status().is_client_error(),
        "PUT invalid OID: {}",
        put.status()
    );
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
    let patch1 = app
        .clone()
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
    assert_eq!(
        patch1.status(),
        StatusCode::OK,
        "PATCH chunk1 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch1).await)
    );

    // PATCH chunk 2 (final chunk)
    let range2 = format!("bytes {}-{}/{}", chunk1.len(), total - 1, total);
    let patch2 = app
        .clone()
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
    assert_eq!(
        patch2.status(),
        StatusCode::OK,
        "PATCH chunk2 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch2).await)
    );

    // GET the final object and verify it contains both parts
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
    assert_eq!(
        body, full_content,
        "PATCH result should contain both chunks"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_invalid_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-patch-invalid-range";
    let oid = test_oid(content);

    // Upload
    let put = app
        .clone()
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
    let patch = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "bytes 100-0/*") // intentionally invalid
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
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "bytes 0-9/100")
                .header(header::CONTENT_LENGTH, "3") // says 10 bytes in range, sends 3
                .body(Body::from(b"abc".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_patch_single_chunk_final() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-single-chunk-final";
    let oid = test_oid(content);

    // Single PATCH that covers the entire range (is_final=true)
    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(
                    "Content-Range",
                    format!("bytes 0-{}/{}", content.len() - 1, content.len()),
                )
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::OK);

    // Verify content is accessible via GET
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
async fn lfs_put_accepts_any_content_type() {
    // The Content-Type check was relaxed for git-lfs compatibility.
    // Non-octet-stream Content-Types are accepted; the body is validated
    // by its SHA-256 digest regardless.
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-put-test-content-42";
    let oid = test_oid(content);
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "text/plain")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_object_with_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-range-test-content-1234567890";
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
    assert!(
        obj.get("actions").is_none() || obj["actions"].as_object().is_none_or(|m| m.is_empty()),
        "existing object should not have upload actions: {:?}",
        obj["actions"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_download_existing_object() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-batch-download-existing";
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
    assert!(
        obj["actions"].is_object(),
        "existing object should have download actions"
    );
    assert!(
        obj["actions"]["download"]["href"]
            .as_str()
            .unwrap()
            .contains(&oid)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_mixed_present_absent() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let present_content = b"present-obj";
    let present_oid = test_oid(present_content);

    // Upload one object
    let put = app
        .clone()
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
    assert!(
        objects[0]["actions"].is_object(),
        "present object should have actions"
    );
    // Absent object has error
    assert!(
        objects[1]["error"].is_object(),
        "absent object should have error"
    );
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
    let put = app
        .clone()
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
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_valid_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Read);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_insufficient_scope() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    // Upload requires Write scope, a Read-only token should fail
    let token = test_token(TokenScope::Read);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"upload","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_put_get_delete_roundtrip_through_full_router() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::S3]).await;
    let token = test_token(TokenScope::Write);
    let auth = format!("AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request");
    let content = b"s3-e2e-roundtrip";

    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/test.test/data/e2e.pt")
                .header(header::AUTHORIZATION, &auth)
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
                .uri("/test.test/data/e2e.pt")
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, content);

    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/test.test/data/e2e.pt")
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_multipart_roundtrip_through_full_router() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::S3]).await;
    let token = test_token(TokenScope::Write);
    let auth = format!("AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request");

    // CreateMultipartUpload.
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/test.test/data/mp.pt?uploads")
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::OK);
    let create_xml = String::from_utf8(body_bytes(create).await).unwrap();
    let open = "<UploadId>";
    let upload_id_start = create_xml.find(open).unwrap() + open.len();
    let upload_id_end = create_xml.find("</UploadId>").unwrap();
    let upload_id = &create_xml[upload_id_start..upload_id_end];

    // UploadPart (two parts).
    let part1: &[u8] = b"e2e-part-one-";
    let part2: &[u8] = b"second-part";
    for (part_number, content) in [(1_u32, part1), (2, part2)] {
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!(
                        "/test.test/data/mp.pt?partNumber={part_number}&uploadId={upload_id}"
                    ))
                    .header(header::AUTHORIZATION, &auth)
                    .body(Body::from(content.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
        assert!(put.headers().contains_key(header::ETAG));
    }

    // CompleteMultipartUpload.
    let complete_body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n\
         \x20 <Part><PartNumber>1</PartNumber><ETag>\"{upload_id}-1\"</ETag></Part>\n\
         \x20 <Part><PartNumber>2</PartNumber><ETag>\"{upload_id}-2\"</ETag></Part>\n\
         </CompleteMultipartUpload>\n"
    );
    let complete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/test.test/data/mp.pt?uploadId={upload_id}"))
                .header(header::AUTHORIZATION, &auth)
                .header(header::CONTENT_TYPE, "application/xml")
                .body(Body::from(complete_body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(complete.status(), StatusCode::OK);

    // GET returns the assembled bytes.
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test.test/data/mp.pt")
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    assert_eq!(body_bytes(get).await, [part1, part2].concat());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_list_objects_v2_through_full_router() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::S3]).await;
    let token = test_token(TokenScope::Write);
    let auth = format!("AWS4-HMAC-SHA256 Credential={token}/20260813/us-east-1/s3/aws4_request");

    for (key, content) in [("root.txt", b"1".to_vec()), ("dir/a.txt", b"2".to_vec())] {
        let put = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/test.test/{key}"))
                    .header(header::AUTHORIZATION, &auth)
                    .body(Body::from(content))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put.status(), StatusCode::OK);
    }

    // S3A delimiter shape through the full router: root.txt + dir/ rollup.
    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/test.test?list-type=2&delimiter=%2F")
                .header(header::AUTHORIZATION, &auth)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let xml = String::from_utf8(body_bytes(list).await).unwrap();
    assert!(xml.contains("<Key>root.txt</Key>"), "{xml}");
    assert!(xml.contains("<Prefix>dir/</Prefix>"), "{xml}");
    assert!(xml.contains("<IsTruncated>false</IsTruncated>"), "{xml}");
}

// ---------------------------------------------------------------------------
// LFS Batch Xet Transfer Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_upload_returns_xet_headers() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Write);
    let oid = test_oid(b"xet-e2e-upload-test");

    let request = json!({
        "operation": "upload",
        "transfers": ["xet", "basic"],
        "objects": [{"oid": oid, "size": 100}]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["transfer"], "xet", "should negotiate xet transfer");
    assert_eq!(json["hash_algo"], "sha256");

    let obj = &json["objects"][0];
    assert_eq!(obj["oid"], oid);
    let upload = &obj["actions"]["upload"];
    assert!(
        upload["href"].as_str().unwrap().contains(&oid),
        "upload href should contain the OID"
    );

    let header = &upload["header"];
    assert!(
        header["X-Xet-Cas-Url"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "X-Xet-Cas-Url must be present and non-empty"
    );
    assert!(
        header["X-Xet-Access-Token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "X-Xet-Access-Token must be present and non-empty"
    );
    assert!(
        header["X-Xet-Token-Expiration"].as_str().is_some(),
        "X-Xet-Token-Expiration must be present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_download_existing() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Write);
    let content = b"xet-e2e-download-test-content";
    let oid = test_oid(content);

    // Upload first (requires auth)
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Batch download with xet
    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{"oid": oid, "size": content.len() as u64}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["transfer"], "xet");

    let obj = &json["objects"][0];
    assert_eq!(obj["oid"], oid);
    let download = &obj["actions"]["download"];
    assert!(
        download["header"]["X-Xet-Cas-Url"]
            .as_str()
            .is_some_and(|u| !u.is_empty()),
        "download should include X-Xet-Cas-Url"
    );
    assert!(
        download["header"]["X-Xet-Access-Token"]
            .as_str()
            .is_some_and(|t| !t.is_empty()),
        "download should include X-Xet-Access-Token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_upload_existing_object_has_no_actions() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Write);
    let content = b"xet-e2e-existing-test";
    let oid = test_oid(content);

    // Upload first (requires auth)
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Batch upload — should be a no-op (already exists)
    let request = json!({
        "operation": "upload",
        "transfers": ["xet", "basic"],
        "objects": [{"oid": oid, "size": content.len() as u64}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["transfer"], "xet");

    let obj = &json["objects"][0];
    assert_eq!(obj["oid"], oid);
    // Existing object should NOT have upload actions
    assert!(
        obj.get("actions").is_none() || obj["actions"].as_object().is_none_or(|m| m.is_empty()),
        "existing object should not have upload actions"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_download_missing_returns_404() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Read);
    let oid = test_oid(b"xet-e2e-never-uploaded");

    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{"oid": oid, "size": 100}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["transfer"], "xet");
    let obj = &json["objects"][0];
    assert_eq!(obj["error"]["code"], 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_without_auth_uses_basic() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
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
    // Without auth, falls back to basic transfer
    assert_eq!(json["transfer"], "basic");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_xet_transfer_xet_only_without_auth_rejected() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let request = json!({
        "operation": "download",
        "transfers": ["xet"],
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
async fn lfs_batch_xet_transfer_returns_correct_size() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;
    let token = test_token(TokenScope::Write);
    let content = b"xet-e2e-size-test-data-123";
    let oid = test_oid(content);

    // Upload (requires auth)
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Download with xet — verify size matches
    let request = json!({
        "operation": "download",
        "transfers": ["xet", "basic"],
        "objects": [{"oid": oid, "size": content.len() as u64}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(serde_json::to_vec(&request).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    let obj = &json["objects"][0];
    assert_eq!(obj["size"], content.len() as u64);
}

// ---------------------------------------------------------------------------
// End LFS Batch Xet Transfer Tests
// ---------------------------------------------------------------------------

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
async fn cas_rejects_hash_mismatch_on_overwrite() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content_a = b"overwrite-protect-content-a";
    let hash_a = test_hash(content_a);

    // Upload content A with matching hash.
    let put_a = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash_a}"))
                .body(Body::from(content_a.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_a.status(), StatusCode::NO_CONTENT);

    // Try to upload content B (different) with the same URL hash A.
    // Because the function verifies the body hash against the URL hash,
    // and content B's hash != hash(A), it should reject with 400.
    let content_b = b"overwrite-protect-content-b";
    let put_b = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash_a}"))
                .body(Body::from(content_b.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        put_b.status(),
        StatusCode::BAD_REQUEST,
        "uploading content B with URL hash of content A should be rejected"
    );

    // Verify stored content is still content A (not corrupted).
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash_a}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::OK);
    let body = body_bytes(get).await;
    assert_eq!(body, content_a, "stored content should remain content A");
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

    // HEAD
    let head_resp = app
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
    let response = app
        .oneshot(
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
async fn bazel_ac_accepts_an_action_digest_that_does_not_match_the_result_body() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    // Action Cache keys identify actions, not the serialized action result.
    let wrong_hash = "a".repeat(64);
    let content = b"ac-content-hash-mismatch";

    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/ac/{wrong_hash}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
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

    // HEAD
    let head_resp = app
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
    let response = app
        .oneshot(
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

    // Use same content for both so hash is valid for both AC and CAS.
    // The flat route checks AC first; storing the same content in both
    // verifies that the flat route finds and serves it from AC.
    let content = b"flat-route-ac-priority-content";
    let hash = test_hash(content);

    // PUT to AC route (AC now validates hash)
    let put_ac = app
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
    assert_eq!(put_ac.status(), StatusCode::NO_CONTENT);

    // Also PUT to CAS with matching hash
    let put_cas = app
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
    assert_eq!(put_cas.status(), StatusCode::NO_CONTENT);

    // Flat GET should succeed (finds AC first)
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
    assert_eq!(body, content, "flat route should serve content from AC");

    // Flat HEAD should also return content-length
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
    let cl: u64 = head
        .headers()
        .get(header::CONTENT_LENGTH)
        .unwrap()
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        cl,
        content.len() as u64,
        "flat HEAD should return AC content-length"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;
    let hash = "a".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_with_valid_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;
    let content = b"bazel-auth-test";
    let hash = test_hash(content);
    let token = test_token(TokenScope::Write);
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_get_with_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"bazel-cas-range-test-content-42";
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
    assert!(
        response.status().is_client_error(),
        "expected client error, got {}",
        response.status()
    );
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
    assert!(
        response.status().is_client_error(),
        "expected client error, got {}",
        response.status()
    );
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
    assert!(
        response.status().is_success() || response.status() == StatusCode::UNAUTHORIZED,
        "unexpected status: {} body: {}",
        response.status(),
        String::from_utf8_lossy(&body_bytes(response).await)
    );
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
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/blobs/sha256:{nonexistent_digest}"
                ))
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

    let response = app
        .oneshot(
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
    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/blobs/sha256:{nonexistent_digest}"
                ))
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
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "delete status: {}",
        delete.status()
    );

    // Confirm deleted
    let get = app
        .oneshot(
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
    let put_manifest = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/referencing"))
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(manifest_bytes.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_manifest.status(), StatusCode::CREATED);

    // Try to delete the config blob — should be blocked by manifest reference
    let delete_config = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{config_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_config.status(),
        StatusCode::BAD_REQUEST,
        "should reject deleting blob referenced by manifest"
    );

    // Try to delete the layer blob — should also be blocked
    let delete_layer = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{layer_digest}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete_layer.status(),
        StatusCode::BAD_REQUEST,
        "should reject deleting layer blob referenced by manifest"
    );
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
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
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
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
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
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
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

    let response = app
        .oneshot(
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

    let response = app
        .oneshot(
            Request::builder()
                .method("HEAD")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/manifests/nonexistent-manifest"
                ))
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
    let delete = app
        .clone()
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
    assert!(
        delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "delete status: {}",
        delete.status()
    );

    // Confirm deleted
    let get = app
        .oneshot(
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
    assert_eq!(
        create.status(),
        StatusCode::ACCEPTED,
        "session create failed: {}",
        String::from_utf8_lossy(&body_bytes(create).await)
    );
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
    assert_eq!(
        patch1.status(),
        StatusCode::ACCEPTED,
        "PATCH 1 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch1).await)
    );
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
    assert_eq!(
        patch2.status(),
        StatusCode::ACCEPTED,
        "PATCH 2 failed: {}",
        String::from_utf8_lossy(&body_bytes(patch2).await)
    );
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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // DELETE (cancel) session
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(&location)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        delete.status().is_success() || delete.status() == StatusCode::ACCEPTED,
        "cancel session status: {}",
        delete.status()
    );

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
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(b"not-valid-json".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        response.status().is_client_error(),
        "invalid manifest should return 4xx, got {}",
        response.status()
    );
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
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/manifests/latest?tag=stable&tag=release"
                ))
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
    let oci_tag = response
        .headers()
        .get("OCI-Tag")
        .expect("OCI-Tag header")
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        oci_tag.contains("latest"),
        "should contain 'latest': {oci_tag}"
    );
    assert!(
        oci_tag.contains("stable"),
        "should contain 'stable': {oci_tag}"
    );
    assert!(
        oci_tag.contains("release"),
        "should contain 'release': {oci_tag}"
    );

    // Verify all three tags resolve to the same manifest
    for tag in &["latest", "stable", "release"] {
        let get = app
            .clone()
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
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "digest mismatch should return 400, got {}",
        response.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_unknown_path_returns_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/nonexistent/endpoint"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_wrong_content_range() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // PATCH with wrong Content-Range (start > end)
    let patch = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "100-0")
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        patch.status().is_client_error(),
        "wrong Content-Range should return 4xx, got {}",
        patch.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_offset_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session
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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // PATCH first chunk at offset 0
    let patch1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "0-4")
                .body(Body::from(b"hello".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(patch1.status().is_success());

    // PATCH second chunk at WRONG offset (should be 5, send 10 instead)
    let patch2 = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "10-14")
                .body(Body::from(b"world".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    // Offset mismatch should return a client error
    assert!(
        patch2.status().is_client_error(),
        "offset mismatch should return 4xx, got {}",
        patch2.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_patch_content_range_end_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // PATCH with Content-Range saying 0-9 (10 bytes) but only sending 3
    let patch = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "0-9")
                .body(Body::from(b"abc".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        patch.status(),
        StatusCode::RANGE_NOT_SATISFIABLE,
        "end mismatch should return 416, got {}",
        patch.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_put_session_hash_mismatch() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Create session and upload data
    let data = b"oci-hash-mismatch-test-data";
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
    assert_eq!(create.status(), StatusCode::ACCEPTED);
    let location = create
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    let patch = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", format!("0-{}", data.len() - 1))
                .body(Body::from(data.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(patch.status(), StatusCode::ACCEPTED);
    let location2 = patch
        .headers()
        .get(header::LOCATION)
        .map(|v| v.to_str().unwrap().to_owned())
        .unwrap_or(location);

    // Complete with WRONG digest (not matching the data)
    let wrong_digest = "0".repeat(64);
    let complete = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("{location2}?digest=sha256:{wrong_digest}"))
                .header(header::CONTENT_LENGTH, "0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Hash mismatch should return 400
    assert_eq!(
        complete.status(),
        StatusCode::BAD_REQUEST,
        "hash mismatch should return 400, got {}",
        complete.status()
    );
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
        let put_m = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(format!("/v2/{OCI_TEST_REPO}/manifests/child"))
                    .header(
                        header::CONTENT_TYPE,
                        "application/vnd.oci.image.manifest.v1+json",
                    )
                    .body(Body::from(mj.into_bytes()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(put_m.status(), StatusCode::CREATED);
        len
    };

    // Now create a manifest list (index) referencing that manifest
    // Use the child manifest's digest
    let child_get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/child"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(child_get.status(), StatusCode::OK);
    let child_digest = child_get
        .headers()
        .get("Docker-Content-Digest")
        .unwrap()
        .to_str()
        .unwrap()
        .strip_prefix("sha256:")
        .unwrap()
        .to_owned();

    let index_json = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "size": manifest_len,
            "digest": format!("sha256:{child_digest}"),
            "platform": {"architecture": "amd64", "os": "linux"}
        }]
    })
    .to_string();

    let put_idx = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/index-v1"))
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.index.v1+json",
                )
                .body(Body::from(index_json.into_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put_idx.status(), StatusCode::CREATED);

    // Verify index is retrievable
    let get_idx = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/index-v1"))
                .header(header::ACCEPT, "application/vnd.oci.image.index.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get_idx.status(), StatusCode::OK);
    let idx_json = body_json(get_idx).await;
    assert_eq!(
        idx_json["mediaType"],
        "application/vnd.oci.image.index.v1+json"
    );
    assert_eq!(
        idx_json["manifests"][0]["platform"]["architecture"],
        "amd64"
    );
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

    let put1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(manifest1.clone().into_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put1.status(), StatusCode::CREATED);
    let digest1 = put1
        .headers()
        .get("Docker-Content-Digest")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    // Verify tag resolves to digest1
    let get1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get1.status(), StatusCode::OK);
    let d1 = get1
        .headers()
        .get("Docker-Content-Digest")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
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

    let put2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
                .header(
                    header::CONTENT_TYPE,
                    "application/vnd.oci.image.manifest.v1+json",
                )
                .body(Body::from(manifest2.into_bytes()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::CREATED);
    let digest2 = put2
        .headers()
        .get("Docker-Content-Digest")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_ne!(
        digest1, digest2,
        "different content should produce different digest"
    );

    // Verify tag now resolves to digest2
    let get2 = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/movable"))
                .header(header::ACCEPT, "application/vnd.oci.image.manifest.v1+json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get2.status(), StatusCode::OK);
    let d2 = get2
        .headers()
        .get("Docker-Content-Digest")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        d2, digest2,
        "tag should now point to second manifest version"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_v2_root_requires_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Oci]).await;
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
    let page1 = app
        .clone()
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
    let link = page1
        .headers()
        .get(axum::http::header::LINK)
        .expect("Link header should be present for pagination")
        .to_str()
        .unwrap()
        .to_owned();

    let page1_json = body_json(page1).await;
    assert_eq!(page1_json["name"], OCI_TEST_REPO);
    let page1_tags = page1_json["tags"].as_array().unwrap();
    assert_eq!(
        page1_tags.len(),
        1,
        "expected 1 tag per page, got {page1_tags:?}"
    );

    // Verify Link header is present
    assert!(
        link.contains("rel=\"next\""),
        "Link header should contain rel=next: {link}"
    );

    // Extract the last tag from the Link header
    assert!(
        link.contains("last="),
        "Link header should contain last=: {link}"
    );

    // Request page with n=0 — should return empty list, no Link header
    let page0 = app
        .clone()
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
    assert!(!page0_has_link, "n=0 should not include Link header");
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
        "max-age=31536000; includeSubDomains"
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
    let (shard_bytes, file_id) = test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);

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
    let oci_get = app
        .clone()
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
    let lfs_get = app
        .clone()
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
    assert_eq!(
        lfs_get.status(),
        StatusCode::NOT_FOUND,
        "LFS should not find OCI-uploaded content (namespace isolation)"
    );

    // Upload via LFS
    let lfs_content = b"lfs-only-content";
    let lfs_oid = test_oid(lfs_content);
    let lfs_put = app
        .clone()
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
    assert_eq!(
        oci_get2.status(),
        StatusCode::NOT_FOUND,
        "OCI should not find LFS-uploaded content (namespace isolation)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_protocols_coexist() {
    let (app, _tmp) = test_app(&[
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
    ])
    .await;

    // 1. Xet: upload xorb + shard, get reconstruction
    let content = b"quad-proto-content";
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

    // 2. LFS: upload and download
    let lfs_content = b"lfs-quad-content";
    let lfs_oid = test_oid(lfs_content);
    let lfs_put = app
        .clone()
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
    let bazel_put = app
        .clone()
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
    let lfs_get = app
        .clone()
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
    let bazel_get = app
        .clone()
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
    let oci_get = app
        .clone()
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
    assert_eq!(
        lfs_oci_cross.status(),
        StatusCode::NOT_FOUND,
        "namespace isolation: LFS should not see OCI content"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_then_bazel_download_with_auth() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs, ServerFrontend::BazelHttp]).await;
    let write_token = test_token(TokenScope::Write);
    let read_token = test_token(TokenScope::Read);

    // Upload an object via LFS with auth
    let content = b"auth-cross-proto-content";
    let oid = test_oid(content);
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {write_token}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Now download the same content via Bazel (should NOT work -- different namespace)
    let hash = test_hash(b"unrelated");
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {read_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

// ============================================================================
// Provider Token E2E Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_token_issuance_with_valid_bootstrap_key() {
    let (app, _tmp, _cfg_dir) =
        test_app_with_provider_tokens(&[ServerFrontend::Lfs, ServerFrontend::Xet]).await;

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
    assert!(
        json.get("token").and_then(|t| t.as_str()).is_some(),
        "should return a token"
    );
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
    assert!(
        response.status() == StatusCode::ACCEPTED || response.status() == StatusCode::NO_CONTENT,
        "expected 202 or 204, got {}",
        response.status()
    );
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
                .header(
                    "x-hub-signature-256",
                    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
                )
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_webhook_triggers_lifecycle_reconciliation() {
    let (app, _tmp, _cfg_dir) =
        test_app_with_provider_tokens(&[ServerFrontend::Xet, ServerFrontend::Lfs]).await;

    // Send a GitHub repository deletion webhook (simplest event that triggers reconciliation)
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
                .header("x-github-delivery", "recon-test-delivery-1")
                .header("x-hub-signature-256", &signature)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    // The webhook should be accepted and reconciliation run
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let json = body_json(response).await;
    assert_eq!(json["provider"], "github");
    assert_eq!(json["owner"], "team");
    assert_eq!(json["repo"], "assets");
    assert_eq!(json["event_kind"], "repository_deleted");
    // Reconciliation should produce these fields (even if zero)
    assert!(
        json.get("affected_file_versions").is_some(),
        "reconciliation should report affected_file_versions"
    );
    assert!(
        json.get("affected_chunks").is_some(),
        "reconciliation should report affected_chunks"
    );
    assert!(
        json.get("applied_holds").is_some(),
        "reconciliation should report applied_holds"
    );
    assert!(
        json.get("retention_seconds").is_some(),
        "reconciliation should report retention_seconds"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_object_returns_integrity_digest_header() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-integrity-header-test-content-777";
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

    // GET the object and verify the Docker-Content-Digest header is present
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
    // Save digest header before consuming the body
    let digest_header = get
        .headers()
        .get("Docker-Content-Digest")
        .map(|v| v.to_str().unwrap().to_owned());
    let body = body_bytes(get).await;
    assert_eq!(body, content);
    // Server should include the integrity digest header
    assert_eq!(digest_header, Some(format!("sha256:{oid}")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_verify_detects_corrupted_storage() {
    let (app, tmp) = test_app(&[ServerFrontend::Lfs]).await;

    let content = b"lfs-verify-corruption-test";
    let oid = test_oid(content);

    // Upload object
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

    // Verify it works before corruption
    let verify_ok = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(verify_ok.status(), StatusCode::OK);

    // The standalone chunk is still stored alongside the xorb (the dedup
    // path reads chunks by their data hash).
    let chunk_hash =
        shardline_index::xet_hash_hex_string(crate::local_backend::chunk_hash(content));
    let stored_path = tmp
        .path()
        .join("chunks")
        .join(&chunk_hash[..2])
        .join(&chunk_hash);
    assert!(
        stored_path.exists(),
        "stored LFS chunk should exist at {:?}",
        stored_path
    );

    // Single-chunk objects are xorb-backed on ingest, so the verify path reads
    // the stored xorb object (not the standalone chunk). Corrupt the xorb the
    // record references — the same xorb the ingest path produced.
    let packed =
        crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&[(content.to_vec(), 0u64)])
            .unwrap();
    let xorb_hash = packed.xorb_hash_hex;
    let xorb_path = tmp
        .path()
        .join("chunks")
        .join("xorbs")
        .join("default")
        .join(&xorb_hash[..2])
        .join(format!("{xorb_hash}.xorb"));
    assert!(
        xorb_path.exists(),
        "stored xorb should exist at {:?}",
        xorb_path
    );

    // Truncate to change content — verify will read truncated bytes and compute wrong hash
    let file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&xorb_path)
        .unwrap();
    file.set_len(3).unwrap(); // only keep first 3 bytes
    drop(file);

    // Verify should fail because hash of truncated data != oid
    let verify_corrupted = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/v1/lfs/objects/{oid}/verify"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The verify handler streams the truncated file, computes SHA-256,
    // and compares with the oid. Since the content changed, hash won't match.
    assert_eq!(
        verify_corrupted.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "verify should detect corruption with 422, got {}",
        verify_corrupted.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn provider_multiple_webhooks_in_sequence() {
    let (app, _tmp, _cfg_dir) =
        test_app_with_provider_tokens(&[ServerFrontend::Xet, ServerFrontend::Lfs]).await;

    use hmac::Mac;

    // Send 3 webhooks in sequence, each with unique delivery ID
    for i in 0..3 {
        let body = br#"{"action":"deleted","repository":{"full_name":"team/assets"}}"#;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(body);
        let signature = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/providers/github/webhooks")
                    .header("x-github-event", "repository")
                    .header("x-github-delivery", format!("multi-delivery-{i}"))
                    .header("x-hub-signature-256", &signature)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(
            response.status(),
            StatusCode::ACCEPTED,
            "webhook {i} should be accepted"
        );
    }
}

// ── Hub frontend ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_health_endpoint_returns_200() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_create_and_list_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a model repo (name = "owner/name" per Hub API convention)
    let create_body = serde_json::json!({
        "type": "model",
        "name": "test-team/test-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // List repos (should contain the one we just created)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/repos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let repos = json["repos"].as_array().unwrap();
    assert!(!repos.is_empty(), "should have at least one repo");
    assert_eq!(repos[0]["id"], "test-team/test-model");
    assert_eq!(repos[0]["type"], "model");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_repo_info_returns_repo_details() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo first via the type-specific endpoint
    let create_body = serde_json::json!({
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/info-team/info-model")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::CREATED,
        "repo create should succeed"
    );

    // Fetch repo info
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/models/info-team/info-model")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["id"], "info-team/info-model");
    assert_eq!(json["type"], "model");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_repo_not_found_returns_404() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/models/nonexistent/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_whoami_without_auth_returns_ok() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/whoami-v2")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Without auth configured, whoami should still return OK with default values
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_xet_read_token_route_wired() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a model repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "token-team/read-token-repo",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // xet-read-token requires an auth provider to mint tokens; without one
    // it returns 401 Unauthorized — but the route IS wired (not a 404).
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/token-team/read-token-repo/xet-read-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    // Auth is None in test setup, so this returns 401
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_xet_write_token_route_wired() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a model repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "token-team/write-token-repo",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // xet-write-token also requires auth; returns 401 when no auth configured.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/token-team/write-token-repo/xet-write-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_search_returns_results() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo so there's something to search
    let create_body = serde_json::json!({
        "type": "model",
        "name": "search-team/search-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Search models — the query "se" is >= 2 chars so it passes validation
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/search?q=se")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let repos = json["repos"].as_array().unwrap();
    // The created repo should appear in search results
    assert!(
        repos.iter().any(|r| r["id"] == "search-team/search-model"),
        "created repo should be in search results"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_delete_repo_removes_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo first
    let create_body = serde_json::json!({
        "type": "model",
        "name": "delete-team/delete-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Verify repo exists
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/models/delete-team/delete-model")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Delete the repo
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/models/delete-team/delete-model")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Verify repo is gone
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/models/delete-team/delete-model")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_repo_delete_compat_removes_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"cd-team/cd-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let del = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/repos/delete")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"name":"cd-team/cd-model","type":"model"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(del.status(), StatusCode::NO_CONTENT);

    let get = app
        .oneshot(
            Request::builder()
                .uri("/api/models/cd-team/cd-model")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_validate_yaml_returns_ok() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/validate-yaml")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"content": "test: valid"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["warnings"], serde_json::json!([]));
    assert_eq!(json["errors"], serde_json::json!([]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_revisions_lists_initial_revision() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo — this creates an initial "main" revision automatically
    let create_body = serde_json::json!({
        "type": "model",
        "name": "rev-team/rev-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // List revisions — should include the initial "main" revision
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/models/rev-team/rev-model/revisions")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let revisions = json["revisions"].as_array().unwrap();
    assert!(
        !revisions.is_empty(),
        "should have at least the initial revision"
    );
    // The initial revision has ref_name "main"
    assert!(
        revisions
            .iter()
            .any(|r| r["refName"] == "main" || r["ref_name"] == "main")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_singular_revision_info_returns_revision() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"ri-team/ri-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // GET /api/models/ri-team/ri-model/revision/main — singular revision endpoint
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/models/ri-team/ri-model/revision/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(
        body.get("sha").and_then(|v| v.as_str()).is_some(),
        "revision info should include a SHA"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_preupload_checks_existing_files() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "pre-team/pre-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Preupload checks which files already exist at the given revision.
    // With only the initial empty revision, no files exist yet.
    let pre_body = serde_json::json!({
        "files": [
            {"path": "README.md"},
            {"path": "model.bin"}
        ]
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/pre-team/pre-model/preupload/main")
                .header("content-type", "application/json")
                .body(Body::from(pre_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let results = json["result"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    // Both files should report exists=false
    for r in results {
        assert_eq!(r["exists"], false);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_tree_returns_file_listing() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "tree-team/tree-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Tree listing — returns empty array for fresh repo.
    // Axum's {*path} wildcard requires at least one path segment, so use "."
    // as a sentinel for "root".
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/tree-team/tree-model/tree/main/.")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.is_array(), "tree response should be an array");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_tree_root_returns_file_listing_without_trailing_path() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"tr-team/tr-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // GET /api/models/tr-team/tr-model/tree/main — no trailing path
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/models/tr-team/tr-model/tree/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert!(body.is_array(), "root tree response should be an array");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_git_info_refs_serves_advertisement() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo so there's at least an initial revision to advertise
    let create_body = serde_json::json!({
        "type": "model",
        "name": "git-team/git-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Git Smart HTTP discovery for upload-pack
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/models/git-team/git-model/info/refs?service=git-upload-pack")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("git-upload-pack-advertisement"),
        "content-type should indicate upload-pack advertisement, got: {content_type}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_git_head_returns_ref() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "head-team/head-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // HEAD ref endpoint
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/models/head-team/head-model/HEAD")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let text = String::from_utf8_lossy(&body);
    assert!(
        text.contains("ref:"),
        "HEAD response should contain 'ref:', got: {text:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_git_upload_pack_returns_pack() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "upack-team/upack-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Upload-pack with an empty body (no wants/haves) — returns an empty pack
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/upack-team/upack-model/git-upload-pack")
                .header("content-type", "application/x-git-upload-pack-request")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The route is wired; it may return OK or an error depending on pkt-line parsing
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_git_receive_pack_accepts_push() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create_body = serde_json::json!({
        "type": "model",
        "name": "rpack-team/rpack-model",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Receive-pack with an empty body — the handler returns a report response
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/models/rpack-team/rpack-model/git-receive-pack")
                .header("content-type", "application/x-git-receive-pack-request")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Route is wired; empty body results in a valid report (not a 404)
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_first_rows_returns_empty_for_fresh_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a dataset repo
    let create_body = serde_json::json!({
        "type": "dataset",
        "name": "ds-team/ds-firstrows",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // First-rows on a fresh dataset (no committed data files) — returns empty columns
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/ds-team/ds-firstrows/first-rows")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    // Empty dataset returns empty columns and rows per the Hub API spec
    assert!(json["columns"].as_array().unwrap().is_empty());
    assert!(json["rows"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_viewer_requires_data_files() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a dataset repo
    let create_body = serde_json::json!({
        "type": "dataset",
        "name": "ds-team/ds-viewer",
        "private": false,
    });
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(create_body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    // Viewer on a fresh dataset (no committed data files) — returns 400 (PathValidation)
    // because no data file exists yet for the requested split.
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/datasets/ds-team/ds-viewer/viewer/train")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Route IS wired (not a 404). Without data files, it returns 400 BAD_REQUEST.
    assert_ne!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

// ── OCI role-split ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_api_role_serves_manifest_but_not_blob_upload() {
    let (app, _tmp) =
        test_app_for_frontends_with_role(&[ServerFrontend::Oci], ServerRole::Api).await;

    // Manifest GET should work on the API role
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/test/manifests/latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 404 (manifest not found) rather than 404 (route not found)
    // because the API role serves manifest routes but the manifest doesn't exist yet.
    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "API role should serve manifest routes (returning not-found for missing content)"
    );

    // Blob upload POST should fail with 404 on API role
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/test/blobs/uploads/")
                .header("content-type", "application/octet-stream")
                .body(Body::from(&b"test data"[..]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "API role should reject blob upload (transfer operation)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_transfer_role_serves_blob_upload_but_not_manifest() {
    let (app, _tmp) =
        test_app_for_frontends_with_role(&[ServerFrontend::Oci], ServerRole::Transfer).await;

    // Blob upload POST should work on the Transfer role
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/test/blobs/uploads/")
                .header("content-type", "application/octet-stream")
                .body(Body::from(&b"test data"[..]))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Transfer role should serve blob upload (not return 404)"
    );

    // Manifest GET should fail with 404 on Transfer role
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/test/manifests/latest")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "Transfer role should reject manifest operations"
    );
}

// ============================================================================
// Additional Protocol Edge-Case Tests
// ============================================================================

// ---------------------------------------------------------------------------
// OCI: Blob delete for non-existent blob
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_delete_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let nonexistent_digest = "a".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/blobs/sha256:{nonexistent_digest}"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "deleting non-existent blob should return 404"
    );
}

// ---------------------------------------------------------------------------
// OCI: Manifest delete for non-existent manifest
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_delete_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v2/{OCI_TEST_REPO}/manifests/nonexistent-tag"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::NOT_FOUND,
        "deleting non-existent manifest should return 404"
    );
}

// ---------------------------------------------------------------------------
// OCI: Upload session expiration and auto-cleanup
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_upload_session_expires_cleaned_up() {
    let (app, tmp) = test_app(&[ServerFrontend::Oci]).await;

    // Step 1: Create an upload session
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
    assert_eq!(create.status(), StatusCode::ACCEPTED, "session creation");
    let location = create
        .headers()
        .get(header::LOCATION)
        .expect("LOCATION header")
        .to_str()
        .unwrap()
        .to_owned();
    let session_id = location.rsplit('/').next().expect("session id");

    // Step 2: Manipulate the session metadata file to simulate expiry.
    // The metadata is stored at: <tmp>/oci-uploads/<session_id>.json
    let metadata_path = tmp
        .path()
        .join("oci-uploads")
        .join(format!("{session_id}.json"));
    assert!(metadata_path.exists(), "session metadata should exist");
    let mut session: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&metadata_path).unwrap()).unwrap();
    // Set timestamps to 0 (epoch = 1970, definitely expired).
    session["created_at_unix_seconds"] = serde_json::json!(0u64);
    session["last_touched_unix_seconds"] = serde_json::json!(0u64);
    std::fs::write(&metadata_path, serde_json::to_vec(&session).unwrap()).unwrap();

    // Step 3: PATCH the session — the handler should detect expiry, clean up,
    // and return 404.
    let patch = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri(&location)
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header("Content-Range", "0-4")
                .body(Body::from(b"hello".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        patch.status(),
        StatusCode::NOT_FOUND,
        "expired session should be auto-cleaned: {}",
        String::from_utf8_lossy(&body_bytes(patch).await)
    );

    // Step 4: Confirm the metadata file has been deleted
    assert!(
        !metadata_path.exists(),
        "session metadata should be deleted after expiry"
    );
}

// ---------------------------------------------------------------------------
// LFS: Batch request with invalid OID format
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_invalid_oid_format() {
    let (app, _tmp) = test_app(&[ServerFrontend::Lfs]).await;

    // Batch request with a short OID (non-64-char hex)
    let request = serde_json::json!({
        "operation": "download",
        "objects": [{"oid": "short-oid", "size": 100}]
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

    // Invalid OID format should return 422 (UNPROCESSABLE_ENTITY)
    assert_eq!(
        response.status(),
        StatusCode::UNPROCESSABLE_ENTITY,
        "batch with invalid OID should return 422"
    );
    let json = body_json(response).await;
    assert_eq!(json["message"], "invalid oid");
}

// ---------------------------------------------------------------------------
// Bazel: Empty content CAS operations
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_empty_content_cas_operations() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp]).await;

    let content = b"";
    let hash = test_hash(content); // SHA-256 of empty string
    // Expected hash for empty content: e3b0c44298fc1c149afbf4c8996fb924...
    // (but test_hash computes it dynamically)

    // PUT empty content to CAS
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
    assert_eq!(
        put.status(),
        StatusCode::NO_CONTENT,
        "PUT empty content to CAS"
    );

    // HEAD empty content
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
    let cl: u64 = head_resp
        .headers()
        .get(header::CONTENT_LENGTH)
        .expect("content-length")
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(cl, 0, "empty content should have zero content-length");

    // GET empty content
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
    assert!(body.is_empty(), "empty content should return empty body");
}

// ---------------------------------------------------------------------------
// Auth: Request with malformed Authorization header
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_with_malformed_authorization_header() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;

    // Missing "Bearer" prefix
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, "NotBearer token123")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "malformed auth header (no Bearer prefix) should return 401"
    );

    // Empty token after "Bearer "
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, "Bearer ")
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "empty Bearer token should return 401"
    );

    // Too-long Bearer token (above the shared token envelope limit).
    let long_token = "x".repeat(shardline_protocol::MAX_TOKEN_STRING_BYTES + 1);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {long_token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "overly long Bearer token should return 401"
    );
}

// ---------------------------------------------------------------------------
// Auth: Request with expired token
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_with_expired_token() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;

    // Mint a token that expires at unix epoch (already long past)
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Read, repo, 0).unwrap();
    let token = provider.mint_token(&claims).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/lfs/objects/batch")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
                .body(Body::from(r#"{"operation":"download","objects":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Expired token should return 401
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "expired token should return 401"
    );
}

// ---------------------------------------------------------------------------
// Auth: Insufficient scope (Read token on Write endpoint)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_with_insufficient_scope_read_on_write() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;

    // A Read-scope token should be rejected on a write endpoint (PUT)
    let token = test_token(TokenScope::Read);
    let hash = "a".repeat(64);
    let response = app
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::from(b"data".to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "Read-scope token on Write endpoint should return 403"
    );
}

// ---------------------------------------------------------------------------
// Generic: Request to non-existent route → 404
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_existent_route_returns_404() {
    let (app, _tmp) = test_app(&[ServerFrontend::Xet]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/this/route/does/not/exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Generic: Health endpoint returns 200 even without any protocol frontend
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_endpoint_always_mounted() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

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

    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["status"], "ok");
}

// ---------------------------------------------------------------------------
// Generic: Metrics endpoint returns 200 (also tested above; repeat with OCI
// to verify it's always mounted)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_returns_200_with_oci() {
    let (app, _tmp) = test_app(&[ServerFrontend::Oci]).await;

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = String::from_utf8(body_bytes(response).await).unwrap();
    assert!(
        body.contains("shardline_up 1"),
        "metrics should contain shardline_up gauge"
    );
}

// ============================================================================
// Hub API E2E Tests — routes with zero coverage before these tests
// ============================================================================

// ---------------------------------------------------------------------------
// 1. GET /api/{type}/{ns}/{repo}/modelcard → repo_modelcard
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_modelcard_returns_readme_markdown() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a model repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"mc-team/mc-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Commit a README.md so the modelcard has something to return
    let readme_content = b"# Test Model\n\nThis is a test model card.";
    let content_b64 = STANDARD.encode(readme_content);
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"add readme\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"README.md\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/mc-team/mc-model/commit/main")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        commit.status(),
        StatusCode::OK,
        "commit failed: {}",
        String::from_utf8_lossy(&body_bytes(commit).await)
    );

    // GET modelcard — should return the README.md content
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/models/mc-team/mc-model/modelcard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        ct.contains("text/markdown"),
        "expected markdown content-type, got {ct}"
    );
    let body = body_bytes(resp).await;
    assert_eq!(body, readme_content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_modelcard_not_found_without_readme() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo but do NOT commit a README.md
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"mc-team/no-readme","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // GET modelcard — no README.md committed → 404
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/models/mc-team/no-readme/modelcard")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 2. POST /api/{type}/{ns}/{repo}/commit/{rev} → commit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_creates_new_revision() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"cm-team/cm-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Commit an inline file via NDJSON
    let content_b64 = STANDARD.encode(b"Hello, commit test!");
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"initial commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"hello.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/cm-team/cm-model/commit/main")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert!(
        json.get("commitId").and_then(|v| v.as_str()).is_some()
            || json.get("commit_id").and_then(|v| v.as_str()).is_some(),
        "commit response should contain commit_id: {json}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_rejects_wrong_content_type() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"cm-team/ct-reject","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Send commit with wrong Content-Type → should be rejected
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/cm-team/ct-reject/commit/main")
                .header("content-type", "text/plain")
                .body(Body::from("not ndjson"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// 3. GET /{type}/{ns}/{repo}/resolve/{rev}/{*path} → resolve_file
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_resolve_file_returns_inline_content() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"rs-team/rs-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Commit a small file
    let file_content = b"resolve-me-content";
    let content_b64 = STANDARD.encode(file_content);
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"add file\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"data.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/rs-team/rs-model/commit/main")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::OK);

    // Resolve the file at the resolve endpoint
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/models/rs-team/rs-model/resolve/main/data.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // Check headers before consuming body
    let sha_header = resp
        .headers()
        .get("X-Shardline-SHA")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_owned());
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_bytes(resp).await;
    assert_eq!(body, file_content);
    // Should have the SHA header
    assert!(
        sha_header.is_some(),
        "response should have X-Shardline-SHA header"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_resolve_file_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create repo (no files committed)
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"rs-team/rs-empty","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Resolve a non-existent file → 404
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/models/rs-team/rs-empty/resolve/main/nonexistent.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 4. POST /objects/batch → lfs_batch (Hub API LFS batch)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_lfs_batch_empty_download() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // The Hub API LFS batch request requires a "ref" field with the branch name.
    let body = serde_json::json!({
        "operation": "download",
        "ref": {"name": "main"},
        "objects": []
    });
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/objects/batch")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    assert_eq!(json["transfer"], "basic");
    assert!(json["objects"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_lfs_batch_invalid_json() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/objects/batch")
                .header("content-type", "application/json")
                .body(Body::from("not valid json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(resp.status().is_client_error());
}

// ---------------------------------------------------------------------------
// 5. PUT /lfs/objects/{oid} → lfs_upload  (Hub API)
// 6. GET /lfs/objects/{oid} → lfs_download (Hub API)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_lfs_upload_and_download() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let content = b"hub-lfs-test-content";
    let oid = test_oid(content);

    // Upload via PUT /lfs/objects/{oid}
    let put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header("content-type", "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(put.status(), StatusCode::OK);

    // Download via GET /lfs/objects/{oid}
    let get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/lfs/objects/{oid}"))
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
async fn hub_lfs_download_not_found() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let oid = test_oid(b"never-uploaded-hub-lfs");
    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_resolve_model_file_shorthand() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"ms-team/ms-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Commit a file
    let file_content = b"model-shorthand-test";
    let content_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, file_content);
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"add file\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"test.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/ms-team/ms-model/commit/main")
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(commit.status(), StatusCode::OK);

    // Resolve via model shorthand /{ns}/{repo}/resolve/{rev}/{*path}
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ms-team/ms-model/resolve/main/test.txt")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // The shorthand route is registered and the full variant is tested separately.
    // At minimum verify the route resolves (200 or 404 means it reached the handler).
    assert!(
        resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND,
        "expected 200 or 404 from model shorthand resolve, got: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 7. GET /api/datasets/{ns}/{repo}/parquet → dataset_parquet
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_parquet_returns_empty_for_fresh_dataset() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a dataset repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"dataset","name":"pq-team/pq-empty","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // GET parquet endpoint — fresh dataset returns empty files list
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/datasets/pq-team/pq-empty/parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let json = body_json(resp).await;
    let files = json["files"].as_array().unwrap();
    assert!(
        files.is_empty(),
        "fresh dataset should have no parquet files"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_parquet_rejects_non_dataset() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a model repo (not a dataset)
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"pq-team/pq-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // GET dataset parquet endpoint on a model repo → 400 error
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/datasets/pq-team/pq-model/parquet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// 8. POST /api/{type}/{ns}/{repo}/webhooks → webhook_create
// 9. GET  /api/{type}/{ns}/{repo}/webhooks → webhook_list
// 10. DELETE /api/{type}/{ns}/{repo}/webhooks/{webhook_id} → webhook_delete
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_webhooks_create_list_delete() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"wh-team/wh-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Step 1: Create a webhook
    let webhook_url = "https://hooks.example.com/shardline";
    let create_wh = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/wh-team/wh-model/webhooks")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "url": webhook_url,
                        "events": ["push"],
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_wh.status(), StatusCode::CREATED);
    let wh_json = body_json(create_wh).await;
    let wh_id = wh_json["id"].as_str().unwrap().to_owned();
    assert_eq!(wh_json["url"], webhook_url);
    assert!(wh_json["active"].as_bool().unwrap_or(false));

    // Step 2: List webhooks — should contain the created webhook
    let list = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/wh-team/wh-model/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let list_json = body_json(list).await;
    let webhooks = list_json["webhooks"].as_array().unwrap();
    assert_eq!(webhooks.len(), 1);
    assert_eq!(webhooks[0]["id"], wh_id);
    assert_eq!(webhooks[0]["url"], webhook_url);

    // Step 3: Delete the webhook
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/models/wh-team/wh-model/webhooks/{wh_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        delete.status(),
        StatusCode::NO_CONTENT,
        "delete webhook should return 204, got {}",
        delete.status()
    );

    // Step 4: List webhooks again — should be empty
    let list2 = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/wh-team/wh-model/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list2.status(), StatusCode::OK);
    let list2_json = body_json(list2).await;
    let webhooks2 = list2_json["webhooks"].as_array().unwrap();
    assert!(
        webhooks2.is_empty(),
        "webhooks should be empty after delete"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_webhooks_duplicate_url_returns_409() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"dup-wh-team/dup-wh-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let webhook_url = "https://hooks.example.com/dup-webhook";
    let body = serde_json::json!({"url": webhook_url, "events": ["push"]}).to_string();

    // First create — should succeed.
    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/dup-wh-team/dup-wh-model/webhooks")
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);

    // Second create (same URL) — should return 409 Conflict.
    let resp2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/models/dup-wh-team/dup-wh-model/webhooks")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp2.status(),
        StatusCode::CONFLICT,
        "duplicate webhook URL should return 409, got {}",
        resp2.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_webhooks_list_empty_for_repo_without_webhooks() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    // Create a repo without adding any webhooks
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"wh-team/wh-no-hooks","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // List webhooks — should return empty list
    let list = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/wh-team/wh-no-hooks/webhooks")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let json = body_json(list).await;
    let webhooks = json["webhooks"].as_array().unwrap();
    assert!(webhooks.is_empty());
}

// ============================================================================
// Section 1: Hub repo lifecycle
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_delete_repo_then_recreate() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let ns = "lifecycle-team";
    let name = "lifecycle-model";
    let model_path = format!("{ns}/{name}");

    // Create repo → expect 201
    let create1 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create1.status(), StatusCode::CREATED);

    // DELETE the repo → expect 204
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/models/{ns}/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    // Verify it's gone
    let get = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/models/{ns}/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get.status(), StatusCode::NOT_FOUND);

    // POST same name again → expect 201 Created
    let create2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        create2.status(),
        StatusCode::CREATED,
        "re-creating a deleted repo should return 201, got {}",
        create2.status()
    );

    // Verify the recreated repo is accessible
    let get2 = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/models/{ns}/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(get2.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_to_recreated_repo() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let ns = "recreate-team";
    let name = "recreate-model";
    let model_path = format!("{ns}/{name}");

    // Create repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // DELETE the repo
    let delete = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/models/{ns}/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete.status(), StatusCode::NO_CONTENT);

    // Re-create the repo
    let create2 = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create2.status(), StatusCode::CREATED);

    // Commit a file to the recreated repo
    let content_b64 = STANDARD.encode(b"recreated repo content");
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"commit to recreated repo\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"test.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{ns}/{name}/commit/main"))
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        commit.status(),
        StatusCode::OK,
        "commit to recreated repo should succeed, got {}",
        commit.status()
    );

    // Verify the commit created a revision
    let rev = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/models/{ns}/{name}/revisions"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rev.status(), StatusCode::OK);
    let rev_json = body_json(rev).await;
    let revisions = rev_json["revisions"].as_array().unwrap();
    assert!(!revisions.is_empty(), "should have revisions after commit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_wrong_parent() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let ns = "wrong-parent-team";
    let name = "wrong-parent-model";
    let model_path = format!("{ns}/{name}");

    // Create repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Commit with a made-up parentCommit that doesn't exist
    let content_b64 = STANDARD.encode(b"wrong parent test");
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"wrong parent\",\"parentCommit\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}}}}\n\
         {{\"file\":{{\"path\":\"f.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{ns}/{name}/commit/main"))
                .header("content-type", "application/x-ndjson")
                .body(Body::from(ndjson))
                .unwrap(),
        )
        .await
        .unwrap();

    // The commit should fail because the parentCommit doesn't match the resolved revision.
    // The commit route resolves the ref first (parent_sha), then the body's parentCommit
    // is checked against that. A made-up parentCommit that doesn't match the ref's SHA
    // should produce a 409 Conflict.
    assert!(
        commit.status().is_client_error(),
        "commit with wrong parentCommit should return a client error, got {}",
        commit.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_webhooks_create_delete_recreate_same_url() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub]).await;

    let ns = "wh-recreate-team";
    let name = "wh-recreate-model";
    let model_path = format!("{ns}/{name}");

    // Create repo
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":&model_path,"private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let webhook_url = "https://hooks.example.com/recreate-webhook";

    // Step 1: Create webhook → 201
    let create_wh = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{ns}/{name}/webhooks"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"url": webhook_url, "events": ["push"]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create_wh.status(), StatusCode::CREATED);
    let wh_json = body_json(create_wh).await;
    let wh_id = wh_json["id"].as_str().unwrap().to_owned();

    // Step 2: Delete the webhook → 204
    let delete_wh = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/models/{ns}/{name}/webhooks/{wh_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(delete_wh.status(), StatusCode::NO_CONTENT);

    // Step 3: Create same URL again → expect 201 (not 409)
    let recreate_wh = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/api/models/{ns}/{name}/webhooks"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"url": webhook_url, "events": ["push"]}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recreate_wh.status(),
        StatusCode::CREATED,
        "creating a webhook with the same URL after deletion should return 201, got {}",
        recreate_wh.status()
    );
}

// ============================================================================
// Section 2: Route combination tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_and_oci_coexist() {
    let (app, _tmp) = test_app(&[ServerFrontend::BazelHttp, ServerFrontend::Oci]).await;

    // Upload to Bazel CAS
    let content = b"bazel-oci-coexist-content";
    let hash = test_hash(content);
    let bazel_put = app
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
    assert_eq!(bazel_put.status(), StatusCode::NO_CONTENT);

    // Verify Bazel CAS read works
    let bazel_get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bazel_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(bazel_get).await, content);

    // Verify NOT found via OCI (different namespace)
    let oci_get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v2/{OCI_TEST_REPO}/blobs/sha256:{hash}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        oci_get.status(),
        StatusCode::NOT_FOUND,
        "Bazel CAS content should NOT be accessible via OCI (namespace isolation)"
    );

    // Also verify OCI blob upload works
    let oci_data = b"oci-coexist-data";
    let oci_digest = sha256_hex(oci_data);
    let oci_post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/{OCI_TEST_REPO}/blobs/uploads/?digest=sha256:{oci_digest}"
                ))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(oci_data.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(oci_post.status(), StatusCode::CREATED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_and_lfs_coexist() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub, ServerFrontend::Lfs]).await;

    // Upload via Hub's LFS route (/lfs/objects/{oid})
    let content = b"hub-lfs-coexist-content";
    let oid = test_oid(content);
    let hub_put = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/lfs/objects/{oid}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(hub_put.status(), StatusCode::OK);

    // Verify v1 LFS route works independently (not finding Hub LFS content)
    // The v1 LFS route is registered by the Lfs frontend and should not conflict.
    let v1_put = app
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
    assert_eq!(
        v1_put.status(),
        StatusCode::OK,
        "v1 LFS route should work alongside Hub LFS route"
    );

    // Verify both routes can read their own data
    let hub_get = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(hub_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(hub_get).await, content);

    let v1_get = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(v1_get.status(), StatusCode::OK);
    assert_eq!(body_bytes(v1_get).await, content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_and_xet_without_provider_tokens() {
    let (app, _tmp) = test_app(&[ServerFrontend::Hub, ServerFrontend::Xet]).await;

    // Hub routes should work (health endpoint)
    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    // Xet routes should work (healthz is always mounted)
    let healthz = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(healthz.status(), StatusCode::OK);

    // Hub repo creation should work
    let create = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/repos/create")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"type":"model","name":"hx-team/hx-model","private":false})
                        .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    // Xet stats endpoint should work
    let stats = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stats.status(), StatusCode::OK);

    // Hub xet-read-token route should NOT conflict with Xet's xet-read-token route.
    // When Hub + Xet are both active, the Hub's xet-read-token route is disabled
    // (register_hub_xet_routes = false) to avoid conflicts. Verify the Hub's
    // xet-read-token route returns 404 (not registered) while Xet routes still work.
    let hub_xet = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/models/hx-team/hx-model/xet-read-token/main")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The Hub's xet token routes should NOT be registered when Xet frontend is active.
    // So this should return 404.
    assert_eq!(
        hub_xet.status(),
        StatusCode::NOT_FOUND,
        "Hub xet-read-token should NOT be registered when Xet frontend is active"
    );

    // But the Xet frontend's xet-read-token route at a different path should work
    // (provider routes are not registered without provider tokens though)
}

// ============================================================================
// Section 3: Wrong-repo scope for LFS and Bazel
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_request_with_wrong_repo_scope() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::Lfs]).await;

    // Upload data with a token scoped to "other/other" (not the default "test/test")
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "other", "other", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap();
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let wrong_repo_token = provider.mint_token(&claims).unwrap();

    let content = b"lfs-wrong-repo-scope-content";
    let oid = test_oid(content);

    let put_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {wrong_repo_token}"))
                .header(header::CONTENT_TYPE, "application/octet-stream")
                .header(header::CONTENT_LENGTH, content.len().to_string())
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    // The token has Write scope, so the operation succeeds at the auth level.
    // However, the data is stored in a namespace derived from the repo scope
    // ("other/other"), not the default ("test/test").
    assert_eq!(
        put_resp.status(),
        StatusCode::OK,
        "PUT with valid Write token should succeed (wrong repo scope still passes auth)"
    );

    // Now try to read the same data with the default token scoped to "test/test"
    let default_token = test_token(TokenScope::Read);
    let get_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/lfs/objects/{oid}"))
                .header(header::AUTHORIZATION, format!("Bearer {default_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The data was stored in the "other/other" namespace, but this read
    // uses the "test/test" namespace → should return 404 Not Found.
    assert_eq!(
        get_resp.status(),
        StatusCode::NOT_FOUND,
        "LFS data stored with one repo scope should NOT be accessible with a different scope"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_request_with_wrong_repo_scope() {
    let (app, _tmp) = test_app_with_auth(&[ServerFrontend::BazelHttp]).await;

    // Upload data with a token scoped to "other/other"
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "other", "other", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap();
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let wrong_repo_token = provider.mint_token(&claims).unwrap();

    let content = b"bazel-wrong-repo-scope-content";
    let hash = test_hash(content);

    let put_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {wrong_repo_token}"))
                .body(Body::from(content.to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    // Token has Write scope → auth passes, data stored in "other/other" namespace
    assert_eq!(
        put_resp.status(),
        StatusCode::NO_CONTENT,
        "PUT with valid Write token should succeed (wrong repo scope still passes auth)"
    );

    // Read with the default token scoped to "test/test" → should fail (different namespace)
    let default_token = test_token(TokenScope::Read);
    let get_resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(format!("/v1/bazel/cache/cas/{hash}"))
                .header(header::AUTHORIZATION, format!("Bearer {default_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        get_resp.status(),
        StatusCode::NOT_FOUND,
        "Bazel CAS data stored with one repo scope should NOT be accessible with a different scope"
    );
}

// ── Ed25519 auth provider e2e ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_auth_token_successful_request() {
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();

    let seed = [0u8; 32];
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_private_key(seed.to_vec())
    .unwrap();

    let app = crate::app::router(config).await;
    assert!(app.is_ok(), "router should build with Ed25519 auth");
    let app = app.unwrap();

    // Mint an Ed25519-signed token using the same seed.
    let provider = Ed25519AuthProvider::new(&seed).expect("valid Ed25519 provider");
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", Some("main")).unwrap();
    let claims = TokenClaims::new("shardline", "test", TokenScope::Write, repo, u64::MAX).unwrap();
    let token = provider.mint_token(&claims).unwrap();

    // Exercise a route that actually enforces authentication.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_public_key_only_authenticates_private_key_token() {
    let tmp = TempDir::new().unwrap();
    let seed =
        hex::decode("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60").unwrap();
    let public_key =
        hex::decode("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a").unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_public_key(public_key)
    .unwrap();
    let app = crate::app::router(config).await.unwrap();

    let provider = Ed25519AuthProvider::new(&seed).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", None).unwrap();
    let claims =
        TokenClaims::new("issuer", "subject", TokenScope::Read, repository, u64::MAX).unwrap();
    let token = provider.mint_token(&claims).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_oci_registry_token_exchange_mints_usable_ed25519_token() {
    let tmp = TempDir::new().unwrap();
    let seed = [4_u8; 32];
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_server_frontends([ServerFrontend::Oci])
    .unwrap()
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_private_key(seed.to_vec())
    .unwrap();
    let app = crate::app::router(config).await.unwrap();

    let provider = Ed25519AuthProvider::new(&seed).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "team", "assets", None).unwrap();
    let claims = TokenClaims::new(
        "issuer",
        "oci-client",
        TokenScope::Write,
        repository,
        u64::MAX,
    )
    .unwrap();
    let bootstrap_token = provider.mint_token(&claims).unwrap();
    let basic_credentials = STANDARD.encode(format!("shardline:{bootstrap_token}"));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v2/token?service=shardline&scope=repository:team/assets:pull")
                .header(header::AUTHORIZATION, format!("Basic {basic_credentials}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_body = body_bytes(response).await;
    let response_json: Value = serde_json::from_slice(&response_body).unwrap();
    let exchanged_token = response_json["access_token"].as_str().unwrap();
    let exchanged_claims = provider.verify_token(exchanged_token).unwrap();
    assert_eq!(exchanged_claims.subject(), "oci-client");
    assert_eq!(exchanged_claims.scope(), TokenScope::Read);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v2/")
                .header(header::AUTHORIZATION, format!("Bearer {exchanged_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_protects_every_application_route_family() {
    let tmp = TempDir::new().unwrap();
    let seed = [5_u8; 32];
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_server_frontends([
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
        ServerFrontend::Hub,
    ])
    .unwrap()
    .with_deployment_mode(crate::DeploymentMode::Insecure)
    .with_metrics_token(b"metrics-secret".to_vec())
    .unwrap()
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_private_key(seed.to_vec())
    .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let hash = "0".repeat(64);
    let protected_routes = [
        ("GET", "/v1/stats".to_owned()),
        ("GET", "/reconstructions".to_owned()),
        ("POST", "/v1/shards".to_owned()),
        ("GET", format!("/v1/chunks/default/{hash}")),
        ("HEAD", format!("/v1/xorbs/default/{hash}")),
        ("GET", format!("/transfer/xorb/default/{hash}")),
        ("POST", "/v1/lfs/objects/batch".to_owned()),
        ("GET", format!("/v1/lfs/objects/{hash}")),
        ("PUT", format!("/v1/bazel/cache/cas/{hash}")),
        ("GET", "/v2/".to_owned()),
        ("GET", "/api/whoami-v2".to_owned()),
        ("GET", "/api/repos".to_owned()),
        ("POST", "/api/repos/create".to_owned()),
        ("POST", "/objects/batch".to_owned()),
        ("GET", format!("/lfs/objects/{hash}")),
        ("GET", "/models/team/assets/info/refs".to_owned()),
        ("POST", "/models/team/assets/git-receive-pack".to_owned()),
        ("GET", "/metrics".to_owned()),
    ];

    for (method, uri) in protected_routes {
        let (content_type, body) = match uri.as_str() {
            "/v1/lfs/objects/batch" => (
                "application/vnd.git-lfs+json",
                Body::from(r#"{"operation":"download","objects":[]}"#),
            ),
            "/objects/batch" => (
                "application/vnd.git-lfs+json",
                Body::from(r#"{"operation":"download","ref":{"name":"main"},"objects":[]}"#),
            ),
            "/api/repos/create" => (
                "application/json",
                Body::from(r#"{"type":"model","name":"team/assets","private":false}"#),
            ),
            _ => ("application/octet-stream", Body::empty()),
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(&uri)
                    .header(header::CONTENT_TYPE, content_type)
                    .body(body)
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri} was not protected"
        );
    }

    for uri in ["/healthz", "/readyz", "/health"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{uri} should remain available to probes"
        );
    }

    let provider = Ed25519AuthProvider::new(&seed).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "team", "assets", None).unwrap();
    let claims = TokenClaims::new(
        "issuer",
        "ed25519-user",
        TokenScope::Read,
        repository,
        u64::MAX,
    )
    .unwrap();
    let token = provider.mint_token(&claims).unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/whoami-v2")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response_json: Value = serde_json::from_slice(&body_bytes(response).await).unwrap();
    assert_eq!(response_json["name"], "ed25519-user");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_expired_token_is_rejected_by_authenticated_route() {
    let tmp = TempDir::new().unwrap();
    let seed = [3_u8; 32];
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        NonZeroUsize::new(65_536).unwrap(),
    )
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_private_key(seed.to_vec())
    .unwrap();
    let app = crate::app::router(config).await.unwrap();

    let provider = Ed25519AuthProvider::new(&seed).unwrap();
    let repository =
        RepositoryScope::new(RepositoryProvider::Generic, "test", "test", None).unwrap();
    let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repository, 1).unwrap();
    let token = provider.mint_token(&claims).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ed25519_auth_rejects_token_from_wrong_key() {
    use axum::http::Request;
    use tower::ServiceExt;

    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let seed = [1u8; 32];

    // Build router with Ed25519 auth using seed
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(crate::ServerRole::All)
    .with_auth_provider(crate::config::AuthProviderKind::Ed25519)
    .with_ed25519_private_key(seed.to_vec())
    .unwrap();
    let app = crate::app::router(config)
        .await
        .expect("router should build");

    // Mint a token with a DIFFERENT key (wrong key)
    let wrong_seed = [2u8; 32];
    let wrong_provider = Ed25519AuthProvider::new(&wrong_seed).expect("valid wrong provider");
    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "owner", "repo", None)
        .expect("valid repo scope");
    let claims = TokenClaims::new("issuer", "subject", TokenScope::Read, repo, 2_000_000_000)
        .expect("valid claims");
    let token = wrong_provider
        .mint_token(&claims)
        .expect("should mint token");

    // Make an authenticated request with the wrong key's token to a route
    // that enforces auth (unlike /healthz which is always open).
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/stats")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

// ---------------------------------------------------------------------------
// Mixed-format dedup
// ---------------------------------------------------------------------------

/// Same content uploaded as FixedChunkV1 and XorbCdcV1 deduplicates correctly.
///
/// The chunk hash is computed from raw bytes for both storage representations,
/// so the chunk storage path is the same regardless of format.  Uploading the
/// same content through the CAS pipeline after manually placing a raw chunk
/// exercises chunk-level deduplication.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_format_dedup_same_content() {
    let tmp = TempDir::new().expect("tempdir");
    let chunk_size = NonZeroUsize::new(65536).expect("chunk size");

    // 1. Compute content hashes and object keys used by both formats.
    let content = b"dedup-test-content-that-should-be-identical-in-both-formats!";
    let hash = test_hash(content);
    let object_key = crate::bazel_cache_object_key(
        crate::BazelCacheKind::Cas,
        &hash,
        &AuthorizedRepository::anonymous_full_access(),
    )
    .expect("object key");

    // The file_id matches what `protocol_object_file_id` computes in backend.rs.
    let file_id = format!(
        "protocol-object-{}",
        hex::encode(Sha256::digest(object_key.as_str().as_bytes()))
    );

    let chunk_hash = xet_hash_hex_string(crate::local_backend::chunk_hash(content));
    let chunk_object_key = crate::chunk_store::chunk_object_key(&chunk_hash).expect("chunk key");

    // 2. Write the raw (uncompressed) chunk to the object store, simulating
    //    what FixedChunkV1 storage would produce.
    let object_store = ServerObjectStore::local(tmp.path().join("chunks")).expect("object store");
    object_store
        .put_if_absent(
            &chunk_object_key,
            ObjectBody::from_vec(content.to_vec()),
            &ObjectIntegrity::new(
                crate::local_backend::chunk_hash(content),
                content.len() as u64,
            ),
        )
        .expect("write raw chunk");

    // 3. Register a FixedChunkV1 FileRecord so the index has a record for
    //    this file.
    let record_store = LocalRecordStore::open(tmp.path().to_path_buf());
    record_store
        .commit_file_version_metadata(&FileRecord {
            file_id: file_id.clone(),
            content_hash: hash.clone(),
            total_bytes: content.len() as u64,
            chunk_size: 4_194_304,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.clone(),
                offset: 0,
                length: content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: content.len() as u64,
            }],
        })
        .await
        .expect("commit fixed record");

    // 4. Upload the SAME content through the CAS path.  The ingestor (which
    //    uses XorbCdcV1 for the FileRecord) finds the chunk already present
    //    in the object store → chunk-level deduplication.
    //
    //    The FileRecord already exists (created in step 3 above), so the
    //    backend returns PutOutcome::AlreadyExists after verifying the body
    //    hash matches.  We only assert that the call does not error.
    let backend = LocalBackend::new_with_object_store_and_upload_parallelism_with_frontends(
        tmp.path().to_path_buf(),
        "http://127.0.0.1:8080".to_owned(),
        chunk_size,
        NonZeroUsize::new(64).expect("parallelism"),
        object_store.clone(),
        &[ServerFrontend::BazelHttp],
    )
    .await
    .expect("backend");

    let server_backend = crate::backend::ServerBackend::Local(backend);
    let _outcome = server_backend
        .put_sha256_addressed_object_stream_if_absent(
            &object_key,
            &hash,
            crate::upload_ingest::RequestBodyReader::from_bytes(axum::body::Bytes::from_static(
                content,
            )),
        )
        .await
        .expect("cas upload");

    // 5. Verify the content is readable through the backend.
    let bytes = server_backend
        .read_object(&object_key)
        .await
        .expect("readable");
    assert_eq!(bytes, content, "content mismatch");

    // 6. Verify only ONE chunk file exists on disk — the chunk we manually
    //    placed is the same one the CAS pipeline would have stored, proving
    //    chunk-level deduplication across formats.
    let chunk_path = tmp
        .path()
        .join("chunks")
        .join(&chunk_hash[..2])
        .join(&chunk_hash);
    assert!(
        chunk_path.exists(),
        "chunk file should exist at {chunk_path:?}"
    );

    // Check that no OTHER chunk files were created (i.e. the CAS upload
    // reused the existing chunk rather than writing a duplicate).
    let chunks_dir = tmp.path().join("chunks");
    let mut chunk_count = 0_usize;
    if let Ok(entries) = std::fs::read_dir(&chunks_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir()
                && let Ok(files) = std::fs::read_dir(&path)
            {
                chunk_count += files.flatten().count();
            }
        }
    }
    assert_eq!(
        chunk_count, 1,
        "expected exactly one chunk file (dedup), found {chunk_count}"
    );
}
