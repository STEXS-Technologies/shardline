#![cfg(feature = "docker")]
#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or
)]

mod http_contracts;

#[path = "http_contracts/concurrency.rs"]
mod concurrency_contracts;

use sha2::{Digest, Sha256};
use shardline_protocol::SecretString;
use shardline_server::{
    ObjectStorageAdapter, ServerConfig, ServerFrontend, ServerRole, app, test_fixtures,
};
use shardline_storage::S3ObjectStoreConfig;
use shardline_test_support::{DockerLocalStack, S3FaultProxy, S3ProxyFault};
use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::OnceCell;
use tower::ServiceExt;
mod maintenance_profiles;

// ---------------------------------------------------------------------------
// Shared Docker MinIO — one container for all tests.
// ---------------------------------------------------------------------------

static MINIO: OnceCell<(DockerLocalStack, String)> = OnceCell::const_new();

/// Ensure the global Docker MinIO is running with the S3 bucket created.
/// Returns the unique key prefix for object isolation.
async fn ensure_minio() -> &'static str {
    let (_, prefix) = MINIO
        .get_or_init(|| async {
            let stack = DockerLocalStack::builder()
                .with_minio()
                .start()
                .unwrap()
                .expect("Docker MinIO: is docker available?");
            let prefix = stack.unique_s3_key_prefix("e2e-http");
            (stack, prefix)
        })
        .await;
    prefix
}

/// Build an `S3ObjectStoreConfig` from the shared MinIO instance.
fn s3_config(key_prefix: &str) -> S3ObjectStoreConfig {
    let raw = MINIO
        .get()
        .expect("ensure_minio() not called yet")
        .0
        .s3_raw_config(Some(key_prefix))
        .expect("MinIO should be configured");

    S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(
            raw.access_key.map(SecretString::new),
            raw.secret_key.map(SecretString::new),
            raw.session_token.map(SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http)
}

// ---------------------------------------------------------------------------
// Test server harness — starts a real HTTP server on a random port.
// ---------------------------------------------------------------------------

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
/// Admin read token wired into spawned servers so tests verify runtime
/// topology through the authenticated admin API (the unauthenticated /readyz
/// no longer carries runtime metadata).
const TEST_ADMIN_TOKEN: &str = "s3-e2e-admin-read-token";

use http_contracts::mint_token_for;

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    base_url: String,
    token: String,
    _tmp: TempDir,
}

impl TestServer {
    /// Starts a full shardline HTTP server with the given frontends, backed by
    /// the shared Docker MinIO instance (local metadata, S3 object store).
    async fn start(frontends: &[ServerFrontend]) -> Self {
        let key_prefix = ensure_minio().await;
        Self::start_with_s3_config(frontends, s3_config(key_prefix)).await
    }

    async fn start_with_s3_config(
        frontends: &[ServerFrontend],
        object_storage: S3ObjectStoreConfig,
    ) -> Self {
        Self::start_with_s3_config_mode(frontends, object_storage, false).await
    }

    async fn start_with_s3_config_mode(
        frontends: &[ServerFrontend],
        object_storage: S3ObjectStoreConfig,
        selective_serialized_ranges: bool,
    ) -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();

        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(frontends.to_vec())
        .unwrap()
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled()
        .with_admin_read_token(TEST_ADMIN_TOKEN.as_bytes().to_vec())
        .unwrap()
        .with_object_storage(ObjectStorageAdapter::S3, Some(object_storage))
        .with_selective_serialized_xorb_ranges(selective_serialized_ranges);

        config.validate_runtime_requirements().unwrap();

        // Build the router and start serving.
        let app = app::router(config).await.unwrap();

        // Bind to a random port via tokio's TcpListener.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        // Give the server a moment to start.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Mint a bearer token with full scope.
        let token = mint_token_for("test", "test");

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            token,
            _tmp: tmp,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn auth_header(&self) -> &str {
        &self.token
    }

    /// Returns a bearer token scoped to an arbitrary `owner/name` repo, for use
    /// with repo-scoped Hub-API routes whose `{ns}/{repo}` must match the
    /// token's `RepositoryScope`.
    fn auth_header_for(&self, owner: &str, name: &str) -> String {
        mint_token_for(owner, name)
    }
}

fn s3_config_through_proxy(
    raw: shardline_test_support::S3RawConfig,
    proxy: &S3FaultProxy,
) -> S3ObjectStoreConfig {
    S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(Some(proxy.endpoint().to_owned()))
        .with_credentials(
            raw.access_key.map(SecretString::new),
            raw.secret_key.map(SecretString::new),
            raw.session_token.map(SecretString::new),
        )
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_allow_http(raw.allow_http)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_s3_xorb_read_is_rejected_before_transfer_bytes_are_exposed() {
    let key_prefix = ensure_minio().await;
    let raw = MINIO
        .get()
        .expect("MinIO initialized")
        .0
        .s3_raw_config(Some(key_prefix))
        .expect("MinIO S3 config");
    let provider_endpoint = raw.endpoint.clone().expect("MinIO HTTP endpoint");

    let current_content = vec![0x31_u8; 64 * 1024];
    let stale_content = vec![0x72_u8; current_content.len()];
    let (current_xorb, current_hash) = test_fixtures::single_chunk_xorb(&current_content);
    let (stale_xorb, stale_hash) = test_fixtures::single_chunk_xorb(&stale_content);
    assert_ne!(current_hash, stale_hash);
    assert_eq!(
        current_xorb.len(),
        stale_xorb.len(),
        "same-size uncompressed xorbs make the substituted HTTP response well formed",
    );

    let proxy = S3FaultProxy::start_disarmed(
        &provider_endpoint,
        S3ProxyFault::ReplaceSuccessfulGetBody(Arc::from(stale_xorb.as_ref())),
    )
    .expect("start stale-read proxy");
    let object_storage = s3_config_through_proxy(raw, &proxy);
    let server = TestServer::start_with_s3_config(&[ServerFrontend::Xet], object_storage).await;
    let client = reqwest::Client::new();
    let transfer_url = server.url(&format!("/transfer/xorb/default/{current_hash}"));

    let upload = client
        .put(&transfer_url)
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .body(current_xorb.clone())
        .send()
        .await
        .expect("upload current xorb through proxy");
    assert_eq!(upload.status(), 200);

    let (shard_bytes, _file_id) =
        test_fixtures::single_file_shard(&[(&current_content, &current_hash)]);
    let shard = client
        .post(server.url("/v1/shards"))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/octet-stream")
        .body(shard_bytes)
        .send()
        .await
        .expect("register repository reference to current xorb");
    assert_eq!(shard.status(), 200);

    proxy.arm();

    let stale_read = client
        .get(&transfer_url)
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("stale provider read returns a clean server response");
    assert!(proxy.fault_was_injected());
    assert!(
        !stale_read.status().is_success(),
        "a stale provider xorb must never become a successful transfer",
    );

    let recovered = client
        .get(&transfer_url)
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("read after one-shot stale response");
    assert_eq!(recovered.status(), 206);
    let recovered_bytes = recovered.bytes().await.expect("recovered range body");
    assert_eq!(recovered_bytes.as_ref(), &current_xorb[..4096]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opt_in_s3_xorb_range_authenticates_scope_before_provider_payload_reads() {
    let key_prefix = ensure_minio().await;
    let raw = MINIO
        .get()
        .expect("MinIO initialized")
        .0
        .s3_raw_config(Some(key_prefix))
        .expect("MinIO S3 config");
    let provider_endpoint = raw.endpoint.clone().expect("MinIO HTTP endpoint");
    let proxy =
        S3FaultProxy::start_disarmed(&provider_endpoint, S3ProxyFault::ServiceUnavailableOnce)
            .expect("start counted provider proxy");
    let object_storage = s3_config_through_proxy(raw, &proxy);
    let server =
        TestServer::start_with_s3_config_mode(&[ServerFrontend::Xet], object_storage, true).await;
    let client = reqwest::Client::new();
    let (serialized, hash) = test_fixtures::single_chunk_xorb(&vec![0x31_u8; 64 * 1024]);
    let transfer_url = server.url(&format!("/transfer/xorb/default/{hash}"));
    let upload = client
        .put(&transfer_url)
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .body(serialized.clone())
        .send()
        .await
        .expect("upload Xorb");
    assert_eq!(upload.status(), 200);
    let (shard_bytes, _file_id) =
        test_fixtures::single_file_shard(&[(&vec![0x31_u8; 64 * 1024], &hash)]);
    let shard = client
        .post(server.url("/v1/shards"))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/octet-stream")
        .body(shard_bytes)
        .send()
        .await
        .expect("publish repository membership");
    assert_eq!(shard.status(), 200);

    let before = proxy.get_requests();
    let response = client
        .get(&transfer_url)
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("selective Xorb range");
    assert_eq!(response.status(), 206);
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        &serialized[..4096]
    );
    assert!(
        proxy.get_requests() - before >= 3,
        "opt-in range should perform suffix, footer, and selected-member GETs"
    );

    let outsider = server.auth_header_for("outsider", "repo");
    let before_denied = proxy.get_requests();
    let denied = client
        .get(&transfer_url)
        .header("Authorization", format!("Bearer {outsider}"))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("out-of-scope request rejection");
    assert_eq!(denied.status(), 404);
    assert_eq!(
        proxy.get_requests(),
        before_denied,
        "scope denial makes no provider GET"
    );
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// ---------------------------------------------------------------------------
// TestServerBuilder — configurable test server for provider tests
// ---------------------------------------------------------------------------

struct TestServerBuilder {
    frontends: Vec<ServerFrontend>,
    provider_config: Option<(TempDir, std::path::PathBuf)>,
}

impl TestServerBuilder {
    fn new(frontends: &[ServerFrontend]) -> Self {
        Self {
            frontends: frontends.to_vec(),
            provider_config: None,
        }
    }

    fn with_provider(mut self) -> Self {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("providers.json");
        let provider_config = serde_json::json!({
            "providers": [{
                "kind": "generic",
                "integration_subject": "test-app",
                "webhook_secret": "test-secret",
                "repositories": [{
                    "owner": "test",
                    "name": "test",
                    "visibility": "private",
                    "default_revision": "main",
                    "clone_url": "https://example.com/test/test.git",
                    "read_subjects": ["test-user"],
                    "write_subjects": ["test-user"]
                }]
            }]
        });
        std::fs::write(&config_path, serde_json::to_vec(&provider_config).unwrap()).unwrap();
        self.provider_config = Some((tmp, config_path));
        self
    }

    async fn start(&mut self) -> TestServer {
        let key_prefix = ensure_minio().await;
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();

        let mut config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(self.frontends.clone())
        .unwrap()
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled()
        .with_admin_read_token(TEST_ADMIN_TOKEN.as_bytes().to_vec())
        .unwrap()
        .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config(key_prefix)));

        if let Some((_provider_tmp, config_path)) = self.provider_config.as_ref() {
            config = config
                .with_provider_runtime(
                    config_path.clone(),
                    b"test-api-key-16bytes".to_vec(),
                    "test-issuer".to_owned(),
                    NonZeroU64::new(3600).unwrap(),
                )
                .unwrap();
        }

        config.validate_runtime_requirements().unwrap();
        let app = app::router(config).await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let token = mint_token_for("test", "test");

        TestServer {
            shutdown: Some(shutdown_tx),
            base_url,
            token,
            _tmp: tmp,
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

/// Helper: compute SHA-256 hex digest.
fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

// ---------------------------------------------------------------------------
// 2. Ready endpoint — shows s3 object backend
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_readyz_returns_s3_backend() {
    let server = TestServer::start(&[ServerFrontend::Xet]).await;
    let client = reqwest::Client::new();

    let resp = client.get(server.url("/readyz")).send().await.unwrap();

    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["status"], "ok");
    // Backend topology is exposed on the AUTHENTICATED admin status endpoint,
    // not the unauthenticated /readyz.
    let resp = client
        .get(server.url("/api/v1/status"))
        .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["metadata_backend"], "local");
    assert_eq!(json["object_backend"], "s3");
}

// ---------------------------------------------------------------------------
// 6. OCI — session upload (oneshot via app to bypass wildcard routing)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_upload_session() {
    let content = b"OCI session upload content";
    // Repository must match or be under the token scope owner/name (test/test).
    let repo = "test/test";

    // Build a fresh app for oneshot testing.
    let key_prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Oci])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config(key_prefix)));
    config.validate_runtime_requirements().unwrap();

    // Build the same app via the shared router function used by the server
    // binary. This tests the full handler/backend stack including auth,
    // S3 object store, OCI path parsing, and write path.
    let app = app::router(config).await.unwrap();

    // Mint a token with the same signing key.
    let token = mint_token_for("test", "test");

    http_contracts::oci::blob_round_trip(&app, &token, repo, content).await;
}

// ---------------------------------------------------------------------------
// 8. Metrics — contains expected labels
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metrics_contains_expected_content() {
    let server = TestServer::start(&[ServerFrontend::Xet]).await;
    let client = reqwest::Client::new();

    let resp = client.get(server.url("/metrics")).send().await.unwrap();

    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("shardline_up 1"),
        "metrics should contain shardline_up gauge"
    );
    assert!(body.contains("# HELP"), "metrics should contain HELP lines");
    assert!(body.contains("# TYPE"), "metrics should contain TYPE lines");
    assert!(
        body.contains("metadata_backend=\"local\""),
        "metrics should reference local backend: {body}"
    );
    assert!(
        body.contains("object_backend=\"s3\""),
        "metrics should reference s3 backend: {body}"
    );
}

// ---------------------------------------------------------------------------
// 10. All frontends simultaneously — health/ready work with all frontends
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_all_frontends_health_and_ready() {
    let frontends = &[
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
        ServerFrontend::Hub,
    ];
    let server = TestServer::start(frontends).await;
    let client = reqwest::Client::new();

    // Health check.
    let resp = client.get(server.url("/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["status"], "ok");

    // Readiness check.
    let resp = client.get(server.url("/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["status"], "ok");

    // Backend topology is exposed on the AUTHENTICATED admin status endpoint,
    // not the unauthenticated /readyz.
    let resp = client
        .get(server.url("/api/v1/status"))
        .header("Authorization", format!("Bearer {TEST_ADMIN_TOKEN}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["metadata_backend"], "local");
    assert_eq!(json["object_backend"], "s3");
    assert!(json["server_frontends"].as_array().unwrap().len() >= 5);
}

// ===========================================================================
// 11. OCI Manifest + Tag operations (via oneshot — wildcard route limitation)
// ===========================================================================

/// Build a temporary OCI app for oneshot tests backed by S3/local metadata.
async fn oci_oneshot_app() -> (axum::Router, String, TempDir) {
    let key_prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Oci])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config(key_prefix)));
    config.validate_runtime_requirements().unwrap();
    let app = app::router(config).await.unwrap();

    let token = mint_token_for("test", "test");
    (app, token, tmp)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_manifest_push_and_get_by_tag() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::manifest_by_tag(
        &app,
        &token,
        "test/test",
        "s3-v0.0.1",
        b"\x1f\x8b\x08\x00",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_manifest_get_by_digest() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::manifest_by_digest(
        &app,
        &token,
        "test/test",
        "s3-digest-ref",
        b"s3-digest-layer",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_tags_list() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::tags_list(&app, &token, "test/test", "s3-list-me", b"s3-tags-layer").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oci_blob_head() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::blob_head(&app, &token, "test/test", b"s3-head-blob").await;
}

// ===========================================================================
// 12. LFS edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lfs_batch_upload_existing() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::batch_upload_existing(&server, b"lfs-batch-existing-test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lfs_batch_download_missing() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::batch_download_missing(&server, &"f".repeat(64), 42).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lfs_head_object() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::head_object(&server, b"lfs-head-test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_lfs_patch_object() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::patch_object(&server, b"lfs-patch-initial-", b"content").await;
}

// ===========================================================================
// 13. Hub API routes beyond whoami
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hub_create_repo_and_commit() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3e2e-team";
    let name = "s3e2e-model";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    let create_resp = client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"type":"model","name":&model_path,"private":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 201);

    let info_resp = client
        .get(server.url(&format!("/api/models/{ns}/{name}")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(info_resp.status(), 200);
    let info: serde_json::Value = info_resp.json().await.unwrap();
    assert_eq!(info["id"], model_path);

    let content_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"Hello S3 e2e!");
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"s3 commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"hello.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit_resp = client
        .post(server.url(&format!("/api/models/{ns}/{name}/commit/main")))
        .header("Authorization", auth())
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(commit_resp.status(), 200);

    let rev_resp = client
        .get(server.url(&format!("/api/models/{ns}/{name}/revisions")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(rev_resp.status(), 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hub_upload_lfs_and_batch() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();
    let auth = || format!("Bearer {}", server.auth_header());

    let content = b"hub-s3-lfs-content";
    let oid = sha256_hex(content);

    let put = client
        .put(server.url(&format!("/lfs/objects/{oid}")))
        .header("Authorization", auth())
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 200);

    let get = client
        .get(server.url(&format!("/lfs/objects/{oid}")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hub_modelcard() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3card-team";
    let name = "s3card-model";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"type":"model","name":&model_path,"private":false}))
        .send()
        .await
        .unwrap();

    let card_resp = client
        .get(server.url(&format!("/api/models/{ns}/{name}/modelcard")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(card_resp.status(), 404);

    let readme_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"# S3 Model Card",
    );
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"add readme\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"README.md\",\"content\":\"{readme_b64}\"}}}}"
    );
    client
        .post(server.url(&format!("/api/models/{ns}/{name}/commit/main")))
        .header("Authorization", auth())
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();

    let card2 = client
        .get(server.url(&format!("/api/models/{ns}/{name}/modelcard")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(card2.status(), 200);
}

// ===========================================================================
// 21. 409 Conflict — duplicate Hub repo creation (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_hub_create_same_repo_twice_returns_409() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3-conflict-team";
    let name = "s3-conflict-model";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    // First create — should succeed.
    let create1 = client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"type": "model", "name": &model_path, "private": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(create1.status(), 201);

    // Second create (same repo) — should return 409 Conflict.
    let create2 = client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"type": "model", "name": &model_path, "private": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        create2.status(),
        409,
        "duplicate repo creation should return 409"
    );
}

// ===========================================================================
// 14. Error / edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_error_no_auth_on_protected_endpoints() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(server.url("/v1/lfs/objects/batch"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation":"download","objects":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_error_invalid_token() {
    let server = TestServer::start(&[ServerFrontend::Xet]).await;
    let client = reqwest::Client::new();
    let resp = client
        .get(server.url("/v1/stats"))
        .header("Authorization", "Bearer invalid.token.here")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_error_oci_manifest_not_found() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri("/v2/test/test/manifests/nonexistent-tag")
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(resp.status(), 404);
    let b = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let j: serde_json::Value = serde_json::from_slice(&b).unwrap();
    assert_eq!(j["errors"][0]["code"], "MANIFEST_UNKNOWN");
}

// ===========================================================================
// 15. Metrics after operations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metrics_after_lfs_upload() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    let client = reqwest::Client::new();
    let content = b"s3-metrics-content";
    let oid = sha256_hex(content);

    client
        .put(server.url(&format!("/v1/lfs/objects/{oid}")))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let m = client.get(server.url("/metrics")).send().await.unwrap();
    assert_eq!(m.status(), 200);
    assert!(m.text().await.unwrap().contains("shardline_up 1"));
}

// ===========================================================================
// 16. LFS round trip with OCI enabled
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cross_protocol_lfs_to_oci() {
    let server = TestServer::start(&[ServerFrontend::Lfs, ServerFrontend::Oci]).await;
    http_contracts::lfs::put_and_get(&server, b"s3-cross-protocol").await;
}

// ===========================================================================
// 17. Provider token tests (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_provider_issue_token_with_valid_key() {
    let mut builder =
        TestServerBuilder::new(&[ServerFrontend::Xet, ServerFrontend::Oci]).with_provider();
    let server = builder.start().await;
    http_contracts::provider::issue_token(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_provider_issue_token_without_api_key_returns_401() {
    let mut builder = TestServerBuilder::new(&[ServerFrontend::Xet]).with_provider();
    let server = builder.start().await;
    http_contracts::provider::deny_token(&server, None, 401).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_provider_git_lfs_authenticate() {
    let mut builder =
        TestServerBuilder::new(&[ServerFrontend::Xet, ServerFrontend::Oci]).with_provider();
    let server = builder.start().await;
    http_contracts::provider::git_lfs_authenticate(&server).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_provider_xet_read_token() {
    let mut builder = TestServerBuilder::new(&[ServerFrontend::Xet]).with_provider();
    let server = builder.start().await;
    http_contracts::provider::xet_token(&server, "read").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_provider_xet_write_token() {
    let mut builder = TestServerBuilder::new(&[ServerFrontend::Xet]).with_provider();
    let server = builder.start().await;
    http_contracts::provider::xet_token(&server, "write").await;
}

// ===========================================================================
// 18. More OCI error paths (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_upload_invalid_digest_algorithm() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::upload_invalid_digest_algorithm(&app, &token, "test/test", b"x").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_upload_body_hash_mismatch() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::upload_body_hash_mismatch(
        &app,
        &token,
        "test/test",
        b"different",
        b"actual",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_patch_nonexistent_session() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::patch_nonexistent_session(&app, &token, "test/test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_put_finalize_without_digest() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::put_finalize_without_digest(&app, &token, "test/test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_get_nonexistent_blob() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::get_nonexistent_blob(&app, &token, "test/test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_delete_nonexistent_manifest() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::delete_nonexistent_manifest(&app, &token, "test/test").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_oci_push_manifest_with_nonexistent_blob() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    http_contracts::oci::push_manifest_with_nonexistent_blob(&app, &token, "test/test").await;
}

// ===========================================================================
// 19. More LFS edge cases (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_lfs_verify_object() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::verify_existing(&server, b"s3-verify-content").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_lfs_verify_nonexistent() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::verify_missing(&server, &"f".repeat(64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_lfs_batch_mixed() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::batch_mixed(&server, b"s3-mixed-existing", &"d".repeat(64), 99).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_lfs_upload_empty() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    http_contracts::lfs::put_and_get(&server, b"").await;
}

// ===========================================================================
// 20. More Bazel edge cases (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_bazel_ac_put_and_get() {
    let server = TestServer::start(&[ServerFrontend::BazelHttp]).await;
    http_contracts::bazel::ac_put_and_get(&server, b"s3-ac-content").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_bazel_head_existing() {
    let server = TestServer::start(&[ServerFrontend::BazelHttp]).await;
    http_contracts::bazel::head_existing(&server, b"s3-head-cas").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_bazel_head_nonexistent() {
    let server = TestServer::start(&[ServerFrontend::BazelHttp]).await;
    http_contracts::bazel::head_missing(&server, &"f".repeat(64)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_bazel_cas_content_hash_mismatch() {
    let server = TestServer::start(&[ServerFrontend::BazelHttp]).await;
    http_contracts::bazel::cas_hash_mismatch(&server, b"different-body", b"actual-body").await;
}

// ===========================================================================
// 23. Concurrent HTTP E2E tests (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_concurrent_lfs_upload_same_oid() {
    let server = TestServer::start(&[ServerFrontend::Lfs]).await;
    concurrency_contracts::lfs_upload_same_oid(&server, b"concurrent-same-oid-content-s3").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_concurrent_oci_manifest_push_and_pull() {
    let (app, token, _tmp) = oci_oneshot_app().await;
    concurrency_contracts::oci_manifest_reads(
        &app,
        &token,
        "test/test",
        "s3-concurrent-manifest",
        b"s3-concurrent-layer",
    )
    .await;
}

// ===========================================================================
// 24. Cross-protocol content sharing (S3)
// ===========================================================================
//
// Each protocol uses its own storage key namespace — cross-protocol reads
// document namespace isolation (expect 404). Same-protocol reads verify
// the upload was successful.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_cross_bazel_cas_to_lfs() {
    let server = TestServer::start(&[ServerFrontend::BazelHttp, ServerFrontend::Lfs]).await;
    let client = reqwest::Client::new();

    let content = b"s3-bazel-cas-to-lfs-cross";
    let hash = sha256_hex(content);

    // Upload via Bazel CAS
    let put_resp = client
        .put(server.url(&format!("/v1/bazel/cache/cas/{hash}")))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert!(
        put_resp.status().is_success(),
        "Bazel CAS PUT failed: {}",
        put_resp.status()
    );

    // Verify same-protocol read works
    let bazel_get = client
        .get(server.url(&format!("/v1/bazel/cache/cas/{hash}")))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .send()
        .await
        .unwrap();
    assert_eq!(bazel_get.status(), 200);
    assert_eq!(bazel_get.bytes().await.unwrap().as_ref(), content);

    // Cross-protocol read via LFS → 404 (different key namespace)
    let lfs_get = client
        .get(server.url(&format!("/v1/lfs/objects/{hash}")))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        lfs_get.status(),
        404,
        "LFS should not find data stored via Bazel CAS (separate key namespace)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_cross_lfs_to_oci_blob() {
    // Use oneshot because OCI uses wildcard routes
    let key_prefix = ensure_minio().await;
    let tmp = TempDir::new().unwrap();
    let chunk_size = NonZeroUsize::new(65536).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:0".parse().unwrap(),
        "http://127.0.0.1:8080".to_owned(),
        tmp.path().to_path_buf(),
        chunk_size,
    )
    .with_server_role(ServerRole::All)
    .with_server_frontends([ServerFrontend::Lfs, ServerFrontend::Oci])
    .unwrap()
    .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
    .unwrap()
    .with_reconstruction_cache_disabled()
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3_config(key_prefix)));
    config.validate_runtime_requirements().unwrap();
    let app = app::router(config).await.unwrap();

    let token = mint_token_for("test", "test");
    let _keep = Box::new(tmp);

    let content = b"s3-lfs-to-oci-cross";
    let digest = sha256_hex(content);
    let repo = "test/test";

    // Upload via LFS
    let put_uri = format!("/v1/lfs/objects/{digest}");
    let put_req = axum::http::Request::builder()
        .method("PUT")
        .uri(&put_uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(axum::body::Body::from(content.to_vec()))
        .unwrap();
    let put_resp = app.clone().oneshot(put_req).await.unwrap();
    assert!(
        put_resp.status().is_success(),
        "LFS PUT failed: {}",
        put_resp.status()
    );

    // Verify same-protocol read works
    let lfs_get = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("GET")
                .uri(&put_uri)
                .header("Authorization", format!("Bearer {token}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        lfs_get.status(),
        200,
        "LFS GET after LFS upload should work"
    );

    // Cross-protocol read via OCI → 404 (different key namespace)
    let get_uri = format!("/v2/{repo}/blobs/sha256:{digest}");
    let get_req = axum::http::Request::builder()
        .method("GET")
        .uri(&get_uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(axum::body::Body::empty())
        .unwrap();
    let get_resp = app.clone().oneshot(get_req).await.unwrap();
    assert_eq!(
        get_resp.status(),
        404,
        "OCI should not find data stored via LFS (separate key namespace)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_cross_hub_lfs_to_v1_lfs() {
    let server = TestServer::start(&[ServerFrontend::Hub, ServerFrontend::Lfs]).await;
    let client = reqwest::Client::new();

    // Hub LFS keys are namespaced by the token's repository scope, so both the
    // Hub PUT and GET must use a token bound to the same repo for the keys to
    // line up. The V1 LFS read uses a different scope, keeping the namespaces
    // distinct so the cross-protocol read returns 404.
    let ns = "s3-lfs-cross-team";
    let name = "s3-lfs-cross-repo";
    let hub_auth = || format!("Bearer {}", server.auth_header_for(ns, name));
    let v1_auth = || format!("Bearer {}", server.auth_header());

    let content = b"s3-hub-lfs-to-v1-cross";
    let oid = sha256_hex(content);

    // Upload via Hub's /lfs/objects/{oid}
    let put_resp = client
        .put(server.url(&format!("/lfs/objects/{oid}")))
        .header("Authorization", hub_auth())
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(
        put_resp.status(),
        200,
        "Hub LFS PUT failed: {}",
        put_resp.status()
    );

    // Verify same-protocol read via Hub LFS works
    let hub_get = client
        .get(server.url(&format!("/lfs/objects/{oid}")))
        .header("Authorization", hub_auth())
        .send()
        .await
        .unwrap();
    assert_eq!(
        hub_get.status(),
        200,
        "Hub LFS GET should work after upload"
    );
    assert_eq!(hub_get.bytes().await.unwrap().as_ref(), content);

    // Cross-protocol read via V1 LFS → 404 (different storage backend)
    let v1_get = client
        .get(server.url(&format!("/v1/lfs/objects/{oid}")))
        .header("Authorization", v1_auth())
        .send()
        .await
        .unwrap();
    assert_eq!(
        v1_get.status(),
        404,
        "V1 LFS should not find data stored via Hub LFS (separate storage)"
    );
}

// ===========================================================================
// 25. Hub API authenticated requests (S3)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_hub_whoami_with_auth() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let resp = client
        .get(server.url("/api/whoami-v2"))
        .header("Authorization", format!("Bearer {}", server.auth_header()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["name"], "test", "whoami should return subject name");
    assert_eq!(
        json["auth"]["type"], "token",
        "auth.type should be 'token' with valid auth"
    );
    assert_eq!(json["auth"]["identity"]["account"]["name"], "test");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_hub_commit_with_auth() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3-hub-auth-team";
    let name = "s3-hub-auth-model";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    let create_resp = client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "type": "model",
            "name": &model_path,
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        create_resp.status(),
        201,
        "repo create should succeed with auth"
    );

    let content_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        b"s3 hub auth commit content",
    );
    let ndjson = format!(
        "{{\"header\":{{\"message\":\"s3 auth commit\",\"parentCommit\":\"\"}}}}\n\
         {{\"file\":{{\"path\":\"auth.txt\",\"content\":\"{content_b64}\"}}}}"
    );
    let commit_resp = client
        .post(server.url(&format!("/api/models/{ns}/{name}/commit/main")))
        .header("Authorization", auth())
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(commit_resp.status(), 200, "commit should succeed with auth");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_hub_xet_read_token_with_auth() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3-read-token-auth";
    let name = "s3-read-token-repo";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "type": "model",
            "name": &model_path,
            "private": false,
        }))
        .send()
        .await
        .unwrap();

    let token_resp = client
        .get(server.url(&format!("/api/models/{ns}/{name}/xet-read-token/main")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(
        token_resp.status(),
        200,
        "xet-read-token should return 200 with auth"
    );
    let json: serde_json::Value = token_resp.json().await.unwrap();
    assert!(
        !json["token"].as_str().unwrap().is_empty(),
        "should return a token"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_s3_hub_xet_write_token_with_auth() {
    let server = TestServer::start(&[ServerFrontend::Hub]).await;
    let client = reqwest::Client::new();

    let ns = "s3-write-token-auth";
    let name = "s3-write-token-repo";
    let model_path = format!("{ns}/{name}");
    let auth = || format!("Bearer {}", server.auth_header_for(ns, name));

    client
        .post(server.url("/api/repos/create"))
        .header("Authorization", auth())
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "type": "model",
            "name": &model_path,
            "private": false,
        }))
        .send()
        .await
        .unwrap();

    let token_resp = client
        .get(server.url(&format!("/api/models/{ns}/{name}/xet-write-token/main")))
        .header("Authorization", auth())
        .send()
        .await
        .unwrap();
    assert_eq!(
        token_resp.status(),
        200,
        "xet-write-token should return 200 with auth"
    );
    let json: serde_json::Value = token_resp.json().await.unwrap();
    assert!(
        !json["token"].as_str().unwrap().is_empty(),
        "should return a token"
    );
}
