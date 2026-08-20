//! End-to-end tests for the `sdx` file-management CLI lane, invoked through
//! the `shardline xet ...` escape hatch against an in-process shardline server.

#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

use std::num::{NonZeroU64, NonZeroUsize};
use std::process::Command;

use shardline_server::{BenchmarkBackend, ServerConfig, ServerFrontend, ServerRole};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Signing key shared by the server auth layer and provider token service.
const SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
/// Provider bootstrap API key.
const BOOTSTRAP_KEY: &str = "bootstrap";
/// Provider subject authorized for read+write on the test repository.
const SUBJECT: &str = "github-user-1";

const PROVIDER_CONFIG: &[u8] = br#"{
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

/// In-process shardline server with the Xet frontend enabled.
struct TestServer {
    port: u16,
    _upload: BenchmarkBackend,
    _dir: TempDir,
    _task: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        configure_test_pools();
        let dir = TempDir::new().unwrap();
        let cfg_dir = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let config_path = cfg_dir.join("providers.json");
        std::fs::write(&config_path, PROVIDER_CONFIG).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let chunk_size = NonZeroUsize::new(128).unwrap();
        let upload = BenchmarkBackend::isolated_local(
            dir.path().to_path_buf(),
            base_url.clone(),
            chunk_size,
            NonZeroUsize::new(64).unwrap(),
        )
        .await
        .unwrap();

        let config = ServerConfig::new(addr, base_url, dir.path().to_path_buf(), chunk_size)
            .with_server_role(ServerRole::All)
            .with_server_frontends(vec![ServerFrontend::Xet])
            .unwrap()
            .with_token_signing_key(SIGNING_KEY.to_vec())
            .unwrap()
            .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
            .with_provider_runtime(
                config_path,
                BOOTSTRAP_KEY.as_bytes().to_vec(),
                "test-issuer".to_owned(),
                NonZeroU64::new(3_600).unwrap(),
            )
            .unwrap();

        let app = shardline_server::app::router(config).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        wait_ready(&format!("http://127.0.0.1:{}", addr.port())).await;

        Self {
            port: addr.port(),
            _upload: upload,
            _dir: dir,
            _task: task,
        }
    }

    /// Builds the remote URL for a path under `github/team/assets@main`.
    fn url(&self, path: &str) -> String {
        format!(
            "xet://127.0.0.1:{}/github/team/assets/main/{path}",
            self.port
        )
    }

    /// Repository-identity URL (no path) for `branch` operations.
    fn repo_url(&self) -> String {
        format!("xet://127.0.0.1:{}/github/team/assets", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._task.abort();
    }
}

/// Raises the in-process server's execution-pool sizes once per process.
fn configure_test_pools() {
    static CONFIGURED: std::sync::Once = std::sync::Once::new();
    CONFIGURED.call_once(|| {
        // SAFETY: guarded by `Once` so the variables are written exactly once
        // before any server router reads them; this is the standard test-harness
        // pattern for the server's env-configured pool sizes.
        unsafe {
            std::env::set_var("SHARDLINE_PARSING_POOL_SIZE", "64");
            std::env::set_var("SHARDLINE_HASHING_POOL_SIZE", "64");
        }
    });
}

async fn wait_ready(base_url: &str) {
    let client = reqwest::Client::new();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            match client.get(format!("{base_url}/readyz")).send().await {
                Ok(response) if response.status().is_success() => return,
                _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("in-process server did not become ready");
}

/// Auth flags appended to every xet invocation.
const AUTH: [&str; 4] = ["--api-key", BOOTSTRAP_KEY, "--subject", SUBJECT];

/// Runs the shardline binary with the `xet` escape hatch and the given args,
/// returning (stdout, stderr, status).
fn xet(args: &[&str]) -> (String, String, bool) {
    let mut full = Vec::with_capacity(args.len().saturating_add(AUTH.len()));
    full.extend_from_slice(args);
    full.extend_from_slice(&AUTH);
    let output = Command::new(env!("CARGO_BIN_EXE_shardline"))
        .arg("xet")
        .args(&full)
        .output()
        .expect("failed to run shardline binary");
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
        output.status.success(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdx_cp_ls_cat_info_rm_branch_round_trip() {
    let server = TestServer::start().await;

    // Local fixture.
    let sandbox = tempfile::tempdir().unwrap();
    let local = sandbox.path().join("model.bin");
    let content = b"hello shardline xet cli";
    std::fs::write(&local, content).unwrap();

    // Upload via `cp` (local -> remote).
    let remote = server.url("model.bin");
    let (stdout, stderr, ok) = xet(&["cp", local.to_str().unwrap(), &remote]);
    assert!(ok, "cp upload failed: {stderr}\nstdout: {stdout}");
    assert!(stdout.contains("model.bin -> "));

    // `info` reports the uploaded file metadata.
    let (stdout, stderr, ok) = xet(&["info", &remote]);
    assert!(ok, "info failed: {stderr}");
    assert!(stdout.contains("file_id:"));
    assert!(stdout.contains("size:"));

    // `ls` lists the uploaded file.
    let (stdout, stderr, ok) = xet(&["ls", &server.url("")]);
    assert!(ok, "ls failed: {stderr}");
    assert!(stdout.contains("model.bin"), "ls output: {stdout}");

    // `cat` streams the file to stdout.
    let (stdout, stderr, ok) = xet(&["cat", &remote]);
    assert!(ok, "cat failed: {stderr}");
    assert_eq!(stdout.as_bytes(), content);

    // `cp` download (remote -> local) reconstructs identical bytes.
    let out = sandbox.path().join("out.bin");
    let (stdout, stderr, ok) = xet(&["cp", &remote, out.to_str().unwrap()]);
    assert!(ok, "cp download failed: {stderr}\nstdout: {stdout}");
    assert_eq!(std::fs::read(&out).unwrap(), content);

    // `rm` deregisters the path; `ls` no longer lists it.
    let (stdout, stderr, ok) = xet(&["rm", &remote]);
    assert!(ok, "rm failed: {stderr}");
    assert!(stdout.contains("deleted 1 path(s)"), "rm output: {stdout}");
    let (stdout, stderr, ok) = xet(&["ls", &server.url("")]);
    assert!(ok, "ls after rm failed: {stderr}");
    assert!(!stdout.contains("model.bin"), "ls after rm: {stdout}");

    // `branch --create` / list / `--delete`.
    let repo = server.repo_url();
    let (stdout, stderr, ok) = xet(&["branch", &repo, "--create", "feature"]);
    assert!(ok, "branch create failed: {stderr}\nstdout: {stdout}");
    assert!(stdout.contains("created revision feature"));
    let (stdout, stderr, ok) = xet(&["branch", &repo]);
    assert!(ok, "branch list failed: {stderr}");
    assert!(stdout.contains("feature"), "branch list: {stdout}");
    let (stdout, stderr, ok) = xet(&["branch", &repo, "--delete", "feature"]);
    assert!(ok, "branch delete failed: {stderr}");
    assert!(stdout.contains("deleted revision feature"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdx_cp_no_register_skips_registration() {
    let server = TestServer::start().await;

    let sandbox = tempfile::tempdir().unwrap();
    let local = sandbox.path().join("raw.bin");
    std::fs::write(&local, b"not registered content").unwrap();

    // Upload with --no-register: the shard is stored but the path is not
    // registered, so `ls` must not list it.
    let remote = server.url("raw.bin");
    let (stdout, stderr, ok) = xet(&["cp", local.to_str().unwrap(), &remote, "--no-register"]);
    assert!(ok, "no-register cp failed: {stderr}\nstdout: {stdout}");
    assert!(stdout.contains("[not registered]"), "stdout: {stdout}");

    let (stdout, stderr, ok) = xet(&["ls", &server.url("")]);
    assert!(ok, "ls failed: {stderr}");
    assert!(
        !stdout.contains("raw.bin"),
        "ls after no-register: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdx_cp_remote_to_remote_is_rejected() {
    let server = TestServer::start().await;
    let a = server.url("a.bin");
    let b = server.url("b.bin");
    let (stdout, stderr, ok) = xet(&["cp", &a, &b]);
    let _ = stdout;
    assert!(!ok, "remote-to-remote cp should fail");
    assert!(stderr.contains("not supported"), "stderr: {stderr}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdx_missing_credential_is_rejected() {
    let server = TestServer::start().await;
    let remote = server.url("x.bin");
    // No credential flags and no config -> the CLI must error before any I/O.
    let output = Command::new(env!("CARGO_BIN_EXE_shardline"))
        .arg("xet")
        .args(["ls", &remote])
        .env_remove("SHARDLINE_TOKEN")
        .env_remove("SHARDLINE_API_KEY")
        .env_remove("SHARDLINE_TOKEN_FILE")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(!output.status.success(), "missing credential should fail");
    assert!(
        stderr.contains("no credential configured"),
        "stderr: {stderr}"
    );
}
