//! Real-client end-to-end tests for the S3 frontend: `mc` (MinIO client) and
//! pyarrow, driven against an in-process `app::router` on a random port.
//!
//! These tests are **skip-gated**: if `mc` or `python3` with pyarrow is not on
//! `PATH`, the corresponding test prints a clear SKIP message and returns.
//! When present, they exercise the wire behavior real clients depend on
//! (AWS-chunked bodies, trailing-slash bucket paths, `DeleteObjects` batch
//! delete, seekable ranged reads) that the in-process e2e suite cannot cover.

#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::arithmetic_side_effects,
    clippy::string_add,
    clippy::format_push_string,
    clippy::option_if_let_else,
    clippy::or_fun_call,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or
)]

use std::{num::NonZeroUsize, path::Path, time::Duration};

use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tempfile::TempDir;
use tokio::net::TcpListener;

/// Test signing key matching the server's `with_token_signing_key`.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const OWNER: &str = "ac";
const NAME: &str = "assets";
const BUCKET: &str = "ac.assets";

/// Mints a bearer token scoped to `{owner}.{name}` with the test signing key.
fn mint_token(owner: &str, name: &str) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "s3-real-e2e",
        TokenScope::Write,
        repo,
        u64::MAX,
    )
    .unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Test server harness — real HTTP server on a random port (hermetic).
// ---------------------------------------------------------------------------

struct TestServer {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    base_url: String,
    _tmp: TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let tmp = TempDir::new().unwrap();
        let chunk_size = NonZeroUsize::new(65536).unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            tmp.path().to_path_buf(),
            chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(vec![ServerFrontend::S3])
        .unwrap()
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled();

        config.validate_runtime_requirements().unwrap();
        let app = app::router(config).await.unwrap();

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

        tokio::time::sleep(Duration::from_millis(100)).await;

        Self {
            shutdown: Some(shutdown_tx),
            base_url,
            _tmp: tmp,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

// ---------------------------------------------------------------------------
// Client availability gating + blocking subprocess helper.
// ---------------------------------------------------------------------------

/// Runs an `mc` subprocess on the blocking pool with an **isolated** config
/// directory and returns its output.
///
/// `mc` can take seconds for large uploads; running it on the blocking pool
/// keeps the async runtime workers (which serve the S3 frontend) alive.
///
/// Every `mc` invocation must use a per-test `--config-dir` (inside the test's
/// `TempDir`): `mc alias set` reads and rewrites the whole config file without
/// locking, so parallel tests sharing `~/.mc/config.json` race and lose each
/// other's aliases. Worse, `mc` treats a path whose alias is unknown as a
/// **local filesystem path** — `mc mb s3test-*/ac.assets` then creates a
/// directory under the crate CWD instead of hitting the server. The isolated
/// config dir makes the alias registration deterministic and hermetic.
async fn mc_run(config_dir: &Path, args: &[&str]) -> std::process::Output {
    let config_dir = config_dir.to_path_buf();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("mc")
            .arg("--config-dir")
            .arg(config_dir)
            .args(&args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

/// Registers an mc alias and **verifies** it actually took effect.
///
/// `mc alias set` exits `0` even when it fails (e.g. its endpoint probe got a
/// connection error), so exit status alone is not enough: the alias must be
/// usable or every later `mc` command silently falls back to a local path.
async fn mc_alias_set(config_dir: &Path, alias: &str, endpoint: &str, token: &str) {
    let output = mc_run(
        config_dir,
        &["alias", "set", alias, endpoint, token, "dummy-secret"],
    )
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && !stderr.contains("mc: <ERROR>"),
        "mc alias set failed (stderr): {stderr}"
    );
}

/// Returns whether `mc` is on PATH.
fn mc_available() -> bool {
    std::process::Command::new("mc")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Fails the test if an `mc` local-fallback artifact (`s3test-*`) was created
/// in the crate working directory.
///
/// `cargo test` runs with the CWD set to the package dir, so when `mc` cannot
/// resolve an alias it writes `{alias}/{bucket}/{key}` under
/// `crates/shardline-server/`. These leaked blobs were once committed by
/// accident; this guard makes any recurrence a hard test failure.
fn assert_no_leaked_mc_artifacts() {
    let cwd = std::env::current_dir().expect("current dir");
    let leaked: Vec<String> = std::fs::read_dir(&cwd)
        .expect("read crate dir")
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("s3test-"))
        .collect();
    assert!(
        leaked.is_empty(),
        "mc fell back to local paths and leaked artifacts in the crate dir {cwd:?}: {leaked:?}"
    );
}

/// Returns whether `python3` with pyarrow is available.
fn pyarrow_available() -> bool {
    std::process::Command::new("python3")
        .args(["-c", "import pyarrow"])
        .output()
        .is_ok_and(|output| output.status.success())
}

// ---------------------------------------------------------------------------
// mc
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mc_put_get_list_stat_rm_roundtrip() {
    if !mc_available() {
        eprintln!("SKIP: mc is not installed (skipping mc_put_get_list_stat_rm_roundtrip)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let config_dir = tmp.path().join("mc-config");
    let alias_name = format!("s3test-rt-{}", std::process::id());

    mc_alias_set(&config_dir, &alias_name, &server.base_url, &token).await;

    // CreateBucket (mc sends PUT /{bucket}/ — the trailing-slash form).
    let mb = mc_run(&config_dir, &["mb", &format!("{alias_name}/{BUCKET}")]).await;
    assert!(
        mb.status.success(),
        "mc mb failed: {}",
        String::from_utf8_lossy(&mb.stderr)
    );

    // PutObject (mc streams an AWS-chunked body).
    let file = tmp.path().join("hello.txt");
    std::fs::write(&file, b"hello real-client\n").unwrap();
    let cp = mc_run(
        &config_dir,
        &[
            "cp",
            file.to_str().unwrap(),
            &format!("{alias_name}/{BUCKET}/hello.txt"),
        ],
    )
    .await;
    assert!(
        cp.status.success(),
        "mc cp failed: {}",
        String::from_utf8_lossy(&cp.stderr)
    );

    // GetObject (mc cat) — must return the DECODED bytes.
    let cat = mc_run(
        &config_dir,
        &["cat", &format!("{alias_name}/{BUCKET}/hello.txt")],
    )
    .await;
    assert!(cat.status.success(), "mc cat failed");
    assert_eq!(cat.stdout, b"hello real-client\n");

    // ListObjectsV2.
    let ls = mc_run(&config_dir, &["ls", &format!("{alias_name}/{BUCKET}")]).await;
    assert!(ls.status.success(), "mc ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("hello.txt"),
        "mc ls must list hello.txt: {listing}"
    );

    // HeadObject.
    let stat = mc_run(
        &config_dir,
        &["stat", &format!("{alias_name}/{BUCKET}/hello.txt")],
    )
    .await;
    assert!(stat.status.success(), "mc stat failed");
    let stat_out = String::from_utf8_lossy(&stat.stdout);
    assert!(stat_out.contains("hello.txt"), "{stat_out}");

    // DeleteObject (mc rm uses the DeleteObjects batch endpoint).
    let rm = mc_run(
        &config_dir,
        &["rm", &format!("{alias_name}/{BUCKET}/hello.txt")],
    )
    .await;
    assert!(
        rm.status.success(),
        "mc rm failed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );

    let ls_after = mc_run(&config_dir, &["ls", &format!("{alias_name}/{BUCKET}")]).await;
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("hello.txt"),
        "hello.txt must be gone after rm: {listing_after}"
    );

    assert_no_leaked_mc_artifacts();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mc_multipart_upload_matches_source() {
    if !mc_available() {
        eprintln!("SKIP: mc is not installed (skipping mc_multipart_upload_matches_source)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let config_dir = tmp.path().join("mc-config");
    let alias_name = format!("s3test-mp-{}", std::process::id());

    mc_alias_set(&config_dir, &alias_name, &server.base_url, &token).await;
    let mb = mc_run(&config_dir, &["mb", &format!("{alias_name}/{BUCKET}")]).await;
    assert!(mb.status.success(), "mc mb failed");

    // A 20 MiB file forces multipart (mc default part size is 16 MiB).
    let big = tmp.path().join("big.bin");
    let mut source = Vec::with_capacity(20 * 1024 * 1024);
    for i in 0..(20 * 1024 * 1024 / 4096) {
        source.extend_from_slice(&(i as u32).to_le_bytes());
        source.extend_from_slice(&[0xAB; 4096 - 4]);
    }
    std::fs::write(&big, &source).unwrap();

    let up = mc_run(
        &config_dir,
        &[
            "cp",
            big.to_str().unwrap(),
            &format!("{alias_name}/{BUCKET}/big.bin"),
        ],
    )
    .await;
    assert!(
        up.status.success(),
        "mc multipart cp failed: {}",
        String::from_utf8_lossy(&up.stderr)
    );

    // Download and compare bytes (verifies the aws-chunked decode + CDC).
    let cat = mc_run(
        &config_dir,
        &["cat", &format!("{alias_name}/{BUCKET}/big.bin")],
    )
    .await;
    assert!(cat.status.success(), "mc cat big.bin failed");
    assert_eq!(cat.stdout.len(), source.len(), "downloaded size must match");
    assert_eq!(
        cat.stdout, source,
        "multipart object bytes must match the source"
    );

    assert_no_leaked_mc_artifacts();
}

// ---------------------------------------------------------------------------
// pyarrow
// ---------------------------------------------------------------------------

/// Runs the pyarrow assertions in a subprocess with the endpoint + token in
/// the environment; returns the script's output.
fn run_pyarrow(endpoint: &str, token: &str, tmp: &TempDir) -> std::process::Output {
    let script = tmp.path().join("pyarrow_check.py");
    std::fs::write(
        &script,
        r#"
import os
import pyarrow.fs as fs
import pyarrow as pa
import pyarrow.parquet as pq

s3 = fs.S3FileSystem(
    access_key=os.environ["S3_TOKEN"],
    secret_key="dummy",
    scheme="http",
    endpoint_override=os.environ["S3_ENDPOINT"],
    region="us-east-1",
)

# Write a parquet table via multipart (CreateMultipartUpload / UploadPart /
# CompleteMultipartUpload with AWS-chunked parts).
table = pa.table({"a": [1, 2, 3, 4], "b": ["w", "x", "y", "z"]})
with s3.open_output_stream("ac.assets/t.parquet") as out:
    pq.write_table(table, out)
print("write: ok")

# Read it back through the canonical random-access path (ranged GETs).
t = pq.read_table("ac.assets/t.parquet", filesystem=s3)
print("read:", t.to_pydict())

# open_input_file is the seekable stream variant.
with s3.open_input_file("ac.assets/t.parquet") as f:
    assert f.seekable()
    back = pq.read_table(f)
    print("input_file read:", back.to_pydict())

# File and directory info.
info = s3.get_file_info("ac.assets/t.parquet")
print("file_info:", info.type, info.size)
assert info.type == fs.FileType.File and info.size > 0
bucket = s3.get_file_info("ac.assets")
print("bucket_info:", bucket.type)
assert bucket.type == fs.FileType.Directory
listing = s3.get_file_info(fs.FileSelector("ac.assets", recursive=True))
print("recursive listing:", len(listing))
assert any(i.base_name == "t.parquet" for i in listing)
print("ALL PYARROW CHECKS PASSED")
"#,
    )
    .unwrap();
    std::process::Command::new("python3")
        .arg(script.to_str().unwrap())
        .env("S3_ENDPOINT", endpoint)
        .env("S3_TOKEN", token)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pyarrow_parquet_write_read_info() {
    if !pyarrow_available() {
        eprintln!(
            "SKIP: python3 + pyarrow not available (skipping pyarrow_parquet_write_read_info)"
        );
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();

    let output = run_pyarrow(&server.base_url, &token, &tmp);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pyarrow subprocess failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("ALL PYARROW CHECKS PASSED"),
        "pyarrow checks did not complete:\n{stdout}\n{stderr}"
    );

    assert_no_leaked_mc_artifacts();
}
