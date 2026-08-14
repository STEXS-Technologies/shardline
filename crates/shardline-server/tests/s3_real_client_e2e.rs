//! Real-client end-to-end tests for the S3 frontend: `mc` (MinIO client),
//! `aws` CLI, boto3 (AWS SDK), `s3cmd`, `rclone`, and pyarrow, driven against
//! an in-process `app::router` on a random port.
//!
//! These tests are **skip-gated**: if a client is not on `PATH`, the
//! corresponding test prints a clear SKIP message and returns. When present,
//! they exercise the wire behavior real clients depend on (AWS-chunked bodies,
//! trailing-slash bucket paths, `DeleteObjects` batch delete, conditional
//! requests, multipart uploads, seekable ranged reads) that the in-process e2e
//! suite cannot cover.

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

// ---------------------------------------------------------------------------
// AWS CLI (aws s3 / aws s3api)
// ---------------------------------------------------------------------------

/// Returns whether the AWS CLI is on PATH.
fn aws_available() -> bool {
    std::process::Command::new("aws")
        .args(["--version"])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs `aws` against the in-process server. The access key is the Shardline
/// bearer token; the secret is arbitrary (Shardline authenticates the key).
///
/// `AWS_ENDPOINT_URL` is honored by AWS CLI v2 for every subcommand; IMDS
/// probing is disabled so the command does not stall on metadata lookups.
async fn aws_run(endpoint: &str, token: &str, args: &[&str]) -> std::process::Output {
    let endpoint = endpoint.to_owned();
    let token = token.to_owned();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("aws")
            .args(&args)
            .env("AWS_ACCESS_KEY_ID", &token)
            .env("AWS_SECRET_ACCESS_KEY", "dummy-secret")
            .env("AWS_DEFAULT_REGION", "us-east-1")
            .env("AWS_ENDPOINT_URL", &endpoint)
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .env("AWS_PAGER", "")
            .env("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aws_cli_put_get_copy_rm_roundtrip() {
    if !aws_available() {
        eprintln!("SKIP: aws CLI is not installed (skipping aws_cli_put_get_copy_rm_roundtrip)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let bucket_url = format!("s3://{BUCKET}");

    // CreateBucket (`aws s3 mb`).
    let mb = aws_run(&server.base_url, &token, &["s3", "mb", &bucket_url]).await;
    assert!(
        mb.status.success(),
        "aws s3 mb failed: {}",
        String::from_utf8_lossy(&mb.stderr)
    );

    // PutObject (`aws s3 cp` up, single non-multipart PUT).
    let file = tmp.path().join("note.txt");
    std::fs::write(&file, b"aws cli payload\n").unwrap();
    let cp = aws_run(
        &server.base_url,
        &token,
        &[
            "s3",
            "cp",
            file.to_str().unwrap(),
            &format!("{bucket_url}/note.txt"),
        ],
    )
    .await;
    assert!(
        cp.status.success(),
        "aws s3 cp up failed: {}",
        String::from_utf8_lossy(&cp.stderr)
    );

    // ListObjectsV2 (`aws s3 ls`).
    let ls = aws_run(&server.base_url, &token, &["s3", "ls", &bucket_url]).await;
    assert!(ls.status.success(), "aws s3 ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("note.txt"),
        "aws s3 ls must list note.txt: {listing}"
    );

    // GetObject (`aws s3 cp` down) — verify decoded bytes.
    let dl = tmp.path().join("down.txt");
    let get = aws_run(
        &server.base_url,
        &token,
        &[
            "s3",
            "cp",
            &format!("{bucket_url}/note.txt"),
            dl.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        get.status.success(),
        "aws s3 cp down failed: {}",
        String::from_utf8_lossy(&get.stderr)
    );
    assert_eq!(std::fs::read(&dl).unwrap(), b"aws cli payload\n");

    // CopyObject + Delete (`aws s3 mv`), then HeadObject via s3api.
    let mv = aws_run(
        &server.base_url,
        &token,
        &[
            "s3",
            "mv",
            &format!("{bucket_url}/note.txt"),
            &format!("{bucket_url}/renamed.txt"),
        ],
    )
    .await;
    assert!(
        mv.status.success(),
        "aws s3 mv failed: {}",
        String::from_utf8_lossy(&mv.stderr)
    );
    let head = aws_run(
        &server.base_url,
        &token,
        &[
            "s3api",
            "head-object",
            "--bucket",
            BUCKET,
            "--key",
            "renamed.txt",
        ],
    )
    .await;
    assert!(
        head.status.success(),
        "aws s3api head-object failed: {}",
        String::from_utf8_lossy(&head.stderr)
    );

    // DeleteObject (`aws s3 rm`).
    let rm = aws_run(
        &server.base_url,
        &token,
        &["s3", "rm", &format!("{bucket_url}/renamed.txt")],
    )
    .await;
    assert!(
        rm.status.success(),
        "aws s3 rm failed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );
}

// ---------------------------------------------------------------------------
// boto3 (AWS SDK for Python)
// ---------------------------------------------------------------------------

/// Returns whether python3 with boto3 is available.
fn boto3_available() -> bool {
    std::process::Command::new("python3")
        .args(["-c", "import boto3"])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs the boto3 SDK assertions in a subprocess with the endpoint + token in
/// the environment; returns the script's output.
fn run_boto3(endpoint: &str, token: &str, tmp: &TempDir) -> std::process::Output {
    let script = tmp.path().join("boto3_check.py");
    std::fs::write(
        &script,
        r#"
import io
import os
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BConfig

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["S3_ENDPOINT"],
    aws_access_key_id=os.environ["S3_TOKEN"],
    aws_secret_access_key="dummy-secret",
    region_name="us-east-1",
    config=BConfig(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        retries={"max_attempts": 1},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    ),
)
B = "ac.assets"

# CreateBucket (boto3 sends PUT /bucket without a trailing slash).
s3.create_bucket(Bucket=B)

# PutObject with user metadata; HeadObject returns it back.
payload = b"metadata roundtrip\n"
s3.put_object(Bucket=B, Key="meta.txt", Body=payload, Metadata={"purpose": "client-breadth"})
head = s3.head_object(Bucket=B, Key="meta.txt")
assert head["ContentLength"] == len(payload), head["ContentLength"]
assert head["Metadata"].get("purpose") == "client-breadth", head["Metadata"]
assert s3.get_object(Bucket=B, Key="meta.txt")["Body"].read() == payload

# Conditional CopyObject (CopySourceIfMatch) — exercises If-Match + CopyObject.
etag = head["ETag"].strip('"')
s3.copy_object(
    Bucket=B,
    Key="meta-copy.txt",
    CopySource={"Bucket": B, "Key": "meta.txt"},
    CopySourceIfMatch=etag,
)

# ListObjectsV2 with prefix and pagination.
keys = []
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B, Prefix="meta"):
    keys.extend(c["Key"] for c in page.get("Contents", []))
assert "meta-copy.txt" in keys and "meta.txt" in keys, keys

# Multipart through the transfer manager (20 MiB -> several parts).
big = bytes(range(256)) * (20 * 1024 * 1024 // 256)
cfg = TransferConfig(multipart_threshold=8 * 1024 * 1024, multipart_chunksize=8 * 1024 * 1024)
s3.upload_fileobj(io.BytesIO(big), B, "big.bin", Config=cfg)
got = io.BytesIO()
s3.download_fileobj(B, "big.bin", got)
assert got.getvalue() == big, "multipart roundtrip byte mismatch"

# DeleteObjects batch.
s3.delete_objects(
    Bucket=B,
    Delete={"Objects": [{"Key": "meta.txt"}, {"Key": "meta-copy.txt"}, {"Key": "big.bin"}]},
)
remaining = s3.list_objects_v2(Bucket=B)
assert "Contents" not in remaining, remaining
print("ALL BOTO3 CHECKS PASSED")
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
async fn boto3_sdk_roundtrip() {
    if !boto3_available() {
        eprintln!("SKIP: python3 + boto3 not available (skipping boto3_sdk_roundtrip)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();

    let output = run_boto3(&server.base_url, &token, &tmp);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "boto3 subprocess failed:\n{stdout}\n{stderr}"
    );
    assert!(
        stdout.contains("ALL BOTO3 CHECKS PASSED"),
        "boto3 checks did not complete:\n{stdout}\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// s3cmd
// ---------------------------------------------------------------------------

/// Returns whether s3cmd is on PATH.
fn s3cmd_available() -> bool {
    std::process::Command::new("s3cmd")
        .args(["--version"])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs `s3cmd` with a fresh per-test config file (path-style custom endpoint,
/// SigV4, http) and returns the output.
async fn s3cmd_run(host: &str, token: &str, tmp: &TempDir, args: &[&str]) -> std::process::Output {
    let config = tmp.path().join("s3cfg");
    std::fs::write(
        &config,
        format!(
            "[default]\naccess_key = {token}\nsecret_key = dummy-secret\nhost_base = {host}\n\
             host_bucket = {host}\nuse_https = False\nsignature_v2 = False\n\
             check_ssl_certificate = False\n"
        ),
    )
    .unwrap();
    let config = config.clone();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("s3cmd")
            .arg("--config")
            .arg(config)
            .args(&args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3cmd_put_get_ls_del_roundtrip() {
    if !s3cmd_available() {
        eprintln!("SKIP: s3cmd is not installed (skipping s3cmd_put_get_ls_del_roundtrip)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    // s3cmd's host_base/host_bucket must be host:port without a scheme.
    let host = server.base_url.trim_start_matches("http://").to_owned();
    let bucket_url = format!("s3://{BUCKET}");

    let mb = s3cmd_run(&host, &token, &tmp, &["mb", &bucket_url]).await;
    assert!(
        mb.status.success(),
        "s3cmd mb failed: {}",
        String::from_utf8_lossy(&mb.stderr)
    );

    let file = tmp.path().join("note.txt");
    std::fs::write(&file, b"s3cmd payload\n").unwrap();
    let put = s3cmd_run(
        &host,
        &token,
        &tmp,
        &[
            "put",
            file.to_str().unwrap(),
            &format!("{bucket_url}/note.txt"),
        ],
    )
    .await;
    assert!(
        put.status.success(),
        "s3cmd put failed: {}",
        String::from_utf8_lossy(&put.stderr)
    );

    let dl = tmp.path().join("down.txt");
    let get = s3cmd_run(
        &host,
        &token,
        &tmp,
        &[
            "get",
            &format!("{bucket_url}/note.txt"),
            dl.to_str().unwrap(),
        ],
    )
    .await;
    assert!(
        get.status.success(),
        "s3cmd get failed: {}",
        String::from_utf8_lossy(&get.stderr)
    );
    assert_eq!(std::fs::read(&dl).unwrap(), b"s3cmd payload\n");

    let ls = s3cmd_run(&host, &token, &tmp, &["ls", &bucket_url]).await;
    assert!(ls.status.success(), "s3cmd ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("note.txt"),
        "s3cmd ls must list note.txt: {listing}"
    );

    let del = s3cmd_run(
        &host,
        &token,
        &tmp,
        &["del", &format!("{bucket_url}/note.txt")],
    )
    .await;
    assert!(
        del.status.success(),
        "s3cmd del failed: {}",
        String::from_utf8_lossy(&del.stderr)
    );

    let ls_after = s3cmd_run(&host, &token, &tmp, &["ls", &bucket_url]).await;
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("note.txt"),
        "note.txt must be gone after del: {listing_after}"
    );
}

// ---------------------------------------------------------------------------
// rclone
// ---------------------------------------------------------------------------

/// Returns whether rclone is on PATH.
fn rclone_available() -> bool {
    std::process::Command::new("rclone")
        .args(["--version"])
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Runs `rclone` against the in-process server using a Minio-style S3 remote
/// configured purely through environment variables.
async fn rclone_run(endpoint: &str, token: &str, args: &[&str]) -> std::process::Output {
    let endpoint = endpoint.to_owned();
    let token = token.to_owned();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("rclone")
            .args(&args)
            .env("RCLONE_CONFIG_REMOTE_TYPE", "s3")
            .env("RCLONE_CONFIG_REMOTE_PROVIDER", "Minio")
            .env("RCLONE_CONFIG_REMOTE_ENDPOINT", &endpoint)
            .env("RCLONE_CONFIG_REMOTE_ACCESS_KEY_ID", &token)
            .env("RCLONE_CONFIG_REMOTE_SECRET_ACCESS_KEY", "dummy-secret")
            .env("RCLONE_CONFIG_REMOTE_NO_CHECK_BUCKET", "true")
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rclone_copy_ls_cat_delete() {
    if !rclone_available() {
        eprintln!("SKIP: rclone is not installed (skipping rclone_copy_ls_cat_delete)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let remote_bucket = format!("remote:{BUCKET}");

    // CreateBucket.
    let mkdir = rclone_run(&server.base_url, &token, &["mkdir", &remote_bucket]).await;
    assert!(
        mkdir.status.success(),
        "rclone mkdir failed: {}",
        String::from_utf8_lossy(&mkdir.stderr)
    );

    // Copy (upload) + verify via cat.
    let file = tmp.path().join("note.txt");
    std::fs::write(&file, b"rclone payload\n").unwrap();
    let copy = rclone_run(
        &server.base_url,
        &token,
        &["copy", file.to_str().unwrap(), &remote_bucket],
    )
    .await;
    assert!(
        copy.status.success(),
        "rclone copy failed: {}",
        String::from_utf8_lossy(&copy.stderr)
    );
    let cat = rclone_run(
        &server.base_url,
        &token,
        &["cat", &format!("{remote_bucket}/note.txt")],
    )
    .await;
    assert!(cat.status.success(), "rclone cat failed");
    assert_eq!(cat.stdout, b"rclone payload\n");

    // List.
    let ls = rclone_run(&server.base_url, &token, &["ls", &remote_bucket]).await;
    assert!(ls.status.success(), "rclone ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(
        listing.contains("note.txt"),
        "rclone ls must list note.txt: {listing}"
    );

    // Delete + verify gone.
    let del = rclone_run(
        &server.base_url,
        &token,
        &["delete", &format!("{remote_bucket}/note.txt")],
    )
    .await;
    assert!(
        del.status.success(),
        "rclone delete failed: {}",
        String::from_utf8_lossy(&del.stderr)
    );
    let ls_after = rclone_run(&server.base_url, &token, &["ls", &remote_bucket]).await;
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("note.txt"),
        "note.txt must be gone after rclone delete: {listing_after}"
    );
}

// ---------------------------------------------------------------------------
// mc — broader workflows (mirror, stat --json, recursive delete)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mc_mirror_recursive_rm_stat() {
    if !mc_available() {
        eprintln!("SKIP: mc is not installed (skipping mc_mirror_recursive_rm_stat)");
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let config_dir = tmp.path().join("mc-config");
    let alias_name = format!("s3test-mr-{}", std::process::id());

    mc_alias_set(&config_dir, &alias_name, &server.base_url, &token).await;
    let mb = mc_run(&config_dir, &["mb", &format!("{alias_name}/{BUCKET}")]).await;
    assert!(mb.status.success(), "mc mb failed");

    // Recursive directory upload via `mc mirror`.
    let src_dir = tmp.path().join("mirror-src");
    std::fs::create_dir_all(src_dir.join("sub")).unwrap();
    std::fs::write(src_dir.join("a.txt"), b"a").unwrap();
    std::fs::write(src_dir.join("b.txt"), b"b").unwrap();
    std::fs::write(src_dir.join("sub/c.txt"), b"c").unwrap();
    let mirror = mc_run(
        &config_dir,
        &[
            "mirror",
            src_dir.to_str().unwrap(),
            &format!("{alias_name}/{BUCKET}"),
        ],
    )
    .await;
    assert!(
        mirror.status.success(),
        "mc mirror failed: {}",
        String::from_utf8_lossy(&mirror.stderr)
    );

    // Recursive listing shows every key.
    let ls_r = mc_run(
        &config_dir,
        &["ls", "--recursive", &format!("{alias_name}/{BUCKET}")],
    )
    .await;
    assert!(ls_r.status.success(), "mc ls --recursive failed");
    let listing = String::from_utf8_lossy(&ls_r.stdout);
    for key in ["a.txt", "b.txt", "sub/c.txt"] {
        assert!(
            listing.contains(key),
            "mc ls --recursive missing {key}: {listing}"
        );
    }

    // HeadObject via `mc stat --json`.
    let stat = mc_run(
        &config_dir,
        &["stat", "--json", &format!("{alias_name}/{BUCKET}/a.txt")],
    )
    .await;
    assert!(stat.status.success(), "mc stat --json failed");
    let stat_out = String::from_utf8_lossy(&stat.stdout);
    assert!(
        stat_out.contains("a.txt"),
        "stat --json must name a.txt: {stat_out}"
    );

    // Batch delete via `mc rm --recursive` (DeleteObjects); `--force` is
    // required by mc for recursive removal.
    let rm = mc_run(
        &config_dir,
        &[
            "rm",
            "--recursive",
            "--force",
            &format!("{alias_name}/{BUCKET}"),
        ],
    )
    .await;
    assert!(
        rm.status.success(),
        "mc rm --recursive failed: {}",
        String::from_utf8_lossy(&rm.stderr)
    );

    let ls_after = mc_run(
        &config_dir,
        &["ls", "--recursive", &format!("{alias_name}/{BUCKET}")],
    )
    .await;
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        listing_after.trim().is_empty(),
        "bucket must be empty after recursive rm: {listing_after}"
    );

    assert_no_leaked_mc_artifacts();
}
