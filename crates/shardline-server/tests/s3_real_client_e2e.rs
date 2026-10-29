//! Real-client end-to-end tests for the S3 frontend: `mc` (MinIO client),
//! `aws` CLI, boto3 (AWS SDK), DuckDB, `s3cmd`, `rclone`, and pyarrow, driven against
//! an in-process `app::router` on a random port.
//!
//! Missing tools skip optional developer runs, but fail when
//! `SHARDLINE_REQUIRE_NATIVE_CLIENTS=1` enables mandatory qualification. When present,
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

mod s3_real_client;

/// Test signing key matching the server's `with_token_signing_key`.
const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const OWNER: &str = "ac";
const NAME: &str = "assets";
const BUCKET: &str = "ac.assets";

fn client_output(command: &mut std::process::Command) -> std::process::Output {
    shardline_test_support::native_client_output(command, Duration::from_secs(600))
        .expect("native S3 client failed to execute or exceeded its deadline")
}

fn probe_client(program: &str, args: &[&str]) -> bool {
    shardline_test_support::native_client_output(
        std::process::Command::new(program).args(args),
        Duration::from_secs(10),
    )
    .is_ok_and(|output| output.status.success())
}

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
        client_output(
            std::process::Command::new("mc")
                .arg("--config-dir")
                .arg(config_dir)
                .args(&args),
        )
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
        "mc alias set failed with status {} or reported an error",
        output.status
    );
}

/// Returns whether `mc` is on PATH.
fn mc_available() -> bool {
    let available = probe_client("mc", &["--version"]);
    shardline_test_support::require_native_client("mc", available)
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
    let available = probe_client("python3", &["-c", "import pyarrow"]);
    shardline_test_support::require_native_client("python3 + pyarrow", available)
}

/// Returns whether Python DuckDB and PyArrow are available.
fn duckdb_available() -> bool {
    let available = probe_client("python3", &["-c", "import duckdb, pyarrow"]);
    shardline_test_support::require_native_client("python3 + duckdb + pyarrow", available)
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
    assert!(mb.status.success(), "mc mb failed: {}", mb.status);

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
    assert!(cp.status.success(), "mc cp failed: {}", cp.status);

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
    assert!(listing.contains("hello.txt"), "mc ls must list hello.txt");

    // HeadObject.
    let stat = mc_run(
        &config_dir,
        &["stat", &format!("{alias_name}/{BUCKET}/hello.txt")],
    )
    .await;
    assert!(stat.status.success(), "mc stat failed");
    let stat_out = String::from_utf8_lossy(&stat.stdout);
    assert!(
        stat_out.contains("hello.txt"),
        "mc stat must name hello.txt"
    );

    // DeleteObject (mc rm uses the DeleteObjects batch endpoint).
    let rm = mc_run(
        &config_dir,
        &["rm", &format!("{alias_name}/{BUCKET}/hello.txt")],
    )
    .await;
    assert!(rm.status.success(), "mc rm failed: {}", rm.status);

    let ls_after = mc_run(&config_dir, &["ls", &format!("{alias_name}/{BUCKET}")]).await;
    assert!(ls_after.status.success(), "mc ls after removal failed");
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("hello.txt"),
        "hello.txt must be gone after rm"
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
    assert!(up.status.success(), "mc multipart cp failed: {}", up.status);

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
    client_output(
        std::process::Command::new("python3")
            .arg(script.to_str().unwrap())
            .env("S3_ENDPOINT", endpoint)
            .env("S3_TOKEN", token),
    )
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
    assert!(
        output.status.success(),
        "pyarrow subprocess failed with status {}",
        output.status
    );
    assert!(
        stdout.contains("ALL PYARROW CHECKS PASSED"),
        "pyarrow checks did not complete"
    );

    assert_no_leaked_mc_artifacts();
}

// ---------------------------------------------------------------------------
// DuckDB
// ---------------------------------------------------------------------------

/// Exercises DuckDB's real S3 client against exact and glob Parquet reads,
/// projection/filter pushdown, aggregates, and Parquet output upload.
fn run_duckdb(endpoint: &str, token: &str, tmp: &TempDir) -> std::process::Output {
    let script = tmp.path().join("duckdb_check.py");
    std::fs::write(
        &script,
        r#"
import os
import duckdb
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq

endpoint = os.environ["S3_ENDPOINT"]
token = os.environ["S3_TOKEN"]
s3 = fs.S3FileSystem(access_key=token, secret_key="unused", scheme="http",
                      endpoint_override=endpoint, region="us-east-1")
for name, values in (("part-0.parquet", [1, 2, 3]), ("part-1.parquet", [4, 5, 6])):
    table = pa.table({"id": values, "group": ["a", "a", "b"]})
    with s3.open_output_stream("ac.assets/inputs/" + name) as out:
        pq.write_table(table, out)

con = duckdb.connect()
endpoint_host = endpoint.removeprefix("http://").removeprefix("https://")
con.execute("""CREATE SECRET shardline_s3 (
    TYPE S3, KEY_ID ?, SECRET 'unused', ENDPOINT ?, REGION 'us-east-1',
    URL_STYLE 'path', USE_SSL false
)""", [token, endpoint_host])

exact = con.execute("SELECT id, \"group\" FROM read_parquet('s3://ac.assets/inputs/part-0.parquet') ORDER BY id").fetchall()
assert exact == [(1, "a"), (2, "a"), (3, "b")], exact
schema = con.execute("DESCRIBE SELECT * FROM read_parquet('s3://ac.assets/inputs/part-0.parquet')").fetchall()
assert [(row[0], row[1]) for row in schema] == [("id", "BIGINT"), ("group", "VARCHAR")], schema
filtered = con.execute("""SELECT id FROM read_parquet('s3://ac.assets/inputs/*.parquet')
    WHERE id >= 3 ORDER BY id""").fetchall()
assert filtered == [(3,), (4,), (5,), (6,)], filtered
aggregate = con.execute("SELECT count(*), sum(id) FROM read_parquet('s3://ac.assets/inputs/*.parquet')").fetchone()
assert aggregate == (6, 21), aggregate
con.execute("""COPY (SELECT id, \"group\" FROM read_parquet('s3://ac.assets/inputs/*.parquet')
    WHERE id % 2 = 0 ORDER BY id) TO 's3://ac.assets/results/even.parquet' (FORMAT PARQUET)""")
written = con.execute("SELECT id FROM read_parquet('s3://ac.assets/results/even.parquet') ORDER BY id").fetchall()
assert written == [(2,), (4,), (6,)], written

# Exercise DuckDB's S3 listing behavior for both an empty prefix and a
# continuation-token-sized prefix (>1000 objects).
assert con.execute("SELECT count(*) FROM glob('s3://ac.assets/empty/*.parquet')").fetchone()[0] == 0
for index in range(1005):
    with s3.open_output_stream(f"ac.assets/paged/part-{index:04}.parquet") as out:
        pq.write_table(pa.table({"id": [index]}), out)
paged = con.execute("SELECT count(*) FROM glob('s3://ac.assets/paged/*.parquet')").fetchone()[0]
assert paged == 1005, paged

# Error redaction: malformed or missing objects must fail without leaking the
# repository token into a DuckDB-facing exception.
with s3.open_output_stream("ac.assets/inputs/bad.parquet") as out:
    out.write(b"not a parquet file")
for path in ("s3://ac.assets/inputs/missing.parquet", "s3://ac.assets/inputs/bad.parquet"):
    try:
        con.execute("SELECT * FROM read_parquet(?)", [path]).fetchall()
        raise AssertionError("expected DuckDB read failure for " + path)
    except Exception as exc:
        assert token not in str(exc), "credential leaked in DuckDB error"
print("ALL DUCKDB CHECKS PASSED")
"#,
    )
    .unwrap();
    client_output(
        std::process::Command::new("python3")
            .arg(script.to_str().unwrap())
            .env("S3_ENDPOINT", endpoint)
            .env("S3_TOKEN", token),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duckdb_parquet_glob_filter_aggregate_and_write() {
    if !duckdb_available() {
        eprintln!(
            "SKIP: python3 + duckdb + pyarrow not available (skipping duckdb_parquet_glob_filter_aggregate_and_write)"
        );
        return;
    }
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let tmp = TempDir::new().unwrap();
    let output = run_duckdb(&server.base_url, &token, &tmp);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "DuckDB subprocess failed with status {}",
        output.status
    );
    assert!(
        stdout.contains("ALL DUCKDB CHECKS PASSED"),
        "DuckDB checks did not complete"
    );
}

// ---------------------------------------------------------------------------
// AWS CLI (aws s3 / aws s3api)
// ---------------------------------------------------------------------------

/// Returns whether the AWS CLI is on PATH.
fn aws_available() -> bool {
    let available = probe_client("aws", &["--version"]);
    shardline_test_support::require_native_client("aws", available)
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
        client_output(
            std::process::Command::new("aws")
                .args(&args)
                .env("AWS_ACCESS_KEY_ID", &token)
                .env("AWS_SECRET_ACCESS_KEY", "dummy-secret")
                .env("AWS_DEFAULT_REGION", "us-east-1")
                .env("AWS_ENDPOINT_URL", &endpoint)
                .env("AWS_EC2_METADATA_DISABLED", "true")
                .env("AWS_PAGER", "")
                .env("AWS_REQUEST_CHECKSUM_CALCULATION", "when_supported"),
        )
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
    assert!(mb.status.success(), "aws s3 mb failed: {}", mb.status);

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
    assert!(cp.status.success(), "aws s3 cp up failed: {}", cp.status);

    // ListObjectsV2 (`aws s3 ls`).
    let ls = aws_run(&server.base_url, &token, &["s3", "ls", &bucket_url]).await;
    assert!(ls.status.success(), "aws s3 ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(listing.contains("note.txt"), "aws s3 ls must list note.txt");

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
        get.status
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
    assert!(mv.status.success(), "aws s3 mv failed: {}", mv.status);
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
        head.status
    );

    // DeleteObject (`aws s3 rm`).
    let rm = aws_run(
        &server.base_url,
        &token,
        &["s3", "rm", &format!("{bucket_url}/renamed.txt")],
    )
    .await;
    assert!(rm.status.success(), "aws s3 rm failed: {}", rm.status);
}

// ---------------------------------------------------------------------------
// boto3 (AWS SDK for Python)
// ---------------------------------------------------------------------------

fn boto3_python() -> std::ffi::OsString {
    std::env::var_os("BOTOCORE_PYTHON").unwrap_or_else(|| "python3".into())
}

fn boto3_failure_category(stderr: &[u8]) -> &'static str {
    // Do not echo subprocess output: SDK diagnostics and interpreter wrappers
    // can contain credentials or signed URLs, even when our exception hook is
    // careful. Only report a known, constant category.
    for line in String::from_utf8_lossy(stderr).lines() {
        match line {
            "boto3 failure type: ParamValidationError" => return "SDK parameter validation",
            "boto3 failure type: ClientError" => return "S3 service response",
            "boto3 failure type: AssertionError" => return "client contract assertion",
            "boto3 failure type: KeyError" => return "missing SDK response field",
            _ => {}
        }
    }
    "unclassified client failure"
}

fn boto3_failure_script_line(stderr: &[u8]) -> Option<u32> {
    // Only a parsed line number may leave the private subprocess diagnostics.
    String::from_utf8_lossy(stderr)
        .lines()
        .filter_map(|line| line.strip_prefix("boto3 script line: ")?.parse().ok())
        .next_back()
}

/// Uses the same explicitly configured SDK for discovery and execution.
fn boto3_available() -> bool {
    let available = shardline_test_support::native_client_output(
        std::process::Command::new(boto3_python()).args(["-c", "import boto3"]),
        Duration::from_secs(10),
    )
    .is_ok_and(|output| output.status.success());
    shardline_test_support::require_native_client("python3 + boto3", available)
}

/// Runs the boto3 SDK assertions in a subprocess with the endpoint + token in
/// the environment; returns the script's output.
fn run_boto3(endpoint: &str, token: &str, tmp: &TempDir) -> std::process::Output {
    let script = tmp.path().join("boto3_check.py");
    std::fs::write(
        &script,
        r#"
import base64
import datetime
import gzip
import hashlib
import io
import os
import struct
import sys
import traceback
import zlib
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BConfig
from botocore.exceptions import ClientError

# SDK exceptions can contain signed URLs and credentials. Report only exception
# type and script frame locations, never exception values or source lines.
def safe_failure(exception_type, exception, tb):
    print("boto3 failure type: " + exception_type.__name__, file=sys.stderr)
    for frame, line in traceback.walk_tb(tb):
        if frame.f_code.co_filename == __file__:
            print("boto3 script line: " + str(line), file=sys.stderr)

sys.excepthook = safe_failure

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
        # Leave checksum behavior at Botocore's default so current SDK checksum
        # negotiation and the multipart CRC32 path are exercised.
    ),
)
B = "ac.assets"

def crc64nvme(payload):
    polynomial = 0x9A6C9329AC4BC9B5
    value = 0xFFFFFFFFFFFFFFFF
    for byte in payload:
        value ^= byte
        for _ in range(8):
            value = (value >> 1) ^ (polynomial if value & 1 else 0)
    value ^= 0xFFFFFFFFFFFFFFFF
    return base64.b64encode(value.to_bytes(8, "big")).decode()

# CreateBucket (boto3 sends PUT /bucket without a trailing slash).
s3.create_bucket(Bucket=B)
ownership = s3.get_bucket_ownership_controls(Bucket=B)
assert ownership["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced", ownership
bucket_acl = s3.get_bucket_acl(Bucket=B)
assert len(bucket_acl["Grants"]) == 1, bucket_acl
assert bucket_acl["Grants"][0]["Permission"] == "FULL_CONTROL", bucket_acl
assert bucket_acl["Owner"]["ID"], bucket_acl

# ACLs are disabled: the compatible owner-full-control canned ACL is accepted,
# reads report the owner grant, and public ACLs fail without replacing data.
s3.put_object(Bucket=B, Key="acl-check.txt", Body=b"acl", ACL="bucket-owner-full-control")
object_acl = s3.get_object_acl(Bucket=B, Key="acl-check.txt")
assert len(object_acl["Grants"]) == 1, object_acl
assert object_acl["Grants"][0]["Permission"] == "FULL_CONTROL", object_acl
try:
    s3.put_object(Bucket=B, Key="acl-check.txt", Body=b"public", ACL="public-read")
except ClientError as exc:
    assert exc.response["Error"]["Code"] == "AccessControlListNotSupported", exc.response
else:
    raise AssertionError("public ACL must be rejected")
assert s3.get_object(Bucket=B, Key="acl-check.txt")["Body"].read() == b"acl"
s3.delete_object(Bucket=B, Key="acl-check.txt")
ownership = s3.get_bucket_ownership_controls(Bucket=B)
assert ownership["OwnershipControls"]["Rules"][0]["ObjectOwnership"] == "BucketOwnerEnforced", ownership
bucket_acl = s3.get_bucket_acl(Bucket=B)
assert len(bucket_acl["Grants"]) == 1, bucket_acl
assert bucket_acl["Grants"][0]["Permission"] == "FULL_CONTROL", bucket_acl
assert bucket_acl["Owner"]["ID"], bucket_acl

# Bucket-owner-enforced mode accepts its compatible canned ACL, reads object
# ACLs, and rejects ACLs that would grant access outside repository auth.
s3.put_object(Bucket=B, Key="acl-check.txt", Body=b"acl", ACL="bucket-owner-full-control")
object_acl = s3.get_object_acl(Bucket=B, Key="acl-check.txt")
assert len(object_acl["Grants"]) == 1, object_acl
assert object_acl["Grants"][0]["Permission"] == "FULL_CONTROL", object_acl
try:
    s3.put_object(Bucket=B, Key="acl-check.txt", Body=b"public", ACL="public-read")
except ClientError as exc:
    assert exc.response["Error"]["Code"] == "AccessControlListNotSupported", exc.response
else:
    raise AssertionError("public ACL must be rejected")
s3.delete_object(Bucket=B, Key="acl-check.txt")

# Version listing returns the required owner structure without claiming that
# Shardline has historical versions or delete markers.
s3.put_object(Bucket=B, Key="owner-check.txt", Body=b"owner")
listed = s3.list_objects_v2(Bucket=B, Prefix="owner-check", FetchOwner=True)
assert listed["Contents"][0]["Owner"]["ID"], listed
versions = s3.list_object_versions(Bucket=B, Prefix="owner-check")
assert len(versions.get("Versions", [])) == 1, versions
assert versions["Versions"][0]["VersionId"] == "null", versions
assert versions["Versions"][0]["Owner"]["ID"], versions
s3.delete_object(Bucket=B, Key="owner-check.txt")

# ListParts returns the negotiated algorithm and checksum for an active upload.
# Use the SDK's modeled checksum fields instead of hand-parsing HTTP XML.
listed_payload = b"native list-parts checksum"
listed_checksum = base64.b64encode(struct.pack(">I", zlib.crc32(listed_payload) & 0xffffffff)).decode()
listed_upload = s3.create_multipart_upload(
    Bucket=B, Key="list-parts-checksum.bin", ChecksumAlgorithm="CRC32", ChecksumType="COMPOSITE"
)
listed_upload_id = listed_upload["UploadId"]
s3.upload_part(
    Bucket=B, Key="list-parts-checksum.bin", UploadId=listed_upload_id,
    PartNumber=1, Body=listed_payload, ChecksumCRC32=listed_checksum
)
listed_parts = s3.list_parts(Bucket=B, Key="list-parts-checksum.bin", UploadId=listed_upload_id)
assert listed_parts["ChecksumAlgorithm"] == "CRC32", listed_parts
assert listed_parts["ChecksumType"] == "COMPOSITE", listed_parts
assert listed_parts["Parts"][0]["ChecksumCRC32"] == listed_checksum, listed_parts
s3.abort_multipart_upload(Bucket=B, Key="list-parts-checksum.bin", UploadId=listed_upload_id)

# PutObject with user metadata; HeadObject returns it back.
payload = gzip.compress(b"metadata roundtrip\n")
expires = datetime.datetime(2030, 10, 21, 7, 28, tzinfo=datetime.timezone.utc)
s3.put_object(
    Bucket=B, Key="meta.txt", Body=payload,
    ContentType="application/x-shardline-test", CacheControl="max-age=3600",
    ContentDisposition="inline; filename=weights.bin", ContentEncoding="gzip",
    Expires=expires, WebsiteRedirectLocation="/latest",
    Metadata={"purpose": "client-breadth"})
head = s3.head_object(Bucket=B, Key="meta.txt")
assert head["ContentLength"] == len(payload), head["ContentLength"]
assert head["Metadata"].get("purpose") == "client-breadth", head["Metadata"]
assert head["ContentType"] == "application/x-shardline-test", head
assert head["CacheControl"] == "max-age=3600", head
assert head["ContentDisposition"] == "inline; filename=weights.bin", head
assert head["ContentEncoding"] == "gzip", head
assert head["Expires"] == expires, head
assert head["WebsiteRedirectLocation"] == "/latest", head
assert not any(key.startswith("shardline-internal-s3-") for key in head["Metadata"]), head["Metadata"]
metadata_get = s3.get_object(Bucket=B, Key="meta.txt")
assert metadata_get["Body"].read() == payload
assert metadata_get["CacheControl"] == head["CacheControl"], metadata_get
assert metadata_get["ContentDisposition"] == head["ContentDisposition"], metadata_get
assert metadata_get["ContentEncoding"] == head["ContentEncoding"], metadata_get
assert metadata_get["Expires"] == head["Expires"], metadata_get
assert metadata_get["WebsiteRedirectLocation"] == head["WebsiteRedirectLocation"], metadata_get

# Single-PUT checksums remain available after publication through both S3
# checksum read APIs.
checksummed = b"native single put checksum"
checksummed_value = base64.b64encode(hashlib.sha256(checksummed).digest()).decode()
s3.put_object(
    Bucket=B, Key="single-checksum.bin", Body=checksummed,
    ChecksumAlgorithm="SHA256", ChecksumSHA256=checksummed_value
)
single_head = s3.head_object(Bucket=B, Key="single-checksum.bin", ChecksumMode="ENABLED")
assert single_head["ChecksumSHA256"] == checksummed_value, single_head
assert single_head["ChecksumType"] == "FULL_OBJECT", single_head
single_attrs = s3.get_object_attributes(
    Bucket=B, Key="single-checksum.bin", ObjectAttributes=["Checksum"]
)
assert single_attrs["Checksum"] == {
    "ChecksumSHA256": checksummed_value,
    "ChecksumType": "FULL_OBJECT",
}, single_attrs
single_get = s3.get_object(Bucket=B, Key="single-checksum.bin", ChecksumMode="ENABLED")
assert single_get["ChecksumSHA256"] == checksummed_value, single_get
assert single_get["Body"].read() == checksummed

# SHA256 multipart COMPOSITE remains bound to the uploaded part and survives
# ListParts, completion, and GetObjectAttributes.
sha_part = b"native multipart sha256 checksum"
sha_part_checksum = base64.b64encode(hashlib.sha256(sha_part).digest()).decode()
sha_upload = s3.create_multipart_upload(
    Bucket=B, Key="multipart-sha256.bin", ChecksumAlgorithm="SHA256", ChecksumType="COMPOSITE"
)
sha_upload_id = sha_upload["UploadId"]
sha_part_result = s3.upload_part(
    Bucket=B, Key="multipart-sha256.bin", UploadId=sha_upload_id,
    PartNumber=1, Body=sha_part, ChecksumSHA256=sha_part_checksum
)
sha_list = s3.list_parts(Bucket=B, Key="multipart-sha256.bin", UploadId=sha_upload_id)
assert sha_list["ChecksumAlgorithm"] == "SHA256", sha_list
assert sha_list["Parts"][0]["ChecksumSHA256"] == sha_part_checksum, sha_list
sha_complete = s3.complete_multipart_upload(
    Bucket=B, Key="multipart-sha256.bin", UploadId=sha_upload_id,
    MultipartUpload={"Parts": [{
        "PartNumber": 1,
        "ETag": sha_part_result["ETag"],
        "ChecksumSHA256": sha_part_checksum,
    }]},
)
assert "ChecksumSHA256" in sha_complete, sha_complete
sha_attrs = s3.get_object_attributes(
    Bucket=B, Key="multipart-sha256.bin", ObjectAttributes=["Checksum", "ObjectParts"]
)
assert sha_attrs["Checksum"]["ChecksumType"] == "COMPOSITE", sha_attrs
assert sha_attrs["Checksum"]["ChecksumSHA256"].endswith("-1"), sha_attrs
assert sha_attrs["ObjectParts"]["Parts"][0]["ChecksumSHA256"] == sha_part_checksum, sha_attrs
sha_get = s3.get_object(Bucket=B, Key="multipart-sha256.bin", ChecksumMode="ENABLED")
assert sha_get["ChecksumSHA256"] == sha_attrs["Checksum"]["ChecksumSHA256"], sha_get
assert sha_get["Body"].read() == sha_part

sha1_part = b"native multipart sha1 checksum"
sha1_part_checksum = base64.b64encode(hashlib.sha1(sha1_part).digest()).decode()
sha1_upload = s3.create_multipart_upload(
    Bucket=B, Key="multipart-sha1.bin", ChecksumAlgorithm="SHA1", ChecksumType="COMPOSITE"
)
sha1_upload_id = sha1_upload["UploadId"]
sha1_part_result = s3.upload_part(
    Bucket=B, Key="multipart-sha1.bin", UploadId=sha1_upload_id,
    PartNumber=1, Body=sha1_part, ChecksumSHA1=sha1_part_checksum
)
sha1_list = s3.list_parts(Bucket=B, Key="multipart-sha1.bin", UploadId=sha1_upload_id)
assert sha1_list["ChecksumAlgorithm"] == "SHA1", sha1_list
assert sha1_list["Parts"][0]["ChecksumSHA1"] == sha1_part_checksum, sha1_list
s3.complete_multipart_upload(
    Bucket=B, Key="multipart-sha1.bin", UploadId=sha1_upload_id,
    MultipartUpload={"Parts": [{
        "PartNumber": 1,
        "ETag": sha1_part_result["ETag"],
        "ChecksumSHA1": sha1_part_checksum,
    }]},
)
sha1_attrs = s3.get_object_attributes(
    Bucket=B, Key="multipart-sha1.bin", ObjectAttributes=["Checksum", "ObjectParts"]
)
assert sha1_attrs["Checksum"]["ChecksumType"] == "COMPOSITE", sha1_attrs
assert sha1_attrs["Checksum"]["ChecksumSHA1"].endswith("-1"), sha1_attrs
assert sha1_attrs["ObjectParts"]["Parts"][0]["ChecksumSHA1"] == sha1_part_checksum, sha1_attrs

# FULL_OBJECT CRC64NVME checksums are verified against the assembled bytes and
# remain available through checksum-enabled reads and GetObjectAttributes.
full_payload = gzip.compress(b"native multipart full-object CRC64NVME")
full_checksum = crc64nvme(full_payload)
full_upload = s3.create_multipart_upload(
    Bucket=B, Key="multipart-full-crc64.bin",
    ChecksumAlgorithm="CRC64NVME", ChecksumType="FULL_OBJECT",
    ContentType="application/x-shardline-full-object", CacheControl="max-age=7200",
    ContentEncoding="gzip",
    ContentDisposition="attachment; filename=full-object.bin", Expires=expires,
    WebsiteRedirectLocation="/full-object"
)
full_upload_id = full_upload["UploadId"]
full_part_checksum = crc64nvme(full_payload)
full_part = s3.upload_part(
    Bucket=B, Key="multipart-full-crc64.bin", UploadId=full_upload_id,
    PartNumber=1, Body=full_payload, ChecksumCRC64NVME=full_part_checksum
)
full_list = s3.list_parts(Bucket=B, Key="multipart-full-crc64.bin", UploadId=full_upload_id)
assert full_list["ChecksumAlgorithm"] == "CRC64NVME", full_list
assert full_list["ChecksumType"] == "FULL_OBJECT", full_list
assert full_list["Parts"][0]["ChecksumCRC64NVME"] == full_part_checksum, full_list
full_complete = s3.complete_multipart_upload(
    Bucket=B, Key="multipart-full-crc64.bin", UploadId=full_upload_id,
    MultipartUpload={"Parts": [{
        "PartNumber": 1,
        "ETag": full_part["ETag"],
        "ChecksumCRC64NVME": full_part_checksum,
    }]},
    ChecksumCRC64NVME=full_checksum,
    ChecksumType="FULL_OBJECT",
)
assert full_complete["ChecksumCRC64NVME"] == full_checksum, full_complete
full_attrs = s3.get_object_attributes(
    Bucket=B, Key="multipart-full-crc64.bin", ObjectAttributes=["Checksum", "ObjectParts"]
)
assert full_attrs["Checksum"] == {
    "ChecksumCRC64NVME": full_checksum,
    "ChecksumType": "FULL_OBJECT",
}, full_attrs
assert full_attrs["ObjectParts"]["TotalPartsCount"] == 1, full_attrs
full_get = s3.get_object(
    Bucket=B, Key="multipart-full-crc64.bin", ChecksumMode="ENABLED"
)
assert full_get["ChecksumCRC64NVME"] == full_checksum, full_get
assert full_get["Body"].read() == full_payload
full_head = s3.head_object(Bucket=B, Key="multipart-full-crc64.bin")
assert full_head["ContentType"] == "application/x-shardline-full-object", full_head
assert full_head["CacheControl"] == "max-age=7200", full_head
assert full_head["ContentEncoding"] == "gzip", full_head
assert full_head["ContentDisposition"] == "attachment; filename=full-object.bin", full_head
assert full_head["Expires"] == expires, full_head
assert full_head["WebsiteRedirectLocation"] == "/full-object", full_head

# Conditional CopyObject (CopySourceIfMatch) — exercises If-Match + CopyObject.
etag = head["ETag"].strip('"')
s3.copy_object(
    Bucket=B,
    Key="meta-copy.txt",
    CopySource={"Bucket": B, "Key": "meta.txt"},
    CopySourceIfMatch=etag,
)
assert s3.head_object(Bucket=B, Key="meta-copy.txt")["ContentType"] == "application/x-shardline-test"
copied_headers = s3.head_object(Bucket=B, Key="meta-copy.txt")
for name in ("CacheControl", "ContentDisposition", "ContentEncoding", "Expires", "WebsiteRedirectLocation"):
    assert copied_headers[name] == head[name], (name, copied_headers, head)
s3.copy_object(
    Bucket=B, Key="meta-replace.txt", CopySource={"Bucket": B, "Key": "meta.txt"},
    MetadataDirective="REPLACE", ContentType="application/x-shardline-replaced",
    CacheControl="no-cache", ContentDisposition="attachment; filename=replaced.bin",
    ContentEncoding="gzip", Expires=expires, WebsiteRedirectLocation="/replaced",
    Metadata={"purpose": "replacement"})
replaced_headers = s3.head_object(Bucket=B, Key="meta-replace.txt")
assert replaced_headers["ContentType"] == "application/x-shardline-replaced", replaced_headers
assert replaced_headers["CacheControl"] == "no-cache", replaced_headers
assert replaced_headers["ContentDisposition"] == "attachment; filename=replaced.bin", replaced_headers
assert replaced_headers["ContentEncoding"] == "gzip", replaced_headers
assert replaced_headers["Expires"] == expires, replaced_headers
assert replaced_headers["WebsiteRedirectLocation"] == "/replaced", replaced_headers
assert replaced_headers["Metadata"]["purpose"] == "replacement", replaced_headers

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
attributes = s3.get_object_attributes(Bucket=B, Key="big.bin", ObjectAttributes=["Checksum", "ObjectParts"])
assert attributes["Checksum"]["ChecksumType"] == "COMPOSITE", attributes
assert attributes["Checksum"]["ChecksumCRC32"].endswith("-3"), attributes
parts = attributes["ObjectParts"]["Parts"]
assert [part["PartNumber"] for part in parts] == [1, 2, 3], parts
assert [part["Size"] for part in parts] == [8 * 1024 * 1024, 8 * 1024 * 1024, 4 * 1024 * 1024], parts
assert all("ChecksumCRC32" in part for part in parts), parts
part_bytes = [big[:8 * 1024 * 1024], big[8 * 1024 * 1024:16 * 1024 * 1024], big[16 * 1024 * 1024:]]
part_checksums = [struct.pack(">I", zlib.crc32(data) & 0xffffffff) for data in part_bytes]
expected_part_checksums = [base64.b64encode(value).decode() for value in part_checksums]
assert [part["ChecksumCRC32"] for part in parts] == expected_part_checksums, parts
expected_composite = base64.b64encode(struct.pack(">I", zlib.crc32(b"".join(part_checksums)) & 0xffffffff)).decode() + "-3"
assert attributes["Checksum"]["ChecksumCRC32"] == expected_composite, attributes
assert "shardline-internal-s3-checksum-v1" not in s3.head_object(Bucket=B, Key="big.bin")["Metadata"]

# DeleteObjects batch.
s3.delete_objects(
    Bucket=B,
    Delete={"Objects": [
        {"Key": "meta.txt"}, {"Key": "meta-copy.txt"}, {"Key": "meta-replace.txt"}, {"Key": "big.bin"},
        {"Key": "single-checksum.bin"}, {"Key": "multipart-sha256.bin"},
        {"Key": "multipart-sha1.bin"}, {"Key": "multipart-full-crc64.bin"},
    ]},
)
remaining = s3.list_objects_v2(Bucket=B)
assert "Contents" not in remaining, remaining
print("ALL BOTO3 CHECKS PASSED")
"#,
    )
    .unwrap();
    client_output(
        std::process::Command::new(boto3_python())
            .arg(script.to_str().unwrap())
            .env("S3_ENDPOINT", endpoint)
            .env("S3_TOKEN", token),
    )
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
    assert!(
        output.status.success(),
        "boto3 subprocess failed with status {}: {}, script line {:?}",
        output.status,
        boto3_failure_category(&output.stderr),
        boto3_failure_script_line(&output.stderr)
    );
    assert!(
        stdout.contains("ALL BOTO3 CHECKS PASSED"),
        "boto3 checks did not complete"
    );
}

// ---------------------------------------------------------------------------
// s3cmd
// ---------------------------------------------------------------------------

/// Returns whether s3cmd is on PATH.
fn s3cmd_available() -> bool {
    let available = probe_client("s3cmd", &["--version"]);
    shardline_test_support::require_native_client("s3cmd", available)
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
        client_output(
            std::process::Command::new("s3cmd")
                .arg("--config")
                .arg(config)
                .args(&args),
        )
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
    assert!(mb.status.success(), "s3cmd mb failed: {}", mb.status);

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
    assert!(put.status.success(), "s3cmd put failed: {}", put.status);

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
    assert!(get.status.success(), "s3cmd get failed: {}", get.status);
    assert_eq!(std::fs::read(&dl).unwrap(), b"s3cmd payload\n");

    let ls = s3cmd_run(&host, &token, &tmp, &["ls", &bucket_url]).await;
    assert!(ls.status.success(), "s3cmd ls failed");
    let listing = String::from_utf8_lossy(&ls.stdout);
    assert!(listing.contains("note.txt"), "s3cmd ls must list note.txt");

    let del = s3cmd_run(
        &host,
        &token,
        &tmp,
        &["del", &format!("{bucket_url}/note.txt")],
    )
    .await;
    assert!(del.status.success(), "s3cmd del failed: {}", del.status);

    let ls_after = s3cmd_run(&host, &token, &tmp, &["ls", &bucket_url]).await;
    assert!(ls_after.status.success(), "s3cmd ls after removal failed");
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("note.txt"),
        "note.txt must be gone after del"
    );
}

// ---------------------------------------------------------------------------
// rclone
// ---------------------------------------------------------------------------

/// Returns whether rclone is on PATH.
fn rclone_available() -> bool {
    let available = probe_client("rclone", &["--version"]);
    shardline_test_support::require_native_client("rclone", available)
}

/// Runs `rclone` against the in-process server using a Minio-style S3 remote
/// configured purely through environment variables.
async fn rclone_run(endpoint: &str, token: &str, args: &[&str]) -> std::process::Output {
    let endpoint = endpoint.to_owned();
    let token = token.to_owned();
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        client_output(
            std::process::Command::new("rclone")
                .args(&args)
                .env("RCLONE_CONFIG_REMOTE_TYPE", "s3")
                .env("RCLONE_CONFIG_REMOTE_PROVIDER", "Minio")
                .env("RCLONE_CONFIG_REMOTE_ENDPOINT", &endpoint)
                .env("RCLONE_CONFIG_REMOTE_ACCESS_KEY_ID", &token)
                .env("RCLONE_CONFIG_REMOTE_SECRET_ACCESS_KEY", "dummy-secret")
                // Older rclone versions send an explicit ACL by default. Match
                // Shardline's BucketOwnerEnforced policy instead of enabling ACLs.
                .env("RCLONE_CONFIG_REMOTE_ACL", "bucket-owner-full-control")
                .env("RCLONE_CONFIG_REMOTE_NO_CHECK_BUCKET", "true"),
        )
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_conditional_headers_preserve_bytes_over_http() {
    let server = TestServer::start().await;
    let token = mint_token(OWNER, NAME);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let url = format!("{}/{BUCKET}/opaque.bin", server.base_url);
    let put = client
        .put(&url)
        .bearer_auth(&token)
        .body("original")
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), reqwest::StatusCode::OK);
    let etag = put.headers()["etag"].clone();
    let mut list = b"\"\x80,opaque\", ".to_vec();
    list.extend_from_slice(etag.as_bytes());
    let value = reqwest::header::HeaderValue::from_bytes(&list).unwrap();
    for method in [reqwest::Method::GET, reqwest::Method::HEAD] {
        for (name, expected) in [
            ("If-Match", reqwest::StatusCode::OK),
            ("If-None-Match", reqwest::StatusCode::NOT_MODIFIED),
        ] {
            let response = client
                .request(method.clone(), &url)
                .bearer_auth(&token)
                .header(name, value.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let body = response.bytes().await.unwrap();
            if method == reqwest::Method::GET && expected == reqwest::StatusCode::OK {
                assert_eq!(body.as_ref(), b"original");
            } else {
                assert!(body.is_empty());
            }
        }
    }
    let copied = format!("{}/{BUCKET}/copied.bin", server.base_url);
    let copy = client
        .put(&copied)
        .bearer_auth(&token)
        .header("x-amz-copy-source", format!("/{BUCKET}/opaque.bin"))
        .header("x-amz-copy-source-if-match", value.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(copy.status(), reqwest::StatusCode::OK);
    let denied = client
        .put(&url)
        .bearer_auth(&token)
        .header("If-None-Match", value.clone())
        .body("must-not-land")
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::PRECONDITION_FAILED);
    let accepted = client
        .put(&url)
        .bearer_auth(&token)
        .header("If-Match", value.clone())
        .body("replacement")
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    let body = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"replacement");
    let denied = client
        .delete(&copied)
        .bearer_auth(&token)
        .header("If-None-Match", value.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::PRECONDITION_FAILED);
    let body = client
        .get(&copied)
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(body.as_ref(), b"original");
    let deleted = client
        .delete(&copied)
        .bearer_auth(&token)
        .header("If-Match", value)
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), reqwest::StatusCode::NO_CONTENT);
    assert_eq!(
        client
            .get(&copied)
            .bearer_auth(&token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
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
        mkdir.status
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
    assert!(copy.status.success(), "rclone copy failed: {}", copy.status);
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
    assert!(listing.contains("note.txt"), "rclone ls must list note.txt");

    // Delete + verify gone.
    let del = rclone_run(
        &server.base_url,
        &token,
        &["delete", &format!("{remote_bucket}/note.txt")],
    )
    .await;
    assert!(del.status.success(), "rclone delete failed: {}", del.status);
    let ls_after = rclone_run(&server.base_url, &token, &["ls", &remote_bucket]).await;
    assert!(ls_after.status.success(), "rclone ls after removal failed");
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        !listing_after.contains("note.txt"),
        "note.txt must be gone after rclone delete"
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
        mirror.status
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
        assert!(listing.contains(key), "mc ls --recursive missing {key}");
    }

    // HeadObject via `mc stat --json`.
    let stat = mc_run(
        &config_dir,
        &["stat", "--json", &format!("{alias_name}/{BUCKET}/a.txt")],
    )
    .await;
    assert!(stat.status.success(), "mc stat --json failed");
    let stat_out = String::from_utf8_lossy(&stat.stdout);
    assert!(stat_out.contains("a.txt"), "stat --json must name a.txt");

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
        rm.status
    );

    let ls_after = mc_run(
        &config_dir,
        &["ls", "--recursive", &format!("{alias_name}/{BUCKET}")],
    )
    .await;
    assert!(
        ls_after.status.success(),
        "recursive mc ls after removal failed"
    );
    let listing_after = String::from_utf8_lossy(&ls_after.stdout);
    assert!(
        listing_after.trim().is_empty(),
        "bucket must be empty after recursive rm"
    );

    assert_no_leaked_mc_artifacts();
}
