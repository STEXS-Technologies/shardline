//! Real-deployment chaos drills: fault-injection against a live `shardline`
//! server BINARY backed by the `shardline-chaos` Docker stack (Postgres,
//! MinIO, Redis) instead of an in-process test harness.
//!
//! Philosophy (mirrors `fault_drills.rs`): every fault is synchronized on
//! *evidence* that the server is provably inside the operation — a chunk
//! object landing in the object store mid-body. Unlike the hermetic drills,
//! the fault is injected into a *backend container* (`docker stop`, `docker
//! network disconnect`) while the HOST server process keeps running. This
//! models real deployment incidents (DB outage, object-store outage, cache
//! outage, network partition), not process crashes.
//!
//! Runtime-SKIP convention (fault_drills.rs drill3): when the chaos stack is
//! not available the test prints a loud banner and returns — never
//! `#[ignore]`, which is compile-time static.

#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or,
    clippy::arithmetic_side_effects
)]

use serial_test::serial;
use sha2::{Digest, Sha256};
use shardline_protocol::{
    ByteRange, RepositoryProvider, RepositoryScope, SecretString, TokenClaims, TokenScope,
};
use shardline_server::ServerObjectStore;
use shardline_server_core::{AuthProvider, ServerObjectStoreError, auth::LocalHmacProvider};
use shardline_storage::{ObjectKey, ObjectPrefix, ObjectStore as _, S3ObjectStoreConfig};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tempfile::{NamedTempFile, TempDir};
use tokio::{net::TcpStream, sync::mpsc, task::JoinHandle};

// ---------------------------------------------------------------------------
// Deployment constants (must match docker-compose.chaos.yml / Makefile.toml).
// ---------------------------------------------------------------------------

/// Host bind address for the deployed server (avoids dev 18080 / default 8080).
const BIN_ADDR: &str = "127.0.0.1:18081";
const BIN_ADDR_SECONDARY: &str = "127.0.0.1:18082";
/// Chaos Postgres (compose publishes `15432 -> 5432`).
const PG_URL: &str = "postgres://shardline:shardline-dev-password@127.0.0.1:15432/shardline";
/// Design-default MinIO endpoint. Overridden at runtime from the container's
/// actual published port (see [`ChaosStack::resolve`]).
const S3_ENDPOINT_DEFAULT: &str = "http://127.0.0.1:29000";
/// Design-default Redis URL. Overridden at runtime from the container's
/// actual published port: compose exposes chaos-redis on `16380`, and
/// `16379` is frequently occupied by unrelated dev stacks.
const REDIS_URL_DEFAULT: &str = "redis://127.0.0.1:16379/0";
/// Docker network for the chaos stack (compose `networks.chaos-net.name`).
const NET: &str = "shardline-chaos-net";
/// The S3 repository bucket (`{owner}.{name}`) the tokens are scoped to.
const BUCKET: &str = "drill.drill";
/// The object-store bucket the server writes chunk objects into
/// (`SHARDLINE_S3_BUCKET`; created by the compose minio-init service).
const OBJECT_BUCKET: &str = "shardline";

const CONTAINER_POSTGRES: &str = "chaos-postgres";
const CONTAINER_MINIO: &str = "chaos-minio";
const CONTAINER_REDIS: &str = "chaos-redis";
const NETEM_IMAGE: &str = "nicolaka/netshoot:v0.13";

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const CHUNK_SIZE: usize = 65536;

// ---------------------------------------------------------------------------
// Auth / tokens — same signing key as the deployed server (verbatim from
// fault_drills.rs).
// ---------------------------------------------------------------------------

fn mint_token(owner: &str, name: &str, scope: TokenScope) -> String {
    mint_token_expiring_at(owner, name, scope, u64::MAX)
}

fn mint_token_expiring_at(
    owner: &str,
    name: &str,
    scope: TokenScope,
    expires_at_unix_seconds: u64,
) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo =
        RepositoryScope::new(RepositoryProvider::Generic, owner, name, Some("main")).unwrap();
    let claims = TokenClaims::new(
        "shardline",
        "deployment-chaos",
        scope,
        repo,
        expires_at_unix_seconds,
    )
    .unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Small deterministic helpers (verbatim from fault_drills.rs).
// ---------------------------------------------------------------------------

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state & 0xff) as u8);
    }
    out
}

/// Extract the text of the first `<{tag}>..</{tag}>` element from an XML body
/// (used to parse the `UploadId` out of S3 multipart create responses).
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.checked_add(open.len())?;
    let end = xml[start..].find(&close)?.checked_add(start)?;
    Some(xml[start..end].to_owned())
}

/// Slow body: a reqwest stream backed by an mpsc channel. The handler is
/// provably inside its body-read loop until we drop `tx` (no EOF ever sent).
fn slow_body() -> (
    mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    reqwest::Body,
) {
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(1);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|item| (item, rx))
    });
    (tx, reqwest::Body::wrap_stream(stream))
}

/// One-shot TCP reachability probe (verbatim from fault_drills.rs).
async fn require_tcp(host: &str, port: u16) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_millis(250), TcpStream::connect((host, port))).await,
        Ok(Ok(_stream))
    )
}

async fn tcp_ready(host: &str, port: u16) -> bool {
    require_tcp(host, port).await
}

// ---------------------------------------------------------------------------
// Docker helpers (mc_run pattern from s3_real_client_e2e.rs).
// ---------------------------------------------------------------------------

/// Runs a `docker` subprocess on the blocking pool and returns its output
/// (mc_run pattern from s3_real_client_e2e.rs).
async fn docker_run(args: &[&str]) -> std::process::Output {
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(&args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

async fn docker_run_owned(args: Vec<String>) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(args)
            .output()
            .unwrap()
    })
    .await
    .unwrap()
}

async fn replace_netem(container: &str, rule: &str) -> std::process::Output {
    docker_run_owned(vec![
        "run".to_owned(),
        "--rm".to_owned(),
        format!("--network=container:{container}"),
        "--cap-add=NET_ADMIN".to_owned(),
        NETEM_IMAGE.to_owned(),
        "sh".to_owned(),
        "-c".to_owned(),
        format!("tc qdisc replace dev eth0 root netem {rule}"),
    ])
    .await
}

async fn clear_netem(container: &str) -> std::process::Output {
    docker_run_owned(vec![
        "run".to_owned(),
        "--rm".to_owned(),
        format!("--network=container:{container}"),
        "--cap-add=NET_ADMIN".to_owned(),
        NETEM_IMAGE.to_owned(),
        "sh".to_owned(),
        "-c".to_owned(),
        "tc qdisc del dev eth0 root 2>/dev/null || true".to_owned(),
    ])
    .await
}

/// Resolves the host-published port of `container_port` inside `container`
/// (`docker port chaos-minio 9000/tcp` -> `0.0.0.0:29000` -> `29000`).
async fn container_published_port(container: &str, container_port: &str) -> Option<String> {
    let out = docker_run(&["port", container, container_port]).await;
    if !out.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    stdout
        .lines()
        .next()?
        .rsplit(':')
        .next()
        .map(|port| port.trim().to_owned())
}

/// Polls `probe` (an async predicate) until it returns `true` or `timeout`
/// elapses; panics on timeout.
async fn wait_for<F, Fut>(what: &str, mut probe: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("deadline overflow");
    loop {
        if probe().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// `docker start` a backend container, then poll `probe` until it recovers.
async fn restart_and_wait<F, Fut>(name: &str, probe: F, timeout: Duration)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = docker_run(&["start", name]).await;
    assert!(
        start.status.success(),
        "docker start {name} failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    wait_for(&format!("{name} recovery"), probe, timeout).await;
}

/// RAII recovery guard for chaos services. Any service this drill stopped or
/// disconnected is brought back on Drop (best effort, errors ignored), so a
/// panicking drill cannot leave the stack degraded and cascade SKIPs into the
/// next drill.
struct ServiceRecoveryGuard {
    stopped: Vec<&'static str>,
    disconnected: Vec<&'static str>,
    netem: Vec<&'static str>,
}

impl ServiceRecoveryGuard {
    const fn new() -> Self {
        Self {
            stopped: Vec::new(),
            disconnected: Vec::new(),
            netem: Vec::new(),
        }
    }

    /// `docker stop` a service with zero grace period (immediate SIGKILL) and
    /// wait until its TCP port is unreachable, then remember to restore it on
    /// drop. The zero-timeout stop avoids the race between the 10s SIGTERM
    /// grace period and the caller's fault injection.
    async fn stop(&mut self, name: &'static str) {
        let out = docker_run(&["stop", "--time", "0", name]).await;
        assert!(out.status.success(), "docker stop {name}");
        if !self.stopped.contains(&name) {
            self.stopped.push(name);
        }
        // Wait for the service's TCP port to become unreachable so the server's
        // connection pool drops any cached connections before the caller injects
        // the next fault.
        let port = match name {
            CONTAINER_POSTGRES => Some(15432u16),
            CONTAINER_MINIO => Some(39000u16),
            CONTAINER_REDIS => Some(16380u16),
            _ => None,
        };
        if let Some(p) = port {
            wait_for(
                &format!("{name} TCP unreachable on port {p}"),
                || async move { !tcp_ready("127.0.0.1", p).await },
                Duration::from_secs(15),
            )
            .await;
        }
    }

    /// `docker network disconnect` a service and remember to reconnect on drop.
    async fn disconnect(&mut self, name: &'static str) {
        let out = docker_run(&["network", "disconnect", NET, name]).await;
        assert!(out.status.success(), "docker network disconnect {name}");
        if !self.disconnected.contains(&name) {
            self.disconnected.push(name);
        }
    }

    async fn replace_netem(&mut self, name: &'static str, rule: &str) {
        let out = replace_netem(name, rule).await;
        assert!(
            out.status.success(),
            "install netem rule on {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        if !self.netem.contains(&name) {
            self.netem.push(name);
        }
    }

    async fn clear_netem(&mut self, name: &'static str) {
        let out = clear_netem(name).await;
        assert!(
            out.status.success(),
            "clear netem rule on {name}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        self.netem.retain(|container| *container != name);
    }

    /// Marks a service as restored (call after a successful `docker start` /
    /// reconnect + readiness) so Drop won't touch it again.
    fn recovered(&mut self, name: &'static str) {
        self.stopped.retain(|n| *n != name);
        self.disconnected.retain(|n| *n != name);
    }
}

impl Drop for ServiceRecoveryGuard {
    fn drop(&mut self) {
        for name in &self.stopped {
            let _ = std::process::Command::new("docker")
                .args(["start", name])
                .output();
        }
        for name in &self.disconnected {
            let _ = std::process::Command::new("docker")
                .args(["network", "connect", NET, name])
                .output();
        }
        for name in &self.netem {
            let network = format!("--network=container:{name}");
            let _ = std::process::Command::new("docker")
                .args([
                    "run",
                    "--rm",
                    &network,
                    "--cap-add=NET_ADMIN",
                    NETEM_IMAGE,
                    "sh",
                    "-c",
                    "tc qdisc del dev eth0 root 2>/dev/null || true",
                ])
                .output();
        }
    }
}

// ---------------------------------------------------------------------------
// Chaos-stack availability gate (runtime-SKIP, fault_drills.rs drill3 style).
// ---------------------------------------------------------------------------

struct ChaosStack {
    pg_url: String,
    s3_endpoint: String,
    redis_url: String,
}

/// Returns the resolved chaos-stack endpoints, or `None` (after printing a
/// loud banner) ONLY when docker is absent or the chaos containers do not
/// exist. A present-but-degraded stack (e.g. a service left stopped by a
/// previous failed drill) is NOT a skip condition — callers must run
/// [`ensure_chaos_stack_ready`] to self-heal it before proceeding.
async fn chaos_stack_available(drill: &str) -> Option<ChaosStack> {
    let ps = docker_run(&["ps"]).await;
    if !ps.status.success() {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (docker: {})",
            String::from_utf8_lossy(&ps.stderr).trim()
        );
        return None;
    }
    let inspect = docker_run(&[
        "inspect",
        CONTAINER_POSTGRES,
        CONTAINER_MINIO,
        CONTAINER_REDIS,
    ])
    .await;
    if !inspect.status.success() {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (docker: inspect {CONTAINER_POSTGRES} {CONTAINER_MINIO} {CONTAINER_REDIS}: {})",
            String::from_utf8_lossy(&inspect.stderr).trim()
        );
        return None;
    }
    let Some(minio_port) = container_published_port(CONTAINER_MINIO, "9000/tcp").await else {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (chaos-minio has no host-published port; likely a port collision, e.g. 29000/29001 already allocated to another container)"
        );
        return None;
    };
    let s3_endpoint = format!("http://127.0.0.1:{minio_port}");
    let redis_url = container_published_port(CONTAINER_REDIS, "6379/tcp")
        .await
        .map_or_else(
            || REDIS_URL_DEFAULT.to_owned(),
            |port| format!("redis://127.0.0.1:{port}/0"),
        );
    eprintln!("chaos({drill}): stack resolved s3_endpoint={s3_endpoint} redis_url={redis_url}");
    Some(ChaosStack {
        pg_url: PG_URL.to_owned(),
        s3_endpoint,
        redis_url,
    })
}

async fn minio_health_ok(endpoint: &str) -> bool {
    reqwest::Client::new()
        .get(format!("{endpoint}/minio/health/live"))
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .map(|resp| resp.status().is_success())
        .unwrap_or(false)
}

/// Polls `probe` until `true` or `timeout`; returns `false` on timeout (no
/// panic — used by the pre-flight stack readiness gate).
async fn ready_within<F, Fut>(mut probe: F, timeout: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("deadline overflow");
    loop {
        if probe().await {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn redis_pong() -> bool {
    let out = docker_run(&["exec", CONTAINER_REDIS, "redis-cli", "ping"]).await;
    out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "PONG"
}

/// Self-heals a present-but-degraded stack: starts any stopped chaos service
/// and waits for each to be ready. Returns `false` (loud SKIP banner) only if
/// a service cannot be brought up.
async fn ensure_chaos_stack_ready(drill: &str, stack: &ChaosStack) -> bool {
    for name in [CONTAINER_POSTGRES, CONTAINER_MINIO, CONTAINER_REDIS] {
        let running = docker_run(&["inspect", "-f", "{{.State.Running}}", name]).await;
        let is_running =
            running.status.success() && String::from_utf8_lossy(&running.stdout).trim() == "true";
        if !is_running {
            eprintln!("chaos({drill}): starting stopped service {name}");
            let start = docker_run(&["start", name]).await;
            if !start.status.success() {
                eprintln!(
                    "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (could not start {name}: {})",
                    String::from_utf8_lossy(&start.stderr).trim()
                );
                return false;
            }
        }
    }
    // Readiness waits (generous: a freshly started postgres can take ~10s to
    // accept connections; minio and redis similar).
    if !ready_within(|| tcp_ready("127.0.0.1", 15432), Duration::from_secs(90)).await {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (postgres 127.0.0.1:15432 did not come up)"
        );
        return false;
    }
    let s3 = stack.s3_endpoint.clone();
    if !ready_within(
        move || {
            let endpoint = s3.clone();
            async move { minio_health_ok(&endpoint).await }
        },
        Duration::from_secs(90),
    )
    .await
    {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (minio health on {} did not come up)",
            stack.s3_endpoint
        );
        return false;
    }
    if !ready_within(redis_pong, Duration::from_secs(90)).await {
        eprintln!(
            "SKIPPED: {drill} — shardline-chaos stack not available; run `cargo make chaos-deployment` (chaos-redis did not answer PONG)"
        );
        return false;
    }
    true
}

/// Resolves the chaos stack and self-heals it; `None` (loud SKIP) only when
/// docker or the chaos containers are genuinely absent.
async fn boot_chaos_stack(drill: &str) -> Option<ChaosStack> {
    let stack = chaos_stack_available(drill).await?;
    if !ensure_chaos_stack_ready(drill, &stack).await {
        return None;
    }
    Some(stack)
}

// ---------------------------------------------------------------------------
// Postgres migrations (connect-retry pattern from postgres_redis_e2e_http.rs).
// ---------------------------------------------------------------------------

async fn migrate_chaos_postgres(url: &str) {
    let mut last_err = None;
    for _ in 0..5 {
        match sqlx::PgPool::connect(url).await {
            Ok(pool) => {
                shardline_server::apply_database_migrations(&pool)
                    .await
                    .unwrap();
                pool.close().await;
                return;
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    panic!("migrate_chaos_postgres: cannot connect to {url} after 5 retries: {last_err:?}");
}

// ---------------------------------------------------------------------------
// Object-store client for chunk-evidence polling (S3 store, same creds as the
// deployed server).
// ---------------------------------------------------------------------------

fn s3_store(endpoint: &str) -> ServerObjectStore {
    let config = S3ObjectStoreConfig::new(OBJECT_BUCKET.to_owned(), "us-east-1".to_owned())
        .with_endpoint(Some(endpoint.to_owned()))
        .with_credentials(
            Some(SecretString::from_secret("shardline")),
            Some(SecretString::from_secret("shardline-dev-password")),
            None,
        )
        .with_allow_http(true);
    ServerObjectStore::s3(config).expect("build S3 object store for evidence polling")
}

/// Counts objects currently in the object store (chunk-evidence signal).
fn s3_object_count(store: &ServerObjectStore) -> usize {
    let mut count = 0_usize;
    let prefix = ObjectPrefix::parse("").expect("empty prefix");
    match store.visit_prefix(&prefix, |_meta| -> Result<(), ServerObjectStoreError> {
        count = count.saturating_add(1);
        Ok(())
    }) {
        Ok(()) => count,
        Err(e) => {
            eprintln!("s3_object_count: visit_prefix failed: {e:?}");
            0
        }
    }
}

/// Waits until a new chunk object lands in the object store while the slow PUT
/// is still streaming (provable mid-body evidence).
async fn wait_s3_object_count_grows(
    store: &ServerObjectStore,
    baseline: usize,
    put_task: &JoinHandle<reqwest::Result<reqwest::Response>>,
) {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(5))
        .expect("deadline overflow");
    loop {
        let count = s3_object_count(store);
        if count > baseline {
            return;
        }
        if put_task.is_finished() {
            panic!(
                "slow PUT finished before chunk evidence appeared (object count {count} <= baseline {baseline})"
            );
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("no chunk evidence within 5s (object count {count} <= baseline {baseline})");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// DeploymentServer — the HOST `shardline` binary as a child process.
// ---------------------------------------------------------------------------

fn resolve_shardline_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_shardline") {
        return Some(PathBuf::from(path));
    }
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)?;
    // Try debug first, then release (release survives `cargo clean` of debug).
    for profile in ["debug", "release"] {
        let candidate = workspace_root
            .join("target")
            .join(profile)
            .join("shardline");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn resolve_n_minus_one_binary(drill: &str) -> Option<PathBuf> {
    let path = std::env::var_os("SHARDLINE_N_MINUS_ONE_BINARY")
        .map(PathBuf::from)
        .filter(|path| path.is_file());
    if path.is_none() {
        eprintln!(
            "SKIPPED: {drill} — set SHARDLINE_N_MINUS_ONE_BINARY to a built v1.6.0 shardline binary"
        );
    }
    path
}

fn resolve_faketime_library(drill: &str) -> Option<PathBuf> {
    let path = std::env::var_os("SHARDLINE_FAKETIME_LIBRARY")
        .map(PathBuf::from)
        .filter(|path| path.is_file());
    if path.is_none() {
        eprintln!("SKIPPED: {drill} — set SHARDLINE_FAKETIME_LIBRARY to libfaketime.so.1");
    }
    path
}

struct DeploymentServer {
    child: std::process::Child,
    base_url: String,
    _log: NamedTempFile,
}

impl DeploymentServer {
    /// Spawns the host `shardline` binary with deployment-style environment.
    ///
    /// `extra_env` is applied after the base env, so drills can override the
    /// S3 endpoint, frontends, and reconstruction-cache wiring.
    fn spawn(binary: &Path, extra_env: &[(&str, &str)], root: &Path) -> Self {
        Self::spawn_at(binary, BIN_ADDR, extra_env, root)
    }

    fn spawn_at(binary: &Path, bind_addr: &str, extra_env: &[(&str, &str)], root: &Path) -> Self {
        let data_dir = root.join("data");
        std::fs::create_dir_all(&data_dir).unwrap_or_else(|e| panic!("create {data_dir:?}: {e}"));
        let log = NamedTempFile::new().expect("temp log file");
        let stdout = log.reopen().expect("stdout log handle");
        let stderr = log.reopen().expect("stderr log handle");

        let mut cmd = std::process::Command::new(binary);
        cmd.arg("serve");
        cmd.env("SHARDLINE_BIND_ADDR", bind_addr)
            .env("SHARDLINE_PUBLIC_BASE_URL", format!("http://{bind_addr}"))
            .env("SHARDLINE_SERVER_ROLE", "all")
            .env("SHARDLINE_AUTH_PROVIDER", "local")
            .env(
                "SHARDLINE_TOKEN_SIGNING_KEY",
                "0123456789abcdef0123456789abcdef",
            )
            .env("SHARDLINE_ROOT_DIR", &data_dir)
            .env("SHARDLINE_CHUNK_SIZE_BYTES", CHUNK_SIZE.to_string())
            .env("SHARDLINE_OBJECT_STORAGE_ADAPTER", "s3")
            .env("SHARDLINE_S3_BUCKET", OBJECT_BUCKET)
            .env("SHARDLINE_S3_REGION", "us-east-1")
            .env("SHARDLINE_S3_ENDPOINT", S3_ENDPOINT_DEFAULT)
            .env("SHARDLINE_S3_ACCESS_KEY_ID", "shardline")
            .env("SHARDLINE_S3_SECRET_ACCESS_KEY", "shardline-dev-password")
            .env("SHARDLINE_S3_ALLOW_HTTP", "true")
            .env("SHARDLINE_INDEX_POSTGRES_URL", PG_URL)
            .env("SHARDLINE_SERVER_FRONTENDS", "s3")
            .env("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "memory")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        for (key, value) in extra_env {
            cmd.env(key, value);
        }
        let child = cmd
            .spawn()
            .unwrap_or_else(|e| panic!("spawn {binary:?}: {e}"));
        Self {
            child,
            base_url: format!("http://{bind_addr}"),
            _log: log,
        }
    }

    async fn wait_ready(&mut self, timeout: Duration) {
        let client = reqwest::Client::new();
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .expect("deadline overflow");
        loop {
            if !self.alive() {
                panic!(
                    "deployment server exited during startup; see log {:?}",
                    self._log.path()
                );
            }
            if matches!(
                client
                    .get(format!("{}/healthz", self.base_url))
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await,
                Ok(resp) if resp.status().as_u16() == 200
            ) {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "deployment server did not become healthy within {timeout:?}; see log {:?}",
                    self._log.path()
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn alive(&mut self) -> bool {
        self.child
            .try_wait()
            .map(|status| status.is_none())
            .unwrap_or(false)
    }

    fn base_url(&self) -> String {
        self.base_url.clone()
    }
}

impl Drop for DeploymentServer {
    fn drop(&mut self) {
        // SIGKILL cleanup on drop/panic — drills never restart the server
        // process, only the backend containers.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Traffic driver helpers.
// ---------------------------------------------------------------------------

async fn s3_put(base: &str, token: &str, key: &str, bytes: Vec<u8>) -> reqwest::Response {
    reqwest::Client::new()
        .put(format!("{base}/{BUCKET}/{key}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()
        .await
        .expect("s3 PUT request")
}

async fn s3_get(base: &str, token: &str, key: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("{base}/{BUCKET}/{key}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("s3 GET request")
}

async fn assert_s3_bytes(base: &str, token: &str, key: &str, expected: &[u8], context: &str) {
    let response = s3_get(base, token, key).await;
    assert_eq!(response.status().as_u16(), 200, "{context}: GET {key}");
    assert_eq!(
        response.bytes().await.unwrap().as_ref(),
        expected,
        "{context}: exact bytes for {key}"
    );
}

async fn s3_delete(base: &str, token: &str, key: &str) -> reqwest::Response {
    reqwest::Client::new()
        .delete(format!("{base}/{BUCKET}/{key}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("s3 DELETE request")
}

/// Starts a slow PUT (streams one 512KiB chunk, then stalls) and returns the
/// body sender plus the in-flight request task.
///
/// The streamed bytes are derived from `first_chunk_seed` XOR a fresh
/// nanosecond timestamp, so each invocation writes content the (persistent)
/// object store has never seen: content-addressed dedup would otherwise
/// suppress the chunk-evidence signal on reruns against the same volume.
async fn start_slow_put(
    base: &str,
    token: &str,
    key: &str,
    first_chunk_seed: u64,
) -> (
    mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    JoinHandle<reqwest::Result<reqwest::Response>>,
) {
    let unique_seed = first_chunk_seed
        ^ std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
    let (tx, body) = slow_body();
    let client = reqwest::Client::new();
    let url = format!("{base}/{BUCKET}/{key}");
    let token = token.to_owned();
    let task = tokio::spawn(async move {
        client
            .put(url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(
        512 * 1024,
        unique_seed,
    ))))
    .await
    .unwrap();
    (tx, task)
}

/// A slow PUT whose body was completed while a backend was down must fail
/// cleanly (client error or HTTP >= 400) — NEVER a 2xx.
///
/// The resolution budget is generous: the server's PG failure path can take
/// ~30s (sqlx pool acquire/reconnect defaults) on top of `docker stop`'s 10s
/// SIGTERM grace before SIGKILL, so 90s leaves ~2x margin over the measured
/// upper bound. The elapsed time is logged as evidence.
async fn assert_in_flight_fails_cleanly(
    put_task: JoinHandle<reqwest::Result<reqwest::Response>>,
    what: &str,
) {
    let started = tokio::time::Instant::now();
    match tokio::time::timeout(Duration::from_secs(90), put_task).await {
        Err(elapsed) => {
            panic!("in-flight PUT did not resolve within 90s after {what} ({elapsed:?})")
        }
        Ok(Err(join_err)) => panic!("in-flight PUT task panicked: {join_err}"),
        Ok(Ok(Err(_e))) => eprintln!(
            "chaos: in-flight PUT failed client-side after {what} in {:.1}s (clean)",
            started.elapsed().as_secs_f64()
        ),
        Ok(Ok(Ok(resp))) => {
            let status = resp.status().as_u16();
            assert!(
                status >= 400,
                "in-flight PUT must fail (never 2xx) after {what}, got {status}"
            );
            eprintln!(
                "chaos: in-flight PUT rejected with {status} after {what} in {:.1}s (clean)",
                started.elapsed().as_secs_f64()
            );
        }
    }
}

/// A read that must fail (client error or HTTP >= 400, never 2xx) while a
/// backend is down/partitioned.
async fn assert_read_fails(
    request: impl std::future::Future<Output = reqwest::Result<reqwest::Response>>,
    what: &str,
) {
    match tokio::time::timeout(Duration::from_secs(30), request).await {
        Err(_elapsed) => eprintln!("chaos: GET timed out while {what} (expected)"),
        Ok(Ok(resp)) => {
            let status = resp.status().as_u16();
            assert!(
                status >= 400,
                "read must fail (never 2xx) while {what}, got {status}"
            );
            eprintln!("chaos: GET rejected with {status} while {what} (expected)");
        }
        Ok(Err(_e)) => eprintln!("chaos: GET errored while {what} (expected)"),
    }
}

/// Raw S3 GET (no client-side unwrap) for fault-path assertions.
fn s3_get_raw(
    base: &str,
    token: &str,
    key: &str,
) -> impl std::future::Future<Output = reqwest::Result<reqwest::Response>> {
    let url = format!("{base}/{BUCKET}/{key}");
    let auth = format!("Bearer {token}");
    async move {
        reqwest::Client::new()
            .get(url)
            .header("Authorization", auth)
            .send()
            .await
    }
}

// ---------------------------------------------------------------------------
// Durable resumable LFS PATCH helpers.
//
// The resumable LFS PATCH path (durable resumable sessions in Postgres + staged
// bytes in the object store) is the storage machinery introduced by the
// hardening/stability release. These helpers mirror `start_slow_put` /
// `assert_in_flight_fails_cleanly` but drive the LFS PATCH surface so the chaos
// drills can fault-inject a resumable upload the same way they fault-inject a
// plain S3 PUT.
// ---------------------------------------------------------------------------

/// A complete (non-streaming) LFS PATCH for the byte range [start, end_inclusive]
/// of `total`, used to finish / repair a durable resumable session.
async fn lfs_patch_range(
    base: &str,
    token: &str,
    oid: &str,
    start: u64,
    end_inclusive: u64,
    total: u64,
    bytes: &[u8],
) -> reqwest::Response {
    let range = format!("bytes {start}-{end_inclusive}/{total}");
    reqwest::Client::new()
        .patch(format!("{base}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .header("Content-Range", range)
        .body(bytes.to_vec())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("lfs patch range")
}

/// A per-run nonce so repeated drill executions against a *persistent* chaos
/// Postgres volume produce distinct object OIDs (and therefore distinct durable
/// resumable session IDs). Without this, a second run collides with the
/// terminal session left by the first and the LFS PATCH returns 409.
fn lfs_run_nonce() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// GETs the fully assembled LFS object and asserts byte-exact equality.
async fn assert_lfs_object_byte_exact(base: &str, token: &str, oid: &str, expected: &[u8]) {
    let resp = reqwest::Client::new()
        .get(format!("{base}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("lfs object get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "lfs object {oid} must be readable after durable resumable completion"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        body.as_ref(),
        expected,
        "lfs object {oid} must be byte-exact after resumable completion"
    );
}

// ===========================================================================
// DRILL A — POSTGRES KILLED MID-UPLOAD: no lost commits.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_a_postgres_kill_mid_upload_no_lost_commits() {
    let drill = "drill_deploy_a_postgres_kill_mid_upload_no_lost_commits";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    // Seed two acked objects (durable commits before the outage).
    let k1 = "a-k1";
    let v1 = deterministic_bytes(64 * 1024 + 17, 101);
    let put = s3_put(&base, &token, k1, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed {k1}");
    let k2a = "a-k2a";
    let v2a = deterministic_bytes(32 * 1024, 102);
    let put = s3_put(&base, &token, k2a, v2a.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed {k2a}");

    // Slow PUT of k2; wait for a chunk object to land in the object store
    // (server provably inside the upload transaction).
    let store = s3_store(&stack.s3_endpoint);
    let baseline = s3_object_count(&store);
    let (tx, put_task) = start_slow_put(&base, &token, "a-k2", 103).await;
    wait_s3_object_count_grows(&store, baseline, &put_task).await;
    eprintln!(
        "chaos({drill}): chunk evidence at object count {} (baseline {baseline})",
        s3_object_count(&store)
    );

    // Kill the metadata backend mid-upload.
    guard.stop(CONTAINER_POSTGRES).await;

    // Complete the body: the commit to Postgres must now fail cleanly.
    drop(tx);
    assert_in_flight_fails_cleanly(put_task, "postgres kill").await;

    // The HOST server process survives the backend outage.
    assert!(server.alive(), "server must stay alive after postgres kill");
    let health = reqwest::Client::new()
        .get(format!("{base}/healthz"))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    assert!(
        matches!(health, Ok(r) if r.status().as_u16() == 200),
        "healthz must stay 200 after postgres kill"
    );

    // Restore Postgres; verify no committed data was lost.
    restart_and_wait(
        CONTAINER_POSTGRES,
        || tcp_ready("127.0.0.1", 15432),
        Duration::from_secs(60),
    )
    .await;
    migrate_chaos_postgres(&stack.pg_url).await;
    guard.recovered(CONTAINER_POSTGRES);

    for (key, expected) in [(&k1, &v1), (&k2a, &v2a)] {
        let resp = s3_get(&base, &token, key).await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "acked {key} after postgres kill"
        );
        assert_eq!(
            resp.bytes().await.unwrap().as_ref(),
            expected.as_slice(),
            "acked {key} byte-exact"
        );
    }
    eprintln!("chaos({drill}): acked objects byte-exact after postgres restart");

    let k3 = "a-k3";
    let v3 = deterministic_bytes(64 * 1024 + 3, 104);
    let put = s3_put(&base, &token, k3, v3.clone()).await;
    assert_eq!(
        put.status().as_u16(),
        200,
        "fresh PUT {k3} after postgres recovery"
    );
    eprintln!("chaos({drill}): PASS — postgres kill mid-upload: no lost commits, recovery clean");
}

// ===========================================================================
// DRILL B — MINIO KILLED MID-UPLOAD: acked data durable, recovery clean.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_b_minio_kill_mid_upload_durability() {
    let drill = "drill_deploy_b_minio_kill_mid_upload_durability";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let k1 = "b-k1";
    let v1 = deterministic_bytes(64 * 1024 + 13, 201);
    let put = s3_put(&base, &token, k1, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed {k1}");

    let store = s3_store(&stack.s3_endpoint);
    let baseline = s3_object_count(&store);
    let (tx, put_task) = start_slow_put(&base, &token, "b-k2", 202).await;
    wait_s3_object_count_grows(&store, baseline, &put_task).await;
    eprintln!(
        "chaos({drill}): chunk evidence at object count {} (baseline {baseline})",
        s3_object_count(&store)
    );

    guard.stop(CONTAINER_MINIO).await;

    drop(tx);
    assert_in_flight_fails_cleanly(put_task, "minio kill").await;
    assert!(server.alive(), "server must stay alive after minio kill");

    // Restore MinIO; wait for its health endpoint.
    let s3_endpoint = stack.s3_endpoint.clone();
    restart_and_wait(
        CONTAINER_MINIO,
        move || {
            let endpoint = s3_endpoint.clone();
            async move { minio_health_ok(&endpoint).await }
        },
        Duration::from_secs(60),
    )
    .await;
    guard.recovered(CONTAINER_MINIO);
    eprintln!("chaos({drill}): minio healthy after restart");

    let resp = s3_get(&base, &token, k1).await;
    assert_eq!(resp.status().as_u16(), 200, "acked {k1} after minio kill");
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        v1.as_slice(),
        "{k1} byte-exact"
    );

    let k3 = "b-k3";
    let v3 = deterministic_bytes(48 * 1024, 203);
    let put = s3_put(&base, &token, k3, v3.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "fresh PUT after minio recovery");
    eprintln!("chaos({drill}): PASS — minio kill mid-upload: acked data durable, recovery clean");
}

// ===========================================================================
// DRILL C — REDIS KILLED MID-READ: byte-exact fallback to the object store.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_c_redis_kill_mid_read_byte_exact_fallback() {
    let drill = "drill_deploy_c_redis_kill_mid_read_byte_exact_fallback";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    // Drill C wires the reconstruction cache to the deployed Redis.
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3,xet"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
            ("SHARDLINE_RECONSTRUCTION_CACHE_ADAPTER", "redis"),
            ("SHARDLINE_RECONSTRUCTION_CACHE_REDIS_URL", &stack.redis_url),
            ("SHARDLINE_RECONSTRUCTION_CACHE_TTL_SECONDS", "30"),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let write_token = mint_token("drill", "drill", TokenScope::Write);
    let read_token = mint_token("drill", "drill", TokenScope::Read);
    let base = server.base_url();
    let client = reqwest::Client::new();

    // Seed a plain S3 object to assert the S3 read path is unaffected by the
    // cache outage.
    let k1 = "c-k1";
    let v1 = deterministic_bytes(64 * 1024 + 7, 301);
    let put = s3_put(&base, &write_token, k1, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed s3 {k1}");

    // Xet fixtures: upload the xorb, then the shard referencing it.
    let content: &[u8] = b"chaos-redis-kill-content";
    let (xorb_bytes, xorb_hash) = shardline_server::test_fixtures::single_chunk_xorb(content);
    let (shard_bytes, file_id) =
        shardline_server::test_fixtures::single_file_shard(&[(content, xorb_hash.as_str())]);

    let xorb_url = format!("{base}/v1/xorbs/default/{xorb_hash}");
    let resp = client
        .post(&xorb_url)
        .header("Authorization", format!("Bearer {write_token}"))
        .body(xorb_bytes)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "xorb upload: {}", resp.status());

    let shards_url = format!("{base}/v1/shards");
    let resp = client
        .post(&shards_url)
        .header("Authorization", format!("Bearer {write_token}"))
        .body(shard_bytes)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "shard upload: {}",
        resp.status()
    );

    // Warm the reconstruction cache.
    let recon_url = format!("{base}/v1/reconstructions/{file_id}");
    let warm = client
        .get(&recon_url)
        .header("Authorization", format!("Bearer {read_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(warm.status().as_u16(), 200, "warm reconstruction");
    let warm_bytes = warm.bytes().await.unwrap();
    eprintln!(
        "chaos({drill}): warm reconstruction cached, {} bytes",
        warm_bytes.len()
    );

    guard.stop(CONTAINER_REDIS).await;

    // Redis is down: the reconstruction must fall through to the
    // object-store loader and still return byte-exact. If it instead errors,
    // that is a REAL finding (surfaced loudly below).
    match client
        .get(&recon_url)
        .header("Authorization", format!("Bearer {read_token}"))
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status().as_u16();
            assert_eq!(
                status, 200,
                "reconstruction must still succeed after redis kill (fallback), got {status}"
            );
            let body = resp.bytes().await.unwrap();
            assert_eq!(
                body.as_ref(),
                warm_bytes.as_ref(),
                "reconstruction byte-exact after redis kill"
            );
            eprintln!(
                "chaos({drill}): reconstruction byte-exact after redis kill (fallback works)"
            );
        }
        Err(e) => {
            eprintln!(
                "REAL FINDING: reconstruction FAILED after redis kill (expected per-op fallback): {e}"
            );
            assert!(server.alive(), "server must stay alive after redis kill");
            panic!("reconstruction errored after redis kill — see REAL FINDING above");
        }
    }
    assert!(server.alive(), "server must stay alive after redis kill");

    // The plain S3 read path is unaffected by the cache outage.
    let resp = s3_get(&base, &write_token, k1).await;
    assert_eq!(resp.status().as_u16(), 200, "s3 {k1} after redis kill");
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        v1.as_slice(),
        "s3 {k1} byte-exact"
    );

    // Restore Redis and wait for PONG.
    restart_and_wait(CONTAINER_REDIS, redis_pong, Duration::from_secs(60)).await;
    guard.recovered(CONTAINER_REDIS);
    eprintln!("chaos({drill}): PASS — redis kill mid-read: byte-exact fallback, no data loss");
}

// ===========================================================================
// DRILL D — MINIO NETWORK PARTITION + RECOVERY.
// ===========================================================================
//
// Empirical premise check: `docker network disconnect` must sever the
// host-published port on the running docker. If it does (primary premise) we
// partition MinIO and assert the S3 path fails while the host server survives;
// if the published port survives the disconnect, we FALL BACK to partitioning
// the Postgres metadata path instead and assert the object-store read still
// serves directly. The premise actually exercised is reported in the evidence
// lines below.

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_d_minio_network_partition_recovery() {
    let drill = "drill_deploy_d_minio_network_partition_recovery";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let k1 = "d-k1";
    let v1 = deterministic_bytes(64 * 1024 + 11, 401);
    let put = s3_put(&base, &token, k1, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed {k1}");
    // First chunk key of the acked object (for the direct S3 read in the
    // metadata-path fallback).
    let k1_first_chunk = {
        let h = sha256_hex(&v1[..CHUNK_SIZE]);
        ObjectKey::parse(&format!("{}/{h}", &h[..2])).expect("chunk object key")
    };

    let store = s3_store(&stack.s3_endpoint);
    let baseline = s3_object_count(&store);
    let (tx, put_task) = start_slow_put(&base, &token, "d-k2", 402).await;
    wait_s3_object_count_grows(&store, baseline, &put_task).await;
    eprintln!(
        "chaos({drill}): chunk evidence at object count {} (baseline {baseline})",
        s3_object_count(&store)
    );

    // Partition MinIO from the network.
    guard.disconnect(CONTAINER_MINIO).await;

    let premise_severed = !minio_health_ok(&stack.s3_endpoint).await;
    eprintln!("chaos({drill}): empirical — disconnect severs published port: {premise_severed}");

    if premise_severed {
        // PRIMARY PREMISE: the S3 endpoint is partitioned. The in-flight PUT
        // must fail cleanly and the host server must stay alive.
        drop(tx);
        assert_in_flight_fails_cleanly(put_task, "minio network partition").await;
        assert!(
            server.alive(),
            "server must stay alive while minio is partitioned"
        );
        assert_read_fails(s3_get_raw(&base, &token, k1), "minio network partition").await;

        // Reconnect MinIO; the published port must come back.
        let connect = docker_run(&["network", "connect", NET, CONTAINER_MINIO]).await;
        assert!(
            connect.status.success(),
            "docker network connect {CONTAINER_MINIO}"
        );
        let s3_endpoint = stack.s3_endpoint.clone();
        restart_and_wait(
            CONTAINER_MINIO,
            move || {
                let endpoint = s3_endpoint.clone();
                async move { minio_health_ok(&endpoint).await }
            },
            Duration::from_secs(60),
        )
        .await;
        guard.recovered(CONTAINER_MINIO);
        eprintln!("chaos({drill}): minio healthy after reconnect (primary premise)");
    } else {
        // FALLBACK PREMISE: the disconnect did NOT sever the published port on
        // this docker. Restore MinIO and partition the METADATA path
        // (chaos-postgres) instead; assert the metadata path fails while the
        // object-store read still serves, then assert recovery.
        guard.recovered(CONTAINER_MINIO);
        guard.disconnect(CONTAINER_POSTGRES).await;
        wait_for(
            "postgres published port severed",
            || async { !tcp_ready("127.0.0.1", 15432).await },
            Duration::from_secs(15),
        )
        .await;

        drop(tx);
        assert_in_flight_fails_cleanly(put_task, "postgres network partition").await;
        assert!(
            server.alive(),
            "server must stay alive while postgres is partitioned"
        );
        assert_read_fails(s3_get_raw(&base, &token, k1), "postgres network partition").await;

        // The object-store read still serves while metadata is partitioned.
        let range = ByteRange::new(0, u64::try_from(CHUNK_SIZE - 1).unwrap()).unwrap();
        let direct = store.read_range(&k1_first_chunk, range);
        assert!(
            direct.as_ref().is_ok_and(|bytes| bytes.len() == CHUNK_SIZE),
            "direct S3 read must serve while postgres is partitioned: {direct:?}"
        );
        eprintln!(
            "chaos({drill}): metadata path failed while direct S3 read served (fallback premise)"
        );

        let connect_pg = docker_run(&["network", "connect", NET, CONTAINER_POSTGRES]).await;
        assert!(
            connect_pg.status.success(),
            "docker network connect {CONTAINER_POSTGRES}"
        );
        restart_and_wait(
            CONTAINER_POSTGRES,
            || tcp_ready("127.0.0.1", 15432),
            Duration::from_secs(60),
        )
        .await;
        migrate_chaos_postgres(&stack.pg_url).await;
        guard.recovered(CONTAINER_POSTGRES);
    }

    // Recovery assertions common to both premises.
    let resp = s3_get(&base, &token, k1).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "acked {k1} after partition recovery"
    );
    assert_eq!(
        resp.bytes().await.unwrap().as_ref(),
        v1.as_slice(),
        "{k1} byte-exact after partition recovery"
    );
    let k3 = "d-k3";
    let v3 = deterministic_bytes(48 * 1024 + 5, 403);
    let put = s3_put(&base, &token, k3, v3.clone()).await;
    assert_eq!(
        put.status().as_u16(),
        200,
        "fresh PUT after partition recovery"
    );
    eprintln!(
        "chaos({drill}): PASS — partition recovery clean (premise: {})",
        if premise_severed {
            "published-port severance"
        } else {
            "metadata-path fallback"
        }
    );
}

// ===========================================================================
// DRILL E — KERNEL PACKET DUPLICATION/REORDERING + ONE-WAY RESPONSE LOSS.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_e_netem_duplicate_reorder_and_asymmetric_recovery() {
    let drill = "drill_deploy_e_netem_duplicate_reorder_and_asymmetric_recovery";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();
    let seed_key = "e-stable";
    let seed_bytes = deterministic_bytes(96 * 1024 + 31, 501);
    let seeded = s3_put(&base, &token, seed_key, seed_bytes.clone()).await;
    assert_eq!(seeded.status().as_u16(), 200, "seed before netem faults");

    guard
        .replace_netem(
            CONTAINER_MINIO,
            "delay 40ms 20ms 25% duplicate 10% reorder 50% 25%",
        )
        .await;
    for sequence in 0..8_u64 {
        let key = format!("e-netem-{sequence}");
        let bytes = deterministic_bytes(
            (32_usize * 1024)
                .checked_add(usize::try_from(sequence).unwrap())
                .unwrap(),
            510_u64.saturating_add(sequence),
        );
        let put = s3_put(&base, &token, &key, bytes.clone()).await;
        assert_eq!(
            put.status().as_u16(),
            200,
            "PUT through duplicate/reordered packets for {key}"
        );
        let get = s3_get(&base, &token, &key).await;
        assert_eq!(
            get.status().as_u16(),
            200,
            "GET through duplicate/reordered packets for {key}"
        );
        assert_eq!(
            get.bytes().await.unwrap().as_ref(),
            bytes.as_slice(),
            "packet faults must not alter acknowledged bytes for {key}"
        );
    }

    // Apply loss only to packets leaving MinIO's namespace. Requests still
    // travel toward the dependency, but its TCP acknowledgements/responses do
    // not return: an asymmetric response partition rather than a symmetric
    // Docker-network disconnect.
    guard.replace_netem(CONTAINER_MINIO, "loss 100%").await;
    let partitioned_read = reqwest::Client::new()
        .get(format!("{base}/{BUCKET}/{seed_key}"))
        .header("Authorization", format!("Bearer {token}"))
        .timeout(Duration::from_secs(3))
        .send()
        .await;
    assert!(
        partitioned_read
            .as_ref()
            .map_or(true, |response| !response.status().is_success()),
        "one-way MinIO response partition must never return a false successful read"
    );
    assert!(
        server.alive(),
        "server must remain alive during asymmetric dependency partition"
    );

    guard.clear_netem(CONTAINER_MINIO).await;
    let s3_endpoint = stack.s3_endpoint.clone();
    wait_for(
        "MinIO after netem removal",
        move || {
            let endpoint = s3_endpoint.clone();
            async move { minio_health_ok(&endpoint).await }
        },
        Duration::from_secs(30),
    )
    .await;

    let recovered = s3_get(&base, &token, seed_key).await;
    assert_eq!(recovered.status().as_u16(), 200, "read after netem removal");
    assert_eq!(
        recovered.bytes().await.unwrap().as_ref(),
        seed_bytes.as_slice(),
        "pre-fault acknowledged bytes must recover exactly"
    );
    let recovery_key = "e-recovered";
    let recovery_bytes = deterministic_bytes(48 * 1024 + 7, 599);
    let put = s3_put(&base, &token, recovery_key, recovery_bytes.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "write after netem removal");
    let get = s3_get(&base, &token, recovery_key).await;
    assert_eq!(get.status().as_u16(), 200, "read new post-fault write");
    assert_eq!(
        get.bytes().await.unwrap().as_ref(),
        recovery_bytes.as_slice(),
        "post-fault publication must be byte exact"
    );
    eprintln!(
        "chaos({drill}): PASS — kernel duplicate/reorder and asymmetric response loss recovered"
    );
}

// ===========================================================================
// DRILL F — REAL N-1/N MIXED-BINARY ROLLOUT AND ROLLBACK.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_f_real_mixed_version_rollout_and_rollback() {
    let drill = "drill_deploy_f_real_mixed_version_rollout_and_rollback";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(current_binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current shardline binary not found");
        return;
    };
    let Some(previous_binary) = resolve_n_minus_one_binary(drill) else {
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;

    let old_a_root = TempDir::new().unwrap();
    let old_b_root = TempDir::new().unwrap();
    let new_a_root = TempDir::new().unwrap();
    let new_b_root = TempDir::new().unwrap();
    let rollback_root = TempDir::new().unwrap();
    let environment = [("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str())];
    let mut node_a =
        DeploymentServer::spawn_at(&previous_binary, BIN_ADDR, &environment, old_a_root.path());
    let mut node_b = DeploymentServer::spawn_at(
        &previous_binary,
        BIN_ADDR_SECONDARY,
        &environment,
        old_b_root.path(),
    );
    node_a.wait_ready(Duration::from_secs(20)).await;
    node_b.wait_ready(Duration::from_secs(20)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let old_bytes = deterministic_bytes(98_323, 601);
    let old_put = s3_put(&node_a.base_url(), &token, "f-old", old_bytes.clone()).await;
    assert_eq!(old_put.status().as_u16(), 200, "N-1 seed write");
    assert_s3_bytes(&node_b.base_url(), &token, "f-old", &old_bytes, "N-1 peer").await;

    // First rollout step: an N process and an N-1 process actively share the
    // same Postgres metadata and S3 objects.
    drop(node_a);
    let mut node_a =
        DeploymentServer::spawn_at(&current_binary, BIN_ADDR, &environment, new_a_root.path());
    node_a.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &node_a.base_url(),
        &token,
        "f-old",
        &old_bytes,
        "N reads N-1 write",
    )
    .await;
    let new_bytes = deterministic_bytes(114_711, 602);
    let new_put = s3_put(&node_a.base_url(), &token, "f-new", new_bytes.clone()).await;
    assert_eq!(
        new_put.status().as_u16(),
        200,
        "N write during mixed window"
    );
    assert_s3_bytes(
        &node_b.base_url(),
        &token,
        "f-new",
        &new_bytes,
        "N-1 reads N write",
    )
    .await;

    // Finish the rollout, then roll one node back to the real N-1 binary. The
    // previous binary must read state written by N and publish a fresh object
    // that the remaining N node reconstructs exactly.
    drop(node_b);
    let mut node_b = DeploymentServer::spawn_at(
        &current_binary,
        BIN_ADDR_SECONDARY,
        &environment,
        new_b_root.path(),
    );
    node_b.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &node_b.base_url(),
        &token,
        "f-new",
        &new_bytes,
        "N peer after rollout",
    )
    .await;

    drop(node_a);
    let mut rollback_node = DeploymentServer::spawn_at(
        &previous_binary,
        BIN_ADDR,
        &environment,
        rollback_root.path(),
    );
    rollback_node.wait_ready(Duration::from_secs(20)).await;
    assert_s3_bytes(
        &rollback_node.base_url(),
        &token,
        "f-new",
        &new_bytes,
        "N-1 rollback reads N write",
    )
    .await;
    let rollback_bytes = deterministic_bytes(81_949, 603);
    let rollback_put = s3_put(
        &rollback_node.base_url(),
        &token,
        "f-rollback",
        rollback_bytes.clone(),
    )
    .await;
    assert_eq!(rollback_put.status().as_u16(), 200, "N-1 rollback write");
    assert_s3_bytes(
        &node_b.base_url(),
        &token,
        "f-rollback",
        &rollback_bytes,
        "N reads N-1 rollback write",
    )
    .await;
    eprintln!(
        "chaos({drill}): PASS — real N-1/N rollout and one-node rollback preserved exact bytes"
    );
}

// ===========================================================================
// DRILL G — LIVE MULTI-NODE TOKEN VERIFICATION CLOCK SKEW.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_g_live_verifier_clock_skew() {
    let drill = "drill_deploy_g_live_verifier_clock_skew";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — current shardline binary not found");
        return;
    };
    let Some(faketime_library) = resolve_faketime_library(drill) else {
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;

    let fast_root = TempDir::new().unwrap();
    let slow_root = TempDir::new().unwrap();
    let faketime_library = faketime_library.to_str().unwrap();
    let fast_environment = [
        ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
        ("LD_PRELOAD", faketime_library),
        ("FAKETIME", "+120s"),
        ("FAKETIME_DONT_FAKE_MONOTONIC", "1"),
        ("FAKETIME_NO_CACHE", "1"),
    ];
    let slow_environment = [
        ("SHARDLINE_S3_ENDPOINT", stack.s3_endpoint.as_str()),
        ("LD_PRELOAD", faketime_library),
        ("FAKETIME", "-120s"),
        ("FAKETIME_DONT_FAKE_MONOTONIC", "1"),
        ("FAKETIME_NO_CACHE", "1"),
    ];
    let mut fast_node =
        DeploymentServer::spawn_at(&binary, BIN_ADDR, &fast_environment, fast_root.path());
    let mut slow_node = DeploymentServer::spawn_at(
        &binary,
        BIN_ADDR_SECONDARY,
        &slow_environment,
        slow_root.path(),
    );
    fast_node.wait_ready(Duration::from_secs(20)).await;
    slow_node.wait_ready(Duration::from_secs(20)).await;

    let host_now = shardline_protocol::unix_now_seconds_lossy();
    let boundary_token = mint_token_expiring_at(
        "drill",
        "drill",
        TokenScope::Write,
        host_now.saturating_add(60),
    );
    let boundary_bytes = deterministic_bytes(65_573, 701);
    let rejected = s3_put(
        &fast_node.base_url(),
        &boundary_token,
        "g-boundary",
        boundary_bytes.clone(),
    )
    .await;
    assert_eq!(
        rejected.status().as_u16(),
        403,
        "fast verifier must reject a token beyond its local expiry"
    );
    let absent = s3_get(
        &slow_node.base_url(),
        &mint_token("drill", "drill", TokenScope::Write),
        "g-boundary",
    )
    .await;
    assert_eq!(
        absent.status().as_u16(),
        404,
        "rejected fast-node write must have no shared side effect"
    );
    let accepted = s3_put(
        &slow_node.base_url(),
        &boundary_token,
        "g-boundary",
        boundary_bytes.clone(),
    )
    .await;
    assert_eq!(
        accepted.status().as_u16(),
        200,
        "slow verifier must accept the same token before its local expiry"
    );

    let cluster_valid_token = mint_token_expiring_at(
        "drill",
        "drill",
        TokenScope::Write,
        host_now.saturating_add(600),
    );
    assert_s3_bytes(
        &fast_node.base_url(),
        &cluster_valid_token,
        "g-boundary",
        &boundary_bytes,
        "fast peer after slow-node publication",
    )
    .await;
    let cluster_expired_token = mint_token_expiring_at(
        "drill",
        "drill",
        TokenScope::Write,
        host_now.saturating_sub(180),
    );
    for base_url in [fast_node.base_url(), slow_node.base_url()] {
        let response = s3_get(&base_url, &cluster_expired_token, "g-boundary").await;
        assert_eq!(
            response.status().as_u16(),
            403,
            "a token older than the tested skew bound must be rejected on every node"
        );
    }
    assert!(fast_node.alive(), "fast verifier node must stay alive");
    assert!(slow_node.alive(), "slow verifier node must stay alive");
    eprintln!(
        "chaos({drill}): PASS — independent ±120s verifier clocks enforced exact expiry decisions without partial writes"
    );
}

// ===========================================================================
// OPTIONAL H — CHEAP WORKLOAD BURST (no fault injection).
// Gated on SHARDLINE_CHAOS_DEPLOYMENT=1.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn chaos_workload_against_deployment() {
    let drill = "chaos_workload_against_deployment";
    if std::env::var("SHARDLINE_CHAOS_DEPLOYMENT").as_deref() != Ok("1") {
        eprintln!("SKIP: {drill} — set SHARDLINE_CHAOS_DEPLOYMENT=1 to run the workload burst");
        return;
    }
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} — shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();
    for i in 0..5_u64 {
        let key = format!("f-{i}");
        let payload = deterministic_bytes(16 * 1024, 500_u64.saturating_add(i));
        let put = s3_put(&base, &token, &key, payload.clone()).await;
        assert_eq!(put.status().as_u16(), 200, "burst put {key}");
        let get = s3_get(&base, &token, &key).await;
        assert_eq!(get.status().as_u16(), 200, "burst get {key}");
        assert_eq!(
            get.bytes().await.unwrap().as_ref(),
            payload.as_slice(),
            "burst get {key} byte-exact"
        );
        let del = s3_delete(&base, &token, &key).await;
        assert!(del.status().is_success(), "burst delete {key}");
        let gone = s3_get(&base, &token, &key).await;
        assert_eq!(gone.status().as_u16(), 404, "burst delete {key} removes it");
    }
    eprintln!("chaos({drill}): PASS — 5-key put/get/delete burst against deployment");
}

// ===========================================================================
// POSTGRES KILL MID-LFS-PATCH: durable resumable session survives.
//
// The LFS PATCH path introduced by the hardening/stability release persists its
// resumable session (range/part map, generation ownership, completion fence) in
// Postgres and stages immutable bytes in the object store. This drill opens a slow
// streaming PATCH (which creates the durable session + stages the first chunk),
// then kills the *metadata* backend (Postgres) mid-body. The in-flight PATCH must
// fail cleanly, the host server must stay up, and the durable session must
// survive the outage so the object can be completed on a later PATCH and read
// back byte-exact.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_postgres_kill_mid_lfs_patch() {
    let drill = "drill_deploy_postgres_kill_mid_lfs_patch";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "lfs"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let full_content = deterministic_bytes(1024, 777 ^ lfs_run_nonce());
    let oid = sha256_hex(&full_content);
    let total = full_content.len() as u64;

    // Open the durable resumable session and commit the first chunk. This writes
    // the session (range/part map, generation, completion fence) to Postgres and
    // stages the chunk in the object store.
    let first_half = &full_content[0..512];
    let p1 = lfs_patch_range(&base, &token, &oid, 0, 511, total, first_half).await;
    assert_eq!(
        p1.status().as_u16(),
        200,
        "first PATCH opens durable session"
    );

    // Kill the metadata backend. The durable session row in Postgres must
    // survive the outage.
    guard.stop(CONTAINER_POSTGRES).await;

    // A completion PATCH during the outage must fail cleanly (never 2xx): the
    // server cannot reach the durable session store.
    let rest = &full_content[512..];
    let in_flight = lfs_patch_range(&base, &token, &oid, 512, total - 1, total, rest).await;
    assert!(
        in_flight.status().as_u16() >= 400,
        "completion PATCH must fail (never 2xx) while postgres is down"
    );

    // The HOST server process survives the metadata outage.
    assert!(server.alive(), "server must stay alive after postgres kill");
    let health = reqwest::Client::new()
        .get(format!("{base}/healthz"))
        .timeout(Duration::from_secs(2))
        .send()
        .await;
    assert!(
        matches!(health, Ok(r) if r.status().as_u16() == 200),
        "healthz must stay 200 after postgres kill"
    );

    // Restore Postgres. The durable resumable session must still exist so the
    // object can be completed.
    restart_and_wait(
        CONTAINER_POSTGRES,
        || tcp_ready("127.0.0.1", 15432),
        Duration::from_secs(60),
    )
    .await;
    migrate_chaos_postgres(&stack.pg_url).await;
    guard.recovered(CONTAINER_POSTGRES);

    // Complete the object with the remainder of the content (overlapping repair
    // is permitted by the durable session's bounded accounting).
    let repair = lfs_patch_range(&base, &token, &oid, 512, total - 1, total, rest).await;
    assert_eq!(
        repair.status().as_u16(),
        200,
        "resume PATCH after postgres recovery must succeed"
    );

    assert_lfs_object_byte_exact(&base, &token, &oid, &full_content).await;
    eprintln!(
        "chaos({drill}): PASS - durable resumable session survived postgres kill; object byte-exact after resume"
    );
}

// ===========================================================================
// MINIO KILL MID-LFS-PATCH STAGING: object-store outage recoverable.
// The durable resumable path stages immutable bytes in the object store and
// records them in the Postgres part map. Killing the object store after the
// durable session is opened must leave the session metadata (Postgres) intact
// and drop only the in-flight staging; the object can be completed after the
// store returns.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_minio_kill_mid_lfs_patch_staging() {
    let drill = "drill_deploy_minio_kill_mid_lfs_patch_staging";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "lfs"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let full_content = deterministic_bytes(1024, 909 ^ lfs_run_nonce());
    let oid = sha256_hex(&full_content);
    let total = full_content.len() as u64;

    // Open the durable resumable session and stage the first chunk.
    let first_half = &full_content[0..512];
    let p1 = lfs_patch_range(&base, &token, &oid, 0, 511, total, first_half).await;
    assert_eq!(
        p1.status().as_u16(),
        200,
        "first PATCH opens durable session"
    );

    // Kill the object store. The durable session metadata in Postgres survives;
    // the in-flight staging attempt must be the only thing dropped.
    guard.stop(CONTAINER_MINIO).await;

    let rest = &full_content[512..];
    let in_flight = lfs_patch_range(&base, &token, &oid, 512, total - 1, total, rest).await;
    assert!(
        in_flight.status().as_u16() >= 400,
        "completion PATCH must fail (never 2xx) while minio is down"
    );
    assert!(server.alive(), "server must stay alive after minio kill");

    restart_and_wait(
        CONTAINER_MINIO,
        || minio_health_ok(&stack.s3_endpoint),
        Duration::from_secs(60),
    )
    .await;
    guard.recovered(CONTAINER_MINIO);

    // Re-stage + complete the object after the store returns.
    let repair = lfs_patch_range(&base, &token, &oid, 512, total - 1, total, rest).await;
    assert_eq!(
        repair.status().as_u16(),
        200,
        "resume PATCH after minio recovery must succeed"
    );

    assert_lfs_object_byte_exact(&base, &token, &oid, &full_content).await;
    eprintln!(
        "chaos({drill}): PASS - durable resumable staging recovered after minio kill; object byte-exact"
    );
}

// ===========================================================================
// NETWORK PARTITION DURING OVERLAPPING LFS PATCH REPAIR.
// The durable resumable session permits bounded *overlapping* ranges so a later
// PATCH can repair bytes written by an earlier one (the fix in the stability
// release). This drill partitions the server from Postgres mid-repair and
// verifies the already-staged bytes and the durable session reconcile to a
// consistent, byte-exact object with no corruption.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_partition_during_lfs_patch_repair() {
    let drill = "drill_deploy_partition_during_lfs_patch_repair";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "lfs"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let full_content = deterministic_bytes(1024, 313 ^ lfs_run_nonce());
    let oid = sha256_hex(&full_content);
    let total = full_content.len() as u64;

    // Stage the first half normally (committed to the durable session + store).
    let first_half = &full_content[0..512];
    let p1 = lfs_patch_range(&base, &token, &oid, 0, 511, total, first_half).await;
    assert_eq!(p1.status().as_u16(), 200, "first-half PATCH must commit");

    // Partition Postgres from the server with a total packet loss on its network
    // namespace (mirrors the drill_e asymmetric-partition pattern), then issue an
    // overlapping repair PATCH that re-writes an already-staged region (the
    // overlapping-repair feature).
    guard.replace_netem(CONTAINER_POSTGRES, "loss 100%").await;

    let repair_region = &full_content[256..768];
    let repair = lfs_patch_range(&base, &token, &oid, 256, 767, total, repair_region).await;
    assert!(
        repair.status().as_u16() >= 400,
        "overlapping repair PATCH must fail (never 2xx) while partitioned from metadata"
    );
    assert!(server.alive(), "server must stay alive while partitioned");

    // Heal the partition and complete the object with the remaining bytes.
    guard.clear_netem(CONTAINER_POSTGRES).await;
    guard.recovered(CONTAINER_POSTGRES);

    let second_half = &full_content[512..];
    let p2 = lfs_patch_range(&base, &token, &oid, 512, total - 1, total, second_half).await;
    assert_eq!(
        p2.status().as_u16(),
        200,
        "completion PATCH after heal must succeed"
    );

    assert_lfs_object_byte_exact(&base, &token, &oid, &full_content).await;
    eprintln!(
        "chaos({drill}): PASS - overlapping repair during partition dropped cleanly; object byte-exact after heal"
    );
}

// ---------------------------------------------------------------------------
// OCI blob-upload + S3 multipart resumable helpers.
//
// These mirror the LFS PATCH helpers but drive the other two durable resumable
// protocols (OCI blob uploads and S3 multipart) so they can be fault-injected
// the same way. Both persist their resumable session in Postgres + stage bytes
// in the object store via the same ResumableSession store as LFS PATCH.
// ---------------------------------------------------------------------------

const OCI_REPO: &str = "v2/drill/drill/blobs";

/// POSTs an OCI blob-upload session and returns the session id extracted from the
/// `Location` header (`.../blobs/uploads/{id}`), or `None` if the init fails.
async fn oci_init_upload(base: &str, token: &str) -> Option<String> {
    let resp = reqwest::Client::new()
        .post(format!("{base}/{OCI_REPO}/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("oci init upload");
    let status = resp.status().as_u16();
    if status != 202 {
        eprintln!(
            "oci init upload returned {status}: {}",
            resp.text().await.unwrap_or_default()
        );
        return None;
    }
    let location = resp
        .headers()
        .get(reqwest::header::LOCATION)
        .expect("location header")
        .to_str()
        .unwrap()
        .to_owned();
    Some(location.rsplit('/').next().unwrap_or_default().to_owned())
}

/// PATCHes bytes into an OCI blob-upload session.
async fn oci_patch_upload(base: &str, token: &str, session_id: &str, bytes: &[u8]) -> u16 {
    reqwest::Client::new()
        .patch(format!("{base}/{OCI_REPO}/uploads/{session_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(bytes.to_vec())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("oci patch upload")
        .status()
        .as_u16()
}

/// Finalizes an OCI blob-upload session with the content digest.
async fn oci_finalize_upload(base: &str, token: &str, session_id: &str, digest: &str) -> u16 {
    reqwest::Client::new()
        .put(format!(
            "{base}/{OCI_REPO}/uploads/{session_id}?digest=sha256:{digest}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("oci finalize upload")
        .status()
        .as_u16()
}

/// GETs a finalized OCI blob and asserts byte-exact equality.
async fn assert_oci_blob_byte_exact(base: &str, token: &str, digest: &str, expected: &[u8]) {
    let resp = reqwest::Client::new()
        .get(format!("{base}/{OCI_REPO}/sha256:{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("oci blob get");
    assert_eq!(
        resp.status().as_u16(),
        200,
        "oci blob must be readable after durable resumable completion"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        body.as_ref(),
        expected,
        "oci blob must be byte-exact after resumable completion"
    );
}

/// POSTs `?uploads`, returning the UploadId parsed from the XML response.
async fn s3_create_multipart_deployment(base: &str, token: &str, key: &str) -> String {
    let resp = reqwest::Client::new()
        .post(format!("{base}/{BUCKET}/{key}?uploads"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("s3 create multipart")
        .text()
        .await
        .unwrap();
    extract_xml_tag(&resp, "UploadId").expect("UploadId in create-multipart response")
}

/// PUTs one multipart part.
async fn s3_upload_part_deployment(
    base: &str,
    token: &str,
    key: &str,
    upload_id: &str,
    part_number: u32,
    bytes: &[u8],
) -> u16 {
    reqwest::Client::new()
        .put(format!(
            "{base}/{BUCKET}/{key}?partNumber={part_number}&uploadId={upload_id}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(bytes.to_vec())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("s3 upload part")
        .status()
        .as_u16()
}

/// POSTs `?uploadId` to complete a multipart upload.
async fn s3_complete_multipart_deployment(
    base: &str,
    token: &str,
    key: &str,
    upload_id: &str,
    part_numbers: &[u32],
) -> u16 {
    let mut body = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for part in part_numbers {
        use std::fmt::Write;
        let _ = writeln!(
            body,
            "  <Part><PartNumber>{part}</PartNumber><ETag>\"{upload_id}-{part}\"</ETag></Part>"
        );
    }
    body.push_str("</CompleteMultipartUpload>\n");
    let resp = reqwest::Client::new()
        .post(format!("{base}/{BUCKET}/{key}?uploadId={upload_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/xml")
        .body(body)
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("s3 complete multipart");
    resp.status().as_u16()
}

// ===========================================================================
// POSTGRES KILL MID-OCI BLOB-UPLOAD: durable resumable session
// survives. Mirrors drill H but drives the OCI blob-upload resumable protocol:
// open the session + PATCH bytes, kill Postgres, assert the finalizing PUT fails
// cleanly, restore, finalize, and verify the blob is byte-exact.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_postgres_kill_mid_oci_blob_upload() {
    let drill = "drill_deploy_postgres_kill_mid_oci_blob_upload";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "oci"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();

    let full_content = deterministic_bytes(1024, 1515 ^ lfs_run_nonce());
    let digest = sha256_hex(&full_content);
    let Some(session_id) = oci_init_upload(&base, &token).await else {
        eprintln!(
            "chaos({drill}): SKIPPED — OCI blob-upload init failed (index adapter not configured for this deployment profile)"
        );
        return;
    };
    let first_half = &full_content[0..512];
    let p1 = oci_patch_upload(&base, &token, &session_id, first_half).await;
    assert_eq!(p1, 202, "oci PATCH opens durable resumable session");

    guard.stop(CONTAINER_POSTGRES).await;

    let rest = &full_content[512..];
    let in_flight = oci_patch_upload(&base, &token, &session_id, rest).await;
    assert!(
        in_flight >= 400,
        "oci PATCH during postgres outage must fail (never 2xx)"
    );
    assert!(server.alive(), "server must stay alive after postgres kill");

    restart_and_wait(
        CONTAINER_POSTGRES,
        || tcp_ready("127.0.0.1", 15432),
        Duration::from_secs(60),
    )
    .await;
    migrate_chaos_postgres(&stack.pg_url).await;
    guard.recovered(CONTAINER_POSTGRES);

    // Re-stage the remaining bytes after Postgres recovery.
    let retry = oci_patch_upload(&base, &token, &session_id, rest).await;
    assert_eq!(retry, 202, "oci PATCH after postgres recovery must succeed");

    let finalize = oci_finalize_upload(&base, &token, &session_id, &digest).await;
    assert_eq!(
        finalize, 201,
        "oci finalize after postgres recovery must succeed"
    );
    assert_oci_blob_byte_exact(&base, &token, &digest, &full_content).await;
    eprintln!(
        "chaos({drill}): PASS - durable resumable OCI blob upload survived postgres kill; blob byte-exact after recovery"
    );
}

// ===========================================================================
// POSTGRES KILL MID-S3 MULTIPART: durable resumable session survives.
// Mirrors drill H/I but drives the S3 multipart resumable protocol: create the
// upload + PUT part 1, kill Postgres, assert the completion fails cleanly,
// restore, complete, and verify the object is byte-exact.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn drill_deploy_postgres_kill_mid_s3_multipart() {
    let drill = "drill_deploy_postgres_kill_mid_s3_multipart";
    let Some(stack) = boot_chaos_stack(drill).await else {
        return;
    };
    let Some(binary) = resolve_shardline_binary() else {
        eprintln!("SKIPPED: {drill} - shardline binary not found; run `cargo build -p shardline`");
        return;
    };
    let mut guard = ServiceRecoveryGuard::new();
    migrate_chaos_postgres(&stack.pg_url).await;

    let tmp = TempDir::new().unwrap();
    let mut server = DeploymentServer::spawn(
        &binary,
        &[
            ("SHARDLINE_SERVER_FRONTENDS", "s3"),
            ("SHARDLINE_S3_ENDPOINT", &stack.s3_endpoint),
        ],
        tmp.path(),
    );
    server.wait_ready(Duration::from_secs(10)).await;

    let token = mint_token("drill", "drill", TokenScope::Write);
    let base = server.base_url();
    let key = format!("drill-m-{}-obj", lfs_run_nonce());

    let full_content = deterministic_bytes(12 * 1024 * 1024, 2626 ^ lfs_run_nonce());
    let upload_id = s3_create_multipart_deployment(&base, &token, &key).await;
    let part1_size = 6 * 1024 * 1024;
    let first_half = &full_content[..part1_size];
    let p1 = s3_upload_part_deployment(&base, &token, &key, &upload_id, 1, first_half).await;
    assert_eq!(p1, 200, "s3 part 1 opens durable resumable session");

    guard.stop(CONTAINER_POSTGRES).await;

    let rest = &full_content[part1_size..];
    let p2 = s3_upload_part_deployment(&base, &token, &key, &upload_id, 2, rest).await;
    assert!(
        p2 >= 400,
        "s3 part upload during postgres outage must fail (never 2xx)"
    );
    assert!(server.alive(), "server must stay alive after postgres kill");

    restart_and_wait(
        CONTAINER_POSTGRES,
        || tcp_ready("127.0.0.1", 15432),
        Duration::from_secs(60),
    )
    .await;
    migrate_chaos_postgres(&stack.pg_url).await;
    guard.recovered(CONTAINER_POSTGRES);

    // Upload the missing part 2 after postgres recovery.
    let rest = &full_content[part1_size..];
    let p2_after = s3_upload_part_deployment(&base, &token, &key, &upload_id, 2, rest).await;
    assert_eq!(
        p2_after, 200,
        "s3 part 2 upload after postgres recovery must succeed"
    );

    let complete = s3_complete_multipart_deployment(&base, &token, &key, &upload_id, &[1, 2]).await;
    assert_eq!(
        complete, 200,
        "s3 complete after postgres recovery must succeed"
    );

    let get = s3_get(&base, &token, &key).await;
    assert_eq!(get.status().as_u16(), 200, "s3 multipart object readable");
    assert_eq!(
        get.bytes().await.unwrap().as_ref(),
        full_content.as_slice(),
        "s3 multipart object byte-exact after recovery"
    );
    eprintln!(
        "chaos({drill}): PASS - durable resumable S3 multipart survived postgres kill; object byte-exact after recovery"
    );
}
