//! Multi-node chaos drills (v1.6.0): prove the split `api` + `transfer` roles
//! keep serving independently when the sibling role is killed mid-traffic,
//! recover after a proxy-level partition between them, and survive a
//! single-role rolling upgrade on the original port.
//!
//! This file is a SEPARATE integration-test crate. Harness helpers are
//! duplicated verbatim from the e2e reference files (each integration test
//! builds its own crate; the repo convention is per-crate verbatim helper
//! duplication):
//!   - `RoleRuntime`, `bind_with_retry`, `wait_for_health`, `wait_for_ready`,
//!     `wait_for_server_down`, `ready_response`, `role_surface_statuses`,
//!     `expected_surface` from `e2e/rolling_upgrade_e2e.rs`;
//!   - the split proxy (`SplitProxyState`, `handle_split_proxy`,
//!     `select_upstream_base_url`, `is_transfer_path`) and the XET upload
//!     machinery (`authenticated_translator`, `upload_bytes`) from
//!     `e2e/role_split_e2e.rs`;
//!   - `SplitMix64`, `deterministic_bytes`, `sha256_hex`, `count_chunk_files`,
//!     `wait_until` from the drill crates, plus the `PanicStatus` inspector.
//!
//! Two documented adaptations vs the original design (both verified against
//! the current source):
//!   1. The API role mounts `ServerFrontend::S3` in addition to `Xet`. The Xet
//!      frontend alone does NOT register the S3 object routes (the S3 router is
//!      only mounted as the app-level fallback when `ServerFrontend::S3` is in
//!      the frontend list), and the mid-traffic loop is defined as S3 PUTs/GETs
//!      against the API role. The transfer role stays Xet-only (S3 routes are
//!      gated to `role.serves_api()` anyway).
//!   2. The `/v1/reconstructions/{file_id}` probe cannot use S3-created
//!      records: that route applies `validate_hash_path` (exactly 64 lowercase
//!      hex chars) while S3 records are stored under
//!      `protocol-object-{sha256hex}` file_ids. Every drill therefore seeds ONE
//!      native XET upload (POST /v1/xorbs + POST /v1/shards through the proxy),
//!      whose record carries a valid 64-hex `file_id` and references the SAME
//!      stored xorb, and probes reconstruction 200 on THAT id. The same
//!      record's xorb hash drives the transfer-surface range reads.
//!
//! Invariants asserted by every drill:
//!   - the steady role stays ready with its full role surface while the other
//!     role is down / partitioned;
//!   - every ACKed object (ledger) is byte-exact (sha256) after recovery;
//!   - `wait_for_server_down` then same-port re-bind (`bind_with_retry`) then
//!     ready;
//!   - no role task panicked or exited early.

#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or,
    dead_code
)]

use std::{
    collections::HashMap,
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, Response, StatusCode, Uri},
    routing::any,
    serve as serve_http,
};
use reqwest::Client;
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{
    ReadyResponse, ServerConfig, ServerError, ServerFrontend, ServerRole, serve_with_listener,
};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{sleep, timeout},
};
use xet_client::cas_client::auth::AuthConfig;
use xet_data::processing::{
    FileUploadSession, Sha256Policy, XetFileInfo, configurations::TranslatorConfig,
};

const SIGNING_KEY: &[u8] = b"test-signing-key-32-bytes-long!!";
const BUCKET: &str = "mn.mn";
const CHUNK: usize = 65536;
/// The API role must also mount S3: the Xet frontend alone does not register
/// the S3 object routes (they are the app-level fallback, gated on the S3
/// frontend being configured), and the mid-traffic loop is S3 PUTs/GETs.
const API_FRONTENDS: [ServerFrontend; 2] = [ServerFrontend::Xet, ServerFrontend::S3];
const TRANSFER_FRONTENDS: [ServerFrontend; 1] = [ServerFrontend::Xet];
const MID_TRAFFIC_KEYS: [&str; 6] = ["mn-k0", "mn-k1", "mn-k2", "mn-k3", "mn-k4", "mn-k5"];

fn role_frontends(role: ServerRole) -> &'static [ServerFrontend] {
    match role {
        ServerRole::Api => &API_FRONTENDS,
        ServerRole::Transfer => &TRANSFER_FRONTENDS,
        ServerRole::All => panic!("multinode drills only exercise split roles"),
    }
}

const fn ephemeral_addr() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

// ---------------------------------------------------------------------------
// Auth / tokens — same signing key as the server, identical across restarts.
// ---------------------------------------------------------------------------

fn mint_token(owner: &str, name: &str, scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "multinode-chaos", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Small deterministic helpers (copied verbatim from the drill crates).
// ---------------------------------------------------------------------------

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// Deterministic pseudo-random payload (cheap, no RNG dependency).
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

/// SplitMix64 — deterministic RNG (spec-verbatim; kept for the seeded-drill
/// flavor; the kill point itself is FIXED at iteration 3, no RNG).
struct SplitMix64(u64);

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    const fn next_usize(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize).rem_euclid(bound)
    }

    const fn next_range(&mut self, lo: usize, hi: usize) -> usize {
        lo.saturating_add(self.next_usize(hi.saturating_sub(lo).saturating_add(1)))
    }

    const fn pick<'item, T>(&mut self, items: &'item [T]) -> &'item T {
        &items[self.next_usize(items.len())]
    }
}

fn count_files_recursive(dir: &Path) -> usize {
    let mut count: usize = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count = count.saturating_add(count_files_recursive(&path));
            } else {
                count = count.saturating_add(1);
            }
        }
    }
    count
}

fn count_chunk_files(root: &Path) -> usize {
    count_files_recursive(&root.join("chunks"))
}

async fn wait_until(timeout: Duration, what: &str, cond: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("deadline overflow");
    loop {
        if cond() {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

/// Poll an async condition until it returns true or the deadline passes.
async fn poll_until_ok<F, Fut>(timeout: Duration, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("deadline overflow");
    loop {
        if cond().await {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {what}");
        }
        sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// AckedLedger — records ONLY observed HTTP ACKs (2xx PUTs).
// ---------------------------------------------------------------------------

#[derive(Default)]
struct AckedLedger {
    inner: Mutex<HashMap<String, String>>,
}

impl AckedLedger {
    fn record(&self, key: &str, sha256: &str) {
        self.inner
            .lock()
            .unwrap()
            .insert(key.to_owned(), sha256.to_owned());
    }

    fn snapshot(&self) -> Vec<(String, String)> {
        let mut entries: Vec<(String, String)> = self
            .inner
            .lock()
            .unwrap()
            .iter()
            .map(|(key, sha)| (key.clone(), sha.clone()))
            .collect();
        entries.sort();
        entries
    }
}

// ---------------------------------------------------------------------------
// Role task status (from chaos_runner).
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum PanicStatus {
    Running,
    AbortedExpected,
    Panicked(String),
    EarlyExit,
}

/// Inspect a role's serve task: Panicked => panicked (FAIL), EarlyExit =>
/// exited cleanly on its own (FAIL), AbortedExpected => our abort, Running =>
/// healthy (or no live handle).
async fn panic_status(handle: &mut Option<JoinHandle<Result<(), ServerError>>>) -> PanicStatus {
    let Some(handle_ref) = handle.as_mut() else {
        return PanicStatus::Running;
    };
    if !handle_ref.is_finished() {
        return PanicStatus::Running;
    }
    let finished = handle.take().expect("handle present");
    match finished.await {
        Err(e) if e.is_cancelled() => PanicStatus::AbortedExpected,
        Err(e) if e.is_panic() => PanicStatus::Panicked(format!("{e}")),
        Ok(Ok(())) => PanicStatus::EarlyExit,
        Ok(Err(error)) => PanicStatus::Panicked(format!("server exited with error: {error}")),
        Err(_) => PanicStatus::EarlyExit,
    }
}

// ---------------------------------------------------------------------------
// RoleRuntime — a running role-split server (from rolling_upgrade_e2e).
// ---------------------------------------------------------------------------

struct RoleRuntime {
    addr: SocketAddr,
    base_url: String,
    server: Option<JoinHandle<Result<(), ServerError>>>,
}

impl RoleRuntime {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Stops the server and waits for its task to fully unwind. The listener is
    /// dropped before this returns, so the same port can be rebound immediately.
    async fn stop(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(server) = self.server.take() else {
            return Ok(());
        };
        server.abort();
        match server.await {
            Ok(result) => {
                result.map_err(|error| format!("server task failed during stop: {error}").into())
            }
            Err(join_error) if join_error.is_cancelled() => Ok(()),
            Err(join_error) => Err(format!("server task join failed: {join_error}").into()),
        }
    }
}

impl Drop for RoleRuntime {
    fn drop(&mut self) {
        if let Some(server) = &self.server {
            server.abort();
        }
    }
}

async fn bind_with_retry(addr: SocketAddr, attempts: u32) -> Result<TcpListener, Box<dyn Error>> {
    for attempt in 0..attempts {
        match TcpListener::bind(addr).await {
            Ok(listener) => return Ok(listener),
            Err(_error) if attempt.saturating_add(1) < attempts => {
                sleep(Duration::from_millis(
                    50_u64.saturating_mul(u64::from(attempt.saturating_add(1))),
                ))
                .await;
            }
            Err(error) => {
                return Err(
                    format!("failed to bind {addr} after {attempts} attempts: {error}").into(),
                );
            }
        }
    }
    panic!("bind_with_retry loop must return")
}

/// Starts a role-split server. `bind_addr: None` binds an ephemeral port;
/// `Some(addr)` (re)binds a specific port with a small backoff retry loop.
/// `public_base_url` is what the server advertises in reconstruction
/// fetch-info URLs (the proxy base URL, so fetch URLs route back through the
/// split proxy).
async fn spawn_role(
    role: ServerRole,
    frontends: &[ServerFrontend],
    bind_addr: Option<SocketAddr>,
    storage: &Path,
    public_base_url: &str,
) -> Result<RoleRuntime, Box<dyn Error>> {
    let listener = bind_with_retry(bind_addr.unwrap_or_else(ephemeral_addr), 20).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        public_base_url.to_owned(),
        storage.to_path_buf(),
        NonZeroUsize::new(CHUNK).ok_or("chunk size")?,
    )
    .with_server_role(role)
    .with_token_signing_key(SIGNING_KEY.to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    let client = Client::new();
    wait_for_health(&client, &base_url).await?;
    Ok(RoleRuntime {
        addr,
        base_url,
        server: Some(server),
    })
}

async fn wait_for_health(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
    for _attempt in 0..50 {
        if let Ok(response) = client.get(format!("{base_url}/healthz")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(format!("server at {base_url} did not become healthy").into())
}

async fn wait_for_ready(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
    for _attempt in 0..100 {
        if let Ok(response) = client.get(format!("{base_url}/readyz")).send().await
            && response.status().is_success()
        {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(format!("server at {base_url} did not become ready").into())
}

/// Asserts the stopped server can no longer be reached (raw connect probe to
/// bypass reqwest connection pooling).
async fn wait_for_server_down(client: &Client, base_url: &str) -> Result<(), Box<dyn Error>> {
    for _attempt in 0..50 {
        if client
            .get(format!("{base_url}/healthz"))
            .send()
            .await
            .is_err()
        {
            return Ok(());
        }
        let raw = tokio::net::TcpStream::connect(
            base_url
                .trim_start_matches("http://")
                .parse::<SocketAddr>()
                .map_err(|error| format!("bad base url {base_url}: {error}"))?,
        )
        .await;
        if raw.is_err() {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }

    Err(format!("server at {base_url} did not go down after stop").into())
}

async fn ready_response(client: &Client, base_url: &str) -> Result<ReadyResponse, Box<dyn Error>> {
    let response = client
        .get(format!("{base_url}/readyz"))
        .send()
        .await?
        .error_for_status()?;
    Ok(response.json::<ReadyResponse>().await?)
}

/// Probes the two role-specific surfaces: returns (reconstruction-route
/// status, chunk-route status).
async fn role_surface_statuses(
    client: &Client,
    base_url: &str,
) -> Result<(StatusCode, StatusCode), Box<dyn Error>> {
    let reconstruction_status = client
        .request(
            Method::PUT,
            format!("{base_url}/v1/reconstructions/asset.bin"),
        )
        .send()
        .await?
        .status();
    let chunk_status = client
        .request(
            Method::POST,
            format!("{base_url}/v1/chunks/default-merkledb/deadbeef"),
        )
        .send()
        .await?
        .status();
    Ok((reconstruction_status, chunk_status))
}

/// Expected (reconstruction, chunk) statuses for a role. Both routes are
/// GET-only, so a mounted route rejects the probe method with 405 while an
/// unmounted route returns 404.
///
/// NOTE: the API role mounts `ServerFrontend::S3` (the mid-traffic S3 traffic
/// requires it), so the unmounted `/v1/chunks/...` probe falls through to the
/// S3 fallback router and answers 403 (auth-required) instead of 404. The
/// transfer role has no S3 frontend and keeps the plain 404.
fn expected_surface(role: ServerRole) -> (StatusCode, StatusCode) {
    match role {
        ServerRole::Api => (StatusCode::METHOD_NOT_ALLOWED, StatusCode::FORBIDDEN),
        ServerRole::Transfer => (StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED),
        ServerRole::All => panic!("multinode drills only exercise split roles"),
    }
}

// ---------------------------------------------------------------------------
// Split proxy (from role_split_e2e) with a partition gate.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SplitProxyState {
    access_token: String,
    api_base_url: String,
    client: Client,
    transfer_base_url: String,
    /// When set, transfer-path requests are black-holed with 502 (simulates a
    /// network partition between the api and transfer roles).
    partition: Arc<AtomicBool>,
}

async fn handle_split_proxy(
    State(state): State<SplitProxyState>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    let path_and_query = uri
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or_else(|| uri.path().to_owned());

    // Partition gate: black-hole transfer routes while the partition is up.
    if is_transfer_path(&path_and_query) && state.partition.load(Ordering::SeqCst) {
        return response_with_status(
            StatusCode::BAD_GATEWAY,
            "partitioned: transfer route black-holed".to_owned(),
        );
    }

    let upstream_base_url = select_upstream_base_url(&path_and_query, &state);
    let upstream_url = format!("{upstream_base_url}{path_and_query}");
    let has_authorization = headers.contains_key("authorization");
    let request = state
        .client
        .request(method, upstream_url)
        .headers(headers)
        .body(body);
    let request = if has_authorization {
        request
    } else {
        request.bearer_auth(&state.access_token)
    };
    let response = match request.send().await {
        Ok(built_response) => built_response,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("split proxy request failed: {error}"),
            );
        }
    };
    let status = response.status();
    let upstream_headers = response.headers().clone();
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            return response_with_status(
                StatusCode::BAD_GATEWAY,
                format!("split proxy body read failed: {error}"),
            );
        }
    };

    let mut response_builder = Response::builder().status(status);
    for (name, value) in &upstream_headers {
        if name.as_str().eq_ignore_ascii_case("content-length") {
            continue;
        }
        response_builder = response_builder.header(name, value);
    }

    match response_builder.body(Body::from(body_bytes)) {
        Ok(built_response) => built_response,
        Err(error) => response_with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("split proxy response build failed: {error}"),
        ),
    }
}

fn select_upstream_base_url<'state>(
    path_and_query: &str,
    state: &'state SplitProxyState,
) -> &'state str {
    if is_transfer_path(path_and_query) {
        &state.transfer_base_url
    } else {
        &state.api_base_url
    }
}

fn is_transfer_path(path_and_query: &str) -> bool {
    let path = path_and_query
        .split_once('?')
        .map_or(path_and_query, |(path, _query)| path);
    path.starts_with("/v1/chunks/")
        || path.starts_with("/v1/xorbs/")
        || path.starts_with("/transfer/xorb/")
}

fn response_with_status(status: StatusCode, message: String) -> Response<Body> {
    match Response::builder().status(status).body(Body::from(message)) {
        Ok(response) => response,
        Err(_error) => Response::new(Body::from("response build failed")),
    }
}

// ---------------------------------------------------------------------------
// Native XET upload machinery (from role_split_e2e) — used to register a
// reconstruction-addressable record (64-hex file_id) referencing the stored
// xorb, and to capture that xorb's hash for transfer-surface reads.
// ---------------------------------------------------------------------------

fn authenticated_translator(
    endpoint: &str,
    base_dir: &Path,
    token: &str,
) -> Result<Arc<TranslatorConfig>, Box<dyn Error>> {
    let mut translator = TranslatorConfig::test_server_config(endpoint, base_dir)?;
    translator.session.auth = AuthConfig::maybe_new(Some(token.to_owned()), Some(u64::MAX), None);
    if translator.session.auth.is_none() {
        return Err("failed to install xet auth config".into());
    }
    Ok(Arc::new(translator))
}

async fn upload_bytes(
    translator: Arc<TranslatorConfig>,
    name: &str,
    bytes: &[u8],
) -> Result<(XetFileInfo, xet_data::deduplication::DeduplicationMetrics), Box<dyn Error>> {
    let upload_session = FileUploadSession::new(translator).await?;
    let (_clean_id, mut cleaner) = upload_session.start_clean(
        Some(Arc::<str>::from(name)),
        Some(u64::try_from(bytes.len())?),
        Sha256Policy::Compute,
    )?;
    cleaner.add_data(bytes).await?;
    let (file_info, cleaner_metrics) = cleaner.finish().await?;
    let session_metrics = upload_session.finalize().await?;

    let mut metrics = cleaner_metrics;
    metrics.xorb_bytes_uploaded = session_metrics.xorb_bytes_uploaded;
    metrics.shard_bytes_uploaded = session_metrics.shard_bytes_uploaded;
    metrics.total_bytes_uploaded = session_metrics.total_bytes_uploaded;

    Ok((file_info, metrics))
}

// ---------------------------------------------------------------------------
// MultiNodeHarness — one shared storage root, api + transfer role runtimes,
// and the split proxy in front of them.
// ---------------------------------------------------------------------------

struct MultiNodeHarness {
    _storage: tempfile::TempDir,
    client_root: tempfile::TempDir,
    storage_root: PathBuf,
    client: Client,
    token: String,
    api: Option<RoleRuntime>,
    transfer: Option<RoleRuntime>,
    proxy_base_url: String,
    partition: Arc<AtomicBool>,
    proxy_handle: JoinHandle<()>,
}

impl Drop for MultiNodeHarness {
    fn drop(&mut self) {
        self.proxy_handle.abort();
    }
}

impl MultiNodeHarness {
    async fn new() -> Result<Self, Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let storage_root = storage.path().to_path_buf();
        let client_root = tempfile::tempdir()?;

        let proxy_listener = TcpListener::bind(ephemeral_addr()).await?;
        let proxy_addr = proxy_listener.local_addr()?;
        let proxy_base_url = format!("http://{proxy_addr}");

        // Both roles initialize the same local SQLite metadata store. Complete
        // API readiness before Transfer opens it to avoid racing first-use setup.
        let api = spawn_role(
            ServerRole::Api,
            &API_FRONTENDS,
            None,
            &storage_root,
            &proxy_base_url,
        )
        .await?;
        let client = Client::new();
        wait_for_ready(&client, api.base_url()).await?;

        let transfer = spawn_role(
            ServerRole::Transfer,
            &TRANSFER_FRONTENDS,
            None,
            &storage_root,
            &proxy_base_url,
        )
        .await?;
        wait_for_ready(&client, transfer.base_url()).await?;

        let token = mint_token("mn", "mn", TokenScope::Write);
        let partition = Arc::new(AtomicBool::new(false));
        let proxy_router = Router::new()
            .fallback(any(handle_split_proxy))
            .layer(DefaultBodyLimit::max(1024 * 1024 * 1024))
            .with_state(SplitProxyState {
                access_token: token.clone(),
                api_base_url: api.base_url.clone(),
                client: client.clone(),
                transfer_base_url: transfer.base_url.clone(),
                partition: partition.clone(),
            });
        let proxy_handle = tokio::spawn(async move {
            let _ = serve_http(proxy_listener, proxy_router).await;
        });

        let harness = Self {
            _storage: storage,
            client_root,
            storage_root,
            client,
            token,
            api: Some(api),
            transfer: Some(transfer),
            proxy_base_url,
            partition,
            proxy_handle,
        };
        harness.assert_both_ready_and_surfaces().await?;
        Ok(harness)
    }

    fn role(&self, role: ServerRole) -> &RoleRuntime {
        match role {
            ServerRole::Api => self.api.as_ref().expect("api runtime"),
            ServerRole::Transfer => self.transfer.as_ref().expect("transfer runtime"),
            ServerRole::All => panic!("no All role in multinode harness"),
        }
    }

    fn role_mut(&mut self, role: ServerRole) -> &mut RoleRuntime {
        match role {
            ServerRole::Api => self.api.as_mut().expect("api runtime"),
            ServerRole::Transfer => self.transfer.as_mut().expect("transfer runtime"),
            ServerRole::All => panic!("no All role in multinode harness"),
        }
    }

    fn put_role(&mut self, role: ServerRole, runtime: RoleRuntime) {
        match role {
            ServerRole::Api => self.api = Some(runtime),
            ServerRole::Transfer => self.transfer = Some(runtime),
            ServerRole::All => panic!("no All role in multinode harness"),
        }
    }

    async fn assert_both_ready_and_surfaces(&self) -> Result<(), Box<dyn Error>> {
        let api_ready = ready_response(&self.client, self.role(ServerRole::Api).base_url()).await?;
        assert_eq!(api_ready.server_role, ServerRole::Api.as_str());
        let transfer_ready =
            ready_response(&self.client, self.role(ServerRole::Transfer).base_url()).await?;
        assert_eq!(transfer_ready.server_role, ServerRole::Transfer.as_str());
        assert_eq!(
            role_surface_statuses(&self.client, self.role(ServerRole::Api).base_url()).await?,
            expected_surface(ServerRole::Api)
        );
        assert_eq!(
            role_surface_statuses(&self.client, self.role(ServerRole::Transfer).base_url()).await?,
            expected_surface(ServerRole::Transfer)
        );
        Ok(())
    }

    async fn panic_status(&mut self, role: ServerRole) -> PanicStatus {
        panic_status(&mut self.role_mut(role).server).await
    }

    /// S3 PUT through the proxy (routed to the api role).
    async fn s3_put(&self, key: &str, bytes: Vec<u8>) -> reqwest::Response {
        proxy_s3_put(&self.proxy_base_url, &self.client, &self.token, key, bytes)
            .await
            .unwrap_or_else(|| panic!("s3 PUT {key} failed to reach the proxy"))
    }

    /// S3 GET through the proxy (routed to the api role).
    async fn s3_get(&self, key: &str) -> reqwest::Response {
        proxy_s3_get(&self.proxy_base_url, &self.client, &self.token, key)
            .await
            .unwrap_or_else(|| panic!("s3 GET {key} failed to reach the proxy"))
    }

    /// Uploads one object through the native XET path (xorb + shard via the
    /// proxy), yielding a reconstruction-addressable 64-hex file_id.
    async fn xet_upload(&self, name: &str, bytes: Vec<u8>) -> Result<XetFileInfo, Box<dyn Error>> {
        let translator =
            authenticated_translator(&self.proxy_base_url, self.client_root.path(), &self.token)?;
        let (info, _metrics) = upload_bytes(translator, name, &bytes).await?;
        Ok(info)
    }

    /// WARMS the reconstruction plan for a 64-hex file_id: returns the
    /// referenced xorb hash and the first fetch_info URL (both serve through
    /// the transfer surface, so the partition gate governs them).
    async fn warm_reconstruction_plan(
        &self,
        file_id: &str,
    ) -> Result<(String, String), Box<dyn Error>> {
        let resp = self
            .client
            .get(format!(
                "{}/v1/reconstructions/{file_id}",
                self.proxy_base_url
            ))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "reconstruction plan for {file_id}"
        );
        let json: serde_json::Value = resp.json().await?;
        let fetch_info = json
            .get("fetch_info")
            .and_then(serde_json::Value::as_object)
            .ok_or("reconstruction response missing fetch_info")?;
        let (hash, entries) = fetch_info
            .iter()
            .next()
            .ok_or("reconstruction fetch_info empty")?;
        let url = entries
            .as_array()
            .and_then(|entries| entries.first())
            .and_then(|entry| entry.get("url"))
            .and_then(serde_json::Value::as_str)
            .ok_or("reconstruction fetch entry missing url")?
            .to_owned();
        Ok((hash.clone(), url))
    }
}

// ---------------------------------------------------------------------------
// Proxy S3 helpers + mid-traffic primitives.
// ---------------------------------------------------------------------------

/// Tolerant S3 PUT through the proxy (returns None when the connection fails,
/// e.g. after a role kill). Only 2xx responses enter the ledger.
async fn proxy_s3_put(
    proxy_base: &str,
    client: &Client,
    token: &str,
    key: &str,
    bytes: Vec<u8>,
) -> Option<reqwest::Response> {
    client
        .put(format!("{proxy_base}/{BUCKET}/{key}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(bytes)
        .send()
        .await
        .ok()
}

async fn proxy_s3_get(
    proxy_base: &str,
    client: &Client,
    token: &str,
    key: &str,
) -> Option<reqwest::Response> {
    client
        .get(format!("{proxy_base}/{BUCKET}/{key}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .ok()
}

/// One mid-traffic iteration: seeded S3 PUT (recorded in the ledger only on a
/// 2xx) followed by a GET. Failures after a kill are tolerated.
async fn mid_traffic_iteration(
    proxy_base: &str,
    client: &Client,
    token: &str,
    key: &str,
    i: usize,
    ledger: &AckedLedger,
) {
    let payload = deterministic_bytes(
        128 * 1024,
        1001_u64.saturating_add(2_u64.saturating_mul(i as u64)),
    );
    let sha = sha256_hex(&payload);
    if let Some(resp) = proxy_s3_put(proxy_base, client, token, key, payload).await
        && resp.status().is_success()
    {
        ledger.record(key, &sha);
    }
    let _ = proxy_s3_get(proxy_base, client, token, key).await;
}

/// A guaranteed-2xx S3 PUT through the proxy, recorded in the ledger.
async fn acked_put(
    proxy_base: &str,
    client: &Client,
    token: &str,
    key: &str,
    seed: u64,
    ledger: &AckedLedger,
) {
    let payload = deterministic_bytes(128 * 1024, seed);
    let sha = sha256_hex(&payload);
    let resp = proxy_s3_put(proxy_base, client, token, key, payload)
        .await
        .unwrap_or_else(|| panic!("PUT {key} must reach the proxy"));
    assert_eq!(resp.status().as_u16(), 200, "PUT {key}");
    ledger.record(key, &sha);
}

// ===========================================================================
// A1 — mid-traffic kill of one role; the steady role keeps serving.
// ===========================================================================

async fn exercise_kill_role_mid_traffic(killed: ServerRole) -> Result<(), Box<dyn Error>> {
    let mut harness = MultiNodeHarness::new().await?;
    let steady = match killed {
        ServerRole::Api => ServerRole::Transfer,
        ServerRole::Transfer => ServerRole::Api,
        ServerRole::All => panic!("split roles only"),
    };
    let ledger = AckedLedger::default();

    // Seed a reconstruction-addressable record (64-hex file_id, referencing the
    // stored xorb) and warm the reconstruction plan — captures the xorb hash
    // that drives the steady-transfer range read below.
    let xet_info = harness
        .xet_upload("mn-seed.bin", deterministic_bytes(384 * 1024, 7))
        .await?;
    let recon_file_id = xet_info.hash();
    let (xorb_hash, _fetch_url) = harness.warm_reconstruction_plan(recon_file_id).await?;

    // Fixed mid-traffic kill point at iteration 3, evidence-synced: the PUT at
    // iteration 3 must ACK first; for a transfer kill, chunk files must exist
    // on the shared root (the api role wrote them) before we stop it.
    let killed_addr = harness.role(killed).addr;
    let killed_base = harness.role(killed).base_url().to_owned();
    let kill_root = harness.storage_root.clone();
    for (i, key) in MID_TRAFFIC_KEYS.iter().enumerate() {
        mid_traffic_iteration(
            &harness.proxy_base_url,
            &harness.client,
            &harness.token,
            key,
            i,
            &ledger,
        )
        .await;
        if i == 3 {
            if killed == ServerRole::Transfer {
                wait_until(
                    Duration::from_secs(5),
                    "chunk evidence before transfer kill",
                    || count_chunk_files(&kill_root) > 0,
                )
                .await;
            }
            harness.role_mut(killed).stop().await?;
        }
    }

    // Steady role never blinked: ready, correct server_role, full surface.
    let steady_ready = ready_response(&harness.client, harness.role(steady).base_url()).await?;
    assert_eq!(steady_ready.server_role, steady.as_str());
    assert_eq!(
        role_surface_statuses(&harness.client, harness.role(steady).base_url()).await?,
        expected_surface(steady)
    );

    // Steady role keeps serving previously-acked data through ITS surface.
    match steady {
        ServerRole::Api => {
            for (key, sha) in ledger.snapshot() {
                let resp = harness.s3_get(&key).await;
                assert_eq!(resp.status().as_u16(), 200, "steady api serves {key}");
                assert_eq!(sha256_hex(&resp.bytes().await?), sha, "{key} byte-exact");
            }
            // Metadata path works without transfer: reconstruction plan 200.
            let recon = harness
                .client
                .get(format!(
                    "{}/v1/reconstructions/{recon_file_id}",
                    harness.proxy_base_url
                ))
                .header("Authorization", format!("Bearer {}", harness.token))
                .send()
                .await?
                .error_for_status()?;
            assert_eq!(
                recon.status().as_u16(),
                200,
                "reconstruction metadata served without transfer"
            );
        }
        ServerRole::Transfer => {
            // Transfer serves the acked xorb byte range directly.
            let resp = harness
                .client
                .get(format!(
                    "{}/transfer/xorb/default/{xorb_hash}",
                    harness.role(ServerRole::Transfer).base_url()
                ))
                .header("Authorization", format!("Bearer {}", harness.token))
                .header("Range", "bytes=0-65535")
                .send()
                .await?;
            assert_eq!(
                resp.status().as_u16(),
                206,
                "steady transfer xorb range read"
            );
            let content_range = resp
                .headers()
                .get(reqwest::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_owned();
            assert!(
                content_range.starts_with("bytes 0-65535/"),
                "correct Content-Range, got {content_range}"
            );
        }
        ServerRole::All => panic!("split roles only"),
    }

    // Re-spawn the killed role on its ORIGINAL port: prove the old listener is
    // gone, then same-port re-bind (bind_with_retry), then ready.
    wait_for_server_down(&harness.client, &killed_base).await?;
    let respawned = spawn_role(
        killed,
        role_frontends(killed),
        Some(killed_addr),
        &harness.storage_root,
        &harness.proxy_base_url,
    )
    .await?;
    assert_eq!(
        respawned.addr, killed_addr,
        "killed role rebinds its original port"
    );
    harness.put_role(killed, respawned);
    wait_for_ready(&harness.client, harness.role(killed).base_url()).await?;

    // Both roles ready with matching surfaces; every acked ledger key byte-exact.
    harness.assert_both_ready_and_surfaces().await?;
    for (key, sha) in ledger.snapshot() {
        let resp = harness.s3_get(&key).await;
        assert_eq!(resp.status().as_u16(), 200, "post-recovery acked key {key}");
        assert_eq!(
            sha256_hex(&resp.bytes().await?),
            sha,
            "post-recovery {key} byte-exact"
        );
    }

    // No panics: both role tasks Running (the killed task was consumed by
    // `stop()` as AbortedExpected; the re-spawned task is a fresh Running one).
    assert_eq!(
        harness.panic_status(steady).await,
        PanicStatus::Running,
        "steady role task running"
    );
    assert_eq!(
        harness.panic_status(killed).await,
        PanicStatus::Running,
        "restarted killed role task running"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multinode_kill_api_role_mid_traffic_transfer_keeps_serving() {
    match timeout(
        Duration::from_secs(60),
        exercise_kill_role_mid_traffic(ServerRole::Api),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("kill-api mid-traffic drill failed: {error}"),
        Err(elapsed) => panic!("kill-api mid-traffic drill exceeded 60 seconds ({elapsed:?})"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multinode_kill_transfer_role_mid_traffic_api_keeps_serving() {
    match timeout(
        Duration::from_secs(60),
        exercise_kill_role_mid_traffic(ServerRole::Transfer),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("kill-transfer mid-traffic drill failed: {error}"),
        Err(elapsed) => panic!("kill-transfer mid-traffic drill exceeded 60 seconds ({elapsed:?})"),
    }
}

// ===========================================================================
// A2 — proxy-level partition between api and transfer; recovery on reconnect.
// ===========================================================================

async fn exercise_partition_between_api_and_transfer() -> Result<(), Box<dyn Error>> {
    let mut harness = MultiNodeHarness::new().await?;
    let ledger = AckedLedger::default();

    // Upload 2 acked S3 objects through the proxy.
    for (i, key) in ["mn-p0", "mn-p1"].iter().enumerate() {
        acked_put(
            &harness.proxy_base_url,
            &harness.client,
            &harness.token,
            key,
            2001_u64.saturating_add(2_u64.saturating_mul(i as u64)),
            &ledger,
        )
        .await;
    }

    // Seed a reconstruction-addressable record; warm the reconstruction plan
    // and capture the transfer fetch URL + xorb hash.
    let xet_info = harness
        .xet_upload("mn-partition-seed.bin", deterministic_bytes(384 * 1024, 9))
        .await?;
    let recon_file_id = xet_info.hash();
    let (xorb_hash, transfer_url) = harness.warm_reconstruction_plan(recon_file_id).await?;

    // Baseline chunk-route status while both roles are healthy (the route is
    // forwarded and served; after reconnect it must be served again, never 502).
    let chunk_baseline = harness
        .client
        .get(format!(
            "{}/v1/chunks/default/{xorb_hash}",
            harness.proxy_base_url
        ))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await?
        .status();

    // Partition the transfer routes at the proxy.
    harness.partition.store(true, Ordering::SeqCst);

    // Both nodes stay up.
    let api_ready =
        ready_response(&harness.client, harness.role(ServerRole::Api).base_url()).await?;
    assert_eq!(api_ready.server_role, "api");
    let transfer_ready = ready_response(
        &harness.client,
        harness.role(ServerRole::Transfer).base_url(),
    )
    .await?;
    assert_eq!(transfer_ready.server_role, "transfer");

    // Metadata path unaffected (api role serves reconstruction), transfer
    // routes black-holed with 502.
    let recon = harness
        .client
        .get(format!(
            "{}/v1/reconstructions/{recon_file_id}",
            harness.proxy_base_url
        ))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await?;
    assert_eq!(
        recon.status().as_u16(),
        200,
        "reconstruction metadata still served while partitioned"
    );

    let xorb_fetch = harness
        .client
        .get(&transfer_url)
        .header("Authorization", format!("Bearer {}", harness.token))
        .header("Range", "bytes=0-65535")
        .send()
        .await?;
    assert_eq!(
        xorb_fetch.status().as_u16(),
        502,
        "transfer fetch black-holed while partitioned"
    );

    let chunk_fetch = harness
        .client
        .get(format!(
            "{}/v1/chunks/default/{xorb_hash}",
            harness.proxy_base_url
        ))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await?;
    assert_eq!(
        chunk_fetch.status().as_u16(),
        502,
        "chunk route black-holed while partitioned"
    );

    // Bounded window (the only wall-clock sleep in the drills), then reconnect.
    sleep(Duration::from_millis(100)).await;
    harness.partition.store(false, Ordering::SeqCst);

    // Recovery within 5s: the captured transfer URL serves its xorb range again.
    let fetch_ok = {
        let client = harness.client.clone();
        let token = harness.token.clone();
        let url = transfer_url.clone();
        move || {
            let client = client.clone();
            let token = token.clone();
            let url = url.clone();
            async move {
                let resp = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Range", "bytes=0-65535")
                    .send()
                    .await;
                matches!(resp, Ok(r) if r.status().as_u16() == 206)
            }
        }
    };
    poll_until_ok(
        Duration::from_secs(5),
        "transfer URL fetch recovery",
        fetch_ok,
    )
    .await;

    // The chunk route is served again (same status as the healthy baseline),
    // and the xorb HEAD route on the transfer surface answers 200.
    let chunk_after = harness
        .client
        .get(format!(
            "{}/v1/chunks/default/{xorb_hash}",
            harness.proxy_base_url
        ))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await?
        .status();
    assert_eq!(
        chunk_after, chunk_baseline,
        "chunk route served again after reconnect (baseline {chunk_baseline})"
    );
    let xorb_head = harness
        .client
        .head(format!(
            "{}/v1/xorbs/default/{xorb_hash}",
            harness.proxy_base_url
        ))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await?;
    assert_eq!(
        xorb_head.status().as_u16(),
        200,
        "xorb HEAD after reconnect"
    );

    // Fresh S3 PUT + GET through the proxy is byte-exact.
    let fresh_payload = deterministic_bytes(64 * 1024 + 3, 3001);
    let fresh_sha = sha256_hex(&fresh_payload);
    let put = harness.s3_put("mn-fresh", fresh_payload).await;
    assert_eq!(put.status().as_u16(), 200, "fresh S3 PUT");
    let get = harness.s3_get("mn-fresh").await;
    assert_eq!(get.status().as_u16(), 200, "fresh S3 GET");
    assert_eq!(
        sha256_hex(&get.bytes().await?),
        fresh_sha,
        "fresh GET byte-exact"
    );

    // No panics: both role tasks Running.
    assert_eq!(
        harness.panic_status(ServerRole::Api).await,
        PanicStatus::Running,
        "api task running after partition"
    );
    assert_eq!(
        harness.panic_status(ServerRole::Transfer).await,
        PanicStatus::Running,
        "transfer task running after partition"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multinode_partition_between_api_and_transfer_recovers_on_reconnect() {
    match timeout(
        Duration::from_secs(60),
        exercise_partition_between_api_and_transfer(),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("partition drill failed: {error}"),
        Err(elapsed) => panic!("partition drill exceeded 60 seconds ({elapsed:?})"),
    }
}

// ===========================================================================
// A3 — mid-traffic single-role upgrade (stop = listener dropped, then same-port
// re-bind with zero steady-role interruption).
// ===========================================================================

async fn exercise_mid_traffic_upgrade(
    upgrade: ServerRole,
    steady: ServerRole,
) -> Result<(), Box<dyn Error>> {
    let mut harness = MultiNodeHarness::new().await?;
    let ledger = AckedLedger::default();

    // Seed 3 acked objects.
    for i in 0..3 {
        acked_put(
            &harness.proxy_base_url,
            &harness.client,
            &harness.token,
            &format!("mn-a{i}"),
            4001_u64.saturating_add(2_u64.saturating_mul(i)),
            &ledger,
        )
        .await;
    }

    // Reconstruction-addressable seed (also supplies the xorb for the
    // steady-transfer acked-data read).
    let xet_info = harness
        .xet_upload("mn-upgrade-seed.bin", deterministic_bytes(384 * 1024, 11))
        .await?;
    let recon_file_id = xet_info.hash();
    let (xorb_hash, _fetch_url) = harness.warm_reconstruction_plan(recon_file_id).await?;

    // Bounded mid-traffic loop; at iteration 3 the upgraded role is stopped
    // (upgrade-stop: listener dropped before the call returns). Evidence-synced
    // for a transfer upgrade via shared-root chunk files.
    let upgraded_addr = harness.role(upgrade).addr;
    let upgraded_base = harness.role(upgrade).base_url().to_owned();
    let upgrade_root = harness.storage_root.clone();
    for (i, key) in MID_TRAFFIC_KEYS.iter().enumerate() {
        mid_traffic_iteration(
            &harness.proxy_base_url,
            &harness.client,
            &harness.token,
            key,
            i,
            &ledger,
        )
        .await;
        if i == 3 {
            if upgrade == ServerRole::Transfer {
                wait_until(
                    Duration::from_secs(5),
                    "chunk evidence before transfer upgrade",
                    || count_chunk_files(&upgrade_root) > 0,
                )
                .await;
            }
            harness.role_mut(upgrade).stop().await?;
        }
    }

    // The old listener is provably gone before the same-port re-bind.
    wait_for_server_down(&harness.client, &upgraded_base).await?;

    // Steady role kept serving: ready, correct server_role, full surface, and
    // previously-acked data through its surface.
    let steady_ready = ready_response(&harness.client, harness.role(steady).base_url()).await?;
    assert_eq!(steady_ready.server_role, steady.as_str());
    assert_eq!(
        role_surface_statuses(&harness.client, harness.role(steady).base_url()).await?,
        expected_surface(steady)
    );
    match steady {
        ServerRole::Api => {
            for (key, sha) in ledger.snapshot() {
                let resp = harness.s3_get(&key).await;
                assert_eq!(resp.status().as_u16(), 200, "steady api serves {key}");
                assert_eq!(sha256_hex(&resp.bytes().await?), sha, "{key} byte-exact");
            }
        }
        ServerRole::Transfer => {
            let resp = harness
                .client
                .get(format!(
                    "{}/transfer/xorb/default/{xorb_hash}",
                    harness.role(ServerRole::Transfer).base_url()
                ))
                .header("Authorization", format!("Bearer {}", harness.token))
                .header("Range", "bytes=0-65535")
                .send()
                .await?;
            assert_eq!(
                resp.status().as_u16(),
                206,
                "steady transfer xorb range read"
            );
        }
        ServerRole::All => panic!("split roles only"),
    }

    // Re-spawn the upgraded role on its ORIGINAL port (bind_with_retry must
    // not exhaust retries); assert the port is preserved and the surface is
    // correct again.
    let restarted = spawn_role(
        upgrade,
        role_frontends(upgrade),
        Some(upgraded_addr),
        &harness.storage_root,
        &harness.proxy_base_url,
    )
    .await?;
    assert_eq!(
        restarted.addr, upgraded_addr,
        "upgraded role rebinds its original port"
    );
    harness.put_role(upgrade, restarted);
    wait_for_ready(&harness.client, harness.role(upgrade).base_url()).await?;
    let restarted_ready = ready_response(&harness.client, harness.role(upgrade).base_url()).await?;
    assert_eq!(restarted_ready.server_role, upgrade.as_str());
    assert_eq!(
        role_surface_statuses(&harness.client, harness.role(upgrade).base_url()).await?,
        expected_surface(upgrade)
    );

    // Both roles ready; every acked ledger key byte-exact; no panics.
    harness.assert_both_ready_and_surfaces().await?;
    for (key, sha) in ledger.snapshot() {
        let resp = harness.s3_get(&key).await;
        assert_eq!(resp.status().as_u16(), 200, "post-upgrade acked key {key}");
        assert_eq!(
            sha256_hex(&resp.bytes().await?),
            sha,
            "post-upgrade {key} byte-exact"
        );
    }
    assert_eq!(
        harness.panic_status(steady).await,
        PanicStatus::Running,
        "steady role task running"
    );
    assert_eq!(
        harness.panic_status(upgrade).await,
        PanicStatus::Running,
        "upgraded role task running"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multinode_mid_traffic_api_upgrade_transfer_keeps_serving() {
    match timeout(
        Duration::from_secs(60),
        exercise_mid_traffic_upgrade(ServerRole::Api, ServerRole::Transfer),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("api upgrade drill failed: {error}"),
        Err(elapsed) => panic!("api upgrade drill exceeded 60 seconds ({elapsed:?})"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multinode_mid_traffic_transfer_upgrade_api_keeps_serving() {
    match timeout(
        Duration::from_secs(60),
        exercise_mid_traffic_upgrade(ServerRole::Transfer, ServerRole::Api),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("transfer upgrade drill failed: {error}"),
        Err(elapsed) => panic!("transfer upgrade drill exceeded 60 seconds ({elapsed:?})"),
    }
}
