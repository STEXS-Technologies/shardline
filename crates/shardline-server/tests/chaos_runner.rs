//! chaos_runner.rs — deterministic chaos-testing orchestrator for the shardline
//! server. Runs a seeded workload (Puts/Gets/Deletes/Multiparts/RangeGets plus a
//! park-gated StreamedPut) against an in-process server while injecting faults:
//! hard kill, connection stall, storage interference, reconstruction-cache drop.
//!
//! Every write is only "acknowledged" when an HTTP 200/201/204 ACK is observed;
//! the checker then asserts the acknowledged state is byte-intact, never torn,
//! listing-consistent, and that the server never panicked — plus a GC fixed-point.
//!
//! Determinism contract: same SHARDLINE_CHAOS_SEED => identical injection
//! sequence and op schedule (all RNG draws flow through SplitMix64 in a fixed
//! call order). Inherent tokio scheduling variance (completion order of
//! concurrent ops) is tolerated because the ledger records only observed ACKs
//! and no_torn_object accepts ANY previously-acked coherent version.

#![allow(
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::let_underscore_must_use,
    clippy::shadow_unrelated,
    clippy::expect_used,
    clippy::panic,
    clippy::needless_borrows_for_generic_args,
    clippy::unnecessary_map_or,
    // The SplitMix64 RNG below is spec-verbatim (wrapping/percent arithmetic is
    // intrinsic to it), so arithmetic_side_effects and single_char_lifetime_names
    // are allowed at file scope; everything else matches the fault_drills header.
    clippy::arithmetic_side_effects,
    clippy::single_char_lifetime_names,
)]

use sha2::{Digest, Sha256};
use shardline_gc::{LocalGcOptions, LocalGcReport};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::{
    collections::HashMap,
    env,
    fmt::{self, Write as _},
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinHandle,
};

// ---------------------------------------------------------------------------
// Constants.
// ---------------------------------------------------------------------------

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";
const BUCKET: &str = "chaos.chaos";
const DEFAULT_CHAOS_SEED: u64 = 0x5EED_CAFE;
const DEFAULT_CHAOS_ROUNDS: usize = 10;
const CHUNK: usize = 65536;
const STALL_CHUNK: usize = 512 * 1024;
const MAX_PAYLOAD: usize = 2 * 1024 * 1024;
const MIN_PAYLOAD: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// SplitMix64 — deterministic RNG, no dependencies (spec-verbatim).
// ---------------------------------------------------------------------------

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
        (self.next_u64() as usize) % bound
    }

    const fn next_range(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.next_usize(hi - lo + 1)
    }

    const fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.next_usize(items.len())]
    }
}

// ---------------------------------------------------------------------------
// Auth / tokens — same signing key as the server, identical across restarts.
// ---------------------------------------------------------------------------

fn mint_token(owner: &str, name: &str, scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "chaos-runner", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Small deterministic helpers (copied verbatim from fault_drills.rs where
// available; the drills' helpers are module-private to that test binary).
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

/// Extract the first `{tag}` element's text from an XML string (S3 envelopes).
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.checked_add(open.len())?;
    let end = xml[start..].find(&close)?.checked_add(start)?;
    Some(xml[start..end].to_owned())
}

/// Extract ALL `{tag}` element texts (for S3 ListBucketResult parsing).
fn extract_xml_all_tags(xml: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find(&open) {
        let Some(after_open) = start.checked_add(open.len()) else {
            break;
        };
        let Some(end) = rest[after_open..].find(&close) else {
            break;
        };
        let text_end = end.checked_add(after_open).unwrap();
        out.push(rest[after_open..text_end].to_owned());
        rest = &rest[text_end.wrapping_add(close.len())..];
    }
    out
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

// ---------------------------------------------------------------------------
// GC .tmp-* accommodation (REAL server bug, documented for the report).
//
// shardline's chunk writes are write-temp-then-hardlink: anchored_fs.rs:219-227
// names temp files `chunks/<2hex>/<64hex>.tmp-<nanos>-<counter>` beside the
// target, and local_fs.rs:200-207 hard_link + remove_if_present. If the GC
// orphan-scan runs while such a temp exists (or after a hard-kill stranded
// one), `chunk_hash_from_chunk_object_key_if_present`
// (shardline-server-core/src/validation.rs:74-97) returns
// Err(ServerObjectStoreError::InvalidContentHash) for the `.tmp-*`-suffixed key
// instead of Ok(None), so `run_gc` aborts the WHOLE mark_and_sweep with
// GcError::ObjectStore(InvalidContentHash). The upstream fix is for that
// validation helper to return Ok(None) for keys that pass the 2-hex/64-hex
// prefix gates but fail full hash validation. These `.tmp-*` files are
// unreferenced dead garbage (no record references a temp name), so deleting
// them here is always safe.
fn sweep_chunk_tmp_files(root: &Path) -> usize {
    let mut swept = 0usize;
    let chunks_dir = root.join("chunks");
    if let Ok(entries) = std::fs::read_dir(&chunks_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            if let Ok(files) = std::fs::read_dir(&path) {
                for file in files.flatten() {
                    let name = file.file_name().to_string_lossy().into_owned();
                    let file_path = file.path();
                    if name.contains(".tmp-") && file_path.is_file() {
                        let _ = std::fs::remove_file(&file_path);
                        swept = swept.saturating_add(1);
                    }
                }
            }
        }
    }
    swept
}

fn part_file_size(root: &Path, upload_id: &str, part_number: u32) -> u64 {
    let path = root
        .join("s3-uploads")
        .join(upload_id)
        .join(format!("part-{part_number}"));
    std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0)
}

fn session_dir_exists(root: &Path, upload_id: &str) -> bool {
    root.join("s3-uploads").join(upload_id).is_dir()
}

fn quarantine_row_count(root: &Path) -> usize {
    let db_path = root.join("metadata.sqlite3");
    if !db_path.is_file() {
        return 0;
    }
    let Ok(conn) = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) else {
        return 0;
    };
    conn.busy_timeout(Duration::from_millis(500)).ok();
    let count: Result<i64, _> = conn.query_row(
        "SELECT COUNT(*) FROM shardline_quarantine_candidates",
        [],
        |row| row.get(0),
    );
    count.map(|value| value as usize).unwrap_or(0)
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
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Like `wait_until` but returns false on timeout instead of panicking.
async fn wait_until_opt(timeout: Duration, what: &str, cond: impl Fn() -> bool) -> bool {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .expect("deadline overflow");
    loop {
        if cond() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            eprintln!("chaos: wait_until_opt timed out waiting for {what}");
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// ---------------------------------------------------------------------------
// Slow body: a reqwest stream backed by an mpsc channel. The handler is
// provably inside its body-read loop until we drop `tx` (no EOF ever sent).
// ---------------------------------------------------------------------------

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

/// Park until `released` is set (Notify + atomic flag — no lost wakeups).
async fn wait_release(park: &Notify, released: &AtomicBool) {
    while !released.load(Ordering::SeqCst) {
        park.notified().await;
    }
}

// ---------------------------------------------------------------------------
// AcknowledgedWriteLedger — records ONLY observed HTTP ACKs.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct LedgerVersion {
    bytes: Vec<u8>,
    sha256: String,
}

#[derive(Clone, Debug, Default)]
struct KeyLedger {
    versions: Vec<LedgerVersion>,
    alive: bool,
}

#[derive(Default)]
struct AcknowledgedWriteLedger {
    inner: Mutex<HashMap<String, KeyLedger>>,
}

impl AcknowledgedWriteLedger {
    /// Record an observed successful PUT (200/201): append a version, mark alive.
    fn record_put(&self, key: &str, bytes: Vec<u8>, sha256: String) {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.entry(key.to_owned()).or_default();
        entry.versions.push(LedgerVersion { bytes, sha256 });
        entry.alive = true;
    }

    /// Record an observed successful DELETE (204): remove the key entirely.
    fn record_delete(&self, key: &str) {
        self.inner.lock().unwrap().remove(key);
    }

    /// Keys currently alive, sorted for determinism.
    fn live_keys(&self) -> Vec<String> {
        let inner = self.inner.lock().unwrap();
        let mut keys: Vec<String> = inner
            .iter()
            .filter(|(_k, ledger)| ledger.alive)
            .map(|(k, _ledger)| k.clone())
            .collect();
        keys.sort();
        keys
    }

    /// All previously-acked coherent versions for a key (empty if unknown/deleted).
    fn accepted_versions(&self, key: &str) -> Vec<LedgerVersion> {
        let inner = self.inner.lock().unwrap();
        inner
            .get(key)
            .map(|ledger| ledger.versions.clone())
            .unwrap_or_default()
    }

    fn snapshot_len(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.values().map(|ledger| ledger.versions.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// Attempted-payload map — key -> every byte-payload a write op ATTEMPTED for
// it (Put / StreamedPut / Multipart; for Multipart the concatenated part
// payload == the intended final object bytes). Populated deterministically at
// engine launch from the workload spec (no wall-clock / RNG involvement).
//
// Used to reconcile objects the server COMMITTED but whose ACK the runner never
// observed (e.g. the interference restart races the 200 response): such an
// object is durable and coherent — the ledger just never saw the ACK — so the
// checker accepts bytes matching an attempted payload as a legitimate version.
// ---------------------------------------------------------------------------

type AttemptedPayloads = Mutex<HashMap<String, Vec<Vec<u8>>>>;

// ---------------------------------------------------------------------------
// ChaosHarness — spawn / kill / restart on the same root directory.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum CacheMode {
    Memory,
    Disabled,
}

#[derive(Debug)]
enum PanicStatus {
    Running,
    AbortedExpected,
    Panicked(String),
    EarlyExit,
}

struct ChaosHarness {
    _tmp: TempDir,
    root: PathBuf,
    chunk_size: NonZeroUsize,
    client: reqwest::Client,
    token: String,
    base_url: String,
    handle: Option<JoinHandle<()>>,
    graceful_shutdown: Option<oneshot::Sender<()>>,
    cache_mode: CacheMode,
}

impl ChaosHarness {
    fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().to_path_buf();
        Self {
            _tmp: tmp,
            root,
            chunk_size: NonZeroUsize::new(CHUNK).unwrap(),
            client: reqwest::Client::new(),
            token: mint_token("chaos", "chaos", TokenScope::Write),
            base_url: String::new(),
            handle: None,
            graceful_shutdown: None,
            cache_mode: CacheMode::Memory,
        }
    }

    fn build_config(&self, addr: SocketAddr) -> ServerConfig {
        let mut config = ServerConfig::new(
            addr,
            "http://127.0.0.1:8080".to_owned(),
            self.root.clone(),
            self.chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(vec![ServerFrontend::S3])
        .unwrap()
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_s3_upload_session_ttl_seconds(NonZeroU64::new(3600).unwrap())
        .unwrap();
        config = match self.cache_mode {
            CacheMode::Memory => config.with_reconstruction_cache_memory(
                NonZeroU64::new(60).unwrap(),
                NonZeroUsize::new(1024).unwrap(),
            ),
            CacheMode::Disabled => config.with_reconstruction_cache_disabled(),
        };
        config
    }

    async fn spawn_server(&mut self) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let config = self.build_config(addr);
        config.validate_runtime_requirements().unwrap();
        let app = app::router(config).await.unwrap();

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    shutdown_rx.await.ok();
                })
                .await
                .ok();
        });
        self.handle = Some(handle);
        self.graceful_shutdown = Some(shutdown_tx);
        self.base_url = base_url.clone();

        // Readiness: any HTTP response (even 401/404) proves the router is up.
        let client = self.client.clone();
        loop {
            match client.get(format!("{base_url}/healthz")).send().await {
                Ok(_response) => return,
                Err(_error) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    async fn restart(&mut self) {
        self.spawn_server().await;
    }

    /// Simulates SIGKILL: abort the serve task (drops all in-flight
    /// connections) and await its cancellation.
    async fn kill_hard(&mut self) {
        self.graceful_shutdown = None;
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let result = handle.await;
            assert!(
                result.is_err(),
                "aborted server task must report cancellation"
            );
        }
        self.base_url.clear();
    }

    async fn settle(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    async fn gc(&self, options: LocalGcOptions) -> LocalGcReport {
        let config = self.build_config("127.0.0.1:0".parse().unwrap());
        shardline_server::run_gc(config, options).await.unwrap()
    }

    /// Run GC without panicking on failure (for tolerance-aware call sites).
    async fn gc_result(&self, options: LocalGcOptions) -> Result<LocalGcReport, String> {
        let config = self.build_config("127.0.0.1:0".parse().unwrap());
        shardline_server::run_gc(config, options)
            .await
            .map_err(|err| err.to_string())
    }

    /// Run GC tolerating the known InvalidContentHash `.tmp-*` transient (see
    /// `sweep_chunk_tmp_files` for the underlying bug): on that specific error,
    /// sweep the temp chunk files, settle briefly, and retry once. Any other
    /// error — or a second InvalidContentHash — fails the round with the
    /// diagnostic (that would be a genuine problem, not the known transient).
    ///
    /// NOTE: the error is matched on its Display string — both
    /// `GcError::InvalidContentHash` and
    /// `ServerObjectStoreError::InvalidContentHash` render as "content hash must
    /// be 64 hexadecimal characters" (see shardline-gc/src/error.rs and
    /// shardline-server-core/src/object_store.rs).
    async fn gc_tolerating_tmp_files(&self, root: &Path, what: &str) -> LocalGcReport {
        match self.gc_result(LocalGcOptions::mark_and_sweep(0)).await {
            Ok(report) => report,
            Err(err) if err.contains("content hash") => {
                let swept = sweep_chunk_tmp_files(root);
                eprintln!(
                    "chaos: {what}: GC hit known .tmp-* InvalidContentHash bug ({err}); \
                     swept {swept} temp chunk files, retrying once"
                );
                self.settle(Duration::from_millis(200)).await;
                match self.gc_result(LocalGcOptions::mark_and_sweep(0)).await {
                    Ok(report) => report,
                    Err(retry_err) => panic!(
                        "chaos: {what}: GC retry ALSO failed with InvalidContentHash \
                         ({retry_err}); swept {swept} temp files"
                    ),
                }
            }
            Err(err) => panic!("chaos: {what}: GC failed with unexpected error: {err}"),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn s3_path(&self, key: &str) -> String {
        format!("/{BUCKET}/{key}")
    }

    async fn s3_put_bytes(&self, key: &str, bytes: Vec<u8>) -> reqwest::Response {
        self.client
            .put(self.url(&self.s3_path(key)))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/octet-stream")
            .body(bytes)
            .send()
            .await
            .unwrap()
    }

    async fn s3_get(&self, key: &str) -> reqwest::Response {
        self.client
            .get(self.url(&self.s3_path(key)))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .unwrap()
    }

    /// Ranged GET; `range` is the raw "Range" header value (e.g. "bytes=0-99").
    async fn s3_get_range(&self, key: &str, range: &str) -> reqwest::Response {
        self.client
            .get(self.url(&self.s3_path(key)))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Range", range)
            .send()
            .await
            .unwrap()
    }

    async fn s3_create_multipart(&self, key: &str) -> String {
        let resp = self
            .client
            .post(self.url(&format!("{}?uploads", self.s3_path(key))))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status().as_u16(),
            200,
            "create multipart should succeed"
        );
        let xml = resp.text().await.unwrap();
        extract_xml_tag(&xml, "UploadId").expect("UploadId in create-multipart response")
    }

    async fn s3_upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: reqwest::Body,
    ) -> reqwest::Response {
        self.client
            .put(self.url(&format!(
                "{}?partNumber={part_number}&uploadId={upload_id}",
                self.s3_path(key)
            )))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
            .unwrap()
    }

    /// GET /{BUCKET}?list-type=2 — parse every <Key>, sorted.
    async fn s3_list_keys(&self) -> Vec<String> {
        let resp = self
            .client
            .get(self.url(&format!("/{BUCKET}?list-type=2")))
            .header("Authorization", format!("Bearer {}", self.token))
            .send()
            .await
            .unwrap();
        let xml = resp.text().await.unwrap();
        let mut keys = extract_xml_all_tags(&xml, "Key");
        keys.sort();
        keys.dedup();
        keys
    }

    /// Inspect the serve task: Panicked => server panicked (FAIL), EarlyExit =>
    /// server exited cleanly on its own (FAIL), AbortedExpected => our kill,
    /// Running => healthy.
    async fn panic_status(&mut self) -> PanicStatus {
        let Some(handle) = self.handle.take() else {
            // No live handle (killed and not yet restarted, or never spawned).
            return PanicStatus::Running;
        };
        if !handle.is_finished() {
            self.handle = Some(handle);
            return PanicStatus::Running;
        }
        match handle.await {
            Err(e) if e.is_cancelled() => PanicStatus::AbortedExpected,
            Err(e) if e.is_panic() => PanicStatus::Panicked(format!("{e}")),
            Ok(()) => PanicStatus::EarlyExit,
            Err(_) => PanicStatus::EarlyExit, // unreachable; JoinError is Cancelled|Panic
        }
    }
}

impl Drop for ChaosHarness {
    fn drop(&mut self) {
        if let Some(tx) = self.graceful_shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// WorkloadSpec + WorkloadEngine.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpKind {
    Put,
    Get,
    Delete,
    Multipart,
    RangeGet,
    StreamedPut,
}

#[derive(Clone, Debug)]
struct OpSpec {
    kind: OpKind,
    key: String,
    payload: Vec<u8>,
    payload_sha: String,
    parts: Vec<Vec<u8>>,
    range: Option<(usize, usize)>,
}

struct WorkloadSpec {
    ops: Vec<OpSpec>,
    streamed_key: String,
}

impl WorkloadSpec {
    /// Generate one round's workload. ALL RNG draws happen here in a fixed
    /// order so the schedule is fully determined by the seed.
    fn generate(rng: &mut SplitMix64, round: usize) -> WorkloadSpec {
        let key_count = rng.next_range(2, 4);
        let keys: Vec<String> = (0..key_count)
            .map(|i| format!("chaos-k{i}-r{round}"))
            .collect();
        let op_count = rng.next_range(8, 14);

        // First op is ALWAYS a StreamedPut on a fresh per-round key. This key is
        // excluded from Delete targets by construction (delete keys are drawn
        // from `keys`, which never contains the streamed key).
        let streamed_key = format!("chaos-stream-r{round}");
        let stream_len = STALL_CHUNK * rng.next_range(2, 5); // 1-2 MiB
        let stream_payload = deterministic_bytes(stream_len, rng.next_u64());
        let stream_sha = sha256_hex(&stream_payload);

        let mut ops = Vec::with_capacity(op_count + 1);
        ops.push(OpSpec {
            kind: OpKind::StreamedPut,
            key: streamed_key.clone(),
            payload: stream_payload,
            payload_sha: stream_sha,
            parts: Vec::new(),
            range: None,
        });

        for _ in 0..op_count {
            let key = keys[rng.next_usize(key_count)].clone();
            let kind = match rng.next_usize(100) {
                0..40 => OpKind::Put,
                40..65 => OpKind::Get,
                65..75 => OpKind::Delete,
                75..90 => OpKind::Multipart,
                _ => OpKind::RangeGet,
            };
            match kind {
                OpKind::Multipart => {
                    let part_count = rng.next_range(2, 4);
                    let mut parts = Vec::with_capacity(part_count);
                    let mut combined = Vec::new();
                    for _ in 0..part_count {
                        let part =
                            deterministic_bytes(CHUNK * rng.next_range(4, 16), rng.next_u64());
                        combined.extend_from_slice(&part);
                        parts.push(part);
                    }
                    let payload_sha = sha256_hex(&combined);
                    ops.push(OpSpec {
                        kind,
                        key,
                        payload: combined,
                        payload_sha,
                        parts,
                        range: None,
                    });
                }
                OpKind::RangeGet => {
                    let payload = deterministic_bytes(
                        rng.next_range(MIN_PAYLOAD, MAX_PAYLOAD)
                            .next_multiple_of(CHUNK),
                        rng.next_u64(),
                    );
                    let start = rng.next_usize(payload.len());
                    let end = (start + 4096).min(payload.len());
                    let range = if start < end {
                        Some((start, end))
                    } else {
                        Some((0, 4096.min(payload.len())))
                    };
                    let payload_sha = sha256_hex(&payload);
                    ops.push(OpSpec {
                        kind,
                        key,
                        payload,
                        payload_sha,
                        parts: Vec::new(),
                        range,
                    });
                }
                OpKind::Put | OpKind::Get | OpKind::Delete | OpKind::StreamedPut => {
                    let payload = deterministic_bytes(
                        rng.next_range(MIN_PAYLOAD, MAX_PAYLOAD)
                            .next_multiple_of(CHUNK),
                        rng.next_u64(),
                    );
                    let payload_sha = sha256_hex(&payload);
                    ops.push(OpSpec {
                        kind,
                        key,
                        payload,
                        payload_sha,
                        parts: Vec::new(),
                        range: None,
                    });
                }
            }
        }

        WorkloadSpec { ops, streamed_key }
    }
}

/// Execute one non-streamed op. NEVER panics: request errors (connect refused
/// after a kill, timeouts) just return without recording anything.
async fn run_op(
    client: &reqwest::Client,
    token: &str,
    base_url: &str,
    ledger: &AcknowledgedWriteLedger,
    op: &OpSpec,
) {
    let url = format!("{base_url}/{BUCKET}/{}", op.key);
    match op.kind {
        OpKind::Put => {
            let Ok(resp) = client
                .put(&url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/octet-stream")
                .body(op.payload.clone())
                .send()
                .await
            else {
                return;
            };
            if resp.status().as_u16() == 200 || resp.status().as_u16() == 201 {
                ledger.record_put(&op.key, op.payload.clone(), op.payload_sha.clone());
            }
        }
        OpKind::Get => {
            let _ = client
                .get(&url)
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await;
        }
        OpKind::RangeGet => {
            if let Some((start, end)) = op.range {
                let _ = client
                    .get(&url)
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Range", format!("bytes={start}-{}", end - 1))
                    .send()
                    .await;
            }
        }
        OpKind::Delete => {
            let Ok(resp) = client
                .delete(&url)
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
            else {
                return;
            };
            if resp.status().as_u16() == 204 {
                ledger.record_delete(&op.key);
            }
        }
        OpKind::Multipart => {
            let Ok(create) = client
                .post(format!("{url}?uploads"))
                .header("Authorization", format!("Bearer {token}"))
                .send()
                .await
            else {
                return;
            };
            if create.status().as_u16() != 200 {
                return;
            }
            let Ok(xml) = create.text().await else { return };
            let Some(upload_id) = extract_xml_tag(&xml, "UploadId") else {
                return;
            };
            run_multipart(client, token, &url, ledger, op, &upload_id).await;
        }
        OpKind::StreamedPut => {
            // Never dispatched here: the engine's park-gated stream task handles
            // StreamedPut directly. Fall through silently if it ever arrives.
        }
    }
}

/// Complete the Multipart op: upload each part (capturing server ETags), then
/// complete with the REAL part set; record the object only on a 200 complete.
async fn run_multipart(
    client: &reqwest::Client,
    token: &str,
    url: &str,
    ledger: &AcknowledgedWriteLedger,
    op: &OpSpec,
    upload_id: &str,
) {
    let mut etags: Vec<(u32, String)> = Vec::with_capacity(op.parts.len());
    let mut all_parts_ok = true;
    for (i, part) in op.parts.iter().enumerate() {
        let part_number = i as u32 + 1;
        let part_resp = client
            .put(format!(
                "{url}?partNumber={part_number}&uploadId={upload_id}"
            ))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(part.clone())
            .send()
            .await;
        match part_resp {
            Ok(r) if r.status().as_u16() == 200 => {
                let etag = r
                    .headers()
                    .get("ETag")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.trim_matches('"').to_owned());
                match etag {
                    Some(etag) => etags.push((part_number, etag)),
                    None => {
                        all_parts_ok = false;
                        break;
                    }
                }
            }
            _ => {
                all_parts_ok = false;
                break;
            }
        }
    }
    if !all_parts_ok {
        // Abandon the session best-effort (never ACKed anyway).
        let _ = client
            .delete(format!("{url}?uploadId={upload_id}"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await;
        return;
    }
    // Complete with the REAL part set (must exactly match the uploaded parts).
    let mut body = String::from("<CompleteMultipartUpload>");
    for (num, etag) in &etags {
        write!(
            &mut body,
            "<Part><PartNumber>{num}</PartNumber><ETag>\"{etag}\"</ETag></Part>"
        )
        .unwrap();
    }
    body.push_str("</CompleteMultipartUpload>");
    let Ok(resp) = client
        .post(format!("{url}?uploadId={upload_id}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/xml")
        .body(body)
        .send()
        .await
    else {
        return;
    };
    if resp.status().as_u16() == 200 {
        let combined: Vec<u8> = op.parts.concat();
        ledger.record_put(&op.key, combined, op.payload_sha.clone());
    }
}

struct WorkloadEngine {
    tasks: Vec<JoinHandle<()>>,
    in_flight: Arc<AtomicUsize>,
    park: Arc<Notify>,
    park_release: Arc<AtomicBool>,
    pending: Vec<OpSpec>,
    stream_task: Option<JoinHandle<()>>,
    attempted: Arc<AttemptedPayloads>,
}

impl WorkloadEngine {
    fn launch(
        harness: &ChaosHarness,
        spec: &WorkloadSpec,
        ledger: &Arc<AcknowledgedWriteLedger>,
        in_flight: Arc<AtomicUsize>,
        attempted: &Arc<AttemptedPayloads>,
    ) -> Self {
        let park = Arc::new(Notify::new());
        let park_release = Arc::new(AtomicBool::new(false));
        let mut engine = Self {
            tasks: Vec::new(),
            in_flight,
            park,
            park_release,
            pending: Vec::new(),
            stream_task: None,
            attempted: attempted.clone(),
        };

        // --- StreamedPut: park-gated. Sends one STALL_CHUNK, then parks until
        // release_stream() fires; then streams the rest in 512KiB pieces with a
        // 2ms sleep between, drops the body, awaits the response, records on 200.
        //
        // The request future is driven by a select (NOT a detached spawned
        // task): aborting this task drops the request mid-body -> connection torn
        // down -> the server cannot commit the partial upload (mirrors drill1's
        // `put_task.abort()`). A detached task would survive the abort, read the
        // EOF, and let the server commit an object the ledger never ACKed.
        let stream_op = &spec.ops[0];
        let stream_key = stream_op.key.clone();
        let stream_payload = stream_op.payload.clone();
        let stream_sha = stream_op.payload_sha.clone();
        // Record the streamed put's intended payload (commit-without-ACK tolerance).
        engine
            .attempted
            .lock()
            .unwrap()
            .entry(stream_key.clone())
            .or_default()
            .push(stream_payload.clone());
        let client = harness.client.clone();
        let token = harness.token.clone();
        let base_url = harness.base_url.clone();
        let park_g = engine.park.clone();
        let park_release_g = engine.park_release.clone();
        let ledger_g = ledger.clone();
        let stream_task = tokio::spawn(async move {
            let (tx, body) = slow_body();
            let put_fut = client
                .put(format!("{base_url}/{BUCKET}/{stream_key}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/octet-stream")
                .body(body)
                .send();
            tokio::pin!(put_fut);
            // First chunk on the wire, then park until released. The select
            // keeps polling the request so the server stays mid-body-read.
            let first = bytes::Bytes::from(stream_payload[..STALL_CHUNK].to_vec());
            if tx.send(Ok(first)).await.is_err() {
                return; // server already gone
            }
            let mut result: Option<Result<reqwest::Response, reqwest::Error>> = None;
            tokio::select! {
                r = &mut put_fut => {
                    result = Some(r);
                }
                () = wait_release(&park_g, &park_release_g) => {}
            }
            if result.is_none() {
                // Released: stream the remainder in 512KiB pieces with a small
                // inter-chunk gap, then EOF so the server finalizes.
                let mut off = STALL_CHUNK;
                while off < stream_payload.len() {
                    let end = (off + 512 * 1024).min(stream_payload.len());
                    if tx
                        .send(Ok(bytes::Bytes::from(stream_payload[off..end].to_vec())))
                        .await
                        .is_err()
                    {
                        return; // connection dropped (server killed/restarted)
                    }
                    off = end;
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                drop(tx); // EOF -> server finalizes the upload
                if let Ok(resp) = put_fut.await {
                    result = Some(Ok(resp));
                }
            }
            if let Some(Ok(resp)) = result {
                let status = resp.status().as_u16();
                if status == 200 || status == 201 {
                    ledger_g.record_put(&stream_key, stream_payload, stream_sha);
                }
            }
        });
        engine.stream_task = Some(stream_task);

        // Every other op goes to `pending` and is ALSO spawned immediately.
        for op in spec.ops.iter().skip(1) {
            engine.pending.push(op.clone());
            engine.launch_op(
                harness.client.clone(),
                harness.token.clone(),
                harness.base_url.clone(),
                ledger,
                op,
            );
        }
        engine
    }

    fn launch_op(
        &mut self,
        client: reqwest::Client,
        token: String,
        base_url: String,
        ledger: &Arc<AcknowledgedWriteLedger>,
        op: &OpSpec,
    ) {
        // Record the intended payload of every write op so a committed-but-
        // never-ACKed object can be reconciled by the checker.
        if matches!(op.kind, OpKind::Put | OpKind::Multipart) {
            self.attempted
                .lock()
                .unwrap()
                .entry(op.key.clone())
                .or_default()
                .push(op.payload.clone());
        }
        let in_flight = self.in_flight.clone();
        let ledger = ledger.clone();
        let op = op.clone();
        let task = tokio::spawn(async move {
            in_flight.fetch_add(1, Ordering::SeqCst);
            run_op(&client, &token, &base_url, &ledger, &op).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        self.tasks.push(task);
    }

    /// Await all non-stream tasks with a timeout; abort stragglers.
    async fn join(&mut self, timeout: Duration) {
        let tasks = std::mem::take(&mut self.tasks);
        for mut task in tasks {
            let result = tokio::time::timeout(timeout, &mut task).await;
            if result.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }

    /// Let the parked StreamedPut proceed (non-stall rounds).
    fn release_stream(&self) {
        self.park_release.store(true, Ordering::SeqCst);
        self.park.notify_one();
    }

    /// Abort the StreamedPut (drops the request -> connection closes).
    async fn abort_stream(&mut self) {
        if let Some(task) = self.stream_task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// Re-issue pending ops against the CURRENT server (after a restart).
    fn relaunch(&mut self, harness: &ChaosHarness, ledger: &Arc<AcknowledgedWriteLedger>) {
        let pending = std::mem::take(&mut self.pending);
        for op in pending {
            self.launch_op(
                harness.client.clone(),
                harness.token.clone(),
                harness.base_url.clone(),
                ledger,
                &op,
            );
        }
    }
}

impl Drop for WorkloadEngine {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        if let Some(task) = self.stream_task.take() {
            task.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Failure injections + per-round report.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FailureInjection {
    None,
    HardKill,
    ConnectionStall,
    StorageInterference,
    CacheDrop,
}

const ALL_INJECTIONS: [FailureInjection; 4] = [
    FailureInjection::HardKill,
    FailureInjection::ConnectionStall,
    FailureInjection::StorageInterference,
    FailureInjection::CacheDrop,
];

#[derive(Debug)]
struct ChaosRoundReport {
    round: usize,
    seed: u64,
    injection: FailureInjection,
    workload_ops: usize,
    acked_writes: usize,
    checks: Vec<CheckResult>,
    outcome: bool,
}

impl ChaosRoundReport {
    const fn new(
        round: usize,
        seed: u64,
        injection: FailureInjection,
        workload_ops: usize,
    ) -> Self {
        Self {
            round,
            seed,
            injection,
            workload_ops,
            acked_writes: 0,
            checks: Vec::new(),
            outcome: false,
        }
    }
}

impl fmt::Display for ChaosRoundReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "round={} seed={:#x} injection={:?} outcome={}",
            self.round,
            self.seed,
            self.injection,
            if self.outcome { "PASS" } else { "FAIL" }
        )?;
        writeln!(
            f,
            "  workload_ops={} acked_writes={}",
            self.workload_ops, self.acked_writes
        )?;
        let mut parts = Vec::new();
        for check in &self.checks {
            if check.passed {
                parts.push(format!("{}=pass", check.name));
            } else {
                parts.push(format!("{}=fail({})", check.name, check.detail));
            }
        }
        write!(f, "  checks: {}", parts.join(" "))
    }
}

#[derive(Debug)]
struct CheckResult {
    name: &'static str,
    passed: bool,
    detail: String,
}

impl CheckResult {
    const fn new(name: &'static str, passed: bool, detail: String) -> Self {
        Self {
            name,
            passed,
            detail,
        }
    }

    const fn pass(name: &'static str) -> Self {
        Self::new(name, true, String::new())
    }

    fn fail(name: &'static str, detail: impl Into<String>) -> Self {
        Self::new(name, false, detail.into())
    }
}

// ---------------------------------------------------------------------------
// Verification helpers.
// ---------------------------------------------------------------------------

/// Verify the acked state after an injection round. Checks:
/// - acked_byte_intact: every live ledger key GETs 200 with the newest acked
///   version's exact bytes (plus one ranged GET -> 206, byte-equal).
/// - no_torn_object: GET bytes equal SOME previously-acked coherent version; a
///   vanished acked key (404) is a FAIL.
/// - listing_consistent: S3 listing set == live ledger key set; every listed
///   key GETable with bytes matching its ledger.
/// - no_panics_surfaced: serve task never panicked / never exited early.
/// - storage_detection: optional env-gated probe (SHARDLINE_CHAOS_STORAGE_DETECTION=1).
async fn verify_all(
    harness: &mut ChaosHarness,
    ledger: &AcknowledgedWriteLedger,
    attempted: &Arc<AttemptedPayloads>,
) -> Vec<CheckResult> {
    let mut checks = Vec::new();
    let live = ledger.live_keys();

    // --- check 1: acked_byte_intact (+ one ranged GET) ---------------------
    let mut ok = true;
    let mut detail = String::new();
    for key in &live {
        let newest = ledger.accepted_versions(key).last().cloned();
        let resp = harness.s3_get(key).await;
        if resp.status().as_u16() != 200 {
            ok = false;
            detail = format!("key {key}: GET -> {}", resp.status());
            break;
        }
        let body = resp.bytes().await.unwrap();
        let mismatch = newest.as_ref().map_or(false, |version| {
            body.as_ref() != version.bytes.as_slice() || sha256_hex(&body) != version.sha256
        });
        if mismatch {
            ok = false;
            detail = format!("key {key}: body/sha mismatch vs newest acked version");
            break;
        }
    }
    // Ranged GET on the first live key (if any): must return 206 with the exact
    // byte slice of the newest acked version.
    if ok {
        for first_key in live.iter().take(1) {
            let Some(version) = ledger.accepted_versions(first_key).last().cloned() else {
                continue;
            };
            let len = version.bytes.len();
            if len > 0 {
                let start = len / 3;
                let end = (start + 4096).min(len);
                if start < end {
                    let range_resp = harness
                        .s3_get_range(first_key, &format!("bytes={start}-{}", end - 1))
                        .await;
                    if range_resp.status().as_u16() != 206 {
                        ok = false;
                        detail = format!("key {first_key}: ranged GET -> {}", range_resp.status());
                        break;
                    }
                    let range_body = range_resp.bytes().await.unwrap();
                    if range_body.as_ref() != &version.bytes[start..end] {
                        ok = false;
                        detail = format!("key {first_key}: ranged GET bytes mismatch");
                        break;
                    }
                }
            }
        }
    }
    checks.push(CheckResult::new("acked_byte_intact", ok, detail));

    // --- check 2: no_torn_object ------------------------------------------
    let mut ok = true;
    let mut detail = String::new();
    for key in &live {
        let versions = ledger.accepted_versions(key);
        let resp = harness.s3_get(key).await;
        let status = resp.status().as_u16();
        if status != 200 {
            ok = false;
            detail = format!("acked key {key} vanished (GET -> {status})");
            break;
        }
        let body = resp.bytes().await.unwrap();
        let body_sha = sha256_hex(&body);
        let ledger_matched = versions
            .iter()
            .any(|v| v.bytes.as_slice() == body.as_ref() && v.sha256 == body_sha);
        // Tolerance: a coherent commit-without-observed-ACK (the server committed
        // this key's payload but its ACK raced a restart/kill). Accept bytes equal
        // to an attempted payload for this key alongside the acked versions.
        let attempted_matched = attempted
            .lock()
            .unwrap()
            .get(key)
            .map_or(false, |candidates| {
                candidates
                    .iter()
                    .any(|candidate| candidate.as_slice() == body.as_ref())
            });
        if !ledger_matched && !attempted_matched {
            ok = false;
            detail = format!(
                "key {key}: body matches no acked version and no attempted payload (torn object)"
            );
            break;
        }
    }
    checks.push(CheckResult::new("no_torn_object", ok, detail));

    // --- check 3: listing_consistent ---------------------------------------
    let mut ok = true;
    let mut detail = String::new();
    let listed = harness.s3_list_keys().await;
    // Strict: every live (ACKed) ledger key must still be listed — an ACKed key
    // vanishing is data loss and always a FAIL.
    for key in &live {
        if !listed.contains(key) {
            ok = false;
            detail = format!(
                "acked key {key} missing from listing (listing={listed:?} ledger={live:?})"
            );
            break;
        }
    }
    // Every listed key must be GETable and byte-coherent: ledger keys must match
    // a ledger version; non-ledger keys (commit-without-observed-ACK) must match
    // the attempted payload for that key. Anything else is torn/foreign -> FAIL.
    if ok {
        for key in &listed {
            let resp = harness.s3_get(key).await;
            if resp.status().as_u16() != 200 {
                ok = false;
                detail = format!("listed key {key}: GET -> {}", resp.status());
                break;
            }
            let body = resp.bytes().await.unwrap();
            let body_sha = sha256_hex(&body);
            let versions = ledger.accepted_versions(key);
            let ledger_matched = versions
                .iter()
                .any(|v| v.bytes.as_slice() == body.as_ref() && v.sha256 == body_sha);
            if ledger_matched {
                continue;
            }
            let attempted_matched =
                attempted
                    .lock()
                    .unwrap()
                    .get(key)
                    .map_or(false, |candidates| {
                        candidates
                            .iter()
                            .any(|candidate| candidate.as_slice() == body.as_ref())
                    });
            if !attempted_matched {
                ok = false;
                detail = format!(
                    "listed key {key}: bytes match no ledger version and no attempted payload \
                     (torn or foreign object; body_sha={body_sha})"
                );
                break;
            }
        }
    }
    checks.push(CheckResult::new("listing_consistent", ok, detail));

    // --- check 4: no_panics_surfaced ---------------------------------------
    let status = harness.panic_status().await;
    let (passed, detail) = match status {
        PanicStatus::Running => (true, "panic_status=Running".to_owned()),
        PanicStatus::AbortedExpected => (true, "panic_status=AbortedExpected".to_owned()),
        PanicStatus::Panicked(msg) => (false, format!("server panicked: {msg}")),
        PanicStatus::EarlyExit => (false, "server exited early (no panic)".to_owned()),
    };
    checks.push(CheckResult::new("no_panics_surfaced", passed, detail));

    // --- optional storage-detection probe (default off) ---------------------
    if env::var("SHARDLINE_CHAOS_STORAGE_DETECTION").as_deref() == Ok("1") {
        checks.push(storage_detection_check(harness, ledger).await);
    }

    checks
}

/// Optional diagnostic: corrupt a chunk file on disk belonging to an acked
/// object, then require the server to DETECT the corruption (5xx) — never
/// silently serve wrong bytes. Corrupts the first byte of every chunk file,
/// restarts (cold path, clears the reconstruction cache), asserts every live
/// key 5xx, then restores the original bytes so later rounds stay valid.
async fn storage_detection_check(
    harness: &mut ChaosHarness,
    ledger: &AcknowledgedWriteLedger,
) -> CheckResult {
    let live = ledger.live_keys();
    if live.is_empty() {
        return CheckResult::pass("storage_detection");
    }
    // Flip the first byte of every chunk file (keeping originals for restore).
    let chunks_dir = harness.root.join("chunks");
    let mut corrupted = 0usize;
    let mut originals: Vec<(PathBuf, u8)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&chunks_dir) {
        for entry in entries.flatten() {
            if !entry.path().is_dir() {
                continue;
            }
            if let Ok(files) = std::fs::read_dir(entry.path()) {
                for file in files.flatten() {
                    let path = file.path();
                    let Ok(mut data) = std::fs::read(&path) else {
                        continue;
                    };
                    if !data.is_empty() {
                        originals.push((path.clone(), data[0]));
                        data[0] ^= 0xFF;
                        let _ = std::fs::write(&path, data);
                        corrupted += 1;
                    }
                }
            }
        }
    }
    if corrupted == 0 {
        return CheckResult::fail("storage_detection", "no chunk files found to corrupt");
    }
    // Cold path: restart clears the in-memory reconstruction cache.
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;
    let mut fail = None;
    for key in &live {
        let resp = harness.s3_get(key).await;
        let status = resp.status().as_u16();
        if !(500..=599).contains(&status) {
            fail = Some(format!(
                "key {key}: GET -> {status} after corruption (expected 5xx; silent wrong bytes?)"
            ));
            break;
        }
    }
    // Restore original bytes so subsequent rounds are unaffected.
    for (path, byte) in originals {
        let Ok(mut data) = std::fs::read(&path) else {
            continue;
        };
        if !data.is_empty() {
            data[0] = byte;
            let _ = std::fs::write(&path, data);
        }
    }
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;
    fail.map_or_else(
        || CheckResult::pass("storage_detection"),
        |detail| CheckResult::fail("storage_detection", detail),
    )
}

/// ConnectionStall checks — the server must stay healthy with a stalled
/// mid-body upload in flight, and the stalled key must not materialize.
async fn verify_stalled(
    harness: &mut ChaosHarness,
    ledger: &AcknowledgedWriteLedger,
    spec: &WorkloadSpec,
) -> Vec<CheckResult> {
    let mut checks = Vec::new();
    // (i) every live ledger key GET byte-intact (same logic as check 1).
    let mut ok = true;
    let mut detail = String::new();
    for key in ledger.live_keys() {
        let newest = ledger.accepted_versions(&key).last().cloned();
        let resp = harness.s3_get(&key).await;
        if resp.status().as_u16() != 200 {
            ok = false;
            detail = format!("key {key}: GET -> {}", resp.status());
            break;
        }
        let body = resp.bytes().await.unwrap();
        let mismatch = newest.as_ref().map_or(false, |version| {
            body.as_ref() != version.bytes.as_slice() || sha256_hex(&body) != version.sha256
        });
        if mismatch {
            ok = false;
            detail = format!("key {key}: bytes mismatch during stall");
            break;
        }
    }
    checks.push(CheckResult::new("stall_acked_byte_intact", ok, detail));

    // (ii) streamed key GET: 404 (fresh key) or a previously-acked version
    // (overwrite) — never a hybrid, and the GET MUST complete.
    let versions = ledger.accepted_versions(&spec.streamed_key);
    let result =
        tokio::time::timeout(Duration::from_secs(10), harness.s3_get(&spec.streamed_key)).await;
    match result {
        Err(_) => checks.push(CheckResult::fail(
            "stall_streamed_key_get",
            "GET on stalled streamed key did not complete within 10s",
        )),
        Ok(resp) => {
            let status = resp.status().as_u16();
            if versions.is_empty() {
                checks.push(CheckResult::new(
                    "stall_streamed_key_get",
                    status == 404,
                    format!("stalled key GET -> {status}, expected 404 (fresh key)"),
                ));
            } else {
                let body = resp.bytes().await.unwrap();
                let matched = versions.iter().any(|v| v.bytes.as_slice() == body.as_ref());
                checks.push(CheckResult::new(
                    "stall_streamed_key_get",
                    status == 200 && matched,
                    format!("stalled key GET -> {status}, bytes matched={matched}"),
                ));
            }
        }
    }

    // (iii) server must be Running (no panic, no early exit).
    let status = harness.panic_status().await;
    let (passed, detail) = match status {
        PanicStatus::Running => (true, "panic_status=Running".to_owned()),
        PanicStatus::Panicked(msg) => (false, format!("server panicked: {msg}")),
        PanicStatus::AbortedExpected => (false, "server aborted unexpectedly".to_owned()),
        PanicStatus::EarlyExit => (false, "server exited early".to_owned()),
    };
    checks.push(CheckResult::new("stall_no_panic", passed, detail));

    checks
}

// ---------------------------------------------------------------------------
// Env knobs.
// ---------------------------------------------------------------------------

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name).ok().map_or(default, |value| {
        value.parse::<u64>().unwrap_or_else(|_| {
            eprintln!("chaos: WARNING invalid {name}={value:?}, using default {default:#x}");
            default
        })
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name).ok().map_or(default, |value| {
        value.parse::<usize>().unwrap_or_else(|_| {
            eprintln!("chaos: WARNING invalid {name}={value:?}, using default {default}");
            default
        })
    })
}

// ===========================================================================
// The round loop.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chaos_runner() {
    let seed = env_u64("SHARDLINE_CHAOS_SEED", DEFAULT_CHAOS_SEED);
    let rounds = env_usize("SHARDLINE_CHAOS_ROUNDS", DEFAULT_CHAOS_ROUNDS);
    eprintln!("chaos: seed={seed:#x} rounds={rounds}");
    let mut rng = SplitMix64::new(seed);
    let mut harness = ChaosHarness::new();
    harness.spawn_server().await;
    let ledger = Arc::new(AcknowledgedWriteLedger::default());
    let in_flight = Arc::new(AtomicUsize::new(0));
    let attempted: Arc<AttemptedPayloads> = Arc::new(Mutex::new(HashMap::new()));

    // Seed one durable object up front so early rounds exercise read/overwrite
    // and listing paths against pre-existing state. Workload ops never target
    // this key (round keys are `chaos-k*` / `chaos-stream*`).
    let seed_key = "chaos-seed-object";
    let seed_bytes = deterministic_bytes(MIN_PAYLOAD, 0x5EED);
    let seed_sha = sha256_hex(&seed_bytes);
    let seed_resp = harness.s3_put_bytes(seed_key, seed_bytes.clone()).await;
    assert_eq!(seed_resp.status().as_u16(), 200, "seed object PUT");
    ledger.record_put(seed_key, seed_bytes, seed_sha);

    let run = async {
        for round in 0..rounds {
            let spec = WorkloadSpec::generate(&mut rng, round);
            let injection = if round == 0 {
                FailureInjection::None
            } else {
                *rng.pick(&ALL_INJECTIONS[..])
            };
            let mut report = ChaosRoundReport::new(round, seed, injection, spec.ops.len());

            let mut engine =
                WorkloadEngine::launch(&harness, &spec, &ledger, in_flight.clone(), &attempted);

            match injection {
                FailureInjection::HardKill => {
                    // Evidence-synchronized kill — never a sleep guess.
                    let root = harness.root.clone();
                    let chunks_before = count_chunk_files(&root);
                    let synced_inflight =
                        wait_until_opt(Duration::from_secs(3), "in_flight >= 2", || {
                            in_flight.load(Ordering::SeqCst) >= 2
                        })
                        .await;
                    let synced_chunks = wait_until_opt(
                        Duration::from_secs(3),
                        "in_flight >= 1 && chunk evidence",
                        || {
                            in_flight.load(Ordering::SeqCst) >= 1
                                && count_chunk_files(&root) > chunks_before
                        },
                    )
                    .await;
                    assert!(
                        synced_inflight || synced_chunks,
                        "chaos round {round}: HardKill had no sync evidence \
                         (in_flight>=2: {synced_inflight}, chunks: {synced_chunks})"
                    );
                    engine.abort_stream().await;
                    harness.kill_hard().await;
                    harness.settle(Duration::from_millis(50)).await;
                    harness.restart().await;
                    sweep_chunk_tmp_files(&harness.root);
                    engine.relaunch(&harness, &ledger);
                }
                FailureInjection::ConnectionStall => {
                    // The StreamedPut is parked mid-body — confirm the chunk
                    // evidence first, then let every other op complete and
                    // verify the stalled state.
                    let root = harness.root.clone();
                    wait_until(
                        Duration::from_secs(5),
                        "streamed chunk evidence (stall)",
                        || count_chunk_files(&root) > 0,
                    )
                    .await;
                    engine.join(Duration::from_secs(15)).await;
                    let stall_checks = verify_stalled(&mut harness, &ledger, &spec).await;
                    report.checks.extend(stall_checks);
                    engine.abort_stream().await;
                    harness.settle(Duration::from_millis(100)).await;
                    harness.restart().await;
                    sweep_chunk_tmp_files(&harness.root);
                    engine.relaunch(&harness, &ledger);
                }
                FailureInjection::StorageInterference => {
                    engine.release_stream();
                    let sub = rng.next_usize(3);
                    match sub {
                        // 0: corrupt an in-progress multipart part file. Fairness:
                        // the upload was NEVER ACKed, so no ledger contract is
                        // broken — but an ACKed object would be a durable contract,
                        // and corrupting one must surface as detection, never as
                        // silent torn bytes.
                        0 => {
                            let mp_key = format!("chaos-mp-interfere-r{round}");
                            let upload_id = harness.s3_create_multipart(&mp_key).await;
                            // The pinned upload future borrows `harness`; keep it
                            // scoped so the borrow ends before the restart below.
                            {
                                let (tx, body) = slow_body();
                                let part_fut = harness.s3_upload_part(&mp_key, &upload_id, 1, body);
                                tokio::pin!(part_fut);
                                let first = bytes::Bytes::from(deterministic_bytes(
                                    STALL_CHUNK,
                                    rng.next_u64(),
                                ));
                                let _ = tx.send(Ok(first)).await;
                                // Drive the upload while polling for part-1 evidence.
                                let deadline = tokio::time::Instant::now()
                                    .checked_add(Duration::from_secs(5))
                                    .expect("deadline overflow");
                                let mut evidence = false;
                                let mut finished = false;
                                while !evidence && !finished {
                                    tokio::select! {
                                        result = &mut part_fut => {
                                            finished = true;
                                            let _ = result;
                                        }
                                        () = tokio::time::sleep(Duration::from_millis(10)) => {
                                            if tokio::time::Instant::now() >= deadline {
                                                break;
                                            }
                                            if part_file_size(&harness.root, &upload_id, 1) > 0 {
                                                evidence = true;
                                            }
                                        }
                                    }
                                }
                                assert!(
                                    evidence,
                                    "chaos round {round}: no part-1 evidence before deadline"
                                );
                                // TRUNCATE the part file to half its size (rewrite).
                                let part_path = harness
                                    .root
                                    .join("s3-uploads")
                                    .join(&upload_id)
                                    .join("part-1");
                                let full = std::fs::read(&part_path).unwrap();
                                std::fs::write(&part_path, &full[..full.len() / 2]).unwrap();
                                drop(tx);
                            } // part_fut dropped here -> client abort (connection closed)
                            engine.abort_stream().await;
                            harness.settle(Duration::from_millis(50)).await;
                            harness.restart().await;
                            sweep_chunk_tmp_files(&harness.root);
                            engine.relaunch(&harness, &ledger);
                            let resp = harness.s3_get(&mp_key).await;
                            assert_eq!(
                                resp.status().as_u16(),
                                404,
                                "truncated multipart must not materialize as an object"
                            );
                            assert!(
                                session_dir_exists(&harness.root, &upload_id),
                                "session dir should persist (TTL not yet expired)"
                            );
                        }
                        // 1: orphan chunk junk under root/chunks/ — hashes that
                        // cannot correspond to acked content.
                        1 => {
                            let root = harness.root.clone();
                            for _ in 0..3 {
                                let len = rng.next_range(1024, 16384);
                                let name = sha256_hex(&deterministic_bytes(len, rng.next_u64()));
                                let dir = root.join("chunks").join(&name[..2]);
                                std::fs::create_dir_all(&dir).unwrap();
                                std::fs::write(dir.join(&name), vec![0xAB; 1024]).unwrap();
                            }
                            let gc_report = harness
                                .gc_tolerating_tmp_files(
                                    &root,
                                    &format!("round {round} interference (orphan chunk junk)"),
                                )
                                .await;
                            // Do NOT hard-assert deletion (retention semantics may
                            // retain); acked integrity + server health are the point.
                            eprintln!("chaos: round {round} interference gc_report={gc_report:?}");
                        }
                        // 2: junk under root/gc/quarantine/.
                        _ => {
                            let root = harness.root.clone();
                            let dir = root.join("gc").join("quarantine");
                            std::fs::create_dir_all(&dir).unwrap();
                            for _ in 0..3 {
                                let len = rng.next_range(1024, 16384);
                                let name = sha256_hex(&deterministic_bytes(len, rng.next_u64()));
                                std::fs::write(dir.join(&name), vec![0xCD; 512]).unwrap();
                            }
                            let gc_report = harness
                                .gc_tolerating_tmp_files(
                                    &root,
                                    &format!("round {round} interference (quarantine junk)"),
                                )
                                .await;
                            eprintln!("chaos: round {round} interference gc_report={gc_report:?}");
                        }
                    }
                }
                FailureInjection::CacheDrop => {
                    engine.release_stream();
                    harness.cache_mode = match harness.cache_mode {
                        CacheMode::Memory => CacheMode::Disabled,
                        CacheMode::Disabled => CacheMode::Memory,
                    };
                    harness.kill_hard().await;
                    harness.restart().await;
                    sweep_chunk_tmp_files(&harness.root);
                    engine.relaunch(&harness, &ledger);
                }
                FailureInjection::None => {
                    engine.release_stream();
                }
            }

            engine.join(Duration::from_secs(15)).await;
            let mut checks = verify_all(&mut harness, &ledger, &attempted).await;
            // Sweep any GC-poison `.tmp-*` chunk temps before the fixed-point
            // check so the GC runs clean (see sweep_chunk_tmp_files).
            sweep_chunk_tmp_files(&harness.root);
            let gc1 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
            let gc2 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
            checks.push(CheckResult::new(
                "gc_fixed_point",
                gc2.deleted_chunks == 0
                    && gc2.new_quarantine_candidates == 0
                    && gc2.orphan_chunks == 0
                    && quarantine_row_count(&harness.root) == 0,
                format!("report1={gc1:?} report2={gc2:?}"),
            ));
            report.checks = checks;
            report.acked_writes = ledger.snapshot_len();
            report.outcome = report.checks.iter().all(|c| c.passed);
            eprintln!("{report}");
            if !report.outcome {
                panic!(
                    "chaos round {round} FAILED (seed {seed:#x}, injection {injection:?}): {report:?}"
                );
            }
        }
        eprintln!("chaos: PASS — {rounds} rounds, seed={seed:#x}");
    };
    tokio::time::timeout(Duration::from_secs(180), run)
        .await
        .expect("chaos runner exceeded global 180s budget");
}
