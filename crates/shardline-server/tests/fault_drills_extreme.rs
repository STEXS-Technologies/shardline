//! Extreme fault-injection drills (v1.6.0): corrupt-chunk survival, cache
//! degradation (dead Redis / lost in-memory cache), stale-read pinned
//! snapshots, Postgres kill-mid-transaction, repeated kill cycles with a
//! growth bound, concurrency chaos with GC fixed point, partial-read resume
//! after a client abort, and multiversion overwrite kills.
//!
//! This file is a SEPARATE integration-test crate from `fault_drills.rs`. The
//! ~37 harness helpers below are duplicated verbatim from that file (each
//! integration test builds its own crate; duplication keeps `fault_drills.rs`
//! a frozen regression gate). The harness is extended additively with:
//!   - a reconstruction-cache mode (default `Disabled`; `with_redis_cache` /
//!     `with_memory_cache` + `set_redis_cache` for mid-life switches),
//!   - `s3_delete` (DELETE verb),
//!   - chunk-set helpers (`chunk_file_paths`, `corrupt_file`,
//!     `new_chunk_files`, `session_dir_count`), and a seeded LCG for drill 6.
//!
//! Same philosophy as `fault_drills.rs`: every kill is synchronized on
//! *on-disk evidence* (a new chunk file appearing) or on a body that is
//! provably not at EOF — never a sleep guess. After each kill the harness
//! restarts on the SAME root directory, modeling a process crash + restart.
//!
//! Notes on the reconstruction cache: RedisReconstructionCache::new* only
//! performs `redis::Client::open` (URL parse, no TCP), and per-op connection
//! errors fall through to the disk loader; `validate_runtime_requirements()`
//! is pure config validation and does NOT ping Redis. The S3 GET path never
//! consults the reconstruction cache (it is only used by the XET
//! `/v1/reconstructions/{file_id}` route), so the cache drills prove boot +
//! S3 correctness under cache failure.

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

use futures_util::StreamExt;
use sha2::{Digest, Sha256};
use shardline_gc::{LocalGcOptions, LocalGcReport};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::{
    collections::HashSet,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};

/// Port 1 on loopback refuses connections instantly (ECONNREFUSED), so this
/// URL models a dead Redis without a live dependency or a timeout wait.
const DEAD_REDIS_URL: &str = "redis://127.0.0.1:1/0";

// ---------------------------------------------------------------------------
// Auth / tokens — same signing key as the server, identical across restarts.
// ---------------------------------------------------------------------------

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

/// The S3 bucket name (`{owner}.{name}`) all drills operate on.
const BUCKET: &str = "drill.drill";

fn mint_token(owner: &str, name: &str, scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "fault-drills", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Small deterministic helpers.
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

/// Deterministic LCG step for seeded pseudo-random sequences (drill 6).
const fn lcg_next(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

/// Extract the first `{tag}` element's text from an XML string (S3 envelopes).
fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.checked_add(open.len())?;
    let end = xml[start..].find(&close)?.checked_add(start)?;
    Some(xml[start..end].to_owned())
}

// ---------------------------------------------------------------------------
// DrillHarness — spawn / kill / restart on the same root directory.
// ---------------------------------------------------------------------------

/// Reconstruction-cache mode for `build_config`. Default is `Disabled`,
/// preserving the behavior of `fault_drills.rs`.
#[derive(Clone, Debug)]
enum CacheMode {
    Disabled,
    Redis(String),
    Memory(NonZeroU64, NonZeroUsize),
}

struct DrillHarness {
    _tmp: TempDir,
    root: PathBuf,
    chunk_size: NonZeroUsize,
    client: reqwest::Client,
    token: String,
    base_url: String,
    handle: Option<JoinHandle<()>>,
    graceful_shutdown: Option<oneshot::Sender<()>>,
    session_ttl_seconds: NonZeroU64,
    index_postgres_url: Option<String>,
    cache_mode: CacheMode,
}

impl DrillHarness {
    fn new(session_ttl_seconds: u64) -> Self {
        let tmp = TempDir::new().unwrap();
        Self {
            root: tmp.path().to_path_buf(),
            chunk_size: NonZeroUsize::new(65536).unwrap(),
            client: reqwest::Client::new(),
            token: mint_token("drill", "drill", TokenScope::Write),
            base_url: String::new(),
            handle: None,
            graceful_shutdown: None,
            session_ttl_seconds: NonZeroU64::new(session_ttl_seconds.max(1)).unwrap(),
            index_postgres_url: None,
            cache_mode: CacheMode::Disabled,
            _tmp: tmp,
        }
    }

    fn with_postgres(mut self, url: &str) -> Self {
        self.index_postgres_url = Some(url.to_owned());
        self
    }

    fn with_redis_cache(mut self, url: String) -> Self {
        self.cache_mode = CacheMode::Redis(url);
        self
    }

    fn with_memory_cache(mut self, ttl: NonZeroU64, max_entries: NonZeroUsize) -> Self {
        self.cache_mode = CacheMode::Memory(ttl, max_entries);
        self
    }

    /// Switch the cache mode mid-life (used by drill 2b to move from a live
    /// Redis to a dead one across a restart).
    fn set_redis_cache(&mut self, url: String) {
        self.cache_mode = CacheMode::Redis(url);
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
        .unwrap();
        config = match &self.cache_mode {
            CacheMode::Disabled => config.with_reconstruction_cache_disabled(),
            CacheMode::Redis(url) => config
                .with_reconstruction_cache_redis(url.clone(), NonZeroU64::new(30).unwrap())
                .unwrap(),
            CacheMode::Memory(ttl, max_entries) => {
                config.with_reconstruction_cache_memory(*ttl, *max_entries)
            }
        };
        config = config
            .with_s3_upload_session_ttl_seconds(self.session_ttl_seconds)
            .unwrap();
        if let Some(url) = &self.index_postgres_url {
            config = config.with_index_postgres_url(url.clone()).unwrap();
        }
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
            // Under normal execution the abort produces a JoinError (cancelled).
            // Under coverage instrumentation the task may complete before the
            // abort takes effect; both outcomes are acceptable — the point is
            // that the task is no longer running.
            match result {
                Ok(()) => {
                    eprintln!("kill_hard: server task completed before abort took effect");
                }
                Err(e) if e.is_cancelled() => {
                    eprintln!("kill_hard: server task cancelled as expected");
                }
                Err(e) => {
                    panic!("kill_hard: server task panicked: {e}");
                }
            }
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

    async fn s3_delete(&self, key: &str) -> reqwest::Response {
        self.client
            .delete(self.url(&self.s3_path(key)))
            .header("Authorization", format!("Bearer {}", self.token))
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

    async fn s3_complete_multipart(&self, key: &str, upload_id: &str) -> reqwest::Response {
        self.client
            .post(self.url(&format!("{}?uploadId={upload_id}", self.s3_path(key))))
            .header("Authorization", format!("Bearer {}", self.token))
            .header("Content-Type", "application/xml")
            .body(b"<CompleteMultipartUpload></CompleteMultipartUpload>".to_vec())
            .send()
            .await
            .unwrap()
    }
}

impl Drop for DrillHarness {
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

// ---------------------------------------------------------------------------
// On-disk evidence helpers (blocking fs reads are fine in tests).
// ---------------------------------------------------------------------------

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

fn quarantine_manifest_files(root: &Path) -> usize {
    count_files_recursive(&root.join("gc").join("quarantine"))
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

/// Poll for the part-1 file on disk; if the part request resolves early or the
/// deadline passes, dump the session directory for diagnosis.
async fn wait_part_evidence(
    root: &Path,
    upload_id: &str,
    part_task: &JoinHandle<reqwest::Result<reqwest::Response>>,
) {
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(5))
        .expect("deadline overflow");
    loop {
        if part_file_size(root, upload_id, 1) > 0 {
            return;
        }
        if part_task.is_finished() {
            panic!("part task finished before part-1 evidence appeared (upload={upload_id})");
        }
        if tokio::time::Instant::now() >= deadline {
            let uploads = root.join("s3-uploads");
            let listing = std::fs::read_dir(&uploads)
                .map(|rd| {
                    rd.flatten()
                        .map(|entry| entry.file_name().to_string_lossy().into_owned())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            panic!("no part-1 evidence; s3-uploads listing={listing:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// One-shot TCP reachability probe used to SKIP-gate drills needing live
/// external services (Postgres / Redis).
async fn require_tcp(host: &str, port: u16) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_millis(250), TcpStream::connect((host, port))).await,
        Ok(Ok(_stream))
    )
}

/// Migrate the local dev Postgres via the in-process path (the server does not
/// self-migrate). Reachable-but-unmigratable is a hard failure, not a skip.
async fn ensure_dev_postgres_migrated(url: &str) {
    let mut last = None;
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
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
    panic!("failed to connect+migrate PG {url}: {last:?}");
}

// ---------------------------------------------------------------------------
// NEW extreme-drill helpers (additive to the duplicated set above).
// ---------------------------------------------------------------------------

fn collect_files_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                collect_files_recursive(&path, out);
            } else {
                out.push(path);
            }
        }
    }
}

/// All chunk files under `<root>/chunks` (`{hash[..2]}/{hash}`), sorted so the
/// "first file" selection is deterministic.
fn chunk_file_paths(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    collect_files_recursive(&root.join("chunks"), &mut out);
    out.sort();
    out
}

/// Chunk files that appeared since a `before` snapshot (a `HashSet` of the
/// paths returned by [`chunk_file_paths`]).
fn new_chunk_files(root: &Path, before: &HashSet<PathBuf>) -> Vec<PathBuf> {
    chunk_file_paths(root)
        .into_iter()
        .filter(|path| !before.contains(path))
        .collect()
}

/// Flip the byte at `len/2` in place, preserving the file length exactly so
/// the storage-length metadata stays consistent — the corruption is purely
/// content-level and is detected by LZ4 decompression / chunk-hash
/// verification on the read path (never a panic).
fn corrupt_file(path: &Path) {
    let mut data = std::fs::read(path).unwrap();
    assert!(
        !data.is_empty(),
        "cannot corrupt an empty chunk file at {path:?}"
    );
    let mid = data.len() / 2;
    data[mid] ^= 0xFF;
    std::fs::write(path, &data).unwrap();
}

/// Number of live multipart session directories under `<root>/s3-uploads`.
fn session_dir_count(root: &Path) -> usize {
    let dir = root.join("s3-uploads");
    std::fs::read_dir(&dir)
        .map(|rd| rd.flatten().filter(|entry| entry.path().is_dir()).count())
        .unwrap_or(0)
}

/// First chunk hash of the most recent `latest` file record. The upload
/// ingestor rewrites every `FileChunkRecord.hash` to the stored xorb's hash
/// (xorb packing), so `chunks[0].hash` is the hash of the SINGLE object file
/// the read path actually loads — the xorb for packed records, the first
/// individual chunk otherwise. That is the correct corruption target: the
/// individual LZ4 chunk files left under `chunks/` are never read by the
/// record-backed read path.
fn latest_record_first_chunk_hash(root: &Path) -> Option<String> {
    let db_path = root.join("metadata.sqlite3");
    if !db_path.is_file() {
        return None;
    }
    let conn = rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .ok()?;
    conn.busy_timeout(Duration::from_millis(500)).ok();
    // The local index store persists `latest` and `version` rows; the S3 PUT
    // committed a `latest` row.
    let json: String = conn
        .query_row(
            "SELECT record FROM shardline_file_records WHERE record_kind = 'latest' \
             ORDER BY updated_at_unix_seconds DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok()?;
    let value: serde_json::Value = serde_json::from_str(&json).ok()?;
    value
        .get("chunks")?
        .as_array()?
        .first()?
        .get("hash")?
        .as_str()
        .map(str::to_owned)
}

// ===========================================================================
// DRILL 1 — CORRUPT CHUNK / RECONSTRUCTION SURVIVAL
// ===========================================================================

/// Corrupt the object data the read path actually loads, then verify the
/// server detects it (never a clean byte-exact 200), stays healthy, and that
/// repair = remove-file + re-PUT of the SAME bytes restores the object
/// exactly (re-PUT without the removal would hit put_if_absent dedup
/// `AlreadyExists` and leave the corruption in place).
///
/// NOTE (deviation from the original plan): the upload ingestor xorb-packs
/// multi-chunk uploads and rewrites every record chunk hash to the xorb hash,
/// so the S3 read path loads ONE xorb object (`chunks/{xorb[..2]}/{xorb}`)
/// and validates it EAGERLY (xorb hash + per-chunk decode) — it never touches
/// the individual LZ4 chunk files. Corrupting an arbitrary "first sorted chunk
/// file" would therefore be undetected. We target the file the record
/// references (identified via the metadata DB).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_1_corrupt_chunk_reconstruction_survival() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "corrupt-target";
    let payload = deterministic_bytes(512 * 1024 + 37, 1);
    let before = chunk_file_paths(&harness.root)
        .into_iter()
        .collect::<HashSet<_>>();
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed PUT");

    // Fresh root + zero dedup => every chunk-store file is this object's.
    let fresh = new_chunk_files(&harness.root, &before);
    assert!(!fresh.is_empty(), "fresh-root PUT must produce chunk files");

    // The record-referenced object file: the xorb is stored under the store
    // key `xorbs/default/{hash[..2]}/{hash}.xorb` (physically
    // `<root>/chunks/xorbs/default/...`), while the individual LZ4 chunk
    // files live at `chunks/{hash[..2]}/{hash}` and are never read.
    let first_hash = latest_record_first_chunk_hash(&harness.root)
        .expect("latest file record must exist after PUT");
    let target = harness
        .root
        .join("chunks")
        .join("xorbs")
        .join("default")
        .join(&first_hash[..2])
        .join(format!("{first_hash}.xorb"));
    assert!(
        target.is_file(),
        "record-referenced object file must exist at {target:?}"
    );
    assert!(
        fresh.contains(&target),
        "record-referenced object file must be among the files this PUT created"
    );
    corrupt_file(&target);
    eprintln!("drill1: corrupted record-referenced object file {target:?}");

    // The corrupt object file must NEVER be served as a clean byte-exact 200.
    // For xorb-backed records the read validates the xorb EAGERLY (before the
    // response is built) => a status >= 400; the individual-chunk fallback
    // path (when packing failed) verifies lazily while the body streams =>
    // a mid-stream abort. Either way the corruption is never served cleanly.
    let corrupt_result = harness
        .client
        .get(harness.url(&harness.s3_path(key)))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await;
    match corrupt_result {
        Ok(resp) => {
            let status = resp.status().as_u16();
            match resp.bytes().await {
                Ok(body) => {
                    let hash = sha256_hex(&body);
                    eprintln!("drill1: corrupt GET status={status}, body_hash={hash} (diagnosis)");
                    assert!(
                        status >= 400 || hash != sha256_hex(&payload),
                        "corrupt chunk was served as a clean byte-exact 200 — corruption undetected"
                    );
                }
                Err(error) => {
                    eprintln!(
                        "drill1: corrupt GET status={status}, body aborted mid-stream: {error} \
                         (diagnosis)"
                    );
                }
            }
        }
        Err(error) => {
            eprintln!("drill1: corrupt GET connection aborted: {error} (diagnosis)");
        }
    }

    // Server stayed healthy: unrelated PUT + GET still byte-exact.
    let other = "healthy-key";
    let other_payload = deterministic_bytes(64 * 1024 + 3, 2);
    let put2 = harness.s3_put_bytes(other, other_payload.clone()).await;
    assert_eq!(
        put2.status().as_u16(),
        200,
        "unrelated PUT after corruption"
    );
    let get2 = harness.s3_get(other).await;
    assert_eq!(get2.status().as_u16(), 200);
    assert_eq!(
        sha256_hex(&get2.bytes().await.unwrap()),
        sha256_hex(&other_payload)
    );

    // Repair: remove the corrupt file, then re-PUT the SAME bytes. The absent
    // file takes the Inserted path (fresh write) under the same content hash
    // => same path, correct bytes.
    std::fs::remove_file(&target).unwrap();
    let repair = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(repair.status().as_u16(), 200, "repair re-PUT");
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200, "repaired object serves 200");
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        sha256_hex(&body),
        sha256_hex(&payload),
        "repaired byte-exact"
    );
    assert_eq!(body.as_ref(), payload.as_slice());
}

// ===========================================================================
// DRILL 2 — LOSE THE CACHE, DEGRADE
// ===========================================================================

/// 2a: a dead Redis URL must still boot (cache creation is lazy — only
/// `redis::Client::open`, no TCP) and S3 PUT/GET must be byte-exact. The S3
/// GET path bypasses the reconstruction cache entirely, so correctness under
/// cache failure is proven by the served bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_2a_dead_redis_boots_and_serves() {
    let mut harness = DrillHarness::new(3600).with_redis_cache(DEAD_REDIS_URL.to_owned());
    harness.spawn_server().await; // must succeed: lazy cache, no TCP

    let key = "dead-redis";
    let payload = deterministic_bytes(256 * 1024 + 9, 31);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "PUT with dead Redis configured");

    let expected = sha256_hex(&payload);
    for round in 0..3 {
        let resp = harness.s3_get(key).await;
        assert_eq!(resp.status().as_u16(), 200, "GET round {round}");
        let body = resp.bytes().await.unwrap();
        assert_eq!(sha256_hex(&body), expected, "GET round {round} byte-exact");
        assert_eq!(body.as_ref(), payload.as_slice());
    }
}

/// 2b: live Redis for the write+first read, then the server is killed and
/// restarted with a DEAD Redis URL on the SAME root — the object must still
/// serve byte-exact from the disk fallback (cache lost mid-life).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_2b_live_redis_then_redis_down() {
    if !require_tcp("127.0.0.1", 6379).await {
        eprintln!("SKIP: drill_extreme_2b_live_redis_then_redis_down — 127.0.0.1:6379 unreachable");
        return;
    }
    let mut harness =
        DrillHarness::new(3600).with_redis_cache("redis://127.0.0.1:6379/0".to_owned());
    harness.spawn_server().await;

    let key = "redis-down";
    let payload = deterministic_bytes(128 * 1024 + 11, 32);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "PUT with live Redis");
    let expected = sha256_hex(&payload);
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(sha256_hex(&resp.bytes().await.unwrap()), expected);

    // Redis goes away mid-life: restart on the same root with a dead URL.
    harness.kill_hard().await;
    harness.settle(Duration::from_millis(50)).await;
    harness.set_redis_cache(DEAD_REDIS_URL.to_owned());
    harness.restart().await;

    for round in 0..2 {
        let resp = harness.s3_get(key).await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "post-dead-redis GET round {round}"
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(sha256_hex(&body), expected, "disk fallback byte-exact");
        assert_eq!(body.as_ref(), payload.as_slice());
    }
}

/// 2c: in-memory cache is process-local — a kill+restart wipes it. A cold
/// GET after restart must still be byte-exact from disk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_2c_memory_cache_lost_at_restart() {
    let mut harness = DrillHarness::new(3600).with_memory_cache(
        NonZeroU64::new(30).unwrap(),
        NonZeroUsize::new(100).unwrap(),
    );
    harness.spawn_server().await;

    let key = "mem-cache";
    let payload = deterministic_bytes(192 * 1024 + 7, 33);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200);
    let expected = sha256_hex(&payload);

    // Warm read.
    let warm = harness.s3_get(key).await;
    assert_eq!(warm.status().as_u16(), 200);
    assert_eq!(sha256_hex(&warm.bytes().await.unwrap()), expected);

    // In-process cache is gone after the restart; cold read must still work.
    harness.kill_hard().await;
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;
    let cold = harness.s3_get(key).await;
    assert_eq!(cold.status().as_u16(), 200, "cold GET after restart");
    let body = cold.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), expected, "cold GET byte-exact");
    assert_eq!(body.as_ref(), payload.as_slice());
}

// ===========================================================================
// DRILL 3 — STALE READ / PINNED SNAPSHOT
// ===========================================================================

/// Overwrite a key 200 times; concurrently with every overwrite, GETs must
/// always see exactly ONE coherent committed version (the previously
/// committed one or the in-flight one), with `Content-Length == body.len()`,
/// and never a 404, never torn bytes, never old-bytes-with-new-length.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_3_stale_read_pinned_snapshot() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "pinned";
    let v1 = deterministic_bytes(256 * 1024, 5);
    let mut prev_sha = sha256_hex(&v1);
    let put = harness.s3_put_bytes(key, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed v1");

    for i in 0..200u64 {
        // NOTE: `deterministic_bytes` normalizes `state = seed | 1`, so
        // consecutive seeds (1000/1001, 1002/1003, ...) collide into identical
        // payloads. Use odd seeds (1001 + 2*i) so all 200 overwrites are
        // genuinely distinct versions.
        let v2_i = deterministic_bytes(256 * 1024, 1001 + 2 * i);
        let cur_sha = sha256_hex(&v2_i);

        // Concurrent: the overwrite PUT is in flight while GETs run.
        let client = harness.client.clone();
        let token = harness.token.clone();
        let put_url = harness.url(&harness.s3_path(key));
        let put_task = tokio::spawn(async move {
            client
                .put(put_url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/octet-stream")
                .body(v2_i)
                .send()
                .await
                .unwrap()
        });

        let mut get_tasks = Vec::new();
        for _ in 0..2 {
            let client = harness.client.clone();
            let token = harness.token.clone();
            let get_url = harness.url(&harness.s3_path(key));
            get_tasks.push(tokio::spawn(async move {
                client
                    .get(get_url)
                    .header("Authorization", format!("Bearer {token}"))
                    .send()
                    .await
                    .unwrap()
            }));
        }

        for get_task in get_tasks {
            let resp = get_task.await.unwrap();
            let status = resp.status().as_u16();
            assert_eq!(
                status, 200,
                "GET during overwrite #{i} must be 200, got {status}"
            );
            let content_length = resp
                .headers()
                .get(reqwest::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok())
                .expect("GET response must carry a numeric Content-Length");
            let body = resp.bytes().await.unwrap();
            assert_eq!(
                content_length,
                body.len(),
                "Content-Length must equal body length (overwrite #{i})"
            );
            let hash = sha256_hex(&body);
            assert!(
                hash == prev_sha || hash == cur_sha,
                "GET during overwrite #{i} returned a torn/foreign version (hash {hash})"
            );
        }

        let put = put_task.await.unwrap();
        assert_eq!(put.status().as_u16(), 200, "overwrite PUT #{i}");
        prev_sha = cur_sha;
    }

    // The final committed version is v2_199, byte-exact.
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200);
    let body = resp.bytes().await.unwrap();
    assert_eq!(
        sha256_hex(&body),
        sha256_hex(&deterministic_bytes(256 * 1024, 1001 + 2 * 199))
    );
}

// ===========================================================================
// DRILL 4 — POSTGRES KILL-MID-TRANSACTION (SKIP-gated on live Postgres)
// ===========================================================================

/// With the Postgres metadata backend, an in-flight overwrite killed after
/// chunk evidence (provably inside the upload transaction, pre-publish) must
/// leave the committed v1 byte-exact; a subsequent full PUT must restore the
/// metadata path functionality post-crash.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_4_postgres_kill_mid_transaction() {
    if !require_tcp("127.0.0.1", 5432).await {
        eprintln!(
            "SKIP: drill_extreme_4_postgres_kill_mid_transaction — 127.0.0.1:5432 unreachable"
        );
        return;
    }
    ensure_dev_postgres_migrated(
        "postgres://shardline:shardline-dev-password@127.0.0.1:5432/shardline",
    )
    .await;
    let mut harness = DrillHarness::new(3600)
        .with_postgres("postgres://shardline:shardline-dev-password@127.0.0.1:5432/shardline");
    harness.spawn_server().await;

    let key = "pg-extreme";
    let v1 = deterministic_bytes(64 * 1024 + 5, 3);
    let put = harness.s3_put_bytes(key, v1.clone()).await;
    let put_status = put.status().as_u16();
    assert_eq!(put_status, 200, "seed PUT after migrate");

    let chunks_before = count_chunk_files(&harness.root);
    let (tx, body) = slow_body();
    let client = harness.client.clone();
    let token = harness.token.clone();
    let put_url = harness.url(&harness.s3_path(key));
    let put_task = tokio::spawn(async move {
        client
            .put(put_url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 46))))
        .await
        .unwrap();

    let root = harness.root.clone();
    wait_until(Duration::from_secs(5), "chunk evidence (PG)", || {
        count_chunk_files(&root) > chunks_before
    })
    .await;

    put_task.abort();
    harness.kill_hard().await;
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;

    // Committed data not lost, no torn state.
    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "committed v1 must survive PG-mid-transaction kill"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&v1), "v1 byte-exact");
    assert_eq!(body.as_ref(), v1.as_slice());

    // Metadata path fully functional post-crash.
    let v2 = deterministic_bytes(64 * 1024 + 5, 33);
    let put2 = harness.s3_put_bytes(key, v2.clone()).await;
    assert_eq!(put2.status().as_u16(), 200, "full PUT post-crash");
    let resp2 = harness.s3_get(key).await;
    assert_eq!(resp2.status().as_u16(), 200);
    let body2 = resp2.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body2), sha256_hex(&v2), "v2 byte-exact");
    assert_eq!(body2.as_ref(), v2.as_slice());
}

// ===========================================================================
// DRILL 5 — REPEATED KILL CYCLES (GROWTH BOUNDED)
// ===========================================================================

/// Three cycles of: complete PUT (ACKed), then an in-flight overwrite killed
/// at chunk evidence. Every ACKed object must survive byte-exact, chunk
/// growth must be bounded by the measured per-cycle deltas (each capped at
/// <= 16 new files: ~4-5 committed PUT chunks + 8 aborted-overwrite chunks
/// at a 64 KiB chunk size), and no multipart sessions may leak.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_5_repeated_kill_cycles() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let baseline = count_chunk_files(&harness.root);
    let mut total_growth = 0usize;

    for i in 0..3u64 {
        let cycle_start = count_chunk_files(&harness.root);
        let key = format!("cycle-{i}");
        // NOTE: `deterministic_bytes` normalizes `state = seed | 1`, so
        // consecutive even/odd seeds (10/11, 50/51) collide into IDENTICAL
        // payloads and the aborted overwrite would dedup 100% against the
        // previous cycle (no new chunk evidence). Use distinct odd seeds.
        let payload = deterministic_bytes(256 * 1024 + 17 * i as usize, 101 + 2 * i);
        let expected_sha = sha256_hex(&payload);
        let put = harness.s3_put_bytes(&key, payload.clone()).await;
        assert_eq!(put.status().as_u16(), 200, "cycle {i} complete PUT");
        let after_put = count_chunk_files(&harness.root);

        // In-flight overwrite, killed at chunk evidence (pre-publish).
        let (tx, body) = slow_body();
        let client = harness.client.clone();
        let token = harness.token.clone();
        let put_url = harness.url(&harness.s3_path(&key));
        let put_task = tokio::spawn(async move {
            client
                .put(put_url)
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/octet-stream")
                .body(body)
                .send()
                .await
        });
        tx.send(Ok(bytes::Bytes::from(deterministic_bytes(
            512 * 1024,
            201 + 2 * i,
        ))))
        .await
        .unwrap();
        let root = harness.root.clone();
        wait_until(
            Duration::from_secs(5),
            &format!("cycle {i} overwrite chunk evidence"),
            || count_chunk_files(&root) > after_put,
        )
        .await;

        put_task.abort();
        harness.kill_hard().await;
        harness.settle(Duration::from_millis(50)).await;
        harness.restart().await;

        // Every ACKed object is intact.
        let resp = harness.s3_get(&key).await;
        assert_eq!(resp.status().as_u16(), 200, "cycle {i} object after kill");
        let body = resp.bytes().await.unwrap();
        assert_eq!(sha256_hex(&body), expected_sha, "cycle {i} byte-exact");
        assert_eq!(body.as_ref(), payload.as_slice());

        let cycle_end = count_chunk_files(&harness.root);
        let delta = cycle_end - cycle_start;
        assert!(
            delta <= 16,
            "cycle {i} chunk growth {delta} exceeds cap 16 (committed PUT ~4-5 chunks + \
             aborted 512 KiB overwrite ~8 chunks)"
        );
        total_growth += delta;
    }

    // No unbounded growth across cycles.
    let final_count = count_chunk_files(&harness.root);
    assert!(
        final_count <= baseline + total_growth,
        "unbounded chunk growth: {final_count} > {baseline} + {total_growth}"
    );
    // Single-PUT overwrites never create multipart sessions.
    assert_eq!(
        session_dir_count(&harness.root),
        0,
        "no orphaned multipart sessions after kill cycles"
    );
}

// ===========================================================================
// DRILL 6 — CONCURRENCY CHAOS, BOUNDED (LCG-driven workers + GC fixed point)
// ===========================================================================

/// 4 workers x 3 rounds of seeded PUTs against distinct keys, a DELETE
/// worker reclaiming previously-ACKed keys, and one designated slow PUT per
/// round that is aborted at chunk evidence before a kill+restart. GC runs
/// between rounds. After the final restart every acknowledged write is
/// byte-exact, and a second GC run reaches a fixed point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_6_concurrency_chaos_bounded() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    // Shared list of (key, sha256) for every acknowledged write.
    let acked: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));

    // Seed one deletable key per round (the DELETE worker's targets).
    for r in 0..3u64 {
        let key = format!("seed-del-{r}");
        let payload = deterministic_bytes(64 * 1024 + r as usize, 200 + r);
        let put = harness.s3_put_bytes(&key, payload.clone()).await;
        assert_eq!(put.status().as_u16(), 200, "seed DELETE target {r}");
        acked.lock().await.push((key.clone(), sha256_hex(&payload)));
    }

    const ROUNDS: usize = 3;
    const WORKERS: usize = 4;
    const KEYS_PER_WORKER: usize = 3;

    for r in 0..ROUNDS {
        // GC between rounds — never concurrent with in-flight requests.
        harness.gc(LocalGcOptions::mark_and_sweep(0)).await;

        // Worker PUTs of distinct keys, payload sizes driven by a seeded LCG
        // (seed 100+r, per-worker derivation keeps it deterministic).
        let mut workers = Vec::new();
        for w in 0..WORKERS {
            let client = harness.client.clone();
            let token = harness.token.clone();
            let base_url = harness.base_url.clone();
            let bucket = BUCKET.to_owned();
            let acked = acked.clone();
            workers.push(tokio::spawn(async move {
                let mut state = 100 + r as u64 * 1000 + w as u64;
                let mut acks = Vec::new();
                for i in 0..KEYS_PER_WORKER {
                    state = lcg_next(&mut state);
                    let size = 32 + (state % (128 * 1024 - 32 + 1)) as usize;
                    let payload = deterministic_bytes(size, state);
                    let key = format!("key_{r}_{w}_{i}");
                    let resp = client
                        .put(format!("{base_url}/{bucket}/{key}"))
                        .header("Authorization", format!("Bearer {token}"))
                        .header("Content-Type", "application/octet-stream")
                        .body(payload.clone())
                        .send()
                        .await
                        .unwrap();
                    assert_eq!(resp.status().as_u16(), 200, "PUT {key}");
                    acks.push((key, sha256_hex(&payload)));
                }
                acked.lock().await.extend(acks);
            }));
        }

        // DELETE worker: reclaims the seeded key for this round (no longer
        // needed) and drops it from the ACK list.
        {
            let client = harness.client.clone();
            let token = harness.token.clone();
            let base_url = harness.base_url.clone();
            let bucket = BUCKET.to_owned();
            let acked = acked.clone();
            let del_key = format!("seed-del-{r}");
            tokio::spawn(async move {
                let resp = client
                    .delete(format!("{base_url}/{bucket}/{del_key}"))
                    .header("Authorization", format!("Bearer {token}"))
                    .send()
                    .await
                    .unwrap();
                assert!(
                    resp.status().as_u16() == 200 || resp.status().as_u16() == 204,
                    "DELETE {del_key} -> {}",
                    resp.status()
                );
                acked.lock().await.retain(|(k, _)| k != &del_key);
            })
            .await
            .unwrap();
        }

        // Every ACKed write is durable before the kill.
        for worker in workers {
            worker.await.unwrap();
        }

        // One designated slow PUT per round: stream until chunk evidence
        // (wait_until runs inside the spawned task), then abort + kill +
        // restart — never a mid-request guess.
        let root = harness.root.clone();
        let chunks_before = count_chunk_files(&root);
        let (tx, body) = slow_body();
        let client = harness.client.clone();
        let token = harness.token.clone();
        let put_url = harness.url(&harness.s3_path(&format!("slow-{r}")));
        let (evidence_tx, evidence_rx) =
            oneshot::channel::<JoinHandle<Result<reqwest::Response, reqwest::Error>>>();
        let slow_task = tokio::spawn(async move {
            let req_task = tokio::spawn(async move {
                client
                    .put(put_url)
                    .header("Authorization", format!("Bearer {token}"))
                    .header("Content-Type", "application/octet-stream")
                    .body(body)
                    .send()
                    .await
            });
            tx.send(Ok(bytes::Bytes::from(deterministic_bytes(
                512 * 1024,
                300 + r as u64,
            ))))
            .await
            .unwrap();
            wait_until(
                Duration::from_secs(5),
                &format!("round {r} slow-PUT chunk evidence"),
                || count_chunk_files(&root) > chunks_before,
            )
            .await;
            let _ = evidence_tx.send(req_task);
        });
        let req_task = evidence_rx
            .await
            .expect("slow PUT must reach chunk evidence");
        req_task.abort();
        let _ = slow_task.await;
        harness.kill_hard().await;
        harness.settle(Duration::from_millis(50)).await;
        harness.restart().await;

        // Server served after every round (no panic state).
        let health = harness.client.get(harness.url("/healthz")).send().await;
        assert!(
            health.is_ok(),
            "server must serve after round {r} kill+restart"
        );
    }

    // Every acknowledged write is durable and byte-exact.
    let acks = acked.lock().await.clone();
    assert!(!acks.is_empty(), "ACK list must be non-empty");
    for (key, sha) in &acks {
        let resp = harness.s3_get(key).await;
        assert_eq!(
            resp.status().as_u16(),
            200,
            "ACKed key {key} must be served"
        );
        let body = resp.bytes().await.unwrap();
        assert_eq!(&sha256_hex(&body), sha, "ACKed key {key} byte-exact");
    }

    // GC fixed point: the first full run reclaims the final round's aborted
    // overwrite orphans; the second run must find nothing left to do.
    let report1 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
    eprintln!("drill6: report1={report1:?}");
    let report2 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
    eprintln!("drill6: report2={report2:?}");
    assert_eq!(
        report2.deleted_chunks, 0,
        "second GC run must delete nothing: {report2:?}"
    );
    assert_eq!(report2.orphan_chunks, 0);
    assert_eq!(report2.new_quarantine_candidates, 0);

    // All ACKed writes still intact after the GC runs.
    for (key, sha) in &acks {
        let resp = harness.s3_get(key).await;
        assert_eq!(resp.status().as_u16(), 200, "ACKed key {key} after GC");
        assert_eq!(
            &sha256_hex(&resp.bytes().await.unwrap()),
            sha,
            "ACKed key {key} byte-exact after GC"
        );
    }
}

// ===========================================================================
// DRILL 7 — PARTIAL READ RESUME (client abort mid-stream on a Range)
// ===========================================================================

/// A client aborts a ranged GET mid-body (reads the first stream item then
/// drops the connection). The server must stay healthy: a full GET is
/// byte-exact and a second ranged GET serves the exact range.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_7_partial_read_resume() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "partial";
    let payload = deterministic_bytes(2 * 1024 * 1024, 11);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "publish");

    // Open a ranged GET (bytes=0-) and abandon it after the first body item —
    // the client disconnects mid-stream, extending the disconnect pattern to
    // a Range request.
    let client = harness.client.clone();
    let token = harness.token.clone();
    let get_url = harness.url(&harness.s3_path(key));
    let abort_task = tokio::spawn(async move {
        let resp = client
            .get(get_url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Range", "bytes=0-")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 206, "satisfiable range must be 206");
        let mut stream = resp.bytes_stream();
        let first = stream.next().await;
        assert!(first.is_some(), "first body item must arrive before abort");
        // Drop `stream` here: client aborts mid-body.
    });
    abort_task.await.unwrap();

    // Server fully healthy: full GET byte-exact.
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200, "full GET after abort");
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&payload));
    assert_eq!(body.as_ref(), payload.as_slice());

    // Ranged GET bytes=0-4095 -> 206 with exactly the right 4096 bytes.
    let resp = harness
        .client
        .get(harness.url(&harness.s3_path(key)))
        .header("Authorization", format!("Bearer {}", harness.token))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        206,
        "bytes=0-4095 must be 206, got {}",
        resp.status()
    );
    let ranged = resp.bytes().await.unwrap();
    assert_eq!(ranged.len(), 4096);
    assert_eq!(ranged.as_ref(), &payload[0..4096]);
}

// ===========================================================================
// DRILL 8 — MULTIVERSION OVERWRITE KILLS (pre-publish precondition proof)
// ===========================================================================

/// Seed v1, then kill an in-flight v2 overwrite at chunk evidence — the body
/// is provably not at EOF, so publish (which happens at body EOF) cannot have
/// occurred: the committed version must still be v1 byte-exact. A subsequent
/// complete v3 PUT must make the object exactly ONE coherent v3.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill_extreme_8_multiversion_overwrite_kills() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "mv-kill";
    // NOTE: `deterministic_bytes` normalizes `state = seed | 1`, so seeds 22
    // and 23 both collapse to state 23 (identical payloads). Use distinct odd
    // seeds so v1/v2/v3 are genuinely different versions.
    let v1 = deterministic_bytes(256 * 1024, 21);
    let v2 = deterministic_bytes(256 * 1024, 25);
    let v3 = deterministic_bytes(256 * 1024, 29);
    let sha1 = sha256_hex(&v1);
    let sha2 = sha256_hex(&v2);
    let sha3 = sha256_hex(&v3);

    let put = harness.s3_put_bytes(key, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "PUT v1");
    let chunks_before = count_chunk_files(&harness.root);

    // v2 overwrite killed at chunk evidence, body NOT at EOF => provably
    // pre-publish.
    let (tx, body) = slow_body();
    let client = harness.client.clone();
    let token = harness.token.clone();
    let put_url = harness.url(&harness.s3_path(key));
    let put_task = tokio::spawn(async move {
        client
            .put(put_url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 25))))
        .await
        .unwrap();
    let root = harness.root.clone();
    wait_until(Duration::from_secs(5), "v2 chunk evidence", || {
        count_chunk_files(&root) > chunks_before
    })
    .await;

    put_task.abort();
    harness.kill_hard().await;
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;

    // STRICT precondition: the committed version is still v1, byte-exact. If
    // this ever flakes it is a real publish-before-EOF race — do not weaken.
    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "pre-publish kill must leave v1"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha1, "v1 must be byte-exact pre-publish");
    assert_eq!(body.as_ref(), v1.as_slice());

    // v3 completes fully -> 200 -> GET is exactly one coherent v3.
    let put3 = harness.s3_put_bytes(key, v3.clone()).await;
    assert_eq!(put3.status().as_u16(), 200, "PUT v3");
    let resp3 = harness.s3_get(key).await;
    assert_eq!(resp3.status().as_u16(), 200);
    let body3 = resp3.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body3), sha3, "final version is v3 byte-exact");
    assert_eq!(body3.as_ref(), v3.as_slice());

    // Extra coherence check: the object is exactly ONE coherent version from
    // the known candidate set — never torn.
    let final_hash = sha256_hex(&body3);
    assert!(
        final_hash == sha1 || final_hash == sha2 || final_hash == sha3,
        "final object must be one coherent version, got hash {final_hash}"
    );
}
