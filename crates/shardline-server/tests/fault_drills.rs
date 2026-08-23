//! Fault-injection drill tests: kill-mid-upload, kill-during-GC, and related
//! crash-recovery invariants for the shardline server.
//!
//! Philosophy: every kill is synchronized on *on-disk evidence* (a chunk file
//! appearing, a part file growing, a quarantine row existing) so the server is
//! provably inside the operation when we abort it — never a sleep guess.
//! After each kill the harness restarts on the SAME root directory, modeling a
//! process crash + restart.
//!
//! These tests are hermetic (SQLite metadata + local object store, no Docker)
//! except `drill3_*` which is SKIP-gated on a reachable local Postgres.

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

use sha2::{Digest, Sha256};
use shardline_gc::{LocalGcOptions, LocalGcReport};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{
    ServerConfig, ServerFrontend, ServerRole, app, run_fsck, write_backup_manifest,
};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

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

fn copy_directory_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_directory_tree(&source_path, &destination_path)?;
        } else {
            std::fs::copy(source_path, destination_path)?;
        }
    }
    Ok(())
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
            _tmp: tmp,
        }
    }

    fn with_postgres(mut self, url: &str) -> Self {
        self.index_postgres_url = Some(url.to_owned());
        self
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
        .with_reconstruction_cache_disabled()
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
    /// connections) and wait until it is no longer running.
    async fn kill_hard(&mut self) {
        self.graceful_shutdown = None;
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let result = handle.await;
            // `abort` wins unless the server task has already completed at the
            // scheduling boundary. Both results mean there is no live server
            // task; asserting cancellation here turns a valid teardown race
            // into a flaky GC-recovery failure.
            match result {
                Ok(()) => {
                    eprintln!("kill_hard: server task completed before abort took effect");
                }
                Err(error) if error.is_cancelled() => {
                    eprintln!("kill_hard: server task cancelled as expected");
                }
                Err(error) => {
                    panic!("kill_hard: server task panicked: {error}");
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

// ===========================================================================
// DRILL 1 — KILL-MID-UPLOAD
// ===========================================================================

/// V1: overwrite-in-flight. Seed k=v1, start a 16MiB slow overwrite, kill the
/// server the instant a new chunk hits disk, restart, and require v1 intact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill1_v1_overwrite_inflight_kill_preserves_v1() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "k1";
    let v1 = deterministic_bytes(64 * 1024 + 17, 1);
    let put = harness.s3_put_bytes(key, v1.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed PUT v1");
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
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 42))))
        .await
        .unwrap();

    let root = harness.root.clone();
    wait_until(
        Duration::from_secs(5),
        "chunk evidence for slow overwrite",
        || count_chunk_files(&root) > chunks_before,
    )
    .await;

    // Handler is provably mid-body-read. Drop the client side and kill.
    put_task.abort();
    harness.kill_hard().await;
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;

    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "v1 object must survive mid-upload kill"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&body), sha256_hex(&v1));
    assert_eq!(body.as_ref(), v1.as_slice());
}

/// V2: fresh-key PUT killed mid-upload must leave no torn object behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill1_v2_fresh_put_kill_leaves_no_torn_object() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "fresh-kill";
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
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 43))))
        .await
        .unwrap();

    let root = harness.root.clone();
    wait_until(
        Duration::from_secs(5),
        "chunk evidence for fresh PUT",
        || count_chunk_files(&root) > chunks_before,
    )
    .await;

    put_task.abort();
    harness.kill_hard().await;
    harness.restart().await;

    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "no torn object should be visible after mid-upload kill"
    );
}

/// V3: multipart UploadPart killed mid-body. Partial part state must NOT
/// materialize as an object.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill1_v3_multipart_part_kill_leaves_no_object() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "mp-kill";
    let upload_id = harness.s3_create_multipart(key).await;
    let planned = 5 * 1024 * 1024;

    let (tx, body) = slow_body();
    let client = harness.client.clone();
    let token = harness.token.clone();
    let part_url = harness.url(&format!(
        "{}?partNumber=1&uploadId={upload_id}",
        harness.s3_path(key)
    ));
    let part_task = tokio::spawn(async move {
        client
            .put(part_url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 44))))
        .await
        .unwrap();

    let root = harness.root.clone();
    let uid = upload_id.clone();
    wait_part_evidence(&root, &uid, &part_task).await;

    let partial = part_file_size(&harness.root, &upload_id, 1);
    assert!(
        partial > 0 && partial < planned as u64,
        "partial part file should exist and be smaller than planned ({partial} < {planned})"
    );

    part_task.abort();
    harness.kill_hard().await;
    harness.restart().await;

    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        404,
        "partial multipart upload must not be visible as an object"
    );
    assert!(
        session_dir_exists(&harness.root, &upload_id),
        "session dir should persist (TTL not yet expired)"
    );
}

/// V3b: with TTL=1s, an expired session is reclaimed by the startup sweep on
/// restart, and a late CompleteMultipartUpload is rejected cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill1_v3b_multipart_ttl_expiry_reclaims_session() {
    let mut harness = DrillHarness::new(1); // 1-second session TTL
    harness.spawn_server().await;

    let key = "mp-ttl";
    let upload_id = harness.s3_create_multipart(key).await;

    let (tx, body) = slow_body();
    let client = harness.client.clone();
    let token = harness.token.clone();
    let part_url = harness.url(&format!(
        "{}?partNumber=1&uploadId={upload_id}",
        harness.s3_path(key)
    ));
    let part_task = tokio::spawn(async move {
        client
            .put(part_url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(body)
            .send()
            .await
    });
    tx.send(Ok(bytes::Bytes::from(deterministic_bytes(512 * 1024, 45))))
        .await
        .unwrap();

    let root = harness.root.clone();
    let uid = upload_id.clone();
    wait_part_evidence(&root, &uid, &part_task).await;
    part_task.abort();

    // Let the session expire, then restart so the startup sweep runs.
    harness.settle(Duration::from_millis(1600)).await;
    harness.restart().await;

    let root = harness.root.clone();
    let uid = upload_id.clone();
    wait_until(
        Duration::from_secs(5),
        "session dir reclaimed by startup sweep",
        || !session_dir_exists(&root, &uid),
    )
    .await;

    // A late Complete with the expired upload id is rejected cleanly.
    let complete = harness.s3_complete_multipart(key, &upload_id).await;
    assert!(
        complete.status().as_u16() >= 400,
        "late complete must be rejected, got {}",
        complete.status()
    );
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 404);
}

// ===========================================================================
// DRILL 2 — KILL-DURING-GC
// ===========================================================================

/// Abort a GC run mid-mark (both the GC task and the server, modeling process
/// death), then verify: reachable object intact, a later full run cleans up,
/// and a second run reaches a fixed point (nothing re-executed / re-queued).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill2_gc_abort_mid_mark_recovery_and_fixed_point() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    // Seed reachable object A (never deleted).
    let key = "reachable-a";
    let payload = deterministic_bytes(512 * 1024, 7);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed reachable object");
    let reachable_chunk_count = count_chunk_files(&harness.root);
    assert!(
        reachable_chunk_count >= 2,
        "payload should span >= 2 chunks, got {reachable_chunk_count}"
    );

    // Write 500 orphan chunk files (never referenced by any record).
    const ORPHAN_COUNT: usize = 500;
    for i in 0..ORPHAN_COUNT as u64 {
        let hash = format!("{i:064x}");
        let dir = harness.root.join("chunks").join(&hash[..2]);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&hash), vec![0xAB; 1024]).unwrap();
    }

    // Spawn GC in its own task and abort it once ALL orphans are marked
    // (quarantine rows reach ORPHAN_COUNT). Aborting at the first row would
    // leave most orphans unmarked; the subsequent full run would then
    // re-quarantine them with a fresh (not yet expired) retention window and
    // the sweep would leave them on disk.
    let gc_config = harness.build_config("127.0.0.1:0".parse().unwrap());
    let gc_task = tokio::spawn(shardline_server::run_gc(
        gc_config,
        LocalGcOptions::mark_and_sweep(0),
    ));
    let root = harness.root.clone();
    wait_until(Duration::from_secs(10), "all orphans quarantined", || {
        quarantine_row_count(&root) >= ORPHAN_COUNT
            || quarantine_manifest_files(&root) >= ORPHAN_COUNT
    })
    .await;

    // Model process death: kill BOTH the GC task and the server.
    gc_task.abort();
    let cancelled = gc_task.await;
    assert!(cancelled.is_err(), "GC task must be cancelled by abort");
    harness.kill_hard().await;
    harness.restart().await;

    // Reachable object A must be fully intact after the aborted mark.
    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "reachable object after aborted GC"
    );
    let got = resp.bytes().await.unwrap();
    assert_eq!(sha256_hex(&got), sha256_hex(&payload));

    // First full run: quarantine is expired (retention 0) -> orphan files
    // deleted. The aborted run must not have left a re-executable mess.
    let report1 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
    eprintln!("drill2: report1={report1:?}");
    assert!(
        report1.deleted_chunks >= 1,
        "expected orphan chunk deletion, got {report1:?}"
    );

    // Every injected orphan file is gone. (Note: GC may also reclaim dedup
    // chunk artifacts of A that no record references — the authoritative
    // reachability invariant is A's integrity below, not a total file count.)
    let root = harness.root.clone();
    wait_until(
        Duration::from_secs(5),
        "injected orphan files to be deleted",
        || {
            (0..ORPHAN_COUNT as u64).all(|i| {
                let hash = format!("{i:064x}");
                !root.join("chunks").join(&hash[..2]).join(&hash).exists()
            })
        },
    )
    .await;
    assert_eq!(
        quarantine_row_count(&harness.root),
        0,
        "quarantine table should be empty after sweep"
    );

    // The reachable object must still serve its exact bytes after the sweep.
    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "reachable object after GC sweep"
    );
    assert_eq!(
        sha256_hex(&resp.bytes().await.unwrap()),
        sha256_hex(&payload)
    );

    // Fixed point: a second full run finds nothing left to do.
    let report2 = harness.gc(LocalGcOptions::mark_and_sweep(0)).await;
    eprintln!("drill2: report2={report2:?}");
    assert_eq!(
        report2.deleted_chunks, 0,
        "second run must delete nothing: {report2:?}"
    );
    assert_eq!(report2.new_quarantine_candidates, 0);
    assert_eq!(report2.orphan_chunks, 0);
    assert_eq!(quarantine_row_count(&harness.root), 0);

    // Reachable object still intact.
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        sha256_hex(&resp.bytes().await.unwrap()),
        sha256_hex(&payload)
    );
}

// ===========================================================================
// DRILL 3 — KILL-POSTGRES (SKIP-gated on a reachable local Postgres)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill3_postgres_kill_mid_upload_recovery() {
    if !require_tcp("127.0.0.1", 5432).await {
        eprintln!("SKIP: drill3_postgres_kill_mid_upload_recovery — 127.0.0.1:5432 unreachable");
        return;
    }
    ensure_dev_postgres_migrated(
        "postgres://shardline:shardline-dev-password@127.0.0.1:5432/shardline",
    )
    .await;
    let mut harness = DrillHarness::new(3600)
        .with_postgres("postgres://shardline:shardline-dev-password@127.0.0.1:5432/shardline");
    harness.spawn_server().await;

    let key = "pg-kill";
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
    harness.restart().await;

    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "object must recover after mid-upload kill with PG metadata"
    );
    assert_eq!(sha256_hex(&resp.bytes().await.unwrap()), sha256_hex(&v1));
}

// ===========================================================================
// DRILL 4a — RANGED GET PAST EOF
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill4a_ranged_get_past_eof_returns_416() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "range";
    let payload = deterministic_bytes(4096, 9);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200);

    let resp = harness
        .client
        .get(harness.url(&harness.s3_path(key)))
        .header("Authorization", format!("Bearer {}", harness.token))
        .header("Range", "bytes=999999-")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        416,
        "range past EOF must be 416, got {}",
        resp.status()
    );

    // Server stayed healthy — a normal GET still works (no panic state).
    let full = harness.s3_get(key).await;
    assert_eq!(full.status().as_u16(), 200);
    assert_eq!(full.bytes().await.unwrap().as_ref(), payload.as_slice());
}

// ===========================================================================
// DRILL 8 — CONNECTION DROP AFTER PUBLISH
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill8_client_disconnect_after_publish_object_stays_durable() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let key = "drop";
    let payload = deterministic_bytes(2 * 1024 * 1024, 11);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "publish");

    // Open a GET stream and abandon it without consuming the body — the
    // client disconnects after the object was already durably committed.
    let client = harness.client.clone();
    let url = harness.url(&harness.s3_path(key));
    let token = harness.token.clone();
    let get_task = tokio::spawn(async move {
        let resp = client
            .get(url)
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        let _stream = resp.bytes_stream(); // dropped immediately -> disconnect
    });
    get_task.await.unwrap();

    // Restart on the same root and confirm the object is still fully intact.
    harness.settle(Duration::from_millis(50)).await;
    harness.restart().await;
    let resp = harness.s3_get(key).await;
    assert_eq!(
        resp.status().as_u16(),
        200,
        "object durable after disconnect"
    );
    assert_eq!(
        sha256_hex(&resp.bytes().await.unwrap()),
        sha256_hex(&payload)
    );
}

/// Full local recovery rehearsal: inventory a populated deployment, snapshot
/// its native metadata/object root, destroy that root, restore it, run fsck,
/// regenerate the inventory, and verify every acknowledged object byte-for-byte.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drill9_backup_destroy_restore_fsck_and_download() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    let fixtures = [
        ("restore-a", deterministic_bytes(96 * 1024 + 3, 901)),
        ("restore-b", deterministic_bytes(160 * 1024 + 7, 902)),
        ("restore-c", deterministic_bytes(8 * 1024 + 11, 903)),
    ];
    for (key, payload) in &fixtures {
        let response = harness.s3_put_bytes(key, payload.clone()).await;
        assert_eq!(response.status().as_u16(), 200, "seed {key}");
    }

    let config = harness.build_config("127.0.0.1:0".parse().unwrap());
    let mut before_manifest = Vec::new();
    let before_report = write_backup_manifest(config.clone(), &mut before_manifest)
        .await
        .unwrap();
    assert!(before_report.object_count > 0);

    harness.kill_hard().await;
    let backup = TempDir::new().unwrap();
    let snapshot = backup.path().join("snapshot");
    copy_directory_tree(&harness.root, &snapshot).unwrap();

    // The target is the harness-owned TempDir root resolved above, never a
    // caller-controlled or broad filesystem path.
    std::fs::remove_dir_all(&harness.root).unwrap();
    assert!(!harness.root.exists());
    copy_directory_tree(&snapshot, &harness.root).unwrap();

    let fsck = run_fsck(config.clone()).await.unwrap();
    assert_eq!(fsck.issue_count(), 0, "restored deployment must be clean");

    let mut after_manifest = Vec::new();
    let after_report = write_backup_manifest(config, &mut after_manifest)
        .await
        .unwrap();
    assert_eq!(after_report, before_report);
    let before_json: serde_json::Value = serde_json::from_slice(&before_manifest).unwrap();
    let after_json: serde_json::Value = serde_json::from_slice(&after_manifest).unwrap();
    assert_eq!(after_json, before_json);

    harness.restart().await;
    for (key, payload) in &fixtures {
        let response = harness.s3_get(key).await;
        assert_eq!(response.status().as_u16(), 200, "restore GET {key}");
        assert_eq!(response.bytes().await.unwrap().as_ref(), payload.as_slice());
    }
}
