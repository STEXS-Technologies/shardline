//! Reconstruction-chaos drill (v1.6.0): corrupt the xorb object that an S3
//! record references, then verify the metadata reconstruction surface stays
//! blind to content corruption while the S3 read path fails cleanly and the
//! server never panics; a remove-file + re-PUT of the SAME bytes restores the
//! object byte-exact.
//!
//! This file is a SEPARATE integration-test crate from `fault_drills.rs` /
//! `fault_drills_extreme.rs`. The harness helpers below are duplicated
//! verbatim from those files (each integration test builds its own crate;
//! duplication keeps the drills frozen regression gates). The harness is
//! adapted additively for this drill:
//!   - frontends = [ServerFrontend::S3, ServerFrontend::Xet] (S3 for PUT/GET;
//!     Xet mounts `/v1/reconstructions/{file_id}` and the xorb transfer
//!     surface `/transfer/xorb/{prefix}/{hash}`),
//!   - `.with_reconstruction_cache_disabled()` (belt-and-suspenders; the cold
//!     path is guaranteed anyway because the drill never sends a
//!     `?content_hash=` query — see `load_reconstruction_response`, which
//!     bypasses the cache when the hash is absent),
//!   - `ServerConfig::new` receives the LIVE listener URL as the
//!     `public_base_url` (not a hardcoded 8080) so the reconstruction
//!     `fetch_info` URLs resolve to the real listener.
//!
//! Drill shape (see `reconstruction_corrupt_xorb_clean_error_and_reupload_restores`):
//!   1. S3 PUT of a 512 KiB+37 payload (multi-chunk => the ingestor xorb-packs
//!      it: the record references ONE xorb object and the S3 read path
//!      validates that xorb EAGERLY — hash + per-chunk decode — before the
//!      response is built).
//!   2. Register a reconstruction record for the SAME xorb via the Xet shard
//!      upload path (`POST /v1/shards`): the S3 protocol file id
//!      (`protocol-object-{sha256hex}`, 80 chars) is rejected by the
//!      reconstruction route's `validate_hash_path` gate (exactly 64 lowercase
//!      hex), so the drill registers the record under the shard's 64-hex file
//!      id while referencing the identical xorb object. (This gate — S3-created
//!      records are unreachable through the Xet reconstruction API — is a
//!      reported finding, not a src/ change in this lane.)
//!   3. Locate the record-referenced xorb via the metadata.sqlite3 `latest`
//!      record (`chunks[0].hash`), snapshot it, and corrupt it in place.
//!   4. The reconstruction route (`/v1/reconstructions/{file_id}`, Xet
//!      frontend, no `?content_hash=`) is metadata-only: it still returns 200
//!      with terms + fetch_info pointing at the corrupted xorb — the record
//!      lookup is blind to content corruption; what breaks is the xorb LOAD.
//!   5. Two legs hit the corrupt bytes:
//!        - the S3 GET fails cleanly with >= 400 (XorbHashMismatch /
//!          InvalidSerializedXorb map to BAD_REQUEST), never a byte-exact 200,
//!          never a panic;
//!        - the xorb transfer route serves the corrupt bytes undetected
//!          (REPORT-ONLY finding — content validation lives only on the
//!          S3/native read paths).
//!   6. The server stays healthy (unrelated PUT/GET byte-exact, healthz 200,
//!      panic status Running).
//!   7. Repair = remove the corrupt xorb FIRST (a re-PUT without removal would
//!      hit put_if_absent dedup `AlreadyExists` on the identical hash and
//!      leave the corruption in place), then re-PUT the SAME bytes: S3 GET is
//!      byte-exact again and the reconstruction route still serves the same
//!      terms.

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

use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server::{ServerConfig, ServerFrontend, ServerRole, app};
use shardline_server_core::{AuthProvider, auth::LocalHmacProvider};
use std::{
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    time::Duration,
};
use tempfile::TempDir;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

// ---------------------------------------------------------------------------
// Auth / tokens — same signing key as the server, identical across restarts.
// ---------------------------------------------------------------------------

const TEST_SIGNING_KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

/// The S3 bucket name (`{owner}.{name}`) the drill operates on.
const BUCKET: &str = "drill.drill";

fn mint_token(owner: &str, name: &str, scope: TokenScope) -> String {
    let provider = LocalHmacProvider::new(TEST_SIGNING_KEY).unwrap();
    let repo = RepositoryScope::new(RepositoryProvider::Generic, owner, name, None).unwrap();
    let claims = TokenClaims::new("shardline", "recon-chaos", scope, repo, u64::MAX).unwrap();
    provider.mint_token(&claims).unwrap()
}

// ---------------------------------------------------------------------------
// Small deterministic helpers (verbatim from fault_drills_extreme.rs).
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

// ---------------------------------------------------------------------------
// DrillHarness — spawn / kill / restart on the same root directory.
// ---------------------------------------------------------------------------

#[derive(Debug)]
enum PanicStatus {
    Running,
    AbortedExpected,
    Panicked(String),
    EarlyExit,
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
            _tmp: tmp,
        }
    }

    /// Deltas vs fault_drills_extreme::DrillHarness::build_config:
    ///   (a) frontends = [S3, Xet] — S3 serves PUT/GET; Xet mounts
    ///       `/v1/reconstructions/{file_id}` and the `/transfer/xorb/...`
    ///       transfer surface;
    ///   (b) `.with_reconstruction_cache_disabled()` — belt-and-suspenders;
    ///       the cold path is guaranteed anyway by never sending
    ///       `?content_hash=`;
    ///   (c) the LIVE listener URL is passed as the `public_base_url` so the
    ///       reconstruction `fetch_info` URLs resolve to the real listener.
    fn build_config(&self, addr: SocketAddr, base_url: &str) -> ServerConfig {
        ServerConfig::new(
            addr,
            base_url.to_owned(),
            self.root.clone(),
            self.chunk_size,
        )
        .with_server_role(ServerRole::All)
        .with_server_frontends(vec![ServerFrontend::S3, ServerFrontend::Xet])
        .unwrap()
        .with_token_signing_key(TEST_SIGNING_KEY.to_vec())
        .unwrap()
        .with_reconstruction_cache_disabled()
        .with_s3_upload_session_ttl_seconds(self.session_ttl_seconds)
        .unwrap()
    }

    async fn spawn_server(&mut self) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let config = self.build_config(addr, &base_url);
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

    /// Inspect the serve task: Panicked => server panicked (FAIL), EarlyExit =>
    /// server exited cleanly on its own (FAIL), AbortedExpected => our kill,
    /// Running => healthy. (Verbatim from chaos_runner.rs.)
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
// On-disk evidence helpers.
// ---------------------------------------------------------------------------

/// Flip one byte at a deterministic offset, preserving the file length exactly
/// so the storage-length metadata stays consistent. The offset is chosen INSIDE
/// the fetch window used below (`bytes=0-4095`) so the xorb-transfer leg
/// provably serves the flipped byte. Detection on the S3 path is whole-file
/// (xorb hash / per-chunk decode), so any offset triggers it.
fn corrupt_file(path: &Path) {
    let mut data = std::fs::read(path).unwrap();
    assert!(
        data.len() > 2048,
        "corruption target must be > 2048 bytes at {path:?}"
    );
    data[2048] ^= 0xFF;
    std::fs::write(path, &data).unwrap();
}

/// `(file_id, first_chunk_hash)` of the most recent `latest` file record. The
/// upload ingestor rewrites every `FileChunkRecord.hash` to the stored xorb's
/// hash (xorb packing), so `chunks[0].hash` is the hash of the SINGLE object
/// file the read path actually loads — the xorb at
/// `chunks/xorbs/default/{hash[..2]}/{hash}.xorb`. `file_id` is the same
/// record's deterministic protocol-object identifier, used for the
/// reconstruction lookup.
fn latest_record(root: &Path) -> Option<(String, String)> {
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
    let file_id = value.get("file_id")?.as_str()?.to_owned();
    let hash = value
        .get("chunks")?
        .as_array()?
        .first()?
        .get("hash")?
        .as_str()?
        .to_owned();
    Some((file_id, hash))
}

// ===========================================================================
// DRILL — CORRUPT XORB: CLEAN ERROR ON S3, METADATA-ONLY RECONSTRUCTION,
// RE-UPLOAD RESTORES BYTE-EXACT
// ===========================================================================

/// Corrupt the record-referenced xorb, then verify:
///   - the reconstruction route stays 200 metadata (terms + fetch_info URL
///     referencing the corrupted xorb — the metadata path is blind to content
///     corruption; what breaks is the xorb LOAD, not the record lookup);
///   - the S3 GET fails cleanly (>= 400, expected exactly 400) and never a
///     panic;
///   - the xorb transfer route serves the corrupt bytes undetected (a
///     REPORT-ONLY finding: content validation lives only on the S3/native
///     read paths);
///   - the server stays healthy;
///   - repair = remove-file + re-PUT of the SAME bytes restores the object
///     byte-exact and the reconstruction route serves the same terms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconstruction_corrupt_xorb_clean_error_and_reupload_restores() {
    let mut harness = DrillHarness::new(3600);
    harness.spawn_server().await;

    // 1. Seed object: multi-chunk payload so the ingestor xorb-packs it into a
    //    single record-referenced xorb object.
    let key = "recon-corrupt";
    let payload = deterministic_bytes(512 * 1024 + 37, 1);
    let put = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(put.status().as_u16(), 200, "seed PUT");

    // 2. The reconstruction route rejects the S3 protocol file id
    //    (`protocol-object-{sha256hex}` is not 64-hex), so register a
    //    reconstruction record for the SAME xorb via the Xet shard path: the
    //    shard references the S3-created xorb hash and yields a 64-hex file id
    //    the reconstruction route accepts. (Reported finding — see header.)
    let (_protocol_file_id, first_hash) =
        latest_record(&harness.root).expect("latest file record must exist after PUT");
    let (shard_bytes, shard_file_id) =
        shardline_server::test_fixtures::single_file_shard(&[(b"recon-chaos-shard", &first_hash)]);
    let shard_resp = harness
        .client
        .post(harness.url("/v1/shards"))
        .header("Authorization", format!("Bearer {}", harness.token))
        .header("Content-Type", "application/octet-stream")
        .body(shard_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(
        shard_resp.status().as_u16(),
        200,
        "shard registration for the S3-created xorb"
    );
    eprintln!("recon-chaos: xorb_hash={first_hash} shard_file_id={shard_file_id}");

    // 3. Locate the record-referenced xorb and snapshot it (fresh root + zero
    //    dedup => the latest record is this object's).
    let target = harness
        .root
        .join("chunks")
        .join("xorbs")
        .join("default")
        .join(&first_hash[..2])
        .join(format!("{first_hash}.xorb"));
    assert!(
        target.is_file(),
        "record-referenced xorb must exist at {target:?}"
    );
    let snapshot = std::fs::read(&target).unwrap();
    let snapshot_sha = sha256_hex(&snapshot);
    eprintln!(
        "recon-chaos: xorb size={} sha={snapshot_sha}",
        snapshot.len()
    );

    // 4. Corrupt the xorb in place (length-preserving; byte 2048 so the fetch
    //    leg's bytes=0-4095 provably includes it).
    corrupt_file(&target);

    // 5. Reconstruction route — metadata-only: must STILL return 200 with
    //    terms and a fetch_info URL referencing the (now corrupt) xorb. No
    //    `?content_hash=` query => cold path (cache bypassed anyway).
    let recon = harness
        .client
        .get(harness.url(&format!("/v1/reconstructions/{shard_file_id}")))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await
        .unwrap();
    let recon_status = recon.status().as_u16();
    assert_eq!(
        recon_status, 200,
        "reconstruction route must stay 200 (metadata-only path), got {recon_status}"
    );
    let recon_json: serde_json::Value = recon.json().await.unwrap();
    let terms = recon_json
        .get("terms")
        .and_then(serde_json::Value::as_array)
        .expect("terms array");
    assert!(!terms.is_empty(), "terms must be non-empty");
    let fetch_info = recon_json
        .get("fetch_info")
        .and_then(serde_json::Value::as_object)
        .expect("fetch_info object");
    assert_eq!(fetch_info.len(), 1, "single xorb => one fetch entry");
    let (fetch_hash, entries) = fetch_info.iter().next().unwrap();
    assert_eq!(
        fetch_hash.as_str(),
        first_hash,
        "fetch_info must be keyed by the record chunk (xorb) hash"
    );
    let entry = entries
        .as_array()
        .and_then(|array| array.first())
        .expect("fetch entry");
    let fetch_url = entry
        .get("url")
        .and_then(serde_json::Value::as_str)
        .expect("fetch url");
    assert!(
        fetch_url.starts_with(&harness.base_url)
            && fetch_url.ends_with(&format!("/transfer/xorb/default/{first_hash}")),
        "fetch_info URL must resolve to the live listener's transfer route: {fetch_url}"
    );
    eprintln!(
        "recon-chaos: reconstruction 200 — metadata path blind to corruption, fetch_info[0].url={fetch_url}"
    );

    // 5. Two legs hitting the corrupt bytes.
    //    Leg A — the S3 read path validates the xorb EAGERLY: a clean >= 400
    //    (XorbHashMismatch / InvalidSerializedXorb map to BAD_REQUEST), never a
    //    byte-exact 200, never a panic.
    let corrupt_get = harness.s3_get(key).await;
    let corrupt_status = corrupt_get.status().as_u16();
    eprintln!(
        "recon-chaos: corrupt S3 GET status={corrupt_status} (expected >= 400, ideally exactly 400)"
    );
    assert!(
        corrupt_status >= 400,
        "corrupt xorb must fail cleanly on the S3 read, got {corrupt_status}"
    );
    // Drain the body (may be empty for error envelopes) so the connection is
    // fully consumed before the next request.
    let _corrupt_body = corrupt_get.bytes().await.unwrap_or_default();

    //    Leg B — the xorb transfer surface serves the corrupt bytes
    //    undetected. REPORT-ONLY finding: content validation lives only on the
    //    S3/native-download paths. Assert the range provably hits the flipped
    //    byte (body != snapshot prefix); if the route were absent on this role
    //    the request error is noted, not fatal.
    let transfer = harness
        .client
        .get(harness.url(&format!("/transfer/xorb/default/{first_hash}")))
        .header("Authorization", format!("Bearer {}", harness.token))
        .header("Range", "bytes=0-4095")
        .send()
        .await;
    match transfer {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let body = resp.bytes().await.unwrap_or_default();
            let prefix_len = snapshot.len().min(body.len());
            let differs = body.as_ref() != &snapshot[..prefix_len];
            eprintln!(
                "recon-chaos: REPORT-ONLY FINDING — xorb transfer route serves corrupt bytes \
                 undetected; content validation lives only on S3/native-download paths \
                 (status={status}, body_len={}, hits_corrupt_bytes={differs})",
                body.len()
            );
            assert!(
                differs,
                "fetch leg range bytes=0-4095 must include the flipped byte (offset 2048)"
            );
        }
        Err(error) => {
            eprintln!(
                "recon-chaos: transfer leg request error {error} — noted, not fatal (report-only)"
            );
        }
    }

    // 6. Server healthy: unrelated PUT + GET byte-exact, healthz 200, and the
    //    serve task reports Running (never panicked).
    let other_key = "healthy-key";
    let other_payload = deterministic_bytes(64 * 1024 + 3, 2);
    let put2 = harness.s3_put_bytes(other_key, other_payload.clone()).await;
    assert_eq!(
        put2.status().as_u16(),
        200,
        "unrelated PUT after corruption"
    );
    let get2 = harness.s3_get(other_key).await;
    assert_eq!(
        get2.status().as_u16(),
        200,
        "unrelated GET after corruption"
    );
    assert_eq!(
        get2.bytes().await.unwrap().as_ref(),
        other_payload.as_slice(),
        "unrelated GET byte-exact"
    );
    let health = harness
        .client
        .get(harness.url("/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status().as_u16(), 200, "healthz after corruption");
    match harness.panic_status().await {
        PanicStatus::Running => {}
        PanicStatus::AbortedExpected => {
            panic!("server must not panic after corrupt reads: AbortedExpected")
        }
        PanicStatus::Panicked(msg) => panic!("server must not panic after corrupt reads: {msg:?}"),
        PanicStatus::EarlyExit => panic!("server must not panic after corrupt reads: EarlyExit"),
    }

    // 7. Re-upload restores. Remove the corrupt xorb FIRST — put_if_absent
    //    dedup would 409/AlreadyExists on the identical content hash and leave
    //    the corruption in place; with the file absent the fresh write takes
    //    the Inserted path under the same hash => same path, correct bytes.
    std::fs::remove_file(&target).unwrap();
    let repair = harness.s3_put_bytes(key, payload.clone()).await;
    assert_eq!(repair.status().as_u16(), 200, "repair re-PUT");
    let resp = harness.s3_get(key).await;
    assert_eq!(resp.status().as_u16(), 200, "repaired object serves 200");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), payload.as_slice(), "repaired byte-exact");

    // Reconstruction still serves the SAME metadata after the repair (the
    // shard record is unchanged; the re-upload rewrote the protocol record).
    let recon2 = harness
        .client
        .get(harness.url(&format!("/v1/reconstructions/{shard_file_id}")))
        .header("Authorization", format!("Bearer {}", harness.token))
        .send()
        .await
        .unwrap();
    assert_eq!(recon2.status().as_u16(), 200, "reconstruction after repair");
    let recon2_json: serde_json::Value = recon2.json().await.unwrap();
    assert_eq!(
        recon2_json.get("terms"),
        recon_json.get("terms"),
        "reconstruction terms identical after repair"
    );

    // Server still healthy.
    let health2 = harness
        .client
        .get(harness.url("/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(health2.status().as_u16(), 200, "healthz after repair");
    match harness.panic_status().await {
        PanicStatus::Running => {}
        PanicStatus::AbortedExpected => {
            panic!("server must not panic after repair: AbortedExpected")
        }
        PanicStatus::Panicked(msg) => panic!("server must not panic after repair: {msg:?}"),
        PanicStatus::EarlyExit => panic!("server must not panic after repair: EarlyExit"),
    }
    eprintln!("recon-chaos: PASS — clean error, metadata-only reconstruction, byte-exact repair");
}
