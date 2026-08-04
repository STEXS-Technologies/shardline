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

//! End-to-end tests exercising the sdx client against a real in-process
//! shardline server over HTTP.
//!
//! The server boots on an ephemeral port with a local backend, a token
//! signing key, and the provider-issuance surface (bootstrap key `bootstrap`,
//! repository `github/team/assets@main`). Files are uploaded through the
//! server's own ingest (`ServerBackend::upload_file`, server-side CDC) and
//! downloaded through the full sdx stack: provider key → read token → V2
//! reconstruction → ranged `/transfer/xorb` fetch → chunk decode → assembly.
//!
//! Chunk target is 128 bytes so a ~1.3 MiB file spans multiple xorbs
//! (xorb cut = 8192 chunks ≈ 1 MiB) and exercises multi-xorb, multi-range
//! reconstruction.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    ops::RangeInclusive,
    path::Path,
    time::{Duration, Instant},
};

use bytes::Bytes;
use sdx::{Auth, RepositoryId, StreamLimits, XetClientBuilder};
use shardline_protocol::{RepositoryProvider, RepositoryScope};
use shardline_server::{BenchmarkBackend, ServerConfig, ServerFrontend, ServerRole};
use tempfile::TempDir;
use tokio::{net::TcpListener, task::JoinHandle};

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

/// In-process shardline server with the sdx-facing surface enabled.
struct TestServer {
    base_url: String,
    port: u16,
    upload: BenchmarkBackend,
    _dir: TempDir,
    _task: JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with_chunk_size(NonZeroUsize::new(128).unwrap()).await
    }

    async fn start_with_chunk_size(chunk_size: NonZeroUsize) -> Self {
        configure_test_pools();
        let dir = TempDir::new().unwrap();
        let cfg_dir = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let config_path = cfg_dir.join("providers.json");
        std::fs::write(&config_path, PROVIDER_CONFIG).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());

        let upload = BenchmarkBackend::isolated_local(
            dir.path().to_path_buf(),
            base_url.clone(),
            chunk_size,
            NonZeroUsize::new(64).unwrap(),
        )
        .await
        .unwrap();

        let config =
            ServerConfig::new(addr, base_url.clone(), dir.path().to_path_buf(), chunk_size)
                .with_server_role(ServerRole::All)
                .with_server_frontends(vec![ServerFrontend::Xet])
                .unwrap()
                .with_token_signing_key(SIGNING_KEY.to_vec())
                .unwrap()
                .with_provider_runtime(
                    config_path,
                    BOOTSTRAP_KEY.as_bytes().to_vec(),
                    "test-issuer".to_owned(),
                    // The provider token TTL must clear sdx's 30s refresh buffer
                    // (SDX_PLAN.md §5.2) or M1's loop guard rejects the token.
                    NonZeroU64::new(3_600).unwrap(),
                )
                .unwrap();

        let app = shardline_server::app::router(config).await.unwrap();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        wait_ready(&base_url).await;

        Self {
            base_url,
            port: addr.port(),
            upload,
            _dir: dir,
            _task: task,
        }
    }

    /// Uploads `data` under `file_id`, scoped to `github/team/assets@main`.
    async fn upload(&self, file_id: &str, data: &[u8]) {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"))
                .unwrap();
        self.upload
            .upload_file(file_id, Bytes::from(data.to_vec()), Some(&scope))
            .await
            .unwrap();
    }

    fn client(&self) -> sdx::XetClient {
        self.client_with(None, None)
    }

    /// Builds a client with an on-disk chunk cache rooted at `cache_dir`
    /// (plus the given memory cap / limits).
    fn client_with_cache_dir(
        &self,
        cache_dir: &Path,
        buffer_cap: Option<u64>,
        limits: Option<StreamLimits>,
    ) -> sdx::XetClient {
        self.client_with_base(buffer_cap, limits, Some(cache_dir))
    }

    /// Builds a client with optional streaming options: a fixed buffer
    /// capacity (memory bound) and/or custom prefetch limits.
    fn client_with(&self, buffer_cap: Option<u64>, limits: Option<StreamLimits>) -> sdx::XetClient {
        self.client_with_base(buffer_cap, limits, None)
    }

    fn client_with_base(
        &self,
        buffer_cap: Option<u64>,
        limits: Option<StreamLimits>,
        cache_dir: Option<&Path>,
    ) -> sdx::XetClient {
        let auth = Auth::new(
            &self.base_url,
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_api_key(BOOTSTRAP_KEY.to_owned())
        .with_subject(SUBJECT.to_owned());
        let mut builder = XetClientBuilder::new()
            .endpoint(format!(
                "xet://127.0.0.1:{}/github/team/assets/main",
                self.port
            ))
            .auth(auth);
        if let Some(cap) = buffer_cap {
            builder = builder.with_buffer_semaphore(cap);
        }
        if let Some(limits) = limits {
            builder = builder.with_stream_limits(limits);
        }
        if let Some(cache_dir) = cache_dir {
            builder = builder.with_chunk_cache_dir(cache_dir);
        }
        builder.build().unwrap()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._task.abort();
    }
}

/// Raises the in-process server's execution-pool sizes once per process.
///
/// The server reads `SHARDLINE_PARSING_POOL_SIZE` / `SHARDLINE_HASHING_POOL_SIZE`
/// at router-build time (`crates/shardline-server/src/app.rs`). The default
/// 8-parsing-permit pool is too small for several concurrent streaming
/// downloads (each issues 2-3 reconstruction prefetch requests that hold a
/// parsing permit while the term list is generated), which would surface as
/// transient 503 "server work queue is saturated" errors. Set once, before any
/// server boots, so every `TestServer` in this process gets the larger pools.
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
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match client.get(format!("{base_url}/readyz")).send().await {
                Ok(response) if response.status().is_success() => return,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("in-process server did not become ready");
}

fn deterministic_random(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed;
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u8
        })
        .collect()
}

async fn download_file(client: &sdx::XetClient, file_id: &str) -> Vec<u8> {
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("out.bin");
    client
        .download_session()
        .download_file(file_id, &dest)
        .await
        .unwrap();
    tokio::fs::read(&dest).await.unwrap()
}

async fn download_range(
    client: &sdx::XetClient,
    file_id: &str,
    range: RangeInclusive<u64>,
) -> Vec<u8> {
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("out.bin");
    client
        .download_session()
        .download_range(file_id, range, &dest)
        .await
        .unwrap();
    tokio::fs::read(&dest).await.unwrap()
}

fn hex_id(digit: char) -> String {
    let mut id = String::with_capacity(64);
    for _ in 0..64 {
        id.push(digit);
    }
    id
}

const MULTI_XORB_ID: char = 'a';
const SINGLE_XORB_ID: char = 'b';
const TINY_ID: char = 'c';
const ZEROS_ID: char = 'd';
const RANDOM_ID: char = 'e';
const UNKNOWN_ID: char = 'f';
const MEMORY_ID: char = '1';

/// Current resident set size of this process in KiB (Linux `/proc/self/status`).
fn current_rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.trim().trim_end_matches(" kB").parse().unwrap_or(0);
        }
    }
    0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_full_multi_xorb_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // ~1.3 MiB: with a 128-byte chunk target this spans >8192 chunks, so the
    // file crosses the xorb cut and exercises multi-xorb reconstruction.
    let data = deterministic_random(1_310_720, 0x5eed);
    let file_id = hex_id(MULTI_XORB_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded.len(), data.len());
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_single_xorb_multi_chunk_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // ~64 KiB → ~512 chunks → a single xorb, many terms.
    let data = deterministic_random(65_536, 0xa11ce);
    let file_id = hex_id(SINGLE_XORB_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_tiny_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // ~600 bytes → a handful of chunks.
    let data =
        b"a tiny file that spans several small chunks across the 128-byte chunker target".repeat(4);
    let file_id = hex_id(TINY_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_incompressible_random_data_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // Random bytes defeat compression; every chunk is stored raw.
    let data = deterministic_random(262_144, 0xc0ffee);
    let file_id = hex_id(RANDOM_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_zero_filled_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = vec![0u8; 65_536];
    let file_id = hex_id(ZEROS_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_range_middle_of_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(65_536, 0xbad1);
    let file_id = hex_id(SINGLE_XORB_ID);
    server.upload(&file_id, &data).await;

    let start = 10_000;
    let end = 40_000;
    let downloaded = download_range(&client, &file_id, start..=end).await;
    assert_eq!(downloaded, data[start as usize..=end as usize]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_range_full_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(30_000, 0xf11f);
    let file_id = hex_id(TINY_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_range(
        &client,
        &file_id,
        0..=u64::try_from(data.len() - 1).unwrap(),
    )
    .await;
    assert_eq!(downloaded, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_range_starting_mid_chunk_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(50_000, 0xc0de);
    let file_id = hex_id(RANDOM_ID);
    server.upload(&file_id, &data).await;

    // Start offset deliberately not aligned to the 128-byte chunk target.
    let start = 191;
    let end = 33_000;
    let downloaded = download_range(&client, &file_id, start..=end).await;
    assert_eq!(downloaded, data[start as usize..=end as usize]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_range_first_byte_of_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(20_000, 0x1e5);
    let file_id = hex_id(ZEROS_ID);
    server.upload(&file_id, &data).await;

    let downloaded = download_range(&client, &file_id, 0..=0).await;
    assert_eq!(downloaded, data[0..=0]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_range_past_end_of_file_returns_typed_error() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(4_096, 0xdead);
    let file_id = hex_id(TINY_ID);
    server.upload(&file_id, &data).await;

    let len = u64::try_from(data.len()).unwrap();
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("out.bin");
    let error = client
        .download_session()
        .download_range(
            &file_id,
            len.saturating_add(100)..=len.saturating_add(200),
            &dest,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        sdx::SdxError::Transfer(sdx::TransferError::RangeNotSatisfiable(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_unknown_file_returns_typed_error() {
    let server = TestServer::start().await;
    let client = server.client();
    let file_id = hex_id(UNKNOWN_ID);

    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("out.bin");
    let error = client
        .download_session()
        .download_file(&file_id, &dest)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        sdx::SdxError::Transfer(sdx::TransferError::NotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_single_chunk_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // A file smaller than the chunk target produces exactly one CDC chunk.
    // Single-chunk files are xorb-backed on ingest, so the reconstruction
    // fetch info points at the stored xorb and the download is byte-identical.
    let data = b"tiny".to_vec();
    let file_id = hex_id('f');
    server.upload(&file_id, &data).await;

    let downloaded = download_file(&client, &file_id).await;
    assert_eq!(downloaded, data);
}

// ============================================================================
// M2b1 streaming E2E
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_stream_multi_xorb_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    // ~1.3 MiB spans >8192 chunks at the 128-byte chunk target, exercising
    // multi-xorb reconstruction through the pull-based stream.
    let data = deterministic_random(1_310_720, 0x51e4);
    let file_id = hex_id(MULTI_XORB_ID);
    server.upload(&file_id, &data).await;

    let mut stream = client.download_stream(&file_id, None).await.unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        out.extend_from_slice(&chunk);
    }
    assert_eq!(out.len(), data.len());
    assert_eq!(out, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_stream_single_xorb_file_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(65_536, 0x51e4c0de);
    let file_id = hex_id(SINGLE_XORB_ID);
    server.upload(&file_id, &data).await;

    let mut stream = client.download_stream(&file_id, None).await.unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        out.extend_from_slice(&chunk);
    }
    assert_eq!(out, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_unordered_stream_matches_uploaded_bytes_sorted() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(1_310_720, 0x0bf0);
    let file_id = hex_id(MULTI_XORB_ID);
    server.upload(&file_id, &data).await;

    let mut stream = client
        .download_unordered_stream(&file_id, None)
        .await
        .unwrap();
    // Chunks arrive in completion order; reassemble by offset and verify that
    // the offsets tile the file contiguously.
    let mut pieces: Vec<(u64, bytes::Bytes)> = Vec::new();
    while let Some((offset, chunk)) = stream.next().await.unwrap() {
        pieces.push((offset, chunk));
    }
    pieces.sort_by_key(|(offset, _)| *offset);
    let mut out = Vec::new();
    let mut expected_offset = 0u64;
    for (offset, chunk) in &pieces {
        assert_eq!(*offset, expected_offset);
        expected_offset = expected_offset.saturating_add(u64::try_from(chunk.len()).unwrap());
        out.extend_from_slice(chunk);
    }
    assert_eq!(expected_offset, u64::try_from(data.len()).unwrap());
    assert_eq!(out, data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_to_writer_writes_file() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(262_144, 0x02a7);
    let file_id = hex_id(RANDOM_ID);
    server.upload(&file_id, &data).await;

    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("out.bin");
    let file = std::fs::File::create(&dest).unwrap();
    let written = client.download_to_writer(&file_id, file).await.unwrap();
    assert_eq!(written, u64::try_from(data.len()).unwrap());
    assert_eq!(tokio::fs::read(&dest).await.unwrap(), data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_bytes_matches_download_file() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(65_536, 0xbeef);
    let file_id = hex_id(SINGLE_XORB_ID);
    server.upload(&file_id, &data).await;

    let bytes = client.download_bytes(&file_id).await.unwrap();
    assert_eq!(bytes.as_ref(), data.as_slice());
    // Equivalence with the sequential in-memory path.
    let file = download_file(&client, &file_id).await;
    assert_eq!(bytes.as_ref(), file.as_slice());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_stream_byte_range_matches_uploaded_bytes() {
    let server = TestServer::start().await;
    let client = server.client();
    let data = deterministic_random(262_144, 0xbad0);
    let file_id = hex_id(RANDOM_ID);
    server.upload(&file_id, &data).await;

    let start = 10_000u64;
    let end = 200_000u64;
    let mut stream = client
        .download_stream(&file_id, Some(start..end))
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await.unwrap() {
        out.extend_from_slice(&chunk);
    }
    assert_eq!(out, data[start as usize..end as usize]);
}

/// The milestone's key acceptance test: streaming a large file with a small
/// buffer cap must keep resident RAM far below the file size.
///
/// A 1 GiB synthetic file is streamed through a 64 MiB byte-denominated buffer
/// semaphore to a plain thread via `blocking_next()`; the memory added by the
/// streaming pipeline (peak `VmRSS` sampled during the download, minus the
/// pre-download baseline) must stay below 256 MiB — far below the 1 GiB file
/// size. In isolation the absolute peak is ~140 MiB.
///
/// The server runs in-process, and its ingest retains freed-but-unreturned
/// glibc arena memory after the upload; `malloc_trim(0)` releases it before
/// the download. Because the harness runs every E2E test concurrently in one
/// process, the assertion is on the streaming *delta* over the baseline rather
/// than the absolute process RSS.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_large_file_memory_bounded_cat() {
    // 64 KiB chunk target keeps term counts (and thus in-flight tasks) small;
    // the file still spans multiple 64 MiB xorbs.
    let server = TestServer::start_with_chunk_size(NonZeroUsize::new(65_536).unwrap()).await;
    let limits = StreamLimits {
        min_reconstruction_fetch_size: 4 * 1024 * 1024,
        max_reconstruction_fetch_size: 8 * 1024 * 1024,
        min_prefetch_buffer: 8 * 1024 * 1024,
        ..StreamLimits::default()
    };
    let client = server.client_with(Some(64 * 1024 * 1024), Some(limits));

    let file_size = 1024 * 1024 * 1024; // 1 GiB
    let data = deterministic_random(file_size, 0xb00b5e);
    let file_id = hex_id(MEMORY_ID);
    server.upload(&file_id, &data).await;
    // Release the test's 1 GiB buffer before measuring the download's RSS.
    drop(data);
    tokio::task::yield_now().await;
    // The server's in-process ingest retains freed-but-unreturned heap memory
    // (glibc arena), which would mask the download's RSS; release it back to
    // the OS so the assertion measures the streaming pipeline only.
    // SAFETY: malloc_trim is a glibc extension; harmless in a test.
    unsafe {
        libc::malloc_trim(0);
    }
    tokio::task::yield_now().await;
    let baseline_kib = current_rss_kib();

    let mut stream = client.download_stream(&file_id, None).await.unwrap();
    // Consume on a plain (non-async) thread via the dedicated blocking runtime,
    // sampling peak RSS throughout the stream.
    let handle = std::thread::spawn(move || {
        let mut peak_rss_kib = current_rss_kib();
        let mut total: u64 = 0;
        loop {
            peak_rss_kib = peak_rss_kib.max(current_rss_kib());
            match stream.blocking_next() {
                Ok(Some(chunk)) => {
                    total = total.saturating_add(u64::try_from(chunk.len()).unwrap());
                }
                Ok(None) => break,
                Err(error) => return Err(error),
            }
        }
        Ok((total, peak_rss_kib))
    });
    let (total, peak_rss_kib) = handle.join().unwrap().unwrap();
    assert_eq!(
        total,
        u64::try_from(file_size).unwrap(),
        "streamed {total} of {file_size} bytes (byte-identity check)"
    );

    let bound_kib = 256 * 1024; // 256 MiB
    let streaming_delta_kib = peak_rss_kib.saturating_sub(baseline_kib);
    assert!(
        streaming_delta_kib < bound_kib,
        "streaming cat added {streaming_delta_kib} KiB over the {baseline_kib} KiB baseline (peak {peak_rss_kib} KiB), exceeding the {bound_kib} KiB bound for a {file_size}-byte file with a 64 MiB buffer cap"
    );
    eprintln!(
        "memory-bounded cat: file {file_size} bytes, buffer cap 64 MiB, baseline RSS {baseline_kib} KiB, peak VmRSS {peak_rss_kib} KiB (streaming delta {streaming_delta_kib} KiB)"
    );
}

// ============================================================================
// M2b2 E2E: on-disk chunk cache + stream group
// ============================================================================

/// Waits until the cache's spawned best-effort puts have landed (total bytes
/// stable across two 20 ms samples).
async fn wait_for_cache_settle(client: &sdx::XetClient) {
    let cache = client.chunk_cache().expect("cache configured on client");
    let mut previous = 0u64;
    for _ in 0..250 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let total = cache.total_bytes().await.unwrap();
        if total > 0 && total == previous {
            tokio::time::sleep(Duration::from_millis(20)).await;
            if cache.total_bytes().await.unwrap() == total {
                return;
            }
        }
        previous = total;
    }
}

/// The milestone's cache acceptance test: a warm cache serves the second
/// download of the same file with **zero** additional xorb transfer requests.
///
/// The first download populates the on-disk cache (best-effort spawned puts);
/// after it settles, the second download must be byte-identical and issue no
/// new ranged xorb GETs (reconstruction metadata requests still occur).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_warm_second_download_issues_no_xorb_fetches() {
    let server = TestServer::start().await;
    let cache_dir = TempDir::new().unwrap();
    let client = server.client_with_cache_dir(cache_dir.path(), None, None);
    // ~256 KiB → ~2048 chunks at the 128-byte target (single xorb); every
    // chunk is fetched individually by the streaming path, so the warm-cache
    // request-count delta is still meaningful.
    let data = deterministic_random(262_144, 0xc4c4);
    let file_id = hex_id('7');
    server.upload(&file_id, &data).await;

    let first = client.download_bytes(&file_id).await.unwrap();
    assert_eq!(first.as_ref(), data.as_slice());
    let first_fetches = client.xorb_fetch_count();
    assert!(first_fetches > 0, "first download must hit the CAS");
    wait_for_cache_settle(&client).await;

    let second = client.download_bytes(&file_id).await.unwrap();
    assert_eq!(
        second.as_ref(),
        data.as_slice(),
        "warm-cache download must be byte-identical"
    );
    let second_fetches = client.xorb_fetch_count();
    let delta = second_fetches.saturating_sub(first_fetches);
    assert_eq!(
        delta, 0,
        "warm cache must serve the second download: first download issued {first_fetches} xorb fetches, second added {delta}"
    );
    eprintln!(
        "cache-warm download: first download {first_fetches} xorb fetches, second added {delta}"
    );
}

/// Group acceptance test: several files downloaded concurrently through one
/// group are all byte-identical, and streams unregister on Drop.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_downloads_files_concurrently_byte_identical() {
    let server = TestServer::start().await;
    let client = server.client();
    let group = client.new_download_stream_group();

    let files: Vec<(char, Vec<u8>)> = vec![
        ('1', deterministic_random(262_144, 0x1111)),
        ('2', deterministic_random(131_072, 0x2222)),
        ('3', deterministic_random(65_536, 0x3333)),
    ];
    for (digit, data) in &files {
        server.upload(&hex_id(*digit), data).await;
    }

    let mut tasks = Vec::new();
    for (digit, expected) in files.clone() {
        let group = group.clone();
        tasks.push(tokio::spawn(async move {
            let mut stream = group.download_stream(&hex_id(digit), None).await.unwrap();
            let mut out = Vec::new();
            while let Some(chunk) = stream.next().await.unwrap() {
                out.extend_from_slice(&chunk);
            }
            (digit, out, expected)
        }));
    }
    for task in tasks {
        let (digit, out, expected) = task.await.unwrap();
        assert_eq!(out.len(), expected.len(), "file {digit} length");
        assert_eq!(out, expected, "file {digit} byte-identity");
    }
    // All streams were dropped: the group is empty again.
    assert_eq!(group.active_stream_count(), 0);
    assert!(group.status().is_empty());
}

/// Group acceptance test: abort-all during a large download completes promptly.
///
/// A multi-MiB file (32k+ terms at the 128-byte chunk target) is aborted right
/// after the first chunk arrives; the consumer loop must observe `Ok(None)`
/// (no hang) within a tight bound, well before the file is fully downloaded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_abort_during_large_download_completes_promptly() {
    let server = TestServer::start().await;
    let client = server.client();
    let group = client.new_download_stream_group();

    let data = deterministic_random(4 * 1024 * 1024, 0xabb0);
    let file_id = hex_id('9');
    server.upload(&file_id, &data).await;

    let mut stream = group.download_stream(&file_id, None).await.unwrap();
    let stream_id = stream.task_id();
    // Wait for the first chunk so the pipeline is definitely in progress.
    let first = tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .expect("first chunk should arrive")
        .unwrap()
        .expect("first chunk should not error");
    assert!(!first.is_empty());

    let started = Instant::now();
    group.abort();
    let mut received: u64 = u64::try_from(first.len()).unwrap();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("next() must return after abort (no hang)");
        match next.expect("no stream error after abort") {
            Some(chunk) => {
                received = received.saturating_add(u64::try_from(chunk.len()).unwrap());
            }
            None => break,
        }
    }
    let latency = started.elapsed();
    assert!(
        latency < Duration::from_secs(2),
        "group abort took {latency:?} to surface Ok(None)"
    );
    assert!(
        received < u64::try_from(data.len()).unwrap(),
        "aborted download received all {received} bytes before abort"
    );
    assert!(group.is_aborted());
    assert!(
        group
            .status()
            .contains(&(stream_id, sdx::XetTaskState::Cancelled))
    );
    eprintln!(
        "group abort latency: {latency:?} ({received} of {} bytes received)",
        data.len()
    );
}
