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
    time::Duration,
};

use bytes::Bytes;
use sdx::{Auth, RepositoryId, XetClientBuilder};
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
        let dir = TempDir::new().unwrap();
        let cfg_dir = dir.path().join("cfg");
        std::fs::create_dir_all(&cfg_dir).unwrap();
        let config_path = cfg_dir.join("providers.json");
        std::fs::write(&config_path, PROVIDER_CONFIG).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://127.0.0.1:{}", addr.port());
        let chunk_size = NonZeroUsize::new(128).unwrap();

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
        XetClientBuilder::new()
            .endpoint(format!(
                "xet://127.0.0.1:{}/github/team/assets/main",
                self.port
            ))
            .auth(auth)
            .build()
            .unwrap()
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._task.abort();
    }
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
