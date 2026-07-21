#![allow(clippy::indexing_slicing, clippy::panic_in_result_fn)]

mod support;

use std::{
    error::Error as StdError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use reqwest::{
    Client, StatusCode,
    header::{CONTENT_TYPE, RANGE},
};
use serde_json::json;
use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{
    FileReconstructionResponse, ServerConfig, ServerError, ServerFrontend, serve_with_listener,
    test_fixtures::{single_chunk_xorb, single_file_shard},
};
use tokio::{net::TcpListener, sync::Barrier, task::JoinHandle, time::timeout};

use support::{bearer_token, wait_for_health};

// Each round performs a write and a read through every frontend. Twelve rounds
// keep the CI exercise bounded while creating enough interleaving (60
// cross-protocol write/read cycles) to expose state leakage and routing races.
const ROUNDS_PER_FRONTEND: usize = 12;
const SOAK_TIMEOUT: Duration = Duration::from_secs(60);

struct MixedProtocolRuntime {
    _storage: tempfile::TempDir,
    base_url: String,
    server: JoinHandle<Result<(), ServerError>>,
}

impl Drop for MixedProtocolRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn start_mixed_protocol_runtime() -> Result<MixedProtocolRuntime, Box<dyn StdError>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_server_frontends([
        ServerFrontend::Xet,
        ServerFrontend::Lfs,
        ServerFrontend::BazelHttp,
        ServerFrontend::Oci,
        ServerFrontend::Hub,
    ])?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;

    Ok(MixedProtocolRuntime {
        _storage: storage,
        base_url,
        server,
    })
}

fn scoped_token(scope: TokenScope) -> Result<String, Box<dyn StdError>> {
    bearer_token(
        "mixed-protocol-user",
        scope,
        RepositoryProvider::GitHub,
        "team",
        "assets",
        Some("main"),
    )
}

fn payload(protocol: &str, round: usize) -> Vec<u8> {
    format!("mixed-protocol-soak:{protocol}:round:{round}").into_bytes()
}

async fn assert_hub_repository_exists(
    client: &Client,
    base_url: &str,
    write_token: &str,
) -> Result<(), Box<dyn StdError>> {
    let response = client
        .post(format!("{base_url}/api/repos/create"))
        .bearer_auth(write_token)
        .json(&json!({
            "name": "team/assets",
            "type": "model",
            "private": false,
        }))
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::CREATED);
    Ok(())
}

async fn exercise_xet(
    client: Client,
    base_url: String,
    write_token: String,
    read_token: String,
    start: Arc<Barrier>,
) -> Result<(), Box<dyn StdError>> {
    start.wait().await;

    for round in 0..ROUNDS_PER_FRONTEND {
        let content = payload("xet", round);
        let (xorb, xorb_hash) = single_chunk_xorb(&content);
        let (shard, file_hash) = single_file_shard(&[(&content, &xorb_hash)]);

        let upload = client
            .post(format!("{base_url}/v1/xorbs/default/{xorb_hash}"))
            .bearer_auth(&write_token)
            .body(xorb.clone())
            .send()
            .await?;
        assert!(
            upload.status().is_success(),
            "Xet upload failed in round {round}: {}",
            upload.status()
        );

        let shard_upload = client
            .post(format!("{base_url}/v1/shards"))
            .bearer_auth(&write_token)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(shard)
            .send()
            .await?;
        assert!(
            shard_upload.status().is_success(),
            "Xet shard upload failed in round {round}: {}",
            shard_upload.status()
        );

        let xorb_transfer = client
            .get(format!("{base_url}/transfer/xorb/default/{xorb_hash}"))
            .bearer_auth(&read_token)
            .header(RANGE, format!("bytes=0-{}", xorb.len() - 1))
            .send()
            .await?;
        assert_eq!(xorb_transfer.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(xorb_transfer.bytes().await?, xorb);

        let reconstruction = client
            .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
            .bearer_auth(&read_token)
            .send()
            .await?;
        assert_eq!(reconstruction.status(), StatusCode::OK);
        let reconstruction = reconstruction.json::<FileReconstructionResponse>().await?;
        assert_eq!(reconstruction.terms.len(), 1);
        assert_eq!(
            reconstruction.terms[0].unpacked_length,
            u64::try_from(content.len())?
        );
        assert!(reconstruction.fetch_info.contains_key(&xorb_hash));
    }

    Ok(())
}

async fn exercise_lfs(
    client: Client,
    base_url: String,
    write_token: String,
    read_token: String,
    start: Arc<Barrier>,
) -> Result<(), Box<dyn StdError>> {
    start.wait().await;

    for round in 0..ROUNDS_PER_FRONTEND {
        let content = payload("lfs", round);
        let oid = hex::encode(Sha256::digest(&content));
        let upload = client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .bearer_auth(&write_token)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(content.clone())
            .send()
            .await?;
        assert_eq!(upload.status(), StatusCode::OK);

        let download = client
            .get(format!("{base_url}/v1/lfs/objects/{oid}"))
            .bearer_auth(&read_token)
            .send()
            .await?;
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(download.bytes().await?.as_ref(), content.as_slice());
    }

    Ok(())
}

async fn exercise_bazel(
    client: Client,
    base_url: String,
    write_token: String,
    read_token: String,
    start: Arc<Barrier>,
) -> Result<(), Box<dyn StdError>> {
    start.wait().await;

    for round in 0..ROUNDS_PER_FRONTEND {
        let content = payload("bazel", round);
        let digest = hex::encode(Sha256::digest(&content));
        let upload = client
            .put(format!("{base_url}/v1/bazel/cache/cas/{digest}"))
            .bearer_auth(&write_token)
            .body(content.clone())
            .send()
            .await?;
        assert_eq!(upload.status(), StatusCode::NO_CONTENT);

        let download = client
            .get(format!("{base_url}/v1/bazel/cache/cas/{digest}"))
            .bearer_auth(&read_token)
            .send()
            .await?;
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(download.bytes().await?.as_ref(), content.as_slice());
    }

    Ok(())
}

async fn exercise_oci(
    client: Client,
    base_url: String,
    write_token: String,
    read_token: String,
    start: Arc<Barrier>,
) -> Result<(), Box<dyn StdError>> {
    start.wait().await;

    for round in 0..ROUNDS_PER_FRONTEND {
        let content = payload("oci", round);
        let digest = hex::encode(Sha256::digest(&content));
        let upload = client
            .post(format!(
                "{base_url}/v2/team/assets/blobs/uploads?digest=sha256:{digest}"
            ))
            .bearer_auth(&write_token)
            .body(content.clone())
            .send()
            .await?;
        assert_eq!(upload.status(), StatusCode::CREATED);

        let download = client
            .get(format!("{base_url}/v2/team/assets/blobs/sha256:{digest}"))
            .bearer_auth(&read_token)
            .send()
            .await?;
        assert_eq!(download.status(), StatusCode::OK);
        assert_eq!(download.bytes().await?.as_ref(), content.as_slice());
    }

    Ok(())
}

async fn exercise_hub(
    client: Client,
    base_url: String,
    write_token: String,
    read_token: String,
    start: Arc<Barrier>,
) -> Result<(), Box<dyn StdError>> {
    start.wait().await;

    for round in 0..ROUNDS_PER_FRONTEND {
        let content = payload("hub", round);
        let path = format!("soak/hub-{round}.txt");
        let encoded = BASE64_STANDARD.encode(&content);
        let commit = client
            .post(format!("{base_url}/api/models/team/assets/commit/main"))
            .bearer_auth(&write_token)
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(format!(
                "{{\"header\":{{\"summary\":\"mixed protocol soak {round}\"}}}}\n{{\"file\":{{\"path\":\"{path}\",\"content\":\"{encoded}\"}}}}"
            ))
            .send()
            .await?;
        assert_eq!(commit.status(), StatusCode::OK);

        let resolved = client
            .get(format!("{base_url}/models/team/assets/resolve/main/{path}"))
            .bearer_auth(&read_token)
            .send()
            .await?;
        assert_eq!(resolved.status(), StatusCode::OK);
        assert_eq!(resolved.bytes().await?.as_ref(), content.as_slice());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mixed_protocol_concurrent_workload_remains_isolated_and_correct()
-> Result<(), Box<dyn StdError>> {
    let runtime = start_mixed_protocol_runtime().await?;
    let client = Client::new();
    let write_token = scoped_token(TokenScope::Write)?;
    let read_token = scoped_token(TokenScope::Read)?;
    assert_hub_repository_exists(&client, &runtime.base_url, &write_token).await?;

    let start = Arc::new(Barrier::new(5));
    let result = timeout(SOAK_TIMEOUT, async {
        tokio::join!(
            exercise_xet(
                client.clone(),
                runtime.base_url.clone(),
                write_token.clone(),
                read_token.clone(),
                start.clone(),
            ),
            exercise_lfs(
                client.clone(),
                runtime.base_url.clone(),
                write_token.clone(),
                read_token.clone(),
                start.clone(),
            ),
            exercise_bazel(
                client.clone(),
                runtime.base_url.clone(),
                write_token.clone(),
                read_token.clone(),
                start.clone(),
            ),
            exercise_oci(
                client.clone(),
                runtime.base_url.clone(),
                write_token.clone(),
                read_token.clone(),
                start.clone(),
            ),
            exercise_hub(
                client,
                runtime.base_url.clone(),
                write_token,
                read_token,
                start,
            ),
        )
    })
    .await
    .map_err(|_| "mixed-protocol workload timed out")?;
    let (xet, lfs, bazel, oci, hub) = result;
    xet?;
    lfs?;
    bazel?;
    oci?;
    hub?;

    Ok(())
}
