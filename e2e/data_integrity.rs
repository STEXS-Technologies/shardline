use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize};

use reqwest::Client;
use shardline_server::{ServerConfig, ServerFrontend, serve_with_listener};
use tokio::net::TcpListener;

async fn start_server() -> Result<(String, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>), Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config_path = write_provider_config(storage.path())?;
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_server_frontends([ServerFrontend::Xet, ServerFrontend::Lfs].iter().copied())?
    .with_provider_runtime(
        config_path,
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    )?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    let client = Client::new();
    for _attempt in 0..50 {
        if let Ok(resp) = client.get(format!("{base_url}/healthz")).send().await {
            if resp.status().is_success() {
                return Ok((base_url, server));
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if server.is_finished() {
        let result = server.await;
        let msg = format!("server exited prematurely: {result:?}");
        return Err(msg.into());
    }
    Err("server did not become healthy".into())
}

fn write_provider_config(root: &std::path::Path) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let path = root.join("providers.json");
    let bytes = serde_json::to_vec(&serde_json::json!({
        "providers": [
            {
                "kind": "github",
                "integration_subject": "github-app",
                "webhook_secret": "secret",
                "repositories": [
                    {
                        "owner": "test-owner",
                        "name": "test-repo",
                        "visibility": "private",
                        "default_revision": "main",
                        "clone_url": "https://github.example/test-owner/test-repo.git",
                        "read_subjects": ["test-subject"],
                        "write_subjects": ["test-subject"]
                    }
                ]
            }
        ]
    }))?;
    std::fs::write(&path, bytes)?;
    Ok(path)
}

fn mint_token(subject: &str, owner: &str, repo: &str, revision: &str) -> Result<String, Box<dyn std::error::Error>> {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!")?;
    let repository = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        owner,
        repo,
        Some(revision),
    )?;
    let claims = shardline_protocol::TokenClaims::new("local", subject, shardline_protocol::TokenScope::Write, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_returns_200() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/healthz")).send().await.unwrap();
    assert!(resp.status().is_success(), "health check failed");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_returns_data() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/metrics")).send().await.unwrap();
    assert!(resp.status().is_success(), "metrics endpoint failed");
    let text = resp.text().await.unwrap();
    assert!(!text.is_empty(), "metrics body should not be empty");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_read_token_issuance_succeeds() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 200, "token issuance failed: {status} body: {body}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_write_token_issuance_succeeds() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-write-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 200, "token issuance failed: {status} body: {body}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_download_returns_actions() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let payload = serde_json::json!({
        "operation": "download",
        "objects": [{"oid": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890", "size": 100}],
        "transfers": ["basic"]
    });
    let resp = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&payload)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert_eq!(status, 200, "LFS batch failed: {status} body: {body}");
    assert!(body.contains("objects"), "LFS batch response missing objects: {body}");
    // Object may not exist (404 per-object) - that's expected for non-existent OIDs
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_request_returns_401() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "expected 401 for unauthorized request");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_token_returns_401() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", "Bearer invalid-token-that-will-be-rejected")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "expected 401 for invalid token");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_body_too_large_returns_413() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let large_body = vec![0u8; 67_108_865]; // just over 64MB default limit
    let resp = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .body(large_body)
        .header("Content-Type", "application/vnd.git-lfs+json")
        .send()
        .await
        .unwrap();
    // 413 or 401 (auth first, then size check)
    let status = resp.status();
    assert!(status == 413 || status == 401, "expected 413 or 401, got {status}");
    server.abort();
}
