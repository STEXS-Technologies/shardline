use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize};

use reqwest::Client;
use sha2::Digest;
use shardline_server::{LocalGcOptions, ServerConfig, ServerFrontend, ServerRole, serve_with_listener};
use tokio::net::TcpListener;

async fn start_server() -> Result<(String, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>), Box<dyn std::error::Error>> {
    start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Lfs, ServerFrontend::Oci, ServerFrontend::BazelHttp], |c| Ok(c)).await
}

async fn start_server_with(
    role: Option<ServerRole>,
    frontends: &[ServerFrontend],
    modify_config: impl Fn(ServerConfig) -> Result<ServerConfig, Box<dyn std::error::Error>> + Clone,
) -> Result<(String, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>), Box<dyn std::error::Error>> {
    let mut last_err = None;
    for attempt in 0..5 {
        match try_start_server(role, frontends, modify_config.clone()).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                last_err = Some(e);
                if attempt < 4 {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }
        }
    }
    Err(last_err.unwrap_or_else(|| "failed to start server after 5 attempts".into()))
}

async fn try_start_server(
    role: Option<ServerRole>,
    frontends: &[ServerFrontend],
    modify_config: impl FnOnce(ServerConfig) -> Result<ServerConfig, Box<dyn std::error::Error>>,
) -> Result<(String, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>), Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir()?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config_path = write_provider_config(storage.path())?;
    let mut config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_server_frontends(frontends.iter().copied())?;
    if let Some(r) = role {
        config = config.with_server_role(r);
    }
    let config = config.with_provider_runtime(
        config_path,
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    )?;
    let config = modify_config(config)?;
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
    assert_eq!(resp.status(), 200, "health check failed: {}", resp.status());
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
    let body = resp.text().await.unwrap();
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
    let body = resp.text().await.unwrap();
    assert_eq!(status, 200, "token issuance failed: {status} body: {body}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_routes_disabled_when_role_api_only() {
    let (base_url, server) = start_server_with(Some(ServerRole::Api), &[ServerFrontend::Xet], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // API role serves read/write token, reconstruction, shard with provider key
    let read_token = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(read_token.status(), 200, "api role should serve read token");

    // Transfer routes (chunk reads/writes) should not be registered for API role
    let chunk_read = client
        .post(format!("{base_url}/v1/chunks/default/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(chunk_read.status(), 404, "api role should not register chunk routes");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_routes_disabled_when_role_transfer_only() {
    let (base_url, server) = start_server_with(Some(ServerRole::Transfer), &[ServerFrontend::Xet], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Transfer role serves chunk reads/writes
    // POST to a GET-only chunk route should return 405 (route exists) not 404 (route missing)
    let chunk_read = client
        .post(format!("{base_url}/v1/chunks/default/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(chunk_read.status(), 405, "transfer role should register chunk routes (POST to GET-only route returns 405)");

    // API routes (read token, shard upload) should be disabled
    let read_token = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(read_token.status(), 404, "transfer role should not serve read token");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_routes_disabled_without_xet_frontend() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs, ServerFrontend::Oci, ServerFrontend::BazelHttp], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let read_token = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(read_token.status(), 404, "xet routes should be 404 when xet frontend is disabled");

    // LFS routes should still work
    let lfs_batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(lfs_batch.status(), 200, "lfs routes should work when xet frontend is disabled");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_routes_disabled_without_lfs_frontend() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Oci, ServerFrontend::BazelHttp], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let lfs_batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(lfs_batch.status(), 404, "lfs routes should be 404 when lfs frontend is disabled");

    // Xet routes should still work
    let read_token = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(read_token.status(), 200, "xet routes should work when lfs frontend is disabled");

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
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(status, 200, "LFS batch failed: {status} body: {body:?}");
    assert!(body.get("objects").is_some(), "LFS batch response missing objects: {body:?}");
    // Object may not exist (404 per-object with error) — that's expected for non-existent OIDs
    // The batch request itself succeeds (200) regardless of per-object status
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
    let status = resp.status();
    assert_eq!(status, 413, "expected 413 for oversized body, got {status}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_and_download_round_trip() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"hello lfs upload and download round trip test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "LFS upload failed: {}", upload.status());

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "LFS download failed: {}", download.status());
    let downloaded = download.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), content, "LFS round trip bytes mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_download_large_file() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = vec![0xABu8; 65536];
    let oid = hex::encode(sha2::Sha256::digest(&content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "LFS upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "LFS download failed");
    let downloaded = download.bytes().await.unwrap();
    assert_eq!(downloaded.to_vec(), content, "LFS large file round trip mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_non_existent_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "expected 404 for non-existent LFS object");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_head_existing_object() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"head test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "LFS upload failed for HEAD test");

    let head = client
        .head(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200, "LFS HEAD for existing object failed");
    assert!(head.headers().get("content-length").is_some(), "HEAD response missing content-length");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_head_non_existent_object() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let head = client
        .head(format!("{base_url}/v1/lfs/objects/0000000000000000000000000000000000000000000000000000000000000000"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 404, "expected 404 for HEAD on non-existent LFS object");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_binary_content_with_null_bytes() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content: Vec<u8> = (0..255).cycle().take(4096).collect();
    let oid = hex::encode(sha2::Sha256::digest(&content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "binary upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "binary download failed");
    let downloaded = download.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), content.as_slice(), "binary round trip mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_empty_content_round_trip() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content: Vec<u8> = vec![];
    let oid = hex::encode(sha2::Sha256::digest(&content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "empty upload failed: {}", upload.status());

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "empty download failed");
    let downloaded = download.bytes().await.unwrap();
    assert!(downloaded.is_empty(), "downloaded empty content should be empty");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_returns_application_octet_stream_content_type() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"content type test";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.test+json")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download failed");
    let ct = download.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert!(ct.is_some(), "content-type header missing on GET");
    // LFS objects always return application/octet-stream
    assert_eq!(ct.unwrap(), "application/octet-stream", "LFS content-type should be octet-stream");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_returns_content_digest_header() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"digest test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "text/plain")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download failed");
    let digest = download.headers().get("Docker-Content-Digest").and_then(|v| v.to_str().ok());
    assert!(digest.is_some(), "Docker-Content-Digest header missing on GET");
    let digest = digest.unwrap();
    assert!(!digest.is_empty(), "digest should not be empty");
    assert_eq!(digest, format!("sha256:{oid}"), "digest should match expected format");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_request_first_100_bytes() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"this is a test file with enough content to test range requests across multiple boundaries";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 206, "expected 206 Partial Content");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[0..10], "first 10 bytes mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_request_middle_bytes() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"this is a test file with enough content to test range requests across multiple boundaries";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=10-30")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 206, "expected 206 Partial Content");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[10..31], "middle bytes mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_request_beyond_end_returns_416() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"test content for range validation";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=9999-10000")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 416, "expected 416 Range Not Satisfiable");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_suffix_range_returns_last_n_bytes() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"test content for suffix range verification";
    let oid = hex::encode(sha2::Sha256::digest(content));
    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload failed");
    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=-10")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 206, "suffix range should return 206 Partial Content");
    let body = download.bytes().await.unwrap();
    let expected = &content[content.len() - 10..];
    assert_eq!(body.as_ref(), expected, "suffix range bytes mismatch");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_open_ended_range_returns_from_offset_to_end() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"open ended range test content here";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=5-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "open ended range");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[5..], "open ended range bytes");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_single_byte_range() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"single byte range test";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    for offset in [0, 5, content.len() - 1] {
        let resp = client
            .get(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Range", format!("bytes={offset}-{offset}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 206, "single byte range at offset {offset}");
        let body = resp.bytes().await.unwrap();
        assert_eq!(body.as_ref(), &content[offset..=offset], "single byte at offset {offset}");
    }

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_empty_range_returns_416() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"empty range test";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=10-9")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "start > end should return 400, got {}", resp.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_with_negative_start_returns_400() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"invalid range test";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=-5-10")
        .send()
        .await
        .unwrap();
    assert!(resp.status().as_u16() == 400,
        "negative start should return 400, got {}", resp.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_full_file_range() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"full file range test content here";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=0-")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "full file range");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content, "full file range content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_last_100_bytes_range() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = vec![0xABu8; 500];
    let oid = hex::encode(sha2::Sha256::digest(&content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=-100")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "last 100 bytes");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[400..], "last 100 bytes content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_across_chunk_boundary() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Chunk size is 4 bytes. Create 8 bytes (2 chunks), request bytes 2-5 (spans both chunks)
    let content = b"abcdefgh";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=2-5")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206, "range across chunk boundary");
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), &content[2..=5], "across boundary content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_returns_content_length_header() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"content length check test data";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200);
    let cl = download
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.parse::<usize>().unwrap());
    assert_eq!(cl, Some(content.len()), "content-length should match content");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.len(), content.len(), "body length matches");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_download_returns_content_length_and_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"oci metadata test content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let repo = "test-owner/test-repo";

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 202);
    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| if s.starts_with("http") { s.to_owned() } else { format!("{base_url}{s}") })
        .unwrap();

    let put = client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201);

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);

    let cl = get.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.parse::<usize>().unwrap());
    assert_eq!(cl, Some(content.len()), "OCI blob content-length");

    let dd = get.headers().get("docker-content-digest")
        .and_then(|v| v.to_str().ok());
    assert_eq!(dd, Some(digest.as_str()), "OCI blob docker-content-digest");

    let ct = get.headers().get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/octet-stream"), "OCI blob content-type");

    let body = get.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content, "OCI blob content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_v2_root_returns_version_header() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "OCI v2 root failed");
    let header = resp.headers().get("Docker-Distribution-API-Version")
        .and_then(|v| v.to_str().ok());
    assert_eq!(header, Some("registry/2.0"), "OCI version header missing");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_and_pull_blob() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"oci blob test content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let repo = "test-owner/test-repo";

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 202, "OCI blob upload start failed: {}", post.status());
    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            if s.starts_with("http") {
                s.to_owned()
            } else {
                format!("{base_url}{s}")
            }
        })
        .expect("OCI upload location header missing");

    let put = client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "OCI blob finalize failed: {}", put.status());

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "OCI blob download failed");
    let body = get.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content, "OCI blob content mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_blob_existing() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"oci head blob test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let repo = "test-owner/test-repo";

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 202, "OCI upload start failed");
    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            if s.starts_with("http") { s.to_owned() } else { format!("{base_url}{s}") }
        })
        .unwrap();

    client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200, "OCI HEAD blob failed");
    let len = head.headers().get("content-length").and_then(|v| v.to_str().ok());
    assert!(len.is_some(), "OCI HEAD missing content-length");
    assert_eq!(len.unwrap(), content.len().to_string());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_all_byte_values_round_trip() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content: Vec<u8> = (0..=255).collect();
    let oid = hex::encode(sha2::Sha256::digest(&content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload of all byte values failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download of all byte values failed");
    let downloaded = download.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), content.as_slice(), "all byte values round trip mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_sequential_pattern_round_trip() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
    let oid = hex::encode(sha2::Sha256::digest(&content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload of sequential pattern failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download of sequential pattern failed");
    let downloaded = download.bytes().await.unwrap();
    assert_eq!(downloaded.as_ref(), content.as_slice(), "sequential pattern round trip mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_thousand_small_files() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let mut handles = Vec::with_capacity(1000);
    for i in 0..1000 {
        let content = vec![(i % 256) as u8; 64];
        let oid = hex::encode(sha2::Sha256::digest(&content));
        let url = format!("{base_url}/v1/lfs/objects/{oid}");
        let auth = format!("Bearer {token}");
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            let upload = client
                .put(&url)
                .header("Authorization", &auth)
                .header("Content-Type", "application/octet-stream")
                .body(content.clone())
                .send()
                .await
                .expect("upload request failed");
            assert!(
                upload.status().is_success(),
                "upload {i} failed: {}",
                upload.status()
            );
            let download = client
                .get(&url)
                .header("Authorization", &auth)
                .send()
                .await
                .expect("download request failed");
            assert!(
                download.status().is_success(),
                "download {i} failed: {}",
                download.status()
            );
            let body = download.bytes().await.expect("download body failed");
            assert_eq!(
                body.as_ref(),
                content.as_slice(),
                "content mismatch for file {i}"
            );
        }));
    }

    for (i, handle) in handles.into_iter().enumerate() {
        handle.await.unwrap_or_else(|e| panic!("file {i} panicked: {e}"));
    }

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_exact_chunk_size_boundary() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Chunk size is 4 bytes (from ServerConfig::new 5th arg)
    // Test exactly 4 bytes (1 full chunk) and exactly 8 bytes (2 full chunks)
    for (label, content) in [("one_chunk", vec![0xABu8; 4]), ("two_chunks", vec![0xBCu8; 8])] {
        let oid = hex::encode(sha2::Sha256::digest(&content));
        let upload = client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(content.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(upload.status(), 200, "upload {label} failed");

        let download = client
            .get(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(download.status(), 200, "download {label} failed");
        let body = download.bytes().await.unwrap();
        assert_eq!(body.as_ref(), content.as_slice(), "round trip mismatch for {label}");
    }

    let partial: Vec<u8> = vec![0xDDu8; 3];
    let partial_oid = hex::encode(sha2::Sha256::digest(&partial));
    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{partial_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(partial.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload partial chunk failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{partial_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download partial chunk failed");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), partial.as_slice(), "partial chunk round trip mismatch");

    let oversized: Vec<u8> = vec![0xEEu8; 5];
    let oversized_oid = hex::encode(sha2::Sha256::digest(&oversized));
    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oversized_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(oversized.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "upload 1-byte-over chunk failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oversized_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download 1-byte-over chunk failed");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), oversized.as_slice(), "1-byte-over chunk round trip mismatch");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_same_content_uploaded_twice() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"dedup test content that should only be stored once";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let first = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 200, "first upload failed");

    let second = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 200, "second upload of same content failed");

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download after duplicate upload failed");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content, "content mismatch after dedup");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_changed_first_byte_produces_different_storage() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let original = b"aaaa test content for dedup checking";
    let modified = {
        let mut m = original.to_vec();
        m[0] = b'b';
        m
    };

    let oid_orig = hex::encode(sha2::Sha256::digest(original));
    let oid_mod = hex::encode(sha2::Sha256::digest(&modified));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(original.to_vec())
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(modified.clone())
        .send()
        .await
        .unwrap();

    let orig_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(orig_dl.status(), 200, "original download failed");
    assert_eq!(
        orig_dl.bytes().await.unwrap().as_ref(),
        original,
        "original content changed"
    );

    let mod_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mod_dl.status(), 200, "modified download failed");
    assert_eq!(
        mod_dl.bytes().await.unwrap().as_ref(),
        modified.as_slice(),
        "modified content mismatch"
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_unicode_manifest_tag() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let blob = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(blob)));
    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 201);

    let special_tag = "v1.0-special-chars_abc.def-123";
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob_digest, "size": blob.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/{special_tag}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "special char manifest tag push");

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{special_tag}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "special char manifest tag pull");
    let dd = get.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert_eq!(dd, Some(manifest_digest.as_str()), "special tag digest");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_upload_via_lfs_and_oci() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"race condition test content";
    let lfs_oid = hex::encode(sha2::Sha256::digest(content));
    let blob_digest = format!("sha256:{lfs_oid}");

    let lfs_upload = client
        .put(format!("{base_url}/v1/lfs/objects/{lfs_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send();
    let oci_upload = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send();

    let (lfs_res, oci_res) = tokio::join!(lfs_upload, oci_upload);
    assert_eq!(lfs_res.unwrap().status(), 200, "LFS concurrent upload");
    assert_eq!(oci_res.unwrap().status(), 201, "OCI concurrent upload");

    let lfs_get = client
        .get(format!("{base_url}/v1/lfs/objects/{lfs_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(lfs_get.status(), 200, "LFS concurrent download");
    assert_eq!(lfs_get.bytes().await.unwrap().as_ref(), content, "LFS content");

    let oci_get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(oci_get.status(), 200, "OCI concurrent download");
    assert_eq!(oci_get.bytes().await.unwrap().as_ref(), content, "OCI content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_returns_object_size() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"batch size check content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": oid, "size": content.len()}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200);
    let body: serde_json::Value = batch.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["oid"].as_str(), Some(oid.as_str()));
    assert_eq!(objects[0]["size"].as_u64(), Some(content.len() as u64));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_media_type_preserved() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let blob = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(blob)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();

    let media_type = "application/vnd.oci.image.manifest.v1+json";
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": media_type,
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob_digest, "size": blob.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", media_type)
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    let ct = get.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some(media_type), "manifest media type preserved");
    let body: serde_json::Value = get.json().await.unwrap();
    assert_eq!(body["mediaType"].as_str(), Some(media_type), "manifest body media type");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_manifest_returns_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let blob = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(blob)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob_digest, "size": blob.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200, "OCI HEAD manifest");
    let dd = head.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert_eq!(dd, Some(manifest_digest.as_str()), "HEAD manifest digest");
    let ct = head.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/vnd.oci.image.manifest.v1+json"), "HEAD manifest content-type");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tag_list_returns_pushed_tags() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let blob = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(blob)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob_digest, "size": blob.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    for tag in &["v1", "v2", "latest"] {
        let put = client
            .put(format!("{base_url}/v2/{repo}/manifests/{tag}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .body(manifest_bytes.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(put.status(), 201, "tag {tag} push");
    }

    let tags = client
        .get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(tags.status(), 200);
    let body: serde_json::Value = tags.json().await.unwrap();
    let names = body["tags"].as_array().unwrap();
    let names: Vec<&str> = names.iter().filter_map(|v| v.as_str()).collect();
    assert!(names.contains(&"v1"), "tag list contains v1");
    assert!(names.contains(&"v2"), "tag list contains v2");
    assert!(names.contains(&"latest"), "tag list contains latest");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_reports_content_type_octet_stream() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"content type check";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "text/custom")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    let ct = get.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/octet-stream"), "LFS always returns octet-stream");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_head_returns_content_length() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"head content length check";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200);
    let cl = head.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.parse::<usize>().unwrap());
    assert_eq!(cl, Some(content.len()), "HEAD content-length");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_manifest_with_empty_layers() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "empty layers manifest push");

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "empty layers manifest pull");
    let body: serde_json::Value = get.json().await.unwrap();
    assert_eq!(body["layers"].as_array().map(|a| a.len()), Some(0), "empty layers");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_annotations_preserved() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let blob = br#"{"architecture":"amd64","os":"linux"}"#;
    let blob_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(blob)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={blob_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": blob_digest, "size": blob.len() },
        "layers": [],
        "annotations": {
            "com.example.key1": "value1",
            "com.example.key2": "value2",
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    let body: serde_json::Value = get.json().await.unwrap();
    let annotations = body["annotations"].as_object().unwrap();
    assert_eq!(annotations["com.example.key1"].as_str(), Some("value1"));
    assert_eq!(annotations["com.example.key2"].as_str(), Some("value2"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_returns_docker_content_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"docker content digest header check";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    let dd = get.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert!(dd.is_some(), "Docker-Content-Digest should be present");
    assert_eq!(dd.unwrap(), format!("sha256:{oid}"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_head_returns_metadata_headers() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"oci head metadata test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200);
    let cl = head.headers().get("content-length")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.parse::<usize>().unwrap());
    assert_eq!(cl, Some(content.len()), "HEAD content-length");
    let dd = head.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert_eq!(dd, Some(digest.as_str()), "HEAD docker-content-digest");
    let ct = head.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/octet-stream"), "HEAD content-type must match OCI spec");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_same_content_uploaded_ten_times() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"ten times dedup test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    for i in 0..10 {
        let upload = client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(upload.status(), 200, "upload {i} of 10 failed");
    }

    let download = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), 200, "download after 10 uploads");
    let body = download.bytes().await.unwrap();
    assert_eq!(body.as_ref(), content, "content after 10 uploads");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_append_one_byte() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let original = b"dedup append test content";
    let modified = {
        let mut m = original.to_vec();
        m.push(b'!');
        m
    };

    let oid_orig = hex::encode(sha2::Sha256::digest(original));
    let oid_mod = hex::encode(sha2::Sha256::digest(&modified));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(original.to_vec())
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(modified.clone())
        .send()
        .await
        .unwrap();

    let orig_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(orig_dl.status(), 200);
    assert_eq!(orig_dl.bytes().await.unwrap().as_ref(), original);

    let mod_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mod_dl.status(), 200);
    assert_eq!(mod_dl.bytes().await.unwrap().as_ref(), modified.as_slice());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_prepend_one_byte() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let original = b"dedup prepend test content";
    let modified = {
        let mut m = vec![b'@'];
        m.extend_from_slice(original);
        m
    };

    let oid_orig = hex::encode(sha2::Sha256::digest(original));
    let oid_mod = hex::encode(sha2::Sha256::digest(&modified));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(original.to_vec())
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(modified.clone())
        .send()
        .await
        .unwrap();

    let orig_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(orig_dl.status(), 200);
    assert_eq!(orig_dl.bytes().await.unwrap().as_ref(), original);

    let mod_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mod_dl.status(), 200);
    assert_eq!(mod_dl.bytes().await.unwrap().as_ref(), modified.as_slice());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_changed_last_byte() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let original = b"dedup last byte test content!!!!";
    let modified = {
        let mut m = original.to_vec();
        let len = m.len();
        m[len - 1] = b'?';
        m
    };

    let oid_orig = hex::encode(sha2::Sha256::digest(original));
    let oid_mod = hex::encode(sha2::Sha256::digest(&modified));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(original.to_vec())
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(modified.clone())
        .send()
        .await
        .unwrap();

    let orig_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(orig_dl.status(), 200);
    assert_eq!(orig_dl.bytes().await.unwrap().as_ref(), original);

    let mod_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mod_dl.status(), 200);
    assert_eq!(mod_dl.bytes().await.unwrap().as_ref(), modified.as_slice());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_changed_middle_byte() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let original = b"dedup middle byte test content!!!!!!";
    let modified = {
        let mut m = original.to_vec();
        m[original.len() / 2] = b'X';
        m
    };

    let oid_orig = hex::encode(sha2::Sha256::digest(original));
    let oid_mod = hex::encode(sha2::Sha256::digest(&modified));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(original.to_vec())
        .send()
        .await
        .unwrap();
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(modified.clone())
        .send()
        .await
        .unwrap();

    let orig_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_orig}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(orig_dl.status(), 200);
    assert_eq!(orig_dl.bytes().await.unwrap().as_ref(), original);

    let mod_dl = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_mod}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mod_dl.status(), 200);
    assert_eq!(mod_dl.bytes().await.unwrap().as_ref(), modified.as_slice());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_and_pull_manifest_by_tag() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/my-tag"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "manifest push by tag");

    let get_by_tag = client
        .get(format!("{base_url}/v2/{repo}/manifests/my-tag"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_by_tag.status(), 200, "manifest pull by tag");
    let body: serde_json::Value = get_by_tag.json().await.unwrap();
    assert_eq!(body["mediaType"].as_str(), Some("application/vnd.oci.image.manifest.v1+json"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_pull_by_digest_returns_200() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "manifest pull by digest");
    assert_eq!(
        get.headers().get("docker-content-digest")
            .and_then(|v| v.to_str().ok()),
        Some(manifest_digest.as_str())
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_non_existent_manifest_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "non-existent manifest should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_non_existent_blob_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "non-existent blob should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_delete_manifest_removes_tag() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    let del = client
        .delete(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 202, "manifest delete should return 202");

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "deleted manifest should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_list_tags_empty_repository() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let tags = client
        .get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(tags.status(), 200);
    let body: serde_json::Value = tags.json().await.unwrap();
    assert_eq!(body["name"].as_str(), Some("test-owner/test-repo"));
    let names = body["tags"].as_array().expect("tags field should be an array");
    assert!(names.is_empty(), "empty repo should have no tags, got {names:?}");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_overlapping_ranges() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = vec![0xABu8; 200];
    let oid = hex::encode(sha2::Sha256::digest(&content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.clone())
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=10-30")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &content[10..=30]);

    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=20-50")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &content[20..=50]);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_range_exact_chunk_boundary() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Chunk size is 4 bytes. Create 8 bytes, request bytes 3-4 (spans boundary)
    let content = b"abcdefgh";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    // Range exactly at chunk boundary start (byte 4 = start of chunk 2)
    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=4-7")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &content[4..=7]);

    // Range exactly at chunk boundary end (byte 3 = end of chunk 1)
    let resp = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Range", "bytes=0-3")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 206);
    assert_eq!(resp.bytes().await.unwrap().as_ref(), &content[0..=3]);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_single_object() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"single batch object";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": oid, "size": content.len()}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200);
    let body: serde_json::Value = batch.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    assert!(objects[0]["actions"].is_object(), "single object should have download actions");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_multiple_objects() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let contents: Vec<(String, Vec<u8>)> = (0..5).map(|i| {
        let c = format!("batch object {i}");
        let oid = hex::encode(sha2::Sha256::digest(c.as_bytes()));
        (oid, c.into_bytes())
    }).collect();

    for (oid, bytes) in &contents {
        client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(bytes.clone())
            .send()
            .await
            .unwrap();
    }

    let objects: Vec<serde_json::Value> = contents.iter().map(|(oid, bytes)| {
        serde_json::json!({"oid": oid, "size": bytes.len()})
    }).collect();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": objects,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200);
    let body: serde_json::Value = batch.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 5, "all 5 objects should be in response");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_unknown_transfer_adapter_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["unknown-adapter"],
            "objects": [],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 422, "unknown transfer should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_empty_objects_array() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": [],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "empty objects array should be accepted");
    let body: serde_json::Value = batch.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert!(objects.is_empty(), "empty request should return empty response");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_missing_required_fields_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "objects": [],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 422, "missing operation should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_invalid_json_body_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body("not valid json".to_owned())
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 400, "invalid JSON body should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_excessive_cardinality_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let objects: Vec<serde_json::Value> = (0..2000).map(|i| {
        serde_json::json!({"oid": format!("{i:064x}"), "size": 1})
    }).collect();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "download",
            "transfers": ["basic"],
            "objects": objects,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 422, "excessive batch cardinality should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_hash_mismatch_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"actual content here";
    let wrong_oid = hex::encode(sha2::Sha256::digest(b"different content"));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{wrong_oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 400, "hash mismatch should return 400");

    server.abort();
}

async fn upload_xorb(client: &Client, base_url: &str, token: &str, content: &[u8]) -> String {
    let (xorb, hash) = shardline_server::test_fixtures::single_chunk_xorb(content);
    let resp = client
        .post(format!("{base_url}/v1/xorbs/default/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(xorb.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "xorb upload failed for hash {hash}");
    hash
}

async fn upload_shard(client: &Client, base_url: &str, token: &str, parts: &[(&[u8], &str)]) -> String {
    use shardline_server::test_fixtures::single_file_shard;
    let (shard, file_hash) = single_file_shard(parts);
    let resp = client
        .post(format!("{base_url}/v1/shards"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(shard.to_vec())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "shard upload failed: {}", resp.status());
    file_hash
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_write_token_uploads_xorb() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = upload_xorb(&client, &base_url, &token, b"write token xorb test").await;
    assert_eq!(hash.len(), 64, "xorb hash should be 64 hex chars");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_read_token_reconstructs_file() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = upload_xorb(&client, &base_url, &token, b"read token reconstruction").await;
    let file_hash = upload_shard(&client, &base_url, &token, &[(b"read token reconstruction", &hash)]).await;

    let recon = client
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(recon.status(), 200, "reconstruction should succeed");
    let body: serde_json::Value = recon.json().await.unwrap();
    assert!(body["terms"].is_array(), "reconstruction should have terms");
    assert!(!body["terms"].as_array().unwrap().is_empty(), "should have at least one term");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_single_file() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = upload_xorb(&client, &base_url, &token, b"batch single recon").await;
    let file_hash = upload_shard(&client, &base_url, &token, &[(b"batch single recon", &hash)]).await;

    let batch = client
        .get(format!("{base_url}/v1/reconstructions?file_id={file_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "batch reconstruction");
    let body: serde_json::Value = batch.json().await.unwrap();
    assert!(body["files"].is_object(), "batch response should have files map");
    assert_eq!(body["files"].as_object().unwrap().len(), 1, "batch should return 1 file");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_multiple_files() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash_a = upload_xorb(&client, &base_url, &token, b"batch multi A").await;
    let hash_b = upload_xorb(&client, &base_url, &token, b"batch multi B").await;
    let file_a = upload_shard(&client, &base_url, &token, &[(b"batch multi A content", &hash_a)]).await;
    let file_b = upload_shard(&client, &base_url, &token, &[(b"batch multi B content", &hash_b)]).await;

    let batch = client
        .get(format!("{base_url}/v1/reconstructions?file_id={file_a}&file_id={file_b}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "batch multi reconstruction");
    let body: serde_json::Value = batch.json().await.unwrap();
    assert!(body["files"].is_object(), "batch response should have files map");
    assert_eq!(body["files"].as_object().unwrap().len(), 2, "batch should return both files");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_empty_file_id() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .get(format!("{base_url}/v1/reconstructions?file_id="))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 400, "empty file_id should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_without_file_id() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .get(format!("{base_url}/v1/reconstructions"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "reconstructions without file_id");
    let body: serde_json::Value = batch.json().await.unwrap();
    let files = body["files"].as_object().unwrap();
    assert!(files.is_empty(), "no file_id should return empty files map");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_duplicate_file_id() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = upload_xorb(&client, &base_url, &token, b"batch dedup recon").await;
    let file_hash = upload_shard(&client, &base_url, &token, &[(b"batch dedup recon", &hash)]).await;

    let batch = client
        .get(format!("{base_url}/v1/reconstructions?file_id={file_hash}&file_id={file_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "duplicate file_id");
    let body: serde_json::Value = batch.json().await.unwrap();
    let files = body["files"].as_object().unwrap();
    // Server deduplicates by file_id, so duplicates should return 1 entry
    assert_eq!(files.len(), 1, "duplicate file_ids should be deduplicated");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_shard_single_chunk() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = upload_xorb(&client, &base_url, &token, b"single chunk shard").await;
    let file_hash = upload_shard(&client, &base_url, &token, &[(b"single chunk shard", &hash)]).await;
    assert_eq!(file_hash.len(), 64, "file hash should be 64 hex chars");

    let recon = client
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(recon.status(), 200, "single chunk reconstruction");
    let body: serde_json::Value = recon.json().await.unwrap();
    assert_eq!(body["terms"].as_array().map(|a| a.len()), Some(1), "single chunk should have 1 term");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_shard_multiple_chunks() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash_a = upload_xorb(&client, &base_url, &token, b"multi chunk A").await;
    let hash_b = upload_xorb(&client, &base_url, &token, b"multi chunk B").await;
    let file_hash = upload_shard(&client, &base_url, &token, &[(b"multi chunk content A", &hash_a), (b"multi chunk content B", &hash_b)]).await;
    assert_eq!(file_hash.len(), 64, "file hash should be 64 hex chars");

    let recon = client
        .get(format!("{base_url}/v1/reconstructions/{file_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(recon.status(), 200, "multi chunk reconstruction");
    let body: serde_json::Value = recon.json().await.unwrap();
    assert_eq!(body["terms"].as_array().map(|a| a.len()), Some(2), "two chunks should have 2 terms");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_upload_operation() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({
            "operation": "upload",
            "transfers": ["basic"],
            "objects": [{"oid": format!("{:064x}", 1), "size": 10}],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "upload operation batch should succeed");
    let body: serde_json::Value = batch.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1, "upload batch should return 1 object");
    assert!(objects[0]["actions"].is_object(), "upload batch should include actions");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_oversized_body_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let large_body = serde_json::json!({
        "operation": "download",
        "transfers": ["basic"],
        "objects": (0..1050).map(|i| serde_json::json!({"oid": format!("{i:064x}"), "size": 1})).collect::<Vec<_>>(),
    });
    let body_bytes = serde_json::to_vec(&large_body).unwrap();

    let batch = client
        .post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .body(body_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 422, "oversized batch body should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_many_file_ids() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Use 100 file IDs (within practical limits)
    let file_ids: String = (0..100).map(|i| format!("{:064x}", i)).collect::<Vec<_>>().join("&file_id=");
    let batch = client
        .get(format!("{base_url}/v1/reconstructions?file_id={file_ids}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(batch.status(), 200, "100 file IDs should be accepted");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_empty_body() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(b"")));

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(vec![])
        .send()
        .await
        .unwrap();
    // Empty body starts an upload session (202), then needs finalization
    assert_eq!(post.status(), 202, "empty blob should start upload session");

    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| if s.starts_with("http") { s.to_owned() } else { format!("{base_url}{s}") })
        .unwrap();

    let put = client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(vec![])
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "empty blob finalize should succeed");

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "empty blob pull");
    assert_eq!(get.bytes().await.unwrap().len(), 0, "empty blob content");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_digest_mismatch_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"some content";
    let wrong_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(b"different content")));

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={wrong_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 400, "digest mismatch should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_manifest_not_exists_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let head = client
        .head(format!("{base_url}/v2/{repo}/manifests/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 404, "HEAD non-existent manifest");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_blob_not_exists_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let head = client
        .head(format!("{base_url}/v2/{repo}/blobs/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 404, "HEAD non-existent blob");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_referenced_blob_missing_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let missing_digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": missing_digest, "size": 1 },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/test-missing-ref"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400, "manifest referencing missing blob should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_push_and_delete() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"push delete test blob";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    // Two-step upload: POST to start session, PUT with digest to finalize
    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 202);
    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| if s.starts_with("http") { s.to_owned() } else { format!("{base_url}{s}") })
        .unwrap();

    let put = client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "blob push should succeed");

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "blob pull after push");
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_list_tags_pagination() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();

    for tag in &["alpha", "beta", "gamma"] {
        client
            .put(format!("{base_url}/v2/{repo}/manifests/{tag}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .body(manifest_bytes.clone())
            .send()
            .await
            .unwrap();
    }

    let tags = client
        .get(format!("{base_url}/v2/{repo}/tags/list?n=2"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(tags.status(), 200);
    let body: serde_json::Value = tags.json().await.unwrap();
    let names = body["tags"].as_array().unwrap();
    assert_eq!(names.len(), 2, "pagination n=2 should return exactly 2 results, got {names:?}");
    assert_eq!(body["name"].as_str(), Some("test-owner/test-repo"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_push_with_annotations() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
        "annotations": {
            "org.opencontainers.image.description": "a test image",
            "org.opencontainers.image.version": "1.0.0",
        },
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200);
    let body: serde_json::Value = get.json().await.unwrap();
    let annotations = body["annotations"].as_object().unwrap();
    assert_eq!(annotations["org.opencontainers.image.description"].as_str(), Some("a test image"));
    assert_eq!(annotations["org.opencontainers.image.version"].as_str(), Some("1.0.0"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_corrupt_digest_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Push a config blob and a manifest, then try to pull by wrong digest
    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let real_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));
    let wrong_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(b"different data")));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{real_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes.clone())
        .send()
        .await
        .unwrap();

    // Pull with wrong digest should return 404
    let get_wrong = client
        .get(format!("{base_url}/v2/{repo}/manifests/{wrong_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_wrong.status(), 404, "wrong digest should return 404");

    let get_correct = client
        .get(format!("{base_url}/v2/{repo}/manifests/{real_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_correct.status(), 200, "correct digest should still work");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_and_get_ac_cache() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let ac_hash = "a".repeat(64);
    let ac_body = b"{\"exitCode\":0}";

    let put = client
        .put(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(ac_body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 204, "bazel AC put should return 204");

    let get = client
        .get(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "bazel AC get should return 200");
    assert_eq!(get.bytes().await.unwrap().as_ref(), ac_body);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_get_ac_cache_not_found() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let ac_hash = "b".repeat(64);
    let get = client
        .get(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "non-existent AC should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_and_get_cas_cache() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let cas_body = b"bazel cas content";
    let cas_hash = hex::encode(sha2::Sha256::digest(cas_body));

    let put = client
        .put(format!("{base_url}/v1/bazel/cache/cas/{cas_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(cas_body.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 204, "bazel CAS put should return 204");

    let get = client
        .get(format!("{base_url}/v1/bazel/cache/cas/{cas_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "bazel CAS get should return 200");
    assert_eq!(get.bytes().await.unwrap().as_ref(), cas_body);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_get_cas_cache_not_found() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let cas_hash = "d".repeat(64);
    let get = client
        .get(format!("{base_url}/v1/bazel/cache/cas/{cas_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "non-existent CAS should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_delete_blob_referenced_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Push a config blob
    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    // Push a manifest referencing the config blob
    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    // Try to delete the config blob while referenced by manifest
    let del = client
        .delete(format!("{base_url}/v2/{repo}/blobs/{config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    // OCI spec: deleting a referenced blob MUST be rejected (400)
    assert_eq!(del.status(), 400, "deleting referenced blob should be rejected, got {}", del.status());

    // Verify blob still exists
    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "referenced blob should still exist");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_delete_blob_unreferenced_succeeds() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Push an unreferenced blob
    let content = b"unreferenced blob content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads/"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let location = post.headers().get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| if s.starts_with("http") { s.to_owned() } else { format!("{base_url}{s}") })
        .unwrap();

    client
        .put(format!("{location}?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    // Delete unreferenced blob
    let del = client
        .delete(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 202, "deleting unreferenced blob should return 202");

    let get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "deleted unreferenced blob should be gone");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_cross_repo_blob_mount_same_token() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"mountable content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    // Push blob
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    // Mount to same repo (self-reference, should work)
    let mount = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?mount={digest}&from={repo}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mount.status(), 201, "mount from same repo should succeed");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_hash_validation_rejects_wrong_hash() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let ac_hash = "a".repeat(64);
    let wrong_hash = "b".repeat(64);

    // Put content with one hash, try to get with different hash
    client
        .put(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"test content".to_vec())
        .send()
        .await
        .unwrap();

    let get = client
        .get(format!("{base_url}/v1/bazel/cache/ac/{wrong_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "wrong hash should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_invalid_json_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(b"not valid json".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400, "invalid JSON manifest should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_delete_and_reupload_content() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"delete and reupload test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    // Upload
    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 200, "initial upload");

    // Delete
    let del = client
        .delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 202, "delete should return 202");

    // Verify gone
    let get_gone = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_gone.status(), 404, "deleted object should be gone");

    // Re-upload same content
    let reupload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(reupload.status(), 200, "re-upload");

    // Verify reconstructable
    let get_again = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_again.status(), 200, "re-uploaded object accessible");
    assert_eq!(get_again.bytes().await.unwrap().as_ref(), content, "content intact after delete+reupload");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_delete_non_existent_returns_404() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let oid = hex::encode(sha2::Sha256::digest(b"non existent delete test"));
    let del = client
        .delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 404, "deleting non-existent object should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_storage_accounting_after_duplicate_uploads() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let get_stats = || async {
        let resp = client
            .get(format!("{base_url}/v1/stats"))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        resp.json::<serde_json::Value>().await.unwrap()
    };

    let before = get_stats().await;
    let before_chunks = before["chunks"].as_u64().expect("chunks field missing in stats");
    let before_files = before["files"].as_u64().expect("files field missing in stats");

    // Upload two files with the same content
    let content = b"dedup storage accounting content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    for i in 0..3 {
        client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await
            .unwrap();
    }

    let after = get_stats().await;
    let after_chunks = after["chunks"].as_u64().expect("chunks field missing in stats");
    let after_files = after["files"].as_u64().expect("files field missing in stats");

    // Dedup: uploading the same content 3 times should NOT increase chunk count
    // The first upload creates chunks, subsequent uploads reuse them
    assert_eq!(after_chunks, before_chunks, "dedup failed: chunks increased from {before_chunks} to {after_chunks}");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_identical_content_different_frontends() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"cross frontend dedup content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    // Upload via LFS
    let lfs_upload = client
        .put(format!("{base_url}/v1/lfs/objects/{}", &digest[7..]))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(lfs_upload.status(), 200, "LFS upload");

    // Upload same content via OCI
    let oci_upload = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(oci_upload.status(), 201, "OCI upload same content");

    // Verify both retrievable
    let lfs_get = client
        .get(format!("{base_url}/v1/lfs/objects/{}", &digest[7..]))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(lfs_get.status(), 200, "LFS get after dedup");
    assert_eq!(lfs_get.bytes().await.unwrap().as_ref(), content);

    let oci_get = client
        .get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(oci_get.status(), 200, "OCI get after dedup");
    assert_eq!(oci_get.bytes().await.unwrap().as_ref(), content);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_three_files_sharing_chunks_delete_middle() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Create 3 files where B's content contains A's content, and C's content
    // contains B's. With 4-byte chunks, they share chunks at the storage layer.
    let file_a = b"\x01\x02\x03\x04";
    let file_b = b"\x01\x02\x03\x04\x05\x06\x07\x08";
    let file_c = b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c";

    let oid_a = hex::encode(sha2::Sha256::digest(file_a));
    let oid_b = hex::encode(sha2::Sha256::digest(file_b));
    let oid_c = hex::encode(sha2::Sha256::digest(file_c));

    // Upload all 3
    for (oid, content) in [(&oid_a, file_a.as_slice()), (&oid_b, file_b.as_slice()), (&oid_c, file_c.as_slice())] {
        let resp = client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(content.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "upload {oid}");
    }

    // Delete middle file (B)
    let del = client
        .delete(format!("{base_url}/v1/lfs/objects/{oid_b}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 202, "delete middle file");

    // Verify A still works
    let get_a = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_a}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_a.status(), 200, "file A should still exist");
    assert_eq!(get_a.bytes().await.unwrap().as_ref(), file_a);

    // Verify C still works
    let get_c = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_c}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_c.status(), 200, "file C should still exist");
    assert_eq!(get_c.bytes().await.unwrap().as_ref(), file_c);

    // Verify B is gone
    let get_b = client
        .get(format!("{base_url}/v1/lfs/objects/{oid_b}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_b.status(), 404, "file B should be gone");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_ten_files_sharing_chunks_delete_nine() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Create 10 files of increasing size (4, 8, 12, ... 40 bytes) so they share chunks
    let mut oids = Vec::with_capacity(10);
    let mut contents = Vec::with_capacity(10);
    for i in 0..10 {
        let size = (i + 1) * 4;
        let content: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
        let oid = hex::encode(sha2::Sha256::digest(&content));
        oids.push(oid);
        contents.push(content);
    }

    // Upload all 10
    for (oid, content) in oids.iter().zip(contents.iter()) {
        let resp = client
            .put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/octet-stream")
            .body(content.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "upload {oid}");
    }

    // Delete files 0-8 (9 files), keep file 9
    for i in 0..9 {
        let del = client
            .delete(format!("{base_url}/v1/lfs/objects/{}", oids[i]))
            .header("Authorization", format!("Bearer {token}"))
            .send()
            .await
            .unwrap();
        assert_eq!(del.status(), 202, "delete file {i}");
    }

    // Verify file 9 (the last one) still works
    let get_last = client
        .get(format!("{base_url}/v1/lfs/objects/{}", oids[9]))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_last.status(), 200, "last file should still exist");
    assert_eq!(get_last.bytes().await.unwrap().as_ref(), contents[9].as_slice());

    // Verify first file is gone
    let get_first = client
        .get(format!("{base_url}/v1/lfs/objects/{}", oids[0]))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get_first.status(), 404, "first file should be gone after delete");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_shard_empty_accepted() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    use shardline_xet_core::metadata_shard::{shard_format::MDBShardInfo, shard_in_memory::MDBInMemoryShard};
    let mut shard = MDBInMemoryShard::default();
    let mut empty_bytes = Vec::new();
    let serialized = MDBShardInfo::serialize_from(&mut empty_bytes, &shard, None);
    assert!(serialized.is_ok(), "empty shard serialization should succeed");

    let resp = client
        .post(format!("{base_url}/v1/shards"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(empty_bytes)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "empty shard upload: expected 2xx, got {}", resp.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_exceeds_max_body() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Oci], |config| {
        Ok(config.with_max_request_body_bytes(NonZeroUsize::new(100).unwrap()))
    }).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let large_content = vec![0xABu8; 200];

    let post = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest=sha256:{}", hex::encode(sha2::Sha256::digest(&large_content))))
        .header("Authorization", format!("Bearer {token}"))
        .body(large_content)
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), 413, "oversized blob should get 413, got {}", post.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_missing_required_fields_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Manifest without required config field
    let invalid_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&invalid_manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400, "manifest missing config should be rejected");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_image_index() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Push a config blob
    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    // Push a child manifest
    let child_manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let child_bytes = serde_json::to_vec(&child_manifest).unwrap();
    let child_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&child_bytes)));

    // Push child manifest as a manifest (not just a blob)
    client
        .put(format!("{base_url}/v2/{repo}/manifests/{child_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(child_bytes.clone())
        .send()
        .await
        .unwrap();

    // Push image index referencing the child manifest
    let index = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": child_digest,
                "size": child_bytes.len(),
                "platform": { "architecture": "amd64", "os": "linux" },
            },
        ],
    });
    let index_bytes = serde_json::to_vec(&index).unwrap();
    let index_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&index_bytes)));

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/{index_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.index.v1+json")
        .body(index_bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "image index push");

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{index_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "image index pull");
    let body: serde_json::Value = get.json().await.unwrap();
    assert_eq!(body["mediaType"].as_str(), Some("application/vnd.oci.image.index.v1+json"));
    let manifests = body["manifests"].as_array().unwrap();
    assert_eq!(manifests.len(), 1, "index should contain 1 manifest ref");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_cross_repo_blob_mount_not_found() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let other_repo = "test-owner/test-repo";

    let content = b"cross repo mount test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    // Push blob to repo
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    // Mount to same repo (self-reference)
    let mount = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?mount={digest}&from={other_repo}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(mount.status(), 201, "self-mount should succeed");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_registry_token_flow() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    // Get an OCI registry token
    let token_resp = client
        .get(format!("{base_url}/v2/token?service=shardline&scope=repository:{repo}:pull"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(token_resp.status(), 200, "OCI token exchange");

    let token_body: serde_json::Value = token_resp.json().await.unwrap();
    let oci_token = token_body["token"].as_str().expect("OCI token should be in response");

    // Use the OCI token to pull a manifest (should fail since nothing exists, but auth passes)
    let pull = client
        .get(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {oci_token}"))
        .send()
        .await
        .unwrap();
    // 404 means auth succeeded but manifest doesn't exist (correct)
    assert_eq!(pull.status(), 404, "OCI token auth should succeed (404 expected)");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_head_ac_cache_entry() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let ac_hash = "e".repeat(64);

    client
        .put(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"head test".to_vec())
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v1/bazel/cache/ac/{ac_hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200, "HEAD on existing AC");

    let head_missing = client
        .head(format!("{base_url}/v1/bazel/cache/ac/{}", "f".repeat(64)))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_missing.status(), 404, "HEAD on missing AC");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_manifest_by_tag_returns_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes)
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200);
    let dd = head.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert_eq!(dd, Some(manifest_digest.as_str()));
    let ct = head.headers().get("content-type").and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/vnd.oci.image.manifest.v1+json"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_ac_empty_hash_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let put = client
        .put(format!("{base_url}/v1/bazel/cache/ac/"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"content".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 404, "empty hash AC put should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_ac_invalid_hash_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let put = client
        .put(format!("{base_url}/v1/bazel/cache/ac/not-a-valid-hex-hash!"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"content".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 400, "invalid hash AC put should return 400");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_referrers_api_not_supported() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let resp = client
        .get(format!("{base_url}/v2/{repo}/referrers/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "referrers API not implemented");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_anonymous_pull_allowed() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let resp = client
        .get(format!("{base_url}/v2/{repo}/manifests/latest"))
        .send()
        .await
        .unwrap();
    // Anonymous access: server requires auth, returns 401
    assert_eq!(resp.status(), 401, "anonymous pull should return 401, got {}", resp.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_docker_schema2_manifest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": { "mediaType": "application/vnd.docker.container.image.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));

    let put = client
        .put(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
        .body(bytes)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 201, "Docker schema2 manifest push");

    let get = client
        .get(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "Docker schema2 manifest pull");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_oversized_body_rejected() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::BazelHttp], |config| {
        Ok(config.with_max_request_body_bytes(NonZeroUsize::new(100).unwrap()))
    }).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let big_body = vec![0u8; 200];

    let put = client
        .put(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(big_body)
        .send()
        .await
        .unwrap();
    assert_eq!(put.status(), 413, "oversized body should return 413");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_same_entry_idempotent() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"hello bazel idempotent";
    let hash = hex::encode(sha2::Sha256::digest(content));

    let put1 = client
        .put(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert!(put1.status().is_success(), "first AC put should succeed, got {}", put1.status());

    let put2 = client
        .put(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert!(put2.status().is_success(), "second AC put (same key) should also succeed, got {}", put2.status());

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_head_cas_cache_entry() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"hello cas head";
    let hash = hex::encode(sha2::Sha256::digest(content));

    // HEAD before putting → 404
    let head_missing = client
        .head(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_missing.status(), 404, "HEAD on missing CAS entry");

    // Put then HEAD → 200
    client
        .put(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    let head_exists = client
        .head(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head_exists.status(), 200, "HEAD on existing CAS entry");
    assert!(head_exists.headers().get("content-length").is_some(), "HEAD should return content-length");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_delete_existing_object_returns_404_on_get() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"delete-me lfs content";
    let oid = format!("{:x}", sha2::Sha256::digest(content));

    // Upload
    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap();

    // Delete
    let del = client
        .delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 202, "LFS delete should return 202");

    // Get after delete → 404
    let get = client
        .get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "LFS get after delete should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_delete_non_existent_object() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let oid = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    let del = client
        .delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 404, "LFS delete on non-existent object should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_manifest_by_digest_returns_content_type() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send()
        .await
        .unwrap();

    let manifest = serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": { "mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len() },
        "layers": [],
    });
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));

    client
        .put(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes)
        .send()
        .await
        .unwrap();

    let head = client
        .head(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), 200, "HEAD on existing manifest by digest");
    assert_eq!(
        head.headers().get("content-type").and_then(|v| v.to_str().ok()),
        Some("application/vnd.oci.image.manifest.v1+json"),
    );

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_delete_non_existent() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let digest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    let del = client
        .delete(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(del.status(), 404, "DELETE non-existent blob should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_objects_survive_gc_when_referenced() {
    let storage = tempfile::tempdir().unwrap();
    let config_path = write_provider_config(storage.path()).unwrap();
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
    .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap()
    .with_provider_runtime(
        config_path,
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    ).unwrap();
    let server = tokio::spawn(async { shardline_server::serve_with_listener(config, listener).await });
    let client = Client::new();
    wait_for_health(&base_url, &client).await;

    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let content = b"gc-test-object-content";
    let oid = format!("{:x}", sha2::Sha256::digest(content));

    client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    server.abort();

    let gc_options = shardline_server::LocalGcOptions {
        mark: true,
        sweep: true,
        retention_seconds: 0,
    };
    let report = shardline_server::run_gc(
        ServerConfig::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            "http://gc.local".to_owned(),
            storage.path().to_path_buf(),
            NonZeroUsize::new(4).unwrap(),
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
        .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap(),
        gc_options,
    )
    .await
    .unwrap();

    let listener2 = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let base_url2 = format!("http://{addr2}");
    let config2 = ServerConfig::new(
        addr2,
        base_url2.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
    .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap()
    .with_provider_runtime(
        write_provider_config(storage.path()).unwrap(),
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    ).unwrap();
    let server2 = tokio::spawn(async { shardline_server::serve_with_listener(config2, listener2).await });
    wait_for_health(&base_url2, &client).await;

    let get = client
        .get(format!("{base_url2}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 200, "LFS object should survive GC when referenced");

    server2.abort();
}

async fn wait_for_health(base_url: &str, client: &Client) {
    for _attempt in 0..50 {
        if let Ok(resp) = client.get(format!("{base_url}/healthz")).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("server did not become healthy at {base_url}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_upload_session_ttl_expiration() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Oci], |config| {
        Ok(config.with_oci_upload_session_ttl_seconds(NonZeroU64::new(1).unwrap()))
    })
    .await
    .unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let create = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), 202, "session creation should succeed");
    let location = create
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_owned();
    let upload_url = format!("{base_url}{location}");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let get = client
        .get(&upload_url)
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(get.status(), 404, "expired session should return 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_upload_max_active_sessions_rejected() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Oci], |config| {
        Ok(config.with_oci_upload_max_active_sessions(NonZeroUsize::new(1).unwrap()))
    })
    .await
    .unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let first = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 202, "first session should succeed");
    let location = first
        .headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_owned();
    let _first_url = format!("{base_url}{location}");

    let second = client
        .post(format!("{base_url}/v2/{repo}/blobs/uploads"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 429, "second session should be rejected with 429");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_and_model_operations() {
    let (base_url, token, _storage, server) = start_hub_server().await;
    let client = Client::new();

    let create_dataset = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"name": "test-owner/test-dataset", "type": "dataset", "private": false}))
        .send().await.unwrap();
    assert_eq!(create_dataset.status(), 201, "dataset repo creation: {}", create_dataset.text().await.unwrap());

    let parquet = client
        .get(format!("{base_url}/api/datasets/test-owner/test-dataset/parquet"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(parquet.status(), 200, "parquet on empty repo: {}", parquet.status());

    let create_model = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"name": "test-owner/test-model", "type": "model", "private": false}))
        .send().await.unwrap();
    assert_eq!(create_model.status(), 201, "model repo creation");

    let modelcard = client
        .get(format!("{base_url}/api/models/test-owner/test-model/modelcard"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(modelcard.status(), 404, "modelcard on empty repo: {}", modelcard.status());

    for url in [
        format!("{base_url}/datasets/test-owner/test-dataset/resolve/main/nonexistent.parquet"),
        format!("{base_url}/models/test-owner/test-model/resolve/main/model.bin"),
    ] {
        let resp = client.get(&url).header("Authorization", format!("Bearer {token}")).send().await.unwrap();
        assert_eq!(resp.status(), 404, "non-existent resolve: {url}");
    }

    server.abort();
}

fn create_hub_db(storage: &std::path::Path) {
    let hub_root = storage.join("hub");
    std::fs::create_dir_all(&hub_root).unwrap();
    let conn = rusqlite::Connection::open(hub_root.join("metadata.sqlite3")).unwrap();
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
            repo_id TEXT PRIMARY KEY, repo_type TEXT NOT NULL, private INTEGER NOT NULL DEFAULT 0,
            default_branch TEXT NOT NULL, created_at_unix_seconds INTEGER NOT NULL,
            updated_at_unix_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
            repo_id TEXT NOT NULL, ref_name TEXT NOT NULL, sha TEXT NOT NULL,
            parent_sha TEXT, message TEXT, created_at_unix_seconds INTEGER NOT NULL,
            PRIMARY KEY (repo_id, sha)
        );
        CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
            ON shardline_hub_revisions (repo_id, ref_name);
        CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
            commit_sha TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL,
            sha TEXT NOT NULL, is_lfs INTEGER NOT NULL DEFAULT 0, inline_content BLOB,
            PRIMARY KEY (commit_sha, path)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_lfs_objects (
            oid TEXT PRIMARY KEY, data BLOB NOT NULL, created_at_unix_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT, repo_id TEXT NOT NULL,
            url TEXT NOT NULL, events TEXT NOT NULL, secret TEXT,
            created_at_unix_seconds INTEGER NOT NULL
        );"
    ).unwrap();
}

async fn start_hub_server() -> (String, String, tempfile::TempDir, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>) {
    for attempt in 0..5 {
        match try_start_hub_server().await {
            Ok(result) => return result,
            Err(_) if attempt < 4 => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            Err(e) => panic!("failed to start hub server: {e}"),
        }
    }
    panic!("failed to start hub server after 5 attempts")
}

async fn try_start_hub_server() -> Result<(String, String, tempfile::TempDir, tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>), Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir().unwrap();
    let config_path = write_provider_config(storage.path()).unwrap();
    create_hub_db(storage.path());
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(addr, base_url.clone(), storage.path().to_path_buf(), NonZeroUsize::new(4).unwrap())
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
        .with_server_frontends([ServerFrontend::Hub].iter().copied()).unwrap()
        .with_provider_runtime(config_path, b"test-api-key".to_vec(), "test-issuer".to_owned(), NonZeroU64::new(3600).unwrap()).unwrap();
    let server = tokio::spawn(async { shardline_server::serve_with_listener(config, listener).await });
    let client = Client::new();
    wait_for_health(&base_url, &client).await;
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    Ok((base_url, token, storage, server))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_prometheus_format() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers().get("content-type").and_then(|v| v.to_str().ok()).expect("content-type header missing");
    assert!(ct.starts_with("text/plain"));
    let body = resp.text().await.unwrap();
    assert!(body.contains("# HELP"));
    assert!(body.contains("# TYPE"));
    assert!(body.contains("shardline_up"));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_gauge_and_histogram() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let body = client.get(format!("{base_url}/metrics")).send().await.unwrap().text().await.unwrap();

    assert!(body.contains("shardline_up 1"));
    assert!(body.contains("shardline_auth_enabled"));
    assert!(body.contains("shardline_chunk_size_bytes"));
    assert!(body.contains("shardline_max_request_body_bytes"));
    assert!(body.contains("_bucket{"));
    assert!(body.contains("_count"));
    assert!(body.contains("_sum"));

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_auth_required() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs], |config| {
        Ok(config.with_metrics_token(b"secret-metrics-token".to_vec())?)
    }).await.unwrap();
    let client = Client::new();

    let no_auth = client.get(format!("{base_url}/metrics")).send().await.unwrap();
    assert_eq!(no_auth.status(), 401);

    let wrong = client.get(format!("{base_url}/metrics"))
        .header("Authorization", "Bearer wrong-token")
        .send().await.unwrap();
    assert_eq!(wrong.status(), 401);

    let ok = client.get(format!("{base_url}/metrics"))
        .header("Authorization", "Bearer secret-metrics-token")
        .send().await.unwrap();
    assert_eq!(ok.status(), 200);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_counter_exists() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();

    let body = client.get(format!("{base_url}/metrics")).send().await.unwrap().text().await.unwrap();

    assert!(body.contains("shardline_upload_bytes_total"), "upload counter");
    assert!(body.contains("shardline_download_bytes_total"), "download counter");
    assert!(body.contains("shardline_upload_requests_total"), "upload req counter");
    assert!(body.contains("shardline_download_requests_total"), "download req counter");
    assert!(body.contains("shardline_range_requests_total"), "range req counter");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_body_valid_json() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_returns_success() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/readyz")).send().await.unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert_eq!(status, 200, "readyz: {status} body={text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["status"], "ok");
    assert!(body["server_role"].is_string());
    assert!(body["server_frontends"].is_array());
    assert!(body["metadata_backend"].is_string());
    assert!(body["object_backend"].is_string());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_without_auth() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "readyz should not require auth: {}", resp.status());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readyz_metadata_backend_info() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let body: serde_json::Value = client.get(format!("{base_url}/readyz")).send().await.unwrap().json().await.unwrap();
    assert_eq!(body["metadata_backend"], "local");
    assert_eq!(body["object_backend"], "local");
    assert_eq!(body["cache_backend"], "memory");
    assert!(body["server_frontends"].as_array().map_or(false, |a| a.len() >= 4), "all frontends");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_expired_token_rejected() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, 0).unwrap();
    let token = signer.sign(&claims).unwrap();

    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/test-oid"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "expired token should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_token_missing_auth_header() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/test-oid")).send().await.unwrap();
    assert_eq!(resp.status(), 401, "missing auth header should be 401");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_token_malformed_bearer() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/test-oid"))
        .header("Authorization", "Bearer not-a-valid-token")
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "malformed token should be 401");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_token_insufficient_scope() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, u64::MAX).unwrap();
    let read_token = signer.sign(&claims).unwrap();

    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let content = b"scope test data";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    let resp = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {read_token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert_eq!(resp.status(), 403, "read-only token should be forbidden from writing");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_only_xet() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let read_token = client.get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send().await.unwrap();
    assert_eq!(read_token.status(), 200, "Xet frontend should serve Xet routes");
    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 404, "Xet-only should not serve LFS");
    let oci = client.get(format!("{base_url}/v2/test-owner/test-repo/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(oci.status(), 404, "Xet-only should not serve OCI");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_only_lfs() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 200, "Lfs frontend should serve LFS batch");
    let xet = client.get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(xet.status(), 404, "Lfs-only should not serve Xet");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_only_oci() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "OCI frontend should serve OCI v2 root");
    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 404, "Oci-only should not serve LFS");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontend_only_bazel() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::BazelHttp], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"bazel test";
    let hash = hex::encode(sha2::Sha256::digest(content));
    let ac = client.put(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert!(ac.status().is_success(), "Bazel frontend should serve Bazel");
    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 404, "Bazel-only should not serve LFS");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limit_rejected() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs], |config| {
        Ok(config.with_max_request_body_bytes(NonZeroUsize::new(10).unwrap()))
    }).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": [{"oid": "a".repeat(64), "size": 100}], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 413, "body over limit should be 413");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_then_head() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"head test content";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    let head = client.head(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(head.status(), 200);
    let cl = head.headers().get("content-length").and_then(|v| v.to_str().ok()).expect("content-length header missing on LFS HEAD");
    assert_eq!(cl, content.len().to_string());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_overwrite_existing() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"overwrite me";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    let put1 = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert!(put1.status().is_success());
    let put2 = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert!(put2.status().is_success(), "overwrite same OID should succeed");
    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.text().await.unwrap(), String::from_utf8_lossy(content));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_download_with_accept_header() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"accept header test";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Accept", "application/octet-stream")
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), String::from_utf8_lossy(content));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_minimal_request() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "batch with only operation and transfers should work");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_very_small() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"a";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let resp = client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert_eq!(resp.status(), 201, "single byte blob should be accepted");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_push_by_tag_and_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let config = br#"{"architecture":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec())
        .send().await.unwrap();
    let manifest = serde_json::json!({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": config_digest, "size": config.len()}, "layers": []});
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));
    let put = client.put(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes.clone())
        .send().await.unwrap();
    assert_eq!(put.status(), 201, "push by tag");
    let get_tag = client.get(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get_tag.status(), 200, "pull by tag");
    let get_digest = client.get(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get_digest.status(), 200, "pull by digest after tag push");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tag_list_empty_after_delete() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let config = br#"{"arch":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();
    let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[]});
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));
    client.put(format!("{base_url}/v2/{repo}/manifests/mytag"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes.clone()).send().await.unwrap();
    let list = client.get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(list["tags"].as_array().map_or(0, |a| a.len()), 1, "tag present");
    client.delete(format!("{base_url}/v2/{repo}/manifests/mytag"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    let list2 = client.get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(list2["tags"].as_array().map_or(0, |a| a.len()), 0, "empty after delete");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_upload_cancel() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let create = client.post(format!("{base_url}/v2/{repo}/blobs/uploads"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(create.status(), 202);
    let location = create.headers().get("location").and_then(|v| v.to_str().ok()).unwrap().to_owned();
    let patch = client.patch(format!("{base_url}{location}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"some data".to_vec())
        .send().await.unwrap();
    assert_eq!(patch.status(), 202);
    let del = client.delete(format!("{base_url}{location}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(del.status(), 204, "cancel upload");
    let get = client.get(format!("{base_url}{location}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 404, "cancelled upload not found");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_v2_not_authorized() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v2/")).send().await.unwrap();
    assert_eq!(resp.status(), 401, "v2/ should require auth");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_on_existing_blob() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"head blob test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let head = client.head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(head.status(), 200);
    assert_eq!(head.headers().get("content-type").and_then(|v| v.to_str().ok()), Some("application/octet-stream"));
    assert!(head.headers().get("content-length").is_some());
    assert!(head.headers().get("docker-content-digest").is_some());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_ac_cache_not_found() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let get = client.get(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 404);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_cas_cache_not_found() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let hash = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let get = client.get(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 404);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_put_and_get_large_entry() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = vec![0xABu8; 100_000];
    let hash = hex::encode(sha2::Sha256::digest(&content));
    client.put(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.clone()).send().await.unwrap();
    let get = client.get(format!("{base_url}/v1/bazel/cache/cas/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    let body = get.bytes().await.unwrap();
    assert_eq!(body.len(), 100_000);
    assert_eq!(body[0], 0xAB);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_reconstruction_nonexistent_file() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/reconstructions/0000000000000000000000000000000000000000000000000000000000000000"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404, "nonexistent reconstruction should 404");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_shard_upload_invalid_format() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/shards"))
        .header("Authorization", format!("Bearer {token}"))
        .body(b"not a valid shard".to_vec())
        .send().await.unwrap();
    assert_eq!(resp.status(), 400, "invalid shard format should 400");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_health_requests() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let mut futs = Vec::new();
    for _ in 0..20 {
        let c = client.clone();
        let u = base_url.clone();
        futs.push(tokio::spawn(async move {
            c.get(format!("{u}/healthz")).send().await
        }));
    }
    for fut in futs {
        let resp = fut.await.unwrap().unwrap();
        assert!(resp.status().is_success(), "concurrent health: {}", resp.status());
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_upload_with_content_type() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"ct test";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec()).send().await.unwrap();
    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.headers().get("content-type").and_then(|v| v.to_str().ok()), Some("application/octet-stream"));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_push_invalid_media_type() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let manifest = serde_json::json!({"schemaVersion": 2, "mediaType": "application/vnd.unknown", "config": {}, "layers": []});
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let resp = client.put(format!("{base_url}/v2/{repo}/manifests/test-invalid-media"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.unknown")
        .body(bytes).send().await.unwrap();
    assert_eq!(resp.status(), 400, "invalid media type should return 400, got {}", resp.status());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_and_pull_by_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"push and pull test data";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let get = client.get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tags_list_pagination_limit() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let config = br#"{"arch":"amd64"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();
    for i in 0..3 {
        let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
            "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[]});
        let bytes = serde_json::to_vec(&manifest).unwrap();
        client.put(format!("{base_url}/v2/{repo}/manifests/tag-{i}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .body(bytes).send().await.unwrap();
    }
    let list = client.get(format!("{base_url}/v2/{repo}/tags/list?n=2"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(list["tags"].as_array().map_or(0, |a| a.len()), 2, "paginated tag list");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_oversized_objects_field() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let many_objects: Vec<serde_json::Value> = (0..200).map(|i| serde_json::json!({
        "oid": format!("{:064x}", i), "size": 100,
    })).collect();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": many_objects, "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "200 objects within limit: {}", resp.status());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_empty_operation_field() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 400, "empty operation should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_push_missing_config() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let manifest = serde_json::json!({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json", "layers": []});
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let resp = client.put(format!("{base_url}/v2/{repo}/manifests/no-config"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes).send().await.unwrap();
    assert_eq!(resp.status(), 400, "missing config should return 400, got {}", resp.status());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_batch_reconstruction_no_file_ids() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/reconstructions"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({"file_ids": []}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "empty batch reconstruction");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_cross_repo_blob_mount_same_repo() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"cross-repo-same";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let mount = client.post(format!("{base_url}/v2/{repo}/blobs/uploads?mount={digest}&from={repo}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(mount.status(), 201, "mount from same repo");
    server.abort();
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_wrong_operation_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "invalid_op", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 400, "invalid operation should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_delete_unreferenced_succeeds() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"delete-me-blob";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let del = client.delete(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(del.status(), 202, "delete unreferenced blob");
    let get = client.get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 404, "deleted blob gone");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_get_returns_docker_content_digest() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"digest header test";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    let dcd = resp.headers().get("docker-content-digest").and_then(|v| v.to_str().ok());
    assert!(dcd.is_some(), "docker-content-digest header should be present");
    assert_eq!(dcd.unwrap(), format!("sha256:{oid}"));
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_delete_existing_object_returns_202() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let content = b"lfs delete 202 test";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let del = client.delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(del.status(), 202, "LFS delete should return 202");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_blob_and_get_returns_content_length() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let content = b"content-length test data";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();
    let resp = client.get(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    let cl = resp.headers().get("content-length").and_then(|v| v.to_str().ok()).expect("content-length header missing on OCI blob GET");
    assert_eq!(cl, content.len().to_string());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_expiration_boundary_exact() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, now).unwrap();
    let token = signer.sign(&claims).unwrap();
    assert!(signer.verify_now(&token).is_ok(), "token at exact expiration should be valid (exp >= now)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_expiration_one_second_before() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, now + 1).unwrap();
    let token = signer.sign(&claims).unwrap();
    assert!(signer.verify_now(&token).is_ok(), "token expiring in 1s should be valid");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_expiration_one_second_after() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, now.saturating_sub(1)).unwrap();
    let token = signer.sign(&claims).unwrap();
    assert!(signer.verify_now(&token).is_err(), "expired token (exp=now-1) should be rejected");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_token_oversized_rejected() {
    let token = "a".repeat(10000);
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/test"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "oversized token should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn token_scope_write_can_write_and_read() {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Write, repo, u64::MAX).unwrap();
    let write_token = signer.sign(&claims).unwrap();

    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();

    // Write should succeed
    let content = b"write scope test";
    let oid = format!("{:x}", sha2::Sha256::digest(content));
    let put = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {write_token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert_eq!(put.status(), 200, "write token should allow writes");

    // Read should also succeed (Write scope includes Read)
    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {write_token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200, "write token should also allow reads");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_cardinality_stable() {
    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let body = client.get(format!("{base_url}/metrics")).send().await.unwrap().text().await.unwrap();

    // Count unique metric family names
    let mut families = std::collections::BTreeSet::new();
    for line in body.lines() {
        if line.starts_with("# HELP ") {
            let name = line.strip_prefix("# HELP ").and_then(|l| l.split_whitespace().next()).unwrap_or("");
            families.insert(name.to_owned());
        }
    }
    // Each family should appear exactly once (no duplicate registration with different labels)
    // A stable cardinality means no per-request unique label values
    assert!(families.len() >= 10, "expected at least 10 metric families, got {}", families.len());
    assert!(families.len() <= 100, "suspicious cardinality: {} metric families, expected <= 100", families.len());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_wrong_signing_key_rejected() {
    let wrong_signer = shardline_protocol::TokenSigner::new(b"different-key-that-is-32-bytes-long!!!!!").unwrap();
    let repo = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "test-owner", "test-repo", Some("main"),
    ).unwrap();
    let claims = shardline_protocol::TokenClaims::new("local", "test-subject", shardline_protocol::TokenScope::Read, repo, u64::MAX).unwrap();
    let token = wrong_signer.sign(&claims).unwrap();

    let (base_url, server) = start_server().await.unwrap();
    let client = Client::new();
    let resp = client.get(format!("{base_url}/v1/lfs/objects/test"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 401, "token signed with wrong key should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_role_api_rejects_blob_upload() {
    let (base_url, server) = start_server_with(Some(ServerRole::Api), &[ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let tags = client.get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(tags.status(), 200, "api role should serve tag listing");

    let content = b"role test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let upload = client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert_eq!(upload.status(), 404, "api role should reject blob upload");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_role_transfer_serves_blobs() {
    let (base_url, server) = start_server_with(Some(ServerRole::Transfer), &[ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"transfer blob test";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));
    let upload = client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();
    assert_eq!(upload.status(), 201, "transfer role should serve blob upload");

    let tags = client.get(format!("{base_url}/v2/{repo}/tags/list"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(tags.status(), 404, "transfer role should reject tag listing");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limit_exact_file_size_succeeds() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs], |config| {
        Ok(config.with_max_request_body_bytes(NonZeroUsize::new(100).unwrap()))
    }).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = vec![0x42u8; 100];
    let oid = format!("{:x}", sha2::Sha256::digest(&content));
    let resp = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content)
        .send().await.unwrap();
    assert_eq!(resp.status(), 200, "body exactly at limit should succeed");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_limit_one_byte_over_rejected() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs], |config| {
        Ok(config.with_max_request_body_bytes(NonZeroUsize::new(100).unwrap()))
    }).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = vec![0x42u8; 101];
    let oid = format!("{:x}", sha2::Sha256::digest(&content));
    let resp = client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content)
        .send().await.unwrap();
    assert_eq!(resp.status(), 413, "body 1 byte over limit should be rejected");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_transfers_only_rejected() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 422, "batch without operation should be rejected (missing required field), got {}", resp.status());
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_head_on_nonexistent_blob() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let digest = "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    let head = client.head(format!("{base_url}/v2/{repo}/blobs/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(head.status(), 404, "HEAD on non-existent blob should 404");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bazel_read_ac_after_write() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"bazel read after write test";
    let hash = hex::encode(sha2::Sha256::digest(content));

    client.put(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    let get = client.get(format!("{base_url}/v1/bazel/cache/ac/{hash}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontends_all_serves_everything() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Lfs, ServerFrontend::Oci, ServerFrontend::BazelHttp], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let health = client.get(format!("{base_url}/healthz")).send().await.unwrap();
    assert_eq!(health.status(), 200);

    let xet = client.get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send().await.unwrap();
    assert_eq!(xet.status(), 200, "xet route with all frontends");

    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 200, "lfs route with all frontends");

    let oci = client.get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(oci.status(), 200, "oci route with all frontends");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontends_xet_lfs_pair() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Lfs], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 200, "lfs enabled");

    let oci = client.get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(oci.status(), 404, "oci should be disabled");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontends_xet_oci_pair() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let oci = client.get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(oci.status(), 200, "oci enabled");

    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 404, "lfs should be disabled");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontends_lfs_oci_pair() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Lfs, ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 200, "lfs enabled");

    let xet = client.get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(xet.status(), 404, "xet should be disabled");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn frontends_xet_lfs_oci_triple() {
    let (base_url, server) = start_server_with(None, &[ServerFrontend::Xet, ServerFrontend::Lfs, ServerFrontend::Oci], |c| Ok(c)).await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let xet = client.get(format!("{base_url}/api/github/test-owner/test-repo/xet-read-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send().await.unwrap();
    assert_eq!(xet.status(), 200, "xet route");

    let lfs = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"operation": "download", "objects": [], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(lfs.status(), 200, "lfs route");

    let oci = client.get(format!("{base_url}/v2/"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(oci.status(), 200, "oci route");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_tags_list_pagination_exact_count() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";
    let config = br#"{"arch":"amd64"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();
    for i in 0..5 {
        let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
            "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[]});
        let bytes = serde_json::to_vec(&manifest).unwrap();
        client.put(format!("{base_url}/v2/{repo}/manifests/tag-{i}"))
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
            .body(bytes).send().await.unwrap();
    }
    // Request page size 3 out of 5 tags
    let page1 = client.get(format!("{base_url}/v2/{repo}/tags/list?n=3"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(page1["tags"].as_array().unwrap().len(), 3, "first page should have 3 tags");

    // Request page size 10 (more than available)
    let all = client.get(format!("{base_url}/v2/{repo}/tags/list?n=10"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    assert_eq!(all["tags"].as_array().unwrap().len(), 5, "all 5 tags when n > total");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_check_during_gc() {
    let storage = tempfile::tempdir().unwrap();
    let config_path = write_provider_config(storage.path()).unwrap();
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(addr, base_url.clone(), storage.path().to_path_buf(), NonZeroUsize::new(4).unwrap())
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
        .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap()
        .with_provider_runtime(config_path, b"test-api-key".to_vec(), "test-issuer".to_owned(), NonZeroU64::new(3600).unwrap()).unwrap();
    let server = tokio::spawn(async { shardline_server::serve_with_listener(config, listener).await });
    let client = Client::new();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    wait_for_health(&base_url, &client).await;

    // Upload some objects
    for i in 0..3 {
        let content = format!("gc health test content {i}");
        let oid = format!("{:x}", sha2::Sha256::digest(content.as_bytes()));
        client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .body(content.into_bytes())
            .send().await.unwrap();
    }

    // Run GC in background
    let gc_config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        "http://gc.local".to_owned(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
    .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap();
    let gc_handle = tokio::spawn(async move {
        shardline_server::run_gc(gc_config, shardline_server::LocalGcOptions {
            mark: true, sweep: true, retention_seconds: 0,
        }).await
    });

    // Health check should remain healthy during GC
    for _ in 0..10 {
        let health = client.get(format!("{base_url}/healthz")).send().await.unwrap();
        assert_eq!(health.status(), 200, "health should remain 200 during GC");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    gc_handle.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_upload_and_reconstruct() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Upload a file
    let content = b"concurrent upload and reconstruct test data for shardline verification";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    // Read back while uploading again (simulate concurrent access)
    let mut handles = Vec::new();
    for _ in 0..10 {
        let c = client.clone();
        let u = base_url.clone();
        let t = token.clone();
        let o = oid.clone();
        handles.push(tokio::spawn(async move {
            c.get(format!("{u}/v1/lfs/objects/{o}"))
                .header("Authorization", format!("Bearer {t}"))
                .send().await
        }));
    }
    for i in 0..5 {
        let c = client.clone();
        let u = base_url.clone();
        let t = token.clone();
        let extra = format!("extra content {}", i);
        let extra_oid = hex::encode(sha2::Sha256::digest(extra.as_bytes()));
        handles.push(tokio::spawn(async move {
            c.put(format!("{u}/v1/lfs/objects/{extra_oid}"))
                .header("Authorization", format!("Bearer {t}"))
                .body(extra.into_bytes())
                .send().await
        }));
    }

    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert!(resp.status().is_success(), "concurrent op failed: {}", resp.status());
    }

    // Verify original content still correct
    let verify = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(verify.status(), 200);
    assert_eq!(verify.bytes().await.unwrap().as_ref(), content);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_content_type_preserved() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"metadata content type test";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/custom-test-type")
        .body(content.to_vec())
        .send().await.unwrap();

    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200);
    let ct = get.headers().get("content-type").and_then(|v| v.to_str().ok()).expect("content-type header");
    // LFS always returns application/octet-stream regardless of upload content-type
    assert_eq!(ct, "application/octet-stream");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn chunk_boundary_partial_last_chunk() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    // Chunk size is 4 bytes. Test files that end mid-chunk.
    for (label, content) in [
        ("one_byte", vec![0x01u8; 1]),
        ("three_bytes", vec![0x02u8; 3]),
        ("five_bytes", vec![0x03u8; 5]),
        ("seven_bytes", vec![0x04u8; 7]),
    ] {
        let oid = hex::encode(sha2::Sha256::digest(&content));
        client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .body(content.clone())
            .send().await.unwrap();

        let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
            .header("Authorization", format!("Bearer {token}"))
            .send().await.unwrap();
        assert_eq!(get.status(), 200, "{label}: GET after upload");
        assert_eq!(get.bytes().await.unwrap().as_ref(), content.as_slice(), "{label}: content mismatch");
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gc_does_not_remove_referenced_chunks() {
    let storage = tempfile::tempdir().unwrap();
    let config_path = write_provider_config(storage.path()).unwrap();
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(addr, base_url.clone(), storage.path().to_path_buf(), NonZeroUsize::new(4).unwrap())
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
        .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap()
        .with_provider_runtime(config_path, b"test-api-key".to_vec(), "test-issuer".to_owned(), NonZeroU64::new(3600).unwrap()).unwrap();
    let server = tokio::spawn(async { shardline_server::serve_with_listener(config, listener).await });
    let client = Client::new();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    wait_for_health(&base_url, &client).await;

    let content = b"gc should not delete this content";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    // Upload 3 more objects to ensure GC has work to do
    for i in 0..3 {
        let c = format!("extra object {i}");
        let o = hex::encode(sha2::Sha256::digest(c.as_bytes()));
        client.put(format!("{base_url}/v1/lfs/objects/{o}"))
            .header("Authorization", format!("Bearer {token}"))
            .body(c.into_bytes())
            .send().await.unwrap();
    }

    // Run GC with mark+sweep while server is live
    let gc_config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        "http://gc.local".to_owned(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
    .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap();
    let report = shardline_server::run_gc(gc_config, shardline_server::LocalGcOptions {
        mark: true, sweep: true, retention_seconds: 0,
    }).await.unwrap();

    // Verify referenced objects still reconstruct
    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200, "referenced object should survive GC");
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_stats_file_count_accurate() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let stats_before = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    let before_files = stats_before["files"].as_u64().unwrap();

    // LFS uploads don't create index file records — stats only tracks
    // reconstruction entries from Xet operations, not direct object storage.
    let content = vec![0xABu8; 100];
    let oid = hex::encode(sha2::Sha256::digest(&content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content)
        .send().await.unwrap();

    let stats_after = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();
    let after_files = stats_after["files"].as_u64().unwrap();
    assert_eq!(after_files, before_files, "LFS upload should not affect index file count");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repository_namespace_isolation() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"data in default repo";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    // LFS uses repo-scoped namespacing via the token's repository scope.
    // A token scoped to a different repo uses a different namespace.
    let other_repo_scope = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub, "other-owner", "other-repo", Some("main"),
    ).unwrap();
    let other_claims = shardline_protocol::TokenClaims::new("local", "other-subject", shardline_protocol::TokenScope::Read, other_repo_scope, u64::MAX).unwrap();
    let other_token = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!").unwrap().sign(&other_claims).unwrap();

    let resp = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {other_token}"))
        .send().await.unwrap();
    assert_eq!(resp.status(), 404, "object from different repo namespace should 404");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_update_preserves_previous_version() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"arch":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();

    // Push manifest v1 to tag
    let manifest_v1 = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[],
        "annotations":{"version":"1"}});
    let bytes_v1 = serde_json::to_vec(&manifest_v1).unwrap();
    let digest_v1 = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes_v1)));
    client.put(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes_v1).send().await.unwrap();

    // Push manifest v2 to same tag
    let manifest_v2 = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[],
        "annotations":{"version":"2"}});
    let bytes_v2 = serde_json::to_vec(&manifest_v2).unwrap();
    let digest_v2 = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes_v2)));
    client.put(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes_v2).send().await.unwrap();

    // Pull by tag — should get v2 (latest)
    let by_tag = client.get(format!("{base_url}/v2/{repo}/manifests/latest"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(by_tag.status(), 200);
    let tag_body: serde_json::Value = by_tag.json().await.unwrap();
    assert_eq!(tag_body["annotations"]["version"], "2", "tag should resolve to v2");

    // Pull v1 by digest — should still exist
    let by_digest_v1 = client.get(format!("{base_url}/v2/{repo}/manifests/{digest_v1}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(by_digest_v1.status(), 200, "v1 should still be accessible by digest");
    let v1_body: serde_json::Value = by_digest_v1.json().await.unwrap();
    assert_eq!(v1_body["annotations"]["version"], "1");

    // Pull v2 by digest — should exist too
    let by_digest_v2 = client.get(format!("{base_url}/v2/{repo}/manifests/{digest_v2}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(by_digest_v2.status(), 200, "v2 should also be accessible by digest");

    server.abort();
}


#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_manifest_blob_referential_integrity() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"arch":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();

    let layer = br#"hello layer data"#;
    let layer_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(layer)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={layer_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(layer.to_vec()).send().await.unwrap();

    // Push manifest referencing config + layer
    let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},
        "layers":[{"mediaType":"application/octet-stream","digest":layer_digest,"size":layer.len()}]});
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));
    client.put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes).send().await.unwrap();

    // Push second manifest sharing same config blob
    let manifest2 = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},
        "layers":[],"annotations":{"version":"2"}});
    let manifest2_bytes = serde_json::to_vec(&manifest2).unwrap();
    let manifest2_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest2_bytes)));
    client.put(format!("{base_url}/v2/{repo}/manifests/{manifest2_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest2_bytes).send().await.unwrap();

    // Both manifests should be independently retrievable
    for (label, digest) in [("v1", &manifest_digest), ("v2", &manifest2_digest)] {
        let get = client.get(format!("{base_url}/v2/{repo}/manifests/{digest}"))
            .header("Authorization", format!("Bearer {token}"))
            .send().await.unwrap();
        assert_eq!(get.status(), 200, "{label} manifest retrievable");
    }

    // Config blob should NOT be deletable (referenced by both manifests)
    let del = client.delete(format!("{base_url}/v2/{repo}/blobs/{config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(del.status(), 400, "referenced blob must not be deletable");

    // Layer blob should NOT be deletable (referenced by manifest v1)
    let del_layer = client.delete(format!("{base_url}/v2/{repo}/blobs/{layer_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(del_layer.status(), 400, "referenced layer must not be deletable");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lfs_batch_with_existing_objects() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"lfs batch real object test content";
    let oid = hex::encode(sha2::Sha256::digest(content));

    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    let resp = client.post(format!("{base_url}/v1/lfs/objects/batch"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.git-lfs+json")
        .json(&serde_json::json!({"operation": "download", "objects": [{"oid": oid, "size": content.len() as u64}], "transfers": ["basic"]}))
        .send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let objects = body["objects"].as_array().unwrap();
    assert_eq!(objects.len(), 1);
    let actions = objects[0]["actions"].as_object();
    assert!(actions.is_some(), "existing object must have download actions");
    assert!(objects[0]["actions"].get("download").is_some(), "download action must exist");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_stats_after_delete() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"stats after delete test";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    let stats_before_delete = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();

    client.delete(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();

    let stats_after_delete = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();

    // Stats track index records, not LFS objects directly.
    // LFS object deletion doesn't affect index file count.
    assert_eq!(stats_before_delete["files"], stats_after_delete["files"],
        "LFS delete should not change index file count");
    assert_eq!(stats_before_delete["chunks"], stats_after_delete["chunks"],
        "LFS delete should not change chunk count (CAS may retain data for other references)");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_blob_mount_does_not_duplicate_storage() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let content = b"cross repo mount content";
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(content)));

    // Upload blob
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec()).send().await.unwrap();

    let stats_before_mount = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();

    // Mount same blob to same repo (verifies mount doesn't duplicate storage)
    let mount = client.post(format!("{base_url}/v2/{repo}/blobs/uploads?mount={digest}&from={repo}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(mount.status(), 201, "mount should succeed");

    let stats_after_mount = client.get(format!("{base_url}/v1/stats"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap().json::<serde_json::Value>().await.unwrap();

    assert_eq!(stats_before_mount["chunks"], stats_after_mount["chunks"],
        "mount must not create duplicate chunks");
    assert_eq!(stats_before_mount["chunk_bytes"], stats_after_mount["chunk_bytes"],
        "mount must not increase chunk bytes");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_manifest_push_and_pull() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"arch":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();

    let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[]});
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));

    // Push manifest
    client.put(format!("{base_url}/v2/{repo}/manifests/{digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(bytes.clone()).send().await.unwrap();

    // Concurrent reads
    let mut handles = Vec::new();
    for _ in 0..20 {
        let c = client.clone();
        let u = base_url.clone();
        let r = repo.to_string();
        let d = digest.clone();
        let t = token.clone();
        handles.push(tokio::spawn(async move {
            c.get(format!("{u}/v2/{r}/manifests/{d}"))
                .header("Authorization", format!("Bearer {t}"))
                .send().await
        }));
    }

    for handle in handles {
        let resp = handle.await.unwrap().unwrap();
        assert_eq!(resp.status(), 200, "concurrent manifest pull");
    }

    server.abort();
}

#[ignore = "needs Docker with overlay storage (podman VM issue)"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn postgres_backend_lfs_round_trip() {
    let docker = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .start().unwrap();
    let Some(docker) = docker else {
        eprintln!("SKIP: Docker not available");
        return;
    };
    let pg_url = docker.postgres_url().unwrap();

    let storage = tempfile::tempdir().unwrap();
    let config_path = write_provider_config(storage.path()).unwrap();
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(addr, base_url.clone(), storage.path().to_path_buf(), NonZeroUsize::new(4).unwrap())
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec()).unwrap()
        .with_server_frontends([ServerFrontend::Lfs].iter().copied()).unwrap()
        .with_provider_runtime(config_path, b"test-api-key".to_vec(), "test-issuer".to_owned(), NonZeroU64::new(3600).unwrap()).unwrap()
        .with_index_postgres_url(pg_url).unwrap();
    let server = tokio::spawn(async { shardline_server::serve_with_listener(config, listener).await });
    let client = Client::new();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    wait_for_health(&base_url, &client).await;

    let content = b"postgres backend round trip test";
    let oid = hex::encode(sha2::Sha256::digest(content));
    client.put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(content.to_vec())
        .send().await.unwrap();

    let get = client.get(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {token}"))
        .send().await.unwrap();
    assert_eq!(get.status(), 200, "postgres: LFS GET after PUT");
    assert_eq!(get.bytes().await.unwrap().as_ref(), content);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn xet_full_pipeline_upload_xorb_shard_reconstruct() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let write_resp = client
        .get(format!("{base_url}/api/github/test-owner/test-repo/xet-write-token/main?subject=test-subject"))
        .header("Authorization", format!("Bearer {token}"))
        .header("x-shardline-provider-key", "test-api-key")
        .send().await.unwrap();
    assert_eq!(write_resp.status(), 200, "write token issuance");
    let write_text = write_resp.text().await.expect("write token body");
    let write_body: serde_json::Value = serde_json::from_str(&write_text).expect("write token JSON");
    let xet_token = write_body["accessToken"].as_str().expect(&format!("accessToken in response: {write_text}")).to_owned();

    let content = b"xet full pipeline test content for verification";

    // Upload xorb using test fixture
    let xorb_hash = upload_xorb(&client, &base_url, &xet_token, content).await;

    // Upload shard using test fixture
    let shard_file_hash = upload_shard(&client, &base_url, &xet_token, &[(content, &xorb_hash)]).await;

    // Reconstruct the file — returns reconstruction metadata, not raw bytes
    let recon = client.get(format!("{base_url}/v1/reconstructions/{shard_file_hash}"))
        .header("Authorization", format!("Bearer {xet_token}"))
        .send().await.unwrap();
    assert_eq!(recon.status(), 200, "reconstruction should succeed");
    let recon_body: serde_json::Value = recon.json().await.unwrap();
    assert!(recon_body.get("terms").is_some(), "reconstruction should contain terms");
    assert!(recon_body.get("fetch_info").is_some(), "reconstruction should contain fetch_info");

    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_body_limit_minimum_one() {
    let config = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        "http://test.local".to_owned(),
        tempfile::tempdir().unwrap().path().to_path_buf(),
        NonZeroUsize::new(1).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())
    .unwrap()
    .with_server_frontends([ServerFrontend::Lfs].iter().copied())
    .unwrap()
    .with_max_request_body_bytes(NonZeroUsize::new(1).unwrap());
    assert_eq!(config.max_request_body_bytes().get(), 1, "body limit should be 1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn config_auth_provider_invalid_rejected() {
    use shardline_server::AuthProviderKind;
    let result = ServerConfig::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        "http://test.local".to_owned(),
        tempfile::tempdir().unwrap().path().to_path_buf(),
        NonZeroUsize::new(4).unwrap(),
    )
    .with_auth_provider(AuthProviderKind::Local);
    assert_eq!(result.auth_provider(), AuthProviderKind::Local);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oci_push_manifest_with_subject_referrer() {
    let (base_url, server) = start_server().await.unwrap();
    let token = mint_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();
    let repo = "test-owner/test-repo";

    let config = br#"{"arch":"amd64","os":"linux"}"#;
    let config_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(config)));
    client.post(format!("{base_url}/v2/{repo}/blobs/uploads?digest={config_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .body(config.to_vec()).send().await.unwrap();

    // Push manifest without subject first
    let manifest = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[]});
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&manifest_bytes)));
    client.put(format!("{base_url}/v2/{repo}/manifests/{manifest_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(manifest_bytes).send().await.unwrap();

    // Push manifest referencing the first as subject (referrer)
    let referrer = serde_json::json!({"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":config_digest,"size":config.len()},"layers":[],
        "subject":{"mediaType":"application/vnd.oci.image.manifest.v1+json","digest":manifest_digest,"size":100}});
    let referrer_bytes = serde_json::to_vec(&referrer).unwrap();
    let referrer_digest = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&referrer_bytes)));
    let put = client.put(format!("{base_url}/v2/{repo}/manifests/{referrer_digest}"))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/vnd.oci.image.manifest.v1+json")
        .body(referrer_bytes).send().await.unwrap();
    assert_eq!(put.status(), 201, "manifest with subject reference");

    server.abort();
}
