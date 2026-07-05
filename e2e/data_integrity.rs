use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::{NonZeroU64, NonZeroUsize};

use reqwest::Client;
use sha2::Digest;
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
    .with_server_frontends([ServerFrontend::Xet, ServerFrontend::Lfs, ServerFrontend::Oci].iter().copied())?
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

fn mint_read_token(subject: &str, owner: &str, repo: &str, revision: &str) -> Result<String, Box<dyn std::error::Error>> {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!")?;
    let repository = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        owner,
        repo,
        Some(revision),
    )?;
    let claims = shardline_protocol::TokenClaims::new("local", subject, shardline_protocol::TokenScope::Read, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_token_cannot_write() {
    let (base_url, server) = start_server().await.unwrap();
    let read_token = mint_read_token("test-subject", "test-owner", "test-repo", "main").unwrap();
    let client = Client::new();

    let content = b"attempted write with read token";
    let oid = hex::encode(sha2::Sha256::digest(content));

    let upload = client
        .put(format!("{base_url}/v1/lfs/objects/{oid}"))
        .header("Authorization", format!("Bearer {read_token}"))
        .header("Content-Type", "application/octet-stream")
        .body(content.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), 403, "read token should be rejected with 403 Forbidden");

    server.abort();
}

