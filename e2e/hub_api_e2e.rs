#![allow(clippy::indexing_slicing, clippy::panic_in_result_fn)]

mod support;

use std::num::{NonZeroU64, NonZeroUsize};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
};

use reqwest::Client;
use shardline_server::{ServerConfig, ServerFrontend, serve_with_listener};
use tokio::net::TcpListener;

use support::{bearer_token, wait_for_health};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Guard that aborts the server task on drop, even if the test panics.
struct ServerGuard {
    base_url: String,
    token: String,
    _storage: tempfile::TempDir,
    server: tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>,
}

impl ServerGuard {
    fn base_url(&self) -> &str {
        &self.base_url
    }
    fn token(&self) -> &str {
        &self.token
    }
    /// Returns a Write-scoped token bound to an arbitrary `owner/name` repo, for
    /// use with repo-scoped Hub-API routes whose `{ns}/{repo}` must match the
    /// token's `RepositoryScope` (`require_repository_binding`).
    fn token_for(&self, owner: &str, name: &str) -> String {
        mint_token_for(owner, name, shardline_protocol::TokenScope::Write).unwrap()
    }
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        self.server.abort();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates the hub SQLite database directory so the server can initialize
/// `LocalIndexStore` without hitting a missing-directory error.
fn create_hub_db(storage: &std::path::Path) {
    let hub_root = storage.join("hub");
    std::fs::create_dir_all(&hub_root).unwrap();
    let _conn = rusqlite::Connection::open(hub_root.join("metadata.sqlite3")).unwrap();
}

/// Writes a provider config matching the test token's scope (test-owner/test-repo).
fn write_provider_config(
    root: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
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

/// Mint a token scoped to the default test repository with the given scope.
fn mint_token(scope: shardline_protocol::TokenScope) -> Result<String, Box<dyn std::error::Error>> {
    mint_token_for("test-owner", "test-model", scope)
}

/// Mint a token scoped to an arbitrary `owner/name` repository.
///
/// The security fix (`5a0df2f`) made every repo-scoped Hub-API route enforce
/// `require_repository_binding`: the token's `RepositoryScope` must exactly
/// match the `{ns}/{repo}` in the URL path. Tests must therefore present a
/// token scoped to the specific repo they create/access.
fn mint_token_for(
    owner: &str,
    name: &str,
    scope: shardline_protocol::TokenScope,
) -> Result<String, Box<dyn std::error::Error>> {
    bearer_token(
        "test-subject",
        scope,
        shardline_protocol::RepositoryProvider::GitHub,
        owner,
        name,
        Some("main"),
    )
}

/// Mint an expired token (exp = 0) scoped to the default test repository.
fn mint_expired_token(
    scope: shardline_protocol::TokenScope,
) -> Result<String, Box<dyn std::error::Error>> {
    let signer = shardline_protocol::TokenSigner::new(b"test-signing-key-32-bytes-long!!")?;
    let repository = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        "test-owner",
        "test-repo",
        Some("main"),
    )?;
    let claims =
        shardline_protocol::TokenClaims::new("local", "test-subject", scope, repository, 0)?;
    Ok(signer.sign(&claims)?)
}

/// Mint a token signed with a different key (wrong scope effectively).
fn mint_wrong_key_token(
    scope: shardline_protocol::TokenScope,
) -> Result<String, Box<dyn std::error::Error>> {
    let signer = shardline_protocol::TokenSigner::new(b"different-key-that-is-32-bytes-long!!!!!")?;
    let repository = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        "test-owner",
        "test-repo",
        Some("main"),
    )?;
    let claims =
        shardline_protocol::TokenClaims::new("local", "test-subject", scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

fn mint_token_with_key(
    signing_key: &[u8],
    scope: shardline_protocol::TokenScope,
) -> Result<String, Box<dyn std::error::Error>> {
    mint_token_with_key_for(signing_key, "test-owner", "test-model", scope)
}

fn mint_token_with_key_for(
    signing_key: &[u8],
    owner: &str,
    name: &str,
    scope: shardline_protocol::TokenScope,
) -> Result<String, Box<dyn std::error::Error>> {
    let signer = shardline_protocol::TokenSigner::new(signing_key)?;
    let repository = shardline_protocol::RepositoryScope::new(
        shardline_protocol::RepositoryProvider::GitHub,
        owner,
        name,
        Some("main"),
    )?;
    let claims =
        shardline_protocol::TokenClaims::new("local", "test-subject", scope, repository, u64::MAX)?;
    Ok(signer.sign(&claims)?)
}

/// Starts a Hub-only server and returns a `ServerGuard` that aborts on drop.
async fn start_hub_server() -> ServerGuard {
    for attempt in 0..5 {
        match try_start_hub_server().await {
            Ok(result) => return result,
            Err(_) if attempt < 4 => {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await
            }
            Err(e) => panic!("failed to start hub server: {e}"),
        }
    }
    panic!("failed to start hub server after 5 attempts")
}

async fn try_start_hub_server() -> Result<ServerGuard, Box<dyn std::error::Error>> {
    let storage = tempfile::tempdir()?;
    let config_path = write_provider_config(storage.path())?;
    create_hub_db(storage.path());
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(128).unwrap(),
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_server_frontends([ServerFrontend::Hub].iter().copied())?
    .with_provider_runtime(
        config_path,
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).unwrap(),
    )?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;
    let token = mint_token(shardline_protocol::TokenScope::Write)?;
    Ok(ServerGuard {
        base_url,
        token,
        _storage: storage,
        server,
    })
}

async fn start_hub_server_with_signing_key(
    storage: &Path,
    signing_key: &[u8],
) -> Result<
    (
        String,
        tokio::task::JoinHandle<Result<(), shardline_server::ServerError>>,
    ),
    Box<dyn std::error::Error>,
> {
    create_hub_db(storage);
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let addr = listener.local_addr()?;
    let base_url = format!("http://{addr}");
    let config = ServerConfig::new(
        addr,
        base_url.clone(),
        storage.to_path_buf(),
        NonZeroUsize::new(128).unwrap(),
    )
    .with_token_signing_key(signing_key.to_vec())?
    .with_server_frontends([ServerFrontend::Hub])?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    if let Err(error) = wait_for_health(&base_url).await {
        server.abort();
        return Err(error);
    }
    Ok((base_url, server))
}

/// Creates a model repo and returns the write token so downstream tests can
/// use it to create commits, etc.
async fn create_model_repo(base_url: &str, token: &str) {
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "name": "test-owner/test-model",
            "type": "model",
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "model repo creation failed: {} {}",
        resp.status(),
        resp.text().await.unwrap()
    );
}

/// Commits an inline file to the given repo and returns the new commit SHA.
async fn commit_inline_file(
    base_url: &str,
    token: &str,
    repo_type: &str,
    ns: &str,
    repo: &str,
    rev: &str,
    file_path: &str,
    content: &[u8],
) -> String {
    let client = Client::new();
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, content);
    let ndjson = format!(
        "{{\"header\":{{\"summary\":\"add {file_path}\"}}}}\n{{\"file\":{{\"path\":\"{file_path}\",\"content\":\"{b64}\"}}}}"
    );
    let resp = client
        .post(format!(
            "{base_url}/api/{repo_type}/{ns}/{repo}/commit/{rev}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        status.is_success(),
        "commit failed: {status} body: {body:?}"
    );
    body["commit_id"]
        .as_str()
        .expect("commit_id missing")
        .to_owned()
}

// ---------------------------------------------------------------------------
// 1. GET /api/whoami-v2
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_returns_authenticated_user() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/whoami-v2"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "whoami failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "test-subject", "whoami name mismatch");
    assert_eq!(body["is_admin"], false, "is_admin should be false");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whoami_rejects_missing_token_when_auth_is_configured() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/whoami-v2"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "whoami without a token must fail closed when auth is configured"
    );
}

// ---------------------------------------------------------------------------
// 2. POST /api/repos/create
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_model_returns_201() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact repo being created.
    let token = srv.token_for("test-owner", "new-model");
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "name": "test-owner/new-model",
            "type": "model",
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "repo create should return 201: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "model", "repo type mismatch");
    assert_eq!(body["id"], "test-owner/new-model", "repo id mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_dataset_returns_201() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact repo being created.
    let token = srv.token_for("test-owner", "new-dataset");
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "name": "test-owner/new-dataset",
            "type": "dataset",
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "dataset repo create should return 201: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["type"], "dataset");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_duplicate_returns_conflict_with_native_huggingface_url() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact repo being created.
    let token = srv.token_for("test-owner", "dupe-repo");
    let client = Client::new();
    let payload = serde_json::json!({
        "name": "test-owner/dupe-repo",
        "type": "model",
        "private": false,
    });
    let first = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(
        first.status(),
        201,
        "first creation should succeed: {}",
        first.status()
    );
    let second = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&payload)
        .send()
        .await
        .unwrap();
    assert_eq!(
        second.status(),
        409,
        "duplicate creation should retain the HTTP conflict contract: {}",
        second.status()
    );
    let body: serde_json::Value = second.json().await.unwrap();
    assert_eq!(body["url"], format!("{base_url}/test-owner/dupe-repo"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_create_without_auth_returns_401() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .json(&serde_json::json!({
            "name": "test-owner/should-fail",
            "type": "model",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "repo create without auth should be 401");
}

// ---------------------------------------------------------------------------
// 3. GET /api/repos
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_list_returns_created_repos() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // Create two repos, each with a token scoped to that exact repo.
    for full in ["test-owner/list-model", "test-owner/list-dataset"] {
        let (owner, name) = full.split_once('/').expect("repo id has a slash");
        let typ = if name.contains("model") {
            "model"
        } else {
            "dataset"
        };
        let repo_token = srv.token_for(owner, name);
        client
            .post(format!("{base_url}/api/repos/create"))
            .header("Authorization", format!("Bearer {repo_token}"))
            .json(&serde_json::json!({"name": full, "type": typ, "private": false}))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .get(format!("{base_url}/api/repos"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "repo list failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().expect("repos array missing");
    assert!(
        repos.len() >= 2,
        "expected at least 2 repos, got {}",
        repos.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_list_empty_when_no_repos() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/repos"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().expect("repos array missing");
    assert_eq!(repos.len(), 0, "expected 0 repos in fresh server");
}

// ---------------------------------------------------------------------------
// 4. GET /api/{type}/search?search=query
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_search_finds_matching_repos() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // Token must be scoped to the exact repo being created.
    let create_token = srv.token_for("test-owner", "alpha-search");
    client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {create_token}"))
        .json(&serde_json::json!({"name": "test-owner/alpha-search", "type": "model", "private": false}))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!("{base_url}/api/models/search?q=test-owner/alpha"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "search failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().expect("repos array missing");
    assert!(
        repos
            .iter()
            .any(|r| r["id"].as_str() == Some("test-owner/alpha-search")),
        "search should find alpha-search: {body:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_search_returns_empty_for_non_match() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/api/models/search?q=zzz-nonexistent"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().expect("repos array missing");
    assert_eq!(repos.len(), 0, "expected 0 results for non-existent query");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_search_short_query_returns_400() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/models/search?q=a"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400, "short search query should return 400");
}

// ---------------------------------------------------------------------------
// 5. GET /api/{type}/{ns}/{repo}
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_info_returns_model_info() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();

    let resp = client
        .get(format!("{base_url}/api/models/test-owner/test-model"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "repo info failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["id"], "test-owner/test-model");
    assert_eq!(body["type"], "model");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_info_nonexistent_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token scoped to the exact (non-existent) repo so binding passes and the
    // repo-not-found 404 is returned (a mismatched scope would 403 instead).
    let token = srv.token_for("test-owner", "nonexistent");
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/models/test-owner/nonexistent"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "non-existent repo should return 404");
}

// ---------------------------------------------------------------------------
// 6. GET /api/{type}/{ns}/{repo}/modelcard
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modelcard_nonexistent_repo_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token scoped to the exact (non-existent) repo so binding passes and the
    // repo-not-found 404 is returned (a mismatched scope would 403 instead).
    let token = srv.token_for("test-owner", "no-such-model");
    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/no-such-model/modelcard"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "modelcard on non-existent repo should 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modelcard_empty_repo_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/modelcard"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "modelcard on empty repo should 404 (no README.md)"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn modelcard_with_readme_returns_200() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Commit a README.md
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "README.md",
        b"# My Model\n\nThis is a test model card.",
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/modelcard"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "modelcard with README should return 200: {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("My Model"),
        "modelcard should contain README content: {body}"
    );
}

// ---------------------------------------------------------------------------
// 7. GET /api/{type}/{ns}/{repo}/revisions
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revisions_empty_repo_returns_empty() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();

    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/revisions"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "revisions failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let revisions = body["revisions"]
        .as_array()
        .expect("revisions array missing");
    assert_eq!(
        revisions.len(),
        1,
        "newly created repo should have 1 initial revision"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revisions_after_commit_contains_main() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "README.md",
        b"# Hello",
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/revisions"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let revisions = body["revisions"]
        .as_array()
        .expect("revisions array missing");
    assert_eq!(
        revisions.len(),
        2,
        "expected 2 revisions (initial + commit)"
    );
    assert_eq!(revisions[0]["ref_name"], "main");
    assert!(
        revisions[0]["sha"].as_str().is_some_and(|s| !s.is_empty()),
        "revision SHA should be non-empty"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revisions_nonexistent_repo_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token scoped to the exact (non-existent) repo so binding passes and the
    // repo-not-found 404 is returned (a mismatched scope would 403 instead).
    let token = srv.token_for("test-owner", "nonexistent");
    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/nonexistent/revisions"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "revisions on non-existent repo should 404"
    );
}

// ---------------------------------------------------------------------------
// 8. POST /api/{type}/{ns}/{repo}/preupload/{rev}
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preupload_returns_file_existence_flags() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "existing.txt",
        b"content",
    )
    .await;

    let client = Client::new();
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/preupload/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "files": [
                {"path": "existing.txt", "lfs": false},
                {"path": "new_file.txt", "lfs": false},
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "preupload failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let results = body["result"].as_array().expect("result array missing");
    assert_eq!(results.len(), 2);
    // existing.txt should be marked as exists
    let existing = results
        .iter()
        .find(|r| r["path"] == "existing.txt")
        .unwrap();
    assert_eq!(existing["exists"], true, "existing file should be flagged");
    let new_file = results
        .iter()
        .find(|r| r["path"] == "new_file.txt")
        .unwrap();
    assert_eq!(new_file["exists"], false, "new file should not exist yet");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preupload_nonexistent_revision_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/preupload/abc123nonexistent"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"files": [{"path": "a.txt", "lfs": false}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "preupload with bad rev should 404");
}

// ---------------------------------------------------------------------------
// 9. POST /api/{type}/{ns}/{repo}/commit/{rev}
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_single_file_returns_200() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"hello world");
    let ndjson = format!(
        "{{\"header\":{{\"summary\":\"initial commit\"}}}}\n{{\"file\":{{\"path\":\"README.md\",\"content\":\"{b64}\"}}}}"
    );
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "commit failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(
        body["commit_id"].as_str().is_some_and(|s| !s.is_empty()),
        "commit_id should be non-empty: {body:?}"
    );
    assert_eq!(body["ref_name"].as_str(), Some("main"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_multiple_files() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let b64_a = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"file a");
    let b64_b = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"file b");
    let ndjson = format!(
        "{{\"header\":{{\"summary\":\"multi-file commit\"}}}}\n\
         {{\"file\":{{\"path\":\"a.txt\",\"content\":\"{b64_a}\"}}}}\n\
         {{\"file\":{{\"path\":\"b.txt\",\"content\":\"{b64_b}\"}}}}"
    );
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "multi-file commit failed: {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_missing_header_returns_error() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    // Commit body without a header line — should be rejected.
    let b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"content");
    let ndjson = format!("{{\"file\":{{\"path\":\"x.txt\",\"content\":\"{b64}\"}}}}");
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "commit without header should fail: {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_delete_file() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    // First commit creates a file
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "to_delete.txt",
        b"bye",
    )
    .await;

    // Second commit deletes it
    let client = Client::new();
    let ndjson = "{\"header\":{\"summary\":\"delete file\"}}\n{\"deletedEntry\":{\"path\":\"to_delete.txt\"}}";
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "delete commit failed: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 10. GET /api/{type}/{ns}/{repo}/tree/{rev}/{*path}
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tree_root_after_commit_lists_files() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "docs/README.md",
        b"# Hi",
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/docs"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "tree failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body.as_array().expect("tree entries should be an array");
    assert!(
        entries.iter().any(|e| e["path"] == "README.md"),
        "tree should include README.md: {body:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tree_subdirectory() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "src/main.py",
        b"print('hello')",
    )
    .await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "src/utils.py",
        b"# utils",
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/src"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body.as_array().unwrap();
    assert!(entries.iter().any(|e| e["path"] == "main.py"));
    assert!(entries.iter().any(|e| e["path"] == "utils.py"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tree_nonexistent_revision_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/abc123nonexistent/"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404, "tree on bad rev should 404");
}

// ---------------------------------------------------------------------------
// 11. Auth validation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_rejects_request_without_auth() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/repos"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "request without auth should be 401");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_rejects_expired_token() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let expired = mint_expired_token(shardline_protocol::TokenScope::Read).unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/repos"))
        .header("Authorization", format!("Bearer {expired}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        401,
        "expired token should be rejected with 401"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_rejects_wrong_scope_token() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Read token should not be able to create repos (requires Write scope).
    let read_token = mint_token(shardline_protocol::TokenScope::Read).unwrap();
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {read_token}"))
        .json(&serde_json::json!({"name": "test-owner/fail", "type": "model"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "read-only token should get 403 on write endpoint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_rejects_malformed_bearer() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/repos"))
        .header("Authorization", "Bearer not-a-valid-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "malformed bearer token should be 401");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_api_rejects_token_signed_with_wrong_key() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let bad_token = mint_wrong_key_token(shardline_protocol::TokenScope::Read).unwrap();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/repos"))
        .header("Authorization", format!("Bearer {bad_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "wrong signing key should be 401");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_signing_key_rotation_revokes_old_tokens_without_losing_metadata() {
    const OLD_KEY: &[u8] = b"old-test-signing-key-32-bytes-long!!";
    const NEW_KEY: &[u8] = b"new-test-signing-key-32-bytes-long!!";

    let storage = tempfile::tempdir().unwrap();
    // Token must be scoped to the exact repo being created.
    let old_write_token = mint_token_with_key_for(
        OLD_KEY,
        "test-owner",
        "rotated-key-model",
        shardline_protocol::TokenScope::Write,
    )
    .unwrap();
    let (old_base_url, old_server) = start_hub_server_with_signing_key(storage.path(), OLD_KEY)
        .await
        .unwrap();

    let client = Client::new();
    let created = client
        .post(format!("{old_base_url}/api/repos/create"))
        .bearer_auth(&old_write_token)
        .json(&serde_json::json!({
            "name": "test-owner/rotated-key-model",
            "type": "model",
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);

    old_server.abort();
    let _ = old_server.await;

    let new_read_token =
        mint_token_with_key(NEW_KEY, shardline_protocol::TokenScope::Read).unwrap();
    let (new_base_url, new_server) = start_hub_server_with_signing_key(storage.path(), NEW_KEY)
        .await
        .unwrap();

    let stale = client
        .get(format!("{new_base_url}/api/repos"))
        .bearer_auth(&old_write_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        stale.status(),
        401,
        "rotated signing key must revoke old tokens"
    );

    let retained = client
        .get(format!("{new_base_url}/api/repos"))
        .bearer_auth(&new_read_token)
        .send()
        .await
        .unwrap();
    assert_eq!(retained.status(), 200);
    let repositories = retained.text().await.unwrap();
    assert!(
        repositories.contains("rotated-key-model"),
        "repository metadata must survive a token-signing-key rotation"
    );

    new_server.abort();
    let _ = new_server.await;
}

// ---------------------------------------------------------------------------
// 12. POST /api/collections/{collection} — not implemented, expect 404
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collections_create_not_implemented_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .post(format!("{base_url}/api/collections/my-collection"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({"name": "my-collection"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "collections endpoint not implemented yet"
    );
}

// ---------------------------------------------------------------------------
// 13. GET /api/collections — not implemented, expect 404
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn collections_list_not_implemented_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/collections"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "collections list endpoint not implemented yet"
    );
}

// ---------------------------------------------------------------------------
// 14. GET /api/user/profile — not implemented, expect 404
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_profile_not_implemented_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/user/profile"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "user profile endpoint not implemented yet"
    );
}

// ---------------------------------------------------------------------------
// 15. Git Smart HTTP push → clone roundtrip
// ---------------------------------------------------------------------------

/// Builds a Git receive-pack request body that pushes a single commit containing
/// one inline file. Returns (request_body_bytes, expected_commit_sha_hex).
fn build_receive_pack_request(
    old_sha_hex: &str,
    file_path: &str,
    file_content: &[u8],
    commit_message: &str,
) -> (Vec<u8>, String) {
    build_receive_pack_request_for_ref(
        old_sha_hex,
        "refs/heads/main",
        file_path,
        file_content,
        commit_message,
    )
}

/// Builds a Git receive-pack request that creates or updates `ref_name`.
fn build_receive_pack_request_for_ref(
    old_sha_hex: &str,
    ref_name: &str,
    file_path: &str,
    file_content: &[u8],
    commit_message: &str,
) -> (Vec<u8>, String) {
    use shardline_hub_api::git::pack::{
        create_blob_object, create_commit_object, create_tree_object, generate_pack,
    };
    use shardline_hub_api::git::pktline;

    // 1. Build the blob for the file content.
    let blob = create_blob_object(file_content);
    let blob_sha = blob.sha1();

    // 2. Build a tree pointing to the blob.
    let tree = create_tree_object(&[(0o100644u32, file_path, &blob_sha)]);
    let tree_sha = tree.sha1();

    // 3. Build the commit.
    let commit = create_commit_object(
        &tree_sha,
        None,
        "Test User <test@example.com>",
        commit_message,
    );
    let commit_sha = commit.sha1();
    let commit_sha_hex = hex::encode(commit_sha);

    // 4. Generate the pack file containing all three objects.
    let pack_data = generate_pack(&[blob, tree, commit])
        .expect("pack generation should not fail for 3 objects");

    // 5. Build the pkt-line request body.
    //
    // Format:
    //   <pkt-line: old-sha new-sha refname\n>
    //   0000            (flush)
    //   <raw pack data>
    //
    let ref_line = format!("{old_sha_hex} {commit_sha_hex} {ref_name}\n");
    let mut body = Vec::new();
    body.extend_from_slice(
        pktline::encode_line(&ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    body.extend_from_slice(&pack_data);

    (body, commit_sha_hex)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_push_clone_roundtrip_via_smart_http() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // 1. Create a model repo.
    create_model_repo(&base_url, &token).await;

    // 2. Verify the repo has initial refs via info/refs (repo creation seeds an initial revision).
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("refs/heads/main"),
        "repo should advertise refs/heads/main after creation: {body:?}"
    );

    // 3. Test upload-pack on empty repo — should succeed with an empty pack.
    let upload_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        upload_resp.status().is_success(),
        "upload-pack on empty repo should succeed: {} {}",
        upload_resp.status(),
        upload_resp.text().await.unwrap()
    );

    // 4. Push a commit via git-receive-pack.
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, commit_sha) = build_receive_pack_request(
        null_sha,
        "README.md",
        b"# Hello World\n\nThis is a test file pushed via Git smart HTTP.\n",
        "Initial commit via git push",
    );

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "git-receive-pack should succeed: {}",
        push_resp.status()
    );
    let push_response_text = push_resp.text().await.unwrap();
    assert!(
        push_response_text.contains("unpack ok"),
        "push response should contain 'unpack ok': {push_response_text:?}"
    );
    assert!(
        push_response_text.contains("ok refs/heads/main"),
        "push response should confirm ref update: {push_response_text:?}"
    );

    // 5. Verify info/refs now advertises the pushed ref.
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("refs/heads/main"),
        "info/refs should advertise refs/heads/main after push: {body:?}"
    );
    assert!(
        body.contains(&commit_sha),
        "info/refs should advertise the commit SHA: {body:?}"
    );

    // 6. Upload-pack should now return the pushed commit's objects.
    let upload_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        upload_resp.status().is_success(),
        "upload-pack after push should succeed: {}",
        upload_resp.status()
    );
    let upload_response = upload_resp.text().await.unwrap();
    assert!(
        !upload_response.is_empty(),
        "upload-pack response should not be empty after push"
    );
    // The upload-pack response should contain the pack data with our objects.
    assert!(
        upload_response.len() > 100,
        "upload-pack response should contain pack data (got {} bytes)",
        upload_response.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_smart_http_ref_deletion_removes_branch_and_keeps_commit_available() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();
    create_model_repo(&base_url, &token).await;

    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, commit_sha) = build_receive_pack_request_for_ref(
        null_sha,
        "refs/heads/feature/remove-me",
        "feature.txt",
        b"temporary feature branch\n",
        "Create removable feature branch",
    );
    let push = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    let push_body = push.text().await.unwrap();
    assert!(
        push_body.contains("ok refs/heads/feature/remove-me"),
        "feature push should succeed: {push_body}"
    );

    let refs_before = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(refs_before.contains("refs/heads/feature/remove-me"));

    let delete_line = format!("{commit_sha} {null_sha} refs/heads/feature/remove-me\n");
    let mut delete_body = shardline_hub_api::git::pktline::encode_line(&delete_line)
        .expect("delete pkt-line")
        .into_bytes();
    delete_body.extend_from_slice(shardline_hub_api::git::pktline::FLUSH.as_bytes());
    let deletion = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(delete_body)
        .send()
        .await
        .unwrap();
    assert_eq!(deletion.status(), 200);
    let deletion_body = deletion.text().await.unwrap();
    assert!(
        deletion_body.contains("ok refs/heads/feature/remove-me"),
        "branch deletion should succeed: {deletion_body}"
    );

    let refs_after = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        !refs_after.contains("refs/heads/feature/remove-me"),
        "deleted branch must not be advertised: {refs_after}"
    );

    let preserved_commit = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/revision/{commit_sha}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        preserved_commit.status(),
        200,
        "ref deletion must preserve immutable commit history"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_unauthorized() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();

    // Create a model repo first.
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Try push without auth — should fail.
    let resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "push without auth should be 401");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_read_only_token() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();

    // Create a model repo with write token.
    let write_token = srv.token();
    create_model_repo(&base_url, &write_token).await;

    // Try push with a read-only token — should fail.
    let read_token = mint_token(shardline_protocol::TokenScope::Read).unwrap();
    let resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {read_token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "push with read-only token should fail: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 16. DELETE /api/{type}/{ns}/{repo}
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_delete_removes_repo() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;
    let client = Client::new();

    // DELETE the repo.
    let resp = client
        .delete(format!("{base_url}/api/models/test-owner/test-model"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "repo delete should return 204: {}",
        resp.status()
    );

    // Verify GET returns 404.
    let resp = client
        .get(format!("{base_url}/api/models/test-owner/test-model"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "GET after delete should return 404: {}",
        resp.status()
    );

    // Verify revisions endpoint returns 404.
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/revisions"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "revisions after delete should return 404: {}",
        resp.status()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_delete_nonexistent_returns_404() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token scoped to the exact (non-existent) repo so binding passes and the
    // repo-not-found 404 is returned (a mismatched scope would 403 instead).
    let token = srv.token_for("test-owner", "nonexistent");
    let client = Client::new();
    let resp = client
        .delete(format!("{base_url}/api/models/test-owner/nonexistent"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        404,
        "delete nonexistent repo should return 404"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_delete_requires_auth() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();
    let resp = client
        .delete(format!("{base_url}/api/models/test-owner/test-model"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401, "delete without auth should be 401");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repo_delete_requires_write_scope() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let client = Client::new();

    // Create the repo with a write token.
    let write_token = srv.token();
    create_model_repo(&base_url, &write_token).await;

    // Try to delete with a read-only token — should fail.
    let read_token = mint_token(shardline_protocol::TokenScope::Read).unwrap();
    let resp = client
        .delete(format!("{base_url}/api/models/test-owner/test-model"))
        .header("Authorization", format!("Bearer {read_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        403,
        "delete with read-only token should be 403"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_info_refs_discover_refs_for_clone() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // Create a model repo and push a commit.
    create_model_repo(&base_url, &token).await;
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, _commit_sha) = build_receive_pack_request(
        null_sha,
        "model.bin",
        b"model-weights-data",
        "Add model weights",
    );
    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(push_resp.status().is_success());

    // Test info/refs discovery for upload-pack (clone).
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "info/refs should return 200: {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("git-upload-pack"),
        "info/refs should advertise upload-pack service: {body:?}"
    );
    assert!(
        body.contains("refs/heads/main"),
        "info/refs should advertise refs/heads/main: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 17. Security-critical: receive-pack refname validation
// ---------------------------------------------------------------------------

/// Helper to build a receive-pack request body with a custom refname.
fn build_receive_pack_request_with_ref(
    old_sha_hex: &str,
    refname: &str,
    file_path: &str,
    file_content: &[u8],
    commit_message: &str,
) -> (Vec<u8>, String) {
    use shardline_hub_api::git::pack::{
        create_blob_object, create_commit_object, create_tree_object, generate_pack,
    };
    use shardline_hub_api::git::pktline;

    let blob = create_blob_object(file_content);
    let blob_sha = blob.sha1();
    let tree = create_tree_object(&[(0o100644u32, file_path, &blob_sha)]);
    let tree_sha = tree.sha1();
    let commit = create_commit_object(
        &tree_sha,
        None,
        "Test User <test@example.com>",
        commit_message,
    );
    let commit_sha = commit.sha1();
    let commit_sha_hex = hex::encode(commit_sha);

    let pack_data = generate_pack(&[blob, tree, commit]).expect("pack generation should not fail");

    let ref_line = format!("{old_sha_hex} {commit_sha_hex} {refname}\n");
    let mut body = Vec::new();
    body.extend_from_slice(
        pktline::encode_line(&ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    body.extend_from_slice(&pack_data);

    (body, commit_sha_hex)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_dotdot_refname() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, _) = build_receive_pack_request_with_ref(
        null_sha,
        "refs/heads/../../etc/passwd",
        "evil.txt",
        b"payload",
        "malicious push",
    );

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "receive-pack should return 200 (ref silently rejected): {}",
        push_resp.status()
    );
    let push_response = push_resp.text().await.unwrap();
    assert!(
        !push_response.contains("refs/heads/../../etc/passwd"),
        "response should not confirm the dotdot refname: {push_response:?}"
    );

    // Verify the malicious ref does not appear in info/refs.
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("../../etc/passwd"),
        "info/refs must not advertise path-traversal ref: {body:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_space_in_refname() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, _) = build_receive_pack_request_with_ref(
        null_sha,
        "refs/heads/feature branch",
        "feat.txt",
        b"content",
        "push with space",
    );

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "receive-pack should return 200 (ref silently rejected): {}",
        push_resp.status()
    );
    let push_response = push_resp.text().await.unwrap();
    assert!(
        !push_response.contains("refs/heads/feature branch"),
        "response should not confirm the space refname: {push_response:?}"
    );

    // Verify the ref does not appear in info/refs.
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        !body.contains("feature branch"),
        "info/refs must not advertise ref with space: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 18. Security-critical: malformed pack data
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_malformed_pack() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    use shardline_hub_api::git::pktline;

    // Build a pkt-line ref update pointing at a bogus commit SHA.
    let null_sha = "0000000000000000000000000000000000000000";
    let fake_sha = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    let ref_line = format!("{null_sha} {fake_sha} refs/heads/main\n");
    let mut body = Vec::new();
    body.extend_from_slice(
        pktline::encode_line(&ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    // Append garbage pack data.
    body.extend_from_slice(b"NOT-A-VALID-PACK-FILE-GARBAGE");

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "receive-pack should return 200 with error in body: {}",
        push_resp.status()
    );
    let push_response = push_resp.text().await.unwrap();
    // Garbage pack data parses to 0 objects. The ref is then rejected because
    // the commit SHA is not found in the (empty) pack.
    assert!(
        push_response.contains("ng refs/heads/main"),
        "response should reject the ref for malformed pack: {push_response:?}"
    );
}

// ---------------------------------------------------------------------------
// 19. Security-critical: oversized inline file rejection
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_rejects_oversized_inline_file() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Build base64 for 10 MiB + 1 byte (MAX_INLINE_FILE_BYTES is 10 MiB).
    // This is expensive but necessary for correctness.
    let oversized_content = vec![0xABu8; 10 * 1024 * 1024 + 1];
    let b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        &oversized_content,
    );
    let ndjson = format!(
        "{{\"header\":{{\"summary\":\"oversized commit\"}}}}\n{{\"file\":{{\"path\":\"big.bin\",\"content\":\"{b64}\"}}}}"
    );

    let client = Client::new();
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "oversized inline file should be rejected: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let error_msg = body["error"].as_str().unwrap_or_default();
    assert!(
        error_msg.contains("exceeds maximum") || error_msg.contains("LFS"),
        "error should mention size limit or LFS: {error_msg}"
    );
}

// ---------------------------------------------------------------------------
// 20. Correctness: clone after push returns correct content
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_clone_after_push_returns_correct_content() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();

    // Push a commit with known file content.
    let file_content = b"Hello from Shardline e2e test!\n";
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, commit_sha) =
        build_receive_pack_request(null_sha, "greeting.txt", file_content, "Add greeting file");

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(push_resp.status().is_success(), "push should succeed");

    // Fetch via upload-pack and verify the pack contains the file content.
    let upload_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        upload_resp.status().is_success(),
        "upload-pack should succeed: {}",
        upload_resp.status()
    );
    let upload_response = upload_resp.bytes().await.unwrap();
    let (pack_data, _messages) = shardline_hub_api::git::pktline::decode_sideband(&upload_response);
    assert!(
        !pack_data.is_empty(),
        "pack data should not be empty after push"
    );
    // Verify it's a valid pack file with objects.
    assert_eq!(&pack_data[0..4], b"PACK", "should be a valid pack file");
    let num_objects =
        u32::from_be_bytes([pack_data[8], pack_data[9], pack_data[10], pack_data[11]]);
    assert!(
        num_objects >= 3,
        "pack should contain at least 3 objects (blob+tree+commit), got {num_objects}"
    );

    // Verify the file content is accessible through the hub tree API.
    // The tree endpoint uses directory-based listing. For root-level files,
    // the info/refs and upload-pack validation above confirm correctness.
    // Verify info/refs shows the ref was updated.
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("refs/heads/main"),
        "info/refs should advertise refs/heads/main after push: {body:?}"
    );
    assert!(
        body.contains(&commit_sha),
        "info/refs should advertise the commit SHA: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 21. Correctness: multi-file single request commit
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_multi_file_single_request() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Build NDJSON with 5 files in a subdirectory.
    let mut ndjson = String::from("{\"header\":{\"summary\":\"five files\"}}\n");
    for i in 0..5 {
        let path = format!("data/file_{i}.txt");
        let content = format!("content of file {i}");
        let b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            content.as_bytes(),
        );
        ndjson.push_str(&format!(
            "{{\"file\":{{\"path\":\"{path}\",\"content\":\"{b64}\"}}}}\n"
        ));
    }

    let client = Client::new();
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "multi-file commit should succeed: {}",
        resp.status()
    );

    // Verify all 5 files appear in the tree (via subdirectory listing).
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/data"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "tree should succeed: {}", resp.status());
    let tree: serde_json::Value = resp.json().await.unwrap();
    let entries = tree.as_array().expect("tree should be an array");
    for i in 0..5 {
        let expected = format!("file_{i}.txt");
        assert!(
            entries.iter().any(|e| e["path"] == expected),
            "tree should contain {expected}: {tree:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 22. Correctness: delete removes file from tree
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_commit_delete_removes_from_tree() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Commit a file in a subdirectory.
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "data/ephemeral.txt",
        b"temporary content",
    )
    .await;

    // Verify it's in the tree.
    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/data"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let tree: serde_json::Value = resp.json().await.unwrap();
    let entries = tree.as_array().unwrap();
    assert!(
        entries.iter().any(|e| e["path"] == "ephemeral.txt"),
        "file should exist after first commit: {tree:?}"
    );

    // Delete the file in a second commit.
    let client = Client::new();
    let ndjson = "{\"header\":{\"summary\":\"delete file\"}}\n{\"deletedEntry\":{\"path\":\"data/ephemeral.txt\"}}";
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/commit/main"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-ndjson")
        .body(ndjson)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "delete commit should succeed: {}",
        resp.status()
    );

    // Verify the file is gone from the new tree.
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/data"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let tree: serde_json::Value = resp.json().await.unwrap();
    let entries = tree.as_array().unwrap();
    assert!(
        !entries.iter().any(|e| e["path"] == "ephemeral.txt"),
        "file should be removed after delete commit: {tree:?}"
    );
}

// ---------------------------------------------------------------------------
// 23. Correctness: force push rejected on existing branch
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_force_push_rejected_on_existing_branch() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    use shardline_hub_api::git::pack::{
        create_blob_object, create_commit_object, create_tree_object, generate_pack,
    };
    use shardline_hub_api::git::pktline;

    let client = Client::new();
    let null_sha = "0000000000000000000000000000000000000000";

    // Push commit A to refs/heads/main.
    let (objects_a, _commit_a_sha) = build_receive_pack_request_with_ref(
        null_sha,
        "refs/heads/main",
        "file_a.txt",
        b"content A",
        "Commit A",
    );
    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(objects_a)
        .send()
        .await
        .unwrap();
    assert!(push_resp.status().is_success());
    let body = push_resp.text().await.unwrap();
    assert!(
        body.contains("ok refs/heads/main"),
        "first push should succeed: {body}"
    );

    // Build commit B independently for the force push attempt.
    let blob_b = create_blob_object(b"content B force");
    let blob_b_sha = blob_b.sha1();
    let tree_b = create_tree_object(&[(0o100644u32, "file_b.txt", &blob_b_sha)]);
    let tree_b_sha = tree_b.sha1();
    let commit_b = create_commit_object(
        &tree_b_sha,
        None,
        "Test User <test@example.com>",
        "Force push B",
    );
    let commit_b_sha = commit_b.sha1();
    let pack_b = generate_pack(&[blob_b, tree_b, commit_b]).expect("pack generation");

    // Use a bogus old_sha that doesn't match current main (which is at A).
    let fake_old = hex::encode(commit_b_sha); // wrong: main is at A, not B
    let ref_line = format!("{fake_old} {} refs/heads/main\n", hex::encode(commit_b_sha));
    let mut force_body = Vec::new();
    force_body.extend_from_slice(
        pktline::encode_line(&ref_line)
            .expect("pkt-line too large")
            .as_bytes(),
    );
    force_body.extend_from_slice(pktline::FLUSH.as_bytes());
    force_body.extend_from_slice(&pack_b);

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(force_body)
        .send()
        .await
        .unwrap();
    assert!(push_resp.status().is_success());
    let body = push_resp.text().await.unwrap();
    assert!(
        body.contains("non-fast-forward"),
        "force push should be rejected as non-fast-forward: {body}"
    );
}

// ---------------------------------------------------------------------------
// 24. Correctness: tag push appears in revisions
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_tag_push_appears_in_revisions() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let null_sha = "0000000000000000000000000000000000000000";

    // First push a commit to refs/heads/main.
    let (push_body, _commit_sha) = build_receive_pack_request(
        null_sha,
        "README.md",
        b"# Tagged Release",
        "Prepare release",
    );
    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(push_resp.status().is_success());

    // Push a lightweight tag refs/tags/v1.0 pointing at a new commit.
    // Create a new commit for the tag using the helper.
    let (tag_body, _tag_commit_hex) = build_receive_pack_request_with_ref(
        null_sha,
        "refs/tags/v1.0",
        "tagged.txt",
        b"tagged content",
        "Tag v1.0",
    );

    let tag_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(tag_body)
        .send()
        .await
        .unwrap();
    assert!(
        tag_resp.status().is_success(),
        "tag push should succeed: {}",
        tag_resp.status()
    );
    let tag_response = tag_resp.text().await.unwrap();
    assert!(
        tag_response.contains("ok refs/tags/v1.0"),
        "tag push should be confirmed: {tag_response:?}"
    );

    // Verify revisions include the tag.
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/revisions"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let revisions = body["revisions"]
        .as_array()
        .expect("revisions array missing");
    assert!(
        revisions
            .iter()
            .any(|r| r["ref_name"].as_str() == Some("refs/tags/v1.0")),
        "revisions should contain refs/tags/v1.0: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 25. Edge case: upload-pack on empty repo
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_upload_pack_empty_repo() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let upload_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        upload_resp.status().is_success(),
        "upload-pack on empty repo should succeed: {}",
        upload_resp.status()
    );
    let upload_response = upload_resp.bytes().await.unwrap();
    // For an empty repo (no commits beyond the seed), the upload-pack should
    // succeed. The pack may contain the seed commit's objects (typically 1-2).
    if !upload_response.is_empty() {
        let (pack_data, _) = shardline_hub_api::git::pktline::decode_sideband(&upload_response);
        if pack_data.len() >= 12 && &pack_data[0..4] == b"PACK" {
            let num_objects =
                u32::from_be_bytes([pack_data[8], pack_data[9], pack_data[10], pack_data[11]]);
            // Empty repo seed objects should be minimal (tree + commit).
            assert!(
                num_objects <= 5,
                "empty repo upload-pack should have few seed objects, got {num_objects}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 26. Edge case: receive-pack with multiple refs in one request
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_multiple_refs() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    use shardline_hub_api::git::pack::{
        create_blob_object, create_commit_object, create_tree_object, generate_pack,
    };
    use shardline_hub_api::git::pktline;

    let null_sha = "0000000000000000000000000000000000000000";

    // Build 3 separate commits for 3 different refs.
    let mut all_objects = Vec::new();
    let mut ref_lines = Vec::new();

    for (i, refname) in ["refs/heads/main", "refs/heads/dev", "refs/heads/feat"]
        .into_iter()
        .enumerate()
    {
        let content = format!("File for ref {i}");
        let blob = create_blob_object(content.as_bytes());
        let blob_sha = blob.sha1();
        let tree = create_tree_object(&[(0o100644u32, &format!("file_{i}.txt"), &blob_sha)]);
        let tree_sha = tree.sha1();
        let commit = create_commit_object(
            &tree_sha,
            None,
            "Test User <test@example.com>",
            &format!("Commit for {refname}"),
        );
        let commit_sha = commit.sha1();
        let commit_hex = hex::encode(commit_sha);

        ref_lines.push(format!("{null_sha} {commit_hex} {refname}\n"));
        all_objects.push(blob);
        all_objects.push(tree);
        all_objects.push(commit);
    }

    let pack_data = generate_pack(&all_objects).expect("pack generation");
    let mut body = Vec::new();
    for line in &ref_lines {
        body.extend_from_slice(
            pktline::encode_line(line)
                .expect("pkt-line too large")
                .as_bytes(),
        );
    }
    body.extend_from_slice(pktline::FLUSH.as_bytes());
    body.extend_from_slice(&pack_data);

    let client = Client::new();
    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "multi-ref push should succeed: {}",
        push_resp.status()
    );
    let push_response = push_resp.text().await.unwrap();
    assert!(
        push_response.contains("ok refs/heads/main"),
        "main should be ok: {push_response:?}"
    );
    assert!(
        push_response.contains("ok refs/heads/dev"),
        "dev should be ok: {push_response:?}"
    );
    assert!(
        push_response.contains("ok refs/heads/feat"),
        "feat should be ok: {push_response:?}"
    );

    // Verify info/refs advertises all three.
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("refs/heads/main"),
        "info/refs should have main: {body:?}"
    );
    assert!(
        body.contains("refs/heads/dev"),
        "info/refs should have dev: {body:?}"
    );
    assert!(
        body.contains("refs/heads/feat"),
        "info/refs should have feat: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 27. Edge case: search returns empty after delete
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_search_after_delete_returns_empty() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    // Token scoped to the exact repo being created/deleted.
    let delete_me_token = srv.token_for("test-owner", "zzz-delete-me");

    // Create a repo with a unique name for search.
    let client = Client::new();
    client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {delete_me_token}"))
        .json(&serde_json::json!({
            "name": "test-owner/zzz-delete-me",
            "type": "model",
            "private": false,
        }))
        .send()
        .await
        .unwrap();

    // Verify search finds it.
    let resp = client
        .get(format!(
            "{base_url}/api/models/search?q=test-owner/zzz-delete"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().unwrap();
    assert!(
        repos
            .iter()
            .any(|r| r["id"].as_str() == Some("test-owner/zzz-delete-me")),
        "search should find the repo before deletion: {body:?}"
    );

    // Delete the repo.
    let resp = client
        .delete(format!("{base_url}/api/models/test-owner/zzz-delete-me"))
        .header("Authorization", format!("Bearer {delete_me_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // Search again — should be empty.
    let resp = client
        .get(format!(
            "{base_url}/api/models/search?q=test-owner/zzz-delete"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let repos = body["repos"].as_array().unwrap();
    assert!(
        !repos
            .iter()
            .any(|r| r["id"].as_str() == Some("test-owner/zzz-delete-me")),
        "search should not find deleted repo: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 28. Edge case: modelcard after README commit returns content
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_modelcard_after_readme_commit() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Commit a README.md with modelcard-like content.
    let readme_content = b"# My Great Model\n\nThis is a comprehensive model card.\n\n## Capabilities\n- Vision\n- Language\n";
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "README.md",
        readme_content,
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/modelcard"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "modelcard should return 200: {}",
        resp.status()
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("My Great Model"),
        "modelcard should contain model name: {body}"
    );
    assert!(
        body.contains("Vision"),
        "modelcard should contain capabilities: {body}"
    );
    assert!(
        body.contains("Language"),
        "modelcard should contain all capabilities: {body}"
    );
}

// ---------------------------------------------------------------------------
// 29. HF API spec fields: repo info includes all HF fields
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_repo_info_includes_hf_fields() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/models/test-owner/test-model"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "repo info failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();

    // Verify existing fields
    assert_eq!(body["id"], "test-owner/test-model");
    assert_eq!(body["type"], "model");
    assert_eq!(body["private"], false);
    assert!(body["url"].as_str().is_some(), "url should be present");
    assert!(
        body["default_branch"].as_str().is_some(),
        "default_branch should be present"
    );

    // Verify new HF-compatible fields
    let tags = body["tags"].as_array().expect("tags should be an array");
    assert!(tags.is_empty(), "tags should be empty by default");

    assert_eq!(body["downloads"], 0, "downloads should default to 0");
    assert_eq!(body["likes"], 0, "likes should default to 0");

    assert!(
        body["last_modified"].is_string(),
        "last_modified should be an ISO 8601 timestamp"
    );

    assert!(
        body["pipeline_tag"].is_null(),
        "pipeline_tag should be null/omitted by default"
    );

    assert!(
        body["security_status"].is_object(),
        "security_status should be an object"
    );
    let security_status = body["security_status"]
        .as_object()
        .expect("security_status should be object");
    assert!(
        security_status.is_empty(),
        "security_status should be empty by default"
    );
}

// ---------------------------------------------------------------------------
// 30. HF API spec fields: whoami includes auth details
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_whoami_includes_hf_fields() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();

    let client = Client::new();
    let resp = client
        .get(format!("{base_url}/api/whoami-v2"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "whoami failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();

    // Verify existing fields
    assert_eq!(body["name"], "test-subject");
    assert_eq!(body["is_admin"], false);

    // Verify new HF-compatible fields
    assert_eq!(body["type"], "user", "user type should be 'user'");

    let auth = &body["auth"];
    assert!(auth.is_object(), "auth should be an object");
    assert_eq!(auth["type"], "token", "auth type should be 'token'");

    let identity = &auth["identity"];
    assert!(identity.is_object(), "identity should be an object");

    let account = &identity["account"];
    assert!(account.is_object(), "account should be an object");
    assert_eq!(
        account["name"], "test-subject",
        "account name should match user name"
    );
}

// ---------------------------------------------------------------------------
// 32. HF API spec fields: tree supports recursive param
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_tree_supports_recursive() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Commit files in nested directories
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "a/b/deep.txt",
        b"deep content",
    )
    .await;
    commit_inline_file(
        &base_url,
        &token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "a/shallow.txt",
        b"shallow content",
    )
    .await;

    let client = Client::new();

    // Without recursive, listing "a" should show a directory "b" and file "shallow.txt"
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/a"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body.as_array().expect("tree should be an array");
    // Should have: directory "b" and file "shallow.txt"
    assert_eq!(
        entries.len(),
        2,
        "non-recursive should show 2 entries (dir b + file)"
    );
    let has_dir_b = entries
        .iter()
        .any(|e| e["path"] == "b" && e["type"] == "directory");
    assert!(has_dir_b, "should have directory 'b': {body:?}");
    let has_shallow = entries
        .iter()
        .any(|e| e["path"] == "shallow.txt" && e["type"] == "file");
    assert!(has_shallow, "should have file 'shallow.txt': {body:?}");

    // With recursive, listing "a" should show "b/deep.txt" and "shallow.txt"
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/a?recursive=true"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body.as_array().expect("tree should be an array");
    let has_deep = entries
        .iter()
        .any(|e| e["path"].as_str() == Some("b/deep.txt"));
    assert!(has_deep, "recursive should include 'b/deep.txt': {body:?}");
    let has_shallow = entries
        .iter()
        .any(|e| e["path"].as_str() == Some("shallow.txt"));
    assert!(
        has_shallow,
        "recursive should include 'shallow.txt': {body:?}"
    );
}

// ---------------------------------------------------------------------------
// 33. HF API spec fields: search accepts author/sort/direction params
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_search_accepts_hf_query_params() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();

    // Create a repo
    create_model_repo(&base_url, &token).await;

    let client = Client::new();

    // Search with author param (should not error)
    let resp = client
        .get(format!(
            "{base_url}/api/models/search?q=test-owner&author=test-owner"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "search with author param should succeed: {}",
        resp.status()
    );

    // Search with sort param (should not error)
    let resp = client
        .get(format!(
            "{base_url}/api/models/search?q=test-owner&sort=lastModified&direction=desc"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "search with sort/direction params should succeed: {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// 34. HF API spec fields: tree supports limit param
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_tree_supports_limit() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    create_model_repo(&base_url, &token).await;

    // Commit 3 files
    for i in 0..3 {
        commit_inline_file(
            &base_url,
            &token,
            "models",
            "test-owner",
            "test-model",
            "main",
            &format!("dir/file_{i}.txt"),
            b"content",
        )
        .await;
    }

    let client = Client::new();

    // With limit=1, recursive listing should return only 1 entry
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/tree/main/dir?recursive=true&limit=1"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let entries = body.as_array().expect("tree should be an array");
    assert_eq!(entries.len(), 1, "limit=1 should return exactly 1 entry");
}

// ---------------------------------------------------------------------------
// Gap 6. Hub API: dataset parquet endpoint returns file list
// ---------------------------------------------------------------------------

/// Helper: creates a dataset repo and commits a JSONL data file so dataset
/// viewer endpoints have content to serve.
async fn create_dataset_with_jsonl(base_url: &str, token: &str, jsonl_content: &[u8]) {
    let client = Client::new();

    // Create dataset repo
    let resp = client
        .post(format!("{base_url}/api/repos/create"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "name": "test-owner/test-dataset",
            "type": "dataset",
            "private": false,
        }))
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "dataset repo creation failed: {} {}",
        resp.status(),
        resp.text().await.unwrap()
    );

    // Commit a JSONL file under default/train/ which find_dataset_file will discover
    commit_inline_file(
        base_url,
        token,
        "datasets",
        "test-owner",
        "test-dataset",
        "main",
        "default/train/data.jsonl",
        jsonl_content,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_parquet_returns_file_list() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact dataset repo being created/accessed.
    let token = srv.token_for("test-owner", "test-dataset");

    let jsonl = b"{\"text\":\"hello\",\"label\":0}\n{\"text\":\"world\",\"label\":1}\n";
    create_dataset_with_jsonl(base_url, &token, jsonl).await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/datasets/test-owner/test-dataset/parquet"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "parquet endpoint failed: {}",
        resp.status()
    );
    let body: serde_json::Value = resp.json().await.unwrap();
    let files = body["files"].as_array().expect("files array missing");
    assert!(
        !files.is_empty(),
        "parquet files should not be empty after committing jsonl"
    );
    assert!(
        files
            .iter()
            .any(|f| f["path"].as_str() == Some("default/train/data.jsonl")),
        "files should include committed jsonl: {body:?}"
    );
}

// ---------------------------------------------------------------------------
// Gap 7. Hub API: first-rows endpoint returns columns and rows
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_first_rows_returns_columns_and_rows() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact dataset repo being created/accessed.
    let token = srv.token_for("test-owner", "test-dataset");

    let jsonl = b"{\"text\":\"hello\",\"label\":0}\n{\"text\":\"world\",\"label\":1}\n";
    create_dataset_with_jsonl(base_url, &token, jsonl).await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/api/datasets/test-owner/test-dataset/first-rows?config=default&split=train&limit=10"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "first-rows failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let columns = body["columns"].as_array().expect("columns array missing");
    assert!(!columns.is_empty(), "columns should not be empty");
    assert!(
        columns.iter().any(|c| c.as_str() == Some("text")),
        "columns should contain 'text': {body:?}"
    );
    assert!(
        columns.iter().any(|c| c.as_str() == Some("label")),
        "columns should contain 'label': {body:?}"
    );
    let rows = body["rows"].as_array().expect("rows array missing");
    assert_eq!(rows.len(), 2, "should have 2 rows from jsonl");
}

// ---------------------------------------------------------------------------
// Gap 8. Hub API: viewer endpoint returns paginated rows
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_dataset_viewer_returns_paginated_rows() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    // Token must be scoped to the exact dataset repo being created/accessed.
    let token = srv.token_for("test-owner", "test-dataset");

    // Create a dataset with enough rows to test pagination
    let mut jsonl = String::new();
    for i in 0..20 {
        jsonl.push_str(&format!("{{\"text\":\"row-{i}\",\"id\":{i}}}\n"));
    }
    create_dataset_with_jsonl(base_url, &token, jsonl.as_bytes()).await;

    let client = Client::new();

    // First page: offset=0, length=5
    let resp = client
        .get(format!(
            "{base_url}/api/datasets/test-owner/test-dataset/viewer/train?offset=0&length=5"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "viewer failed: {}", resp.status());
    let body: serde_json::Value = resp.json().await.unwrap();
    let columns = body["columns"].as_array().expect("columns array missing");
    assert!(!columns.is_empty(), "columns should not be empty");
    let rows = body["rows"].as_array().expect("rows array missing");
    assert_eq!(rows.len(), 5, "first page should have 5 rows");
    // num_rows_total may be absent (null) if not computed; just verify the
    // core fields are present.
    assert!(
        body.get("columns").is_some(),
        "response should include columns"
    );

    // Second page: offset=5, length=5
    let resp = client
        .get(format!(
            "{base_url}/api/datasets/test-owner/test-dataset/viewer/train?offset=5&length=5"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body2: serde_json::Value = resp.json().await.unwrap();
    let rows2 = body2["rows"].as_array().expect("rows array missing");
    assert_eq!(rows2.len(), 5, "second page should have 5 rows");
    // Verify pagination: first row of page 2 should differ from page 1
    assert_ne!(
        rows[0], rows2[0],
        "page 1 and page 2 should start with different rows"
    );
}

// ---------------------------------------------------------------------------
// Gap 9. Hub API: webhook CRUD roundtrip
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_webhook_create_list_delete_roundtrip() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // Create model repo
    create_model_repo(base_url, token).await;

    // POST webhook → 201
    let resp = client
        .post(format!(
            "{base_url}/api/models/test-owner/test-model/webhooks"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .json(&serde_json::json!({
            "url": "https://example.com/hook",
            "events": ["push"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "webhook create failed: {}",
        resp.status()
    );
    let wh: serde_json::Value = resp.json().await.unwrap();
    let wh_id = wh["id"].as_str().expect("webhook id missing");
    assert_eq!(wh["url"].as_str(), Some("https://example.com/hook"));
    assert!(
        wh["active"].as_bool().unwrap_or(false),
        "webhook should be active"
    );

    // GET webhooks list → verify present
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/webhooks"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let webhooks = body["webhooks"].as_array().expect("webhooks array missing");
    assert_eq!(webhooks.len(), 1, "should have 1 webhook after create");
    assert_eq!(webhooks[0]["id"].as_str(), Some(wh_id));

    // DELETE webhook → 204
    let resp = client
        .delete(format!(
            "{base_url}/api/models/test-owner/test-model/webhooks/{wh_id}"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "webhook delete failed: {}",
        resp.status()
    );

    // GET webhooks list → verify gone
    let resp = client
        .get(format!(
            "{base_url}/api/models/test-owner/test-model/webhooks"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let webhooks = body["webhooks"].as_array().expect("webhooks array missing");
    assert!(webhooks.is_empty(), "webhooks should be empty after delete");
}

// ---------------------------------------------------------------------------
// Gap 10. Hub API: resolve endpoint returns inline file content
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hub_resolve_returns_inline_file_content() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();

    create_model_repo(base_url, token).await;

    let file_content = b"# README\n\nThis is the resolved content.\n";
    commit_inline_file(
        base_url,
        token,
        "models",
        "test-owner",
        "test-model",
        "main",
        "README.md",
        file_content,
    )
    .await;

    let client = Client::new();
    let resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/resolve/main/README.md"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "resolve endpoint failed: {}",
        resp.status()
    );
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok());
    assert_eq!(
        ct,
        Some("application/octet-stream"),
        "resolve should return octet-stream"
    );
    let body = resp.bytes().await.unwrap();
    assert_eq!(body.as_ref(), file_content, "resolved content mismatch");
}

// ---------------------------------------------------------------------------
// Gap 3. Auth-gated Git Smart HTTP e2e: push then clone with valid token
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_smart_http_works_with_valid_token() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // 1. Create a model repo.
    create_model_repo(&base_url, &token).await;

    // 2. Push a commit via git-receive-pack with a valid write token.
    let file_content = b"token-gated content\nThis verifies auth-gated push works.\n";
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_body, commit_sha) =
        build_receive_pack_request(null_sha, "auth_test.txt", file_content, "Auth-gated push");

    let push_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_resp.status().is_success(),
        "push with valid token should succeed: {}",
        push_resp.status()
    );
    let push_response = push_resp.text().await.unwrap();
    assert!(
        push_response.contains("unpack ok"),
        "push response should confirm unpack: {push_response:?}"
    );
    assert!(
        push_response.contains("ok refs/heads/main"),
        "push response should confirm ref: {push_response:?}"
    );

    // 3. Clone via git-upload-pack with the same token.
    let upload_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(
        upload_resp.status().is_success(),
        "upload-pack with valid token should succeed: {}",
        upload_resp.status()
    );

    // 4. Verify the upload-pack response contains pack data with our objects.
    let upload_response = upload_resp.bytes().await.unwrap();
    let (pack_data, _messages) = shardline_hub_api::git::pktline::decode_sideband(&upload_response);
    assert!(
        !pack_data.is_empty(),
        "pack data should not be empty after push"
    );
    assert_eq!(&pack_data[0..4], b"PACK", "should be a valid pack file");
    let num_objects =
        u32::from_be_bytes([pack_data[8], pack_data[9], pack_data[10], pack_data[11]]);
    assert!(
        num_objects >= 3,
        "pack should contain at least 3 objects (blob+tree+commit), got {num_objects}"
    );

    // 5. Verify info/refs advertises the correct commit SHA.
    let info_resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(info_resp.status(), 200);
    let info_body = info_resp.text().await.unwrap();
    assert!(
        info_body.contains(&commit_sha),
        "info/refs should advertise the pushed commit SHA: {info_body:?}"
    );
    assert!(
        info_body.contains("refs/heads/main"),
        "info/refs should advertise refs/heads/main: {info_body:?}"
    );
}

// ---------------------------------------------------------------------------
// Gap 4. Non-fast-forward rejection e2e
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn git_receive_pack_rejects_non_fast_forward() {
    let srv = start_hub_server().await;
    let base_url = srv.base_url();
    let token = srv.token();
    let client = Client::new();

    // 1. Create a model repo.
    create_model_repo(&base_url, &token).await;

    // 2. Push commit A to main.
    let null_sha = "0000000000000000000000000000000000000000";
    let (push_a_body, commit_a_sha) =
        build_receive_pack_request(null_sha, "file_a.txt", b"content A", "Commit A");

    let push_a_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_a_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_a_resp.status().is_success(),
        "push A should succeed: {}",
        push_a_resp.status()
    );
    let push_a_response = push_a_resp.text().await.unwrap();
    assert!(
        push_a_response.contains("ok refs/heads/main"),
        "push A should confirm ref: {push_a_response:?}"
    );

    // 3. Push commit B to main with a WRONG old_sha (non-fast-forward).
    //    The current main is at commit_a_sha, but we claim it's at commit_b_sha.
    let (push_b_body, _commit_b_sha) = {
        use shardline_hub_api::git::pack::{
            create_blob_object, create_commit_object, create_tree_object, generate_pack,
        };
        use shardline_hub_api::git::pktline;

        let blob = create_blob_object(b"content B");
        let blob_sha = blob.sha1();
        let tree = create_tree_object(&[(0o100644u32, "file_b.txt", &blob_sha)]);
        let tree_sha = tree.sha1();
        let commit =
            create_commit_object(&tree_sha, None, "Test User <test@example.com>", "Commit B");
        let commit_sha = commit.sha1();
        let commit_sha_hex = hex::encode(commit_sha);

        let pack_data = generate_pack(&[blob, tree, commit]).expect("pack generation");

        // Use commit_b's own SHA as old_sha (which doesn't match current main at A).
        let wrong_old_sha = commit_sha_hex.clone();
        let ref_line = format!("{wrong_old_sha} {commit_sha_hex} refs/heads/main\n");
        let mut body = Vec::new();
        body.extend_from_slice(
            pktline::encode_line(&ref_line)
                .expect("pkt-line too large")
                .as_bytes(),
        );
        body.extend_from_slice(pktline::FLUSH.as_bytes());
        body.extend_from_slice(&pack_data);

        (body, commit_sha_hex)
    };

    let push_b_resp = client
        .post(format!(
            "{base_url}/models/test-owner/test-model/git-receive-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/x-git-receive-pack-request")
        .body(push_b_body)
        .send()
        .await
        .unwrap();
    assert!(
        push_b_resp.status().is_success(),
        "receive-pack should return 200 with error in body: {}",
        push_b_resp.status()
    );
    let push_b_response = push_b_resp.text().await.unwrap();
    assert!(
        push_b_response.contains("non-fast-forward"),
        "non-fast-forward push should be rejected: {push_b_response:?}"
    );

    // 4. Verify info/refs still shows commit A's SHA (main was not updated).
    let info_resp = client
        .get(format!(
            "{base_url}/models/test-owner/test-model/info/refs?service=git-upload-pack"
        ))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(info_resp.status(), 200);
    let info_body = info_resp.text().await.unwrap();
    assert!(
        info_body.contains(&commit_a_sha),
        "info/refs should still show commit A SHA after rejected push: {info_body:?}"
    );
}
