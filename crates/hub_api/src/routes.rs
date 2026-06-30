use axum::{
    Json, Router,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, post, put},
};

use std::sync::LazyLock;
use tokio::sync::Semaphore;

use crate::auth::HubAuth;
use crate::commit::{self, CommitInstruction, ParsedCommit};
use crate::error::HubApiError;
use crate::git;
use crate::models::*;
use crate::resolve;
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
use shardline_protocol::TokenScope;

/// Delivers webhook events to registered URLs.
///
/// This fires in the background after a commit. Failures are logged but do not
/// block the commit response.
async fn deliver_webhook_events(
    state: &HubState,
    repo_id: &str,
    event: &str,
    revision: &str,
) {
    static WEBHOOK_DELIVERY_SEMAPHORE: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(16));

    let client = match &state.http_client {
        Some(client) => client.clone(),
        None => return,
    };
    let webhooks = match state.store.webhooks_for_event(repo_id, event) {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("failed to load webhooks for {repo_id}: {e}");
            return;
        }
    };
    if webhooks.is_empty() {
        return;
    }
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let payload = crate::models::WebhookEventPayload {
        event: event.to_owned(),
        repository: repo_id.to_owned(),
        revision: revision.to_owned(),
        timestamp,
        data: serde_json::json!({}),
    };
    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!("failed to serialize webhook payload: {e}");
            return;
        }
    };
    for webhook in &webhooks {
        let url = webhook.url.clone();
        let body = body.clone();
        let secret = webhook.secret.clone();
        let client = client.clone();
        let Ok(_permit) = WEBHOOK_DELIVERY_SEMAPHORE.acquire().await else {
            tracing::warn!("webhook delivery semaphore closed");
            return;
        };
        tokio::spawn(async move {
            if let Err(e) = deliver_one_webhook(&client, &url, &body, secret.as_deref()).await {
                tracing::warn!("webhook delivery to {url} failed: {e}");
            }
        });
    }
}

/// Delivers a single webhook POST with optional HMAC-SHA256 signature.
async fn deliver_one_webhook(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    secret: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut request = client
        .post(url)
        .header("Content-Type", "application/json")
        .header("User-Agent", "shardline-hub/1.0");
    if let Some(secret) = secret {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())?;
        mac.update(body);
        let signature = hex::encode(mac.finalize().into_bytes());
        request = request.header("X-Hub-Signature-256", format!("sha256={signature}"));
    }
    let response = request.body(body.to_vec()).send().await?;
    if !response.status().is_success() {
        return Err(format!("webhook returned {}", response.status()).into());
    }
    Ok(())
}

/// Shared Hub API state.
#[derive(Clone)]
pub struct HubState {
    pub store: BoxedHubStore,
    pub auth: Option<HubAuth>,
    /// Optional HTTP client for webhook delivery.
    pub http_client: Option<reqwest::Client>,
}

impl std::fmt::Debug for HubState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HubState")
            .field("auth", &self.auth.is_some())
            .finish()
    }
}

/// Builds the Hub API router. The returned router is state-generic and can be
/// merged into any Axum router.
pub fn router<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/health", get(health))
        .route("/api/whoami-v2", get(whoami))
        .route(
            "/api/{type}/{ns}/{repo}/xet-read-token/{rev}",
            get(xet_read_token),
        )
        .route(
            "/api/{type}/{ns}/{repo}/xet-write-token/{rev}",
            get(xet_write_token),
        )
        .route("/api/repos/create", post(repo_create))
        .route("/api/repos", get(repo_list))
        .route("/api/{type}/search", get(repo_search))
        .route("/api/{type}/{ns}/{repo}", post(repo_create_type))
        .route("/api/{type}/{ns}/{repo}", get(repo_info))
        .route(
            "/api/{type}/{ns}/{repo}/modelcard",
            get(repo_modelcard),
        )
        .route(
            "/api/{type}/{ns}/{repo}/revisions",
            get(repo_revisions),
        )
        .route(
            "/api/{type}/{ns}/{repo}/preupload/{rev}",
            post(preupload),
        )
        .route(
            "/api/{type}/{ns}/{repo}/commit/{rev}",
            post(commit),
        )
        .route(
            "/api/{type}/{ns}/{repo}/tree/{rev}/{*path}",
            get(file_tree),
        )
        .route(
            "/{type}/{ns}/{repo}/resolve/{rev}/{*path}",
            get(resolve_file),
        )
        .route("/objects/batch", post(lfs_batch))
        .route("/lfs/objects/{oid}", put(lfs_upload))
        .route("/lfs/objects/{oid}", get(lfs_download))
        // Git Smart HTTP endpoints
        .route(
            "/{type}/{ns}/{repo}/info/refs",
            get(git::info_refs),
        )
        .route(
            "/{type}/{ns}/{repo}/HEAD",
            get(git_head),
        )
        .route(
            "/{type}/{ns}/{repo}/git-upload-pack",
            post(git::upload_pack),
        )
        .route(
            "/{type}/{ns}/{repo}/git-receive-pack",
            post(git::receive_pack),
        )
        // Dataset viewer endpoints
        .route(
            "/api/datasets/{ns}/{repo}/parquet",
            get(dataset_parquet),
        )
        .route(
            "/api/datasets/{ns}/{repo}/first-rows",
            get(dataset_first_rows),
        )
        .route(
            "/api/datasets/{ns}/{repo}/viewer/{split}",
            get(dataset_viewer),
        )
        // Webhook endpoints
        .route(
            "/api/{type}/{ns}/{repo}/webhooks",
            post(webhook_create).get(webhook_list),
        )
        .route(
            "/api/{type}/{ns}/{repo}/webhooks/{webhook_id}",
            delete(webhook_delete),
        )
}

// ---- Health ----

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": "ok" }))
}

/// Authorize the request if auth is configured. Returns `Ok(())` when no auth
/// is set (permissive) or when the token satisfies the required scope.
fn authorize(
    state: &HubState,
    headers: &axum::http::HeaderMap,
    required_scope: TokenScope,
) -> Result<(), HubApiError> {
    if let Some(auth) = &state.auth {
        auth.authorize(headers, required_scope)?;
    }
    Ok(())
}

/// Converts a `HubRepoType` to the API path string.
const fn repo_type_path(rt: HubRepoType) -> &'static str {
    match rt {
        HubRepoType::Model => "models",
        HubRepoType::Dataset => "datasets",
        HubRepoType::Space => "spaces",
    }
}

// ---- Whoami ----

async fn whoami(
    headers: axum::http::HeaderMap,
) -> Result<Json<WhoamiResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("whoami", "GET", 200);
    let state = crate::state::get();
    let name = state
        .auth
        .as_ref()
        .and_then(|auth| auth.authorize(&headers, TokenScope::Read).ok())
        .map_or_else(|| "anonymous".to_owned(), |ctx| ctx.subject().to_owned());
    Ok(Json(WhoamiResponse {
        name,
        is_admin: true,
    }))
}

// ---- Token exchange (requires Read scope) ----

async fn xet_read_token(
    headers: axum::http::HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("xet_read_token", "GET", 200);
    let state = crate::state::get();
    let ctx = if let Some(auth) = &state.auth {
        auth.authorize(&headers, TokenScope::Read)?
    } else {
        return Err(HubApiError::Unauthorized);
    };
    let token = state
        .auth
        .as_ref()
        .ok_or(HubApiError::Unauthorized)?
        .provider()
        .mint_token(ctx.claims())
        .map_err(|e| {
            tracing::debug!("failed to mint token: {e}");
            HubApiError::InvalidToken
        })?;
    Ok(Json(TokenExchangeResponse { token }))
}

async fn xet_write_token(
    headers: axum::http::HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("xet_write_token", "GET", 200);
    let state = crate::state::get();
    let ctx = if let Some(auth) = &state.auth {
        auth.authorize(&headers, TokenScope::Write)?
    } else {
        return Err(HubApiError::Unauthorized);
    };
    let token = state
        .auth
        .as_ref()
        .ok_or(HubApiError::Unauthorized)?
        .provider()
        .mint_token(ctx.claims())
        .map_err(|e| {
            tracing::debug!("failed to mint token: {e}");
            HubApiError::InvalidToken
        })?;
    Ok(Json(TokenExchangeResponse { token }))
}

// ---- Repo create (generic, requires Write) ----

async fn repo_create(
    headers: axum::http::HeaderMap,
    Json(request): Json<RepoCreateRequest>,
) -> Result<(StatusCode, Json<RepoResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("repo_create", "POST", 201);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let full_name = request.name.clone();
    let repo_type = match request.repo_type {
        RepoType::Model => HubRepoType::Model,
        RepoType::Dataset => HubRepoType::Dataset,
        RepoType::Space => HubRepoType::Space,
    };
    let repo = state
        .store
        .create_repo(repo_type, &full_name, request.private)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(repo_response_from_hub(&repo))))
}

// ---- Repo create (type-specific, requires Write) ----

async fn repo_create_type(
    headers: axum::http::HeaderMap,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<RepoResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("repo_create_type", "POST", 201);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let rt = HubRepoType::parse_str(&repo_type).ok_or_else(|| {
        HubApiError::PathValidation(format!("invalid repo type: {repo_type}"))
    })?;
    let name = format!("{ns}/{repo}");
    let private = body
        .get("private")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let created = state
        .store
        .create_repo(rt, &name, private)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(repo_response_from_hub(&created))))
}

fn repo_response_from_hub(repo: &shardline_index::hub::HubRepo) -> RepoResponse {
    RepoResponse {
        id: repo.repo_id.clone(),
        repo_type: match repo.repo_type {
            HubRepoType::Model => RepoType::Model,
            HubRepoType::Dataset => RepoType::Dataset,
            HubRepoType::Space => RepoType::Space,
        },
        private: repo.private,
        url: format!(
            "/{}/{repo_id}",
            repo_type_path(repo.repo_type),
            repo_id = repo.repo_id
        ),
        default_branch: Some("main".to_owned()),
    }
}

// ---- Repo list (requires Read) ----

async fn repo_list(
    headers: axum::http::HeaderMap,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_list", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let repos = state
        .store
        .list_repos()
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let response = RepoListResponse {
        repos: repos.iter().map(repo_response_from_hub).collect(),
    };
    Ok(Json(response))
}

// ---- Repo search (requires Read) ----

async fn repo_search(
    headers: axum::http::HeaderMap,
    Path(repo_type): Path<String>,
    Query(query): Query<RepoSearchQuery>,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_search", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let rt = HubRepoType::parse_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    if query.q.len() < 2 {
        return Err(HubApiError::PathValidation(
            "search query must be at least 2 characters".to_owned(),
        ));
    }
    let limit = query.limit.min(200);
    let repos = state
        .store
        .search_repos(Some(rt), &query.q, limit)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let response = RepoListResponse {
        repos: repos.iter().map(repo_response_from_hub).collect(),
    };
    Ok(Json(response))
}

// ---- Repo modelcard (requires Read) ----

async fn repo_modelcard(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_modelcard", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let readme = files
        .iter()
        .find(|f| f.path == "README.md")
        .ok_or(HubApiError::NotFound)?;
    let resp_headers = [
        ("Content-Type", "text/markdown; charset=utf-8"),
        ("X-Shardline-SHA", readme.sha.as_str()),
    ];
    readme.inline_content.as_ref().map_or_else(
        || Ok((resp_headers, format!("model card {} bytes", readme.size)).into_response()),
        |content| Ok((resp_headers, content.clone()).into_response()),
    )
}

// ---- Repo revisions (requires Read) ----

async fn repo_revisions(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<RevisionListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_revisions", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let _ = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let revisions = state
        .store
        .list_revisions(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let response = RevisionListResponse {
        revisions: revisions
            .iter()
            .map(|r| RevisionResponse {
                ref_name: r.ref_name.clone(),
                sha: r.sha.clone(),
            })
            .collect(),
    };
    Ok(Json(response))
}

fn webhook_response_from_hub(webhook: &shardline_index::hub::HubWebhook) -> WebhookResponse {
    WebhookResponse {
        id: webhook.id.clone(),
        url: webhook.url.clone(),
        events: webhook.events.clone(),
        active: webhook.active,
        created_at: webhook.created_at_unix_seconds,
    }
}

// ---- Repo info (requires Read) ----

async fn repo_info(
    headers: axum::http::HeaderMap,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_info", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let _rt = HubRepoType::parse_str(&repo_type).ok_or_else(|| {
        HubApiError::PathValidation(format!("invalid repo type: {repo_type}"))
    })?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    Ok(Json(repo_response_from_hub(&entry)))
}

// ---- Preupload (requires Write) ----

async fn preupload(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    Json(request): Json<PreuploadRequest>,
) -> Result<Json<PreuploadResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("preupload", "POST", 200);
    const MAX_PREUPLOAD_FILES: usize = 10_000;
    if request.files.len() > MAX_PREUPLOAD_FILES {
        return Err(HubApiError::PathValidation(format!(
            "preupload request exceeds maximum of {MAX_PREUPLOAD_FILES} files"
        )));
    }
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;

    let existing_files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    let result: Vec<PreuploadResult> = request
        .files
        .into_iter()
        .map(|f| PreuploadResult {
            exists: existing_files.iter().any(|ef| ef.path == f.path),
            path: f.path,
        })
        .collect();

    Ok(Json(PreuploadResponse { result }))
}

// ---- Commit (requires Write) ----

async fn commit(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    body: String,
) -> Result<Json<CommitResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("commit", "POST", 200);
    shardline_metrics::record_hub_api_commit("ndjson");
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    let parent_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let parsed = commit::parse_ndjson_commit(&body)?;
    apply_commit(state, &name, &parent_sha, &parsed).await
}

async fn apply_commit(
    state: &HubState,
    repo_id: &str,
    parent_sha: &str,
    parsed: &ParsedCommit,
) -> Result<Json<CommitResponse>, HubApiError> {
    let existing_files: Vec<HubFileEntry> = state
        .store
        .get_files(parent_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut files: Vec<HubFileEntry> = existing_files;
    let mut file_hashes = Vec::new();

    for instruction in &parsed.instructions {
        match instruction {
            CommitInstruction::InlineFile { path, content } => {
                let sha = {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut h = DefaultHasher::new();
                    content.hash(&mut h);
                    format!("{:016x}", h.finish())
                };
                let size = content.len() as u64;
                files.retain(|f| f.path != *path);
                files.push(HubFileEntry {
                    path: path.clone(),
                    size,
                    sha: sha.clone(),
                    is_lfs: false,
                    inline_content: Some(content.clone()),
                });
                file_hashes.push(sha);
            }
            CommitInstruction::LfsPointer { path, oid, size } => {
                commit::validate_lfs_oid(oid)?;
                files.retain(|f| f.path != *path);
                files.push(HubFileEntry {
                    path: path.clone(),
                    size: *size,
                    sha: oid.clone(),
                    is_lfs: true,
                    inline_content: None,
                });
                file_hashes.push(oid.clone());
            }
            CommitInstruction::Delete { path } => {
                files.retain(|f| f.path != *path);
            }
        }
    }

    let files_hash = {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut h = DefaultHasher::new();
        for fh in &file_hashes {
            fh.hash(&mut h);
        }
        format!("{:016x}", h.finish())
    };
    let commit_sha = shardline_index::hub::HubRepo::compute_commit_sha(parent_sha, &parsed.message, &files_hash)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    state
        .store
        .store_files(&commit_sha, &files)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    state
        .store
        .create_revision(repo_id, Some(parent_sha), &commit_sha, "main", &parsed.message)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    // Fire webhook deliveries in the background (non-blocking).
    deliver_webhook_events(state, repo_id, "push", &commit_sha).await;

    Ok(Json(CommitResponse {
        commit_id: commit_sha,
        ref_name: Some("main".to_owned()),
    }))
}

// ---- File tree (requires Read) ----

async fn file_tree(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev, file_path)): Path<(String, String, String, String, String)>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    shardline_metrics::record_hub_api_request("file_tree", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let entries = tree_entries_at_path(&files, &file_path);
    Ok(Json(entries))
}

fn tree_entries_at_path(files: &[HubFileEntry], path: &str) -> Vec<TreeEntry> {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };

    let mut entries = Vec::new();
    let mut seen_dirs = std::collections::HashSet::new();

    for file in files {
        if !prefix.is_empty() && !file.path.starts_with(&prefix) {
            continue;
        }
        let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);

        if let Some((dir, _rest)) = relative.split_once('/') {
            if seen_dirs.insert(dir.to_owned()) {
                entries.push(TreeEntry {
                    entry_type: "directory".to_owned(),
                    path: dir.to_owned(),
                    size: None,
                    lfs: None,
                });
            }
        } else {
            let lfs = if file.is_lfs {
                Some(TreeEntryLfs {
                    oid: file.sha.clone(),
                    size: file.size,
                })
            } else {
                None
            };
            entries.push(TreeEntry {
                entry_type: "file".to_owned(),
                path: relative.to_owned(),
                size: Some(file.size),
                lfs,
            });
        }
    }

    entries.sort_by(|a, b| {
        a.entry_type
            .cmp(&b.entry_type)
            .then_with(|| a.path.cmp(&b.path))
    });

    entries
}

// ---- File resolve (download, requires Read) ----

async fn resolve_file(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev, file_path)): Path<(String, String, String, String, String)>,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("resolve_file", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let result = resolve::resolve_file_from_store(state, &commit_sha, &file_path)?;

    match result {
        resolve::DownloadResult::Inline { size, sha, content } => {
            let resp_headers = [
                ("Content-Type", "application/octet-stream"),
                ("X-Shardline-SHA", sha.as_str()),
            ];
            content.map_or_else(
                || Ok((resp_headers, format!("inline file {size} bytes")).into_response()),
                |data| Ok((resp_headers, data).into_response()),
            )
        }
        resolve::DownloadResult::LfsRedirect { oid, .. } => {
            let redirect_url = format!("/lfs/objects/{oid}");
            Ok(Redirect::temporary(&redirect_url).into_response())
        }
    }
}

// ---- LFS batch (requires Read) ----

async fn lfs_batch(
    headers: axum::http::HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_batch", "POST", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;

    let objects: Vec<LfsObjectResponse> = request
        .objects
        .iter()
        .map(|obj| {
            let exists = state
                .store
                .has_lfs_object(&obj.oid)
                .unwrap_or(false);
            let actions = match request.operation {
                LfsBatchOperation::Download => {
                    if exists {
                        Some(LfsObjectActions {
                            download: Some(crate::models::LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                            upload: None,
                            verify: None,
                        })
                    } else {
                        None
                    }
                }
                LfsBatchOperation::Upload => {
                    if exists {
                        Some(LfsObjectActions {
                            download: None,
                            upload: None,
                            verify: Some(crate::models::LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                        })
                    } else {
                        Some(LfsObjectActions {
                            download: None,
                            upload: Some(crate::models::LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                            verify: None,
                        })
                    }
                }
                LfsBatchOperation::Verify => {
                    if exists {
                        Some(LfsObjectActions {
                            download: None,
                            upload: None,
                            verify: Some(crate::models::LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                        })
                    } else {
                        Some(LfsObjectActions {
                            download: None,
                            upload: Some(crate::models::LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                            verify: None,
                        })
                    }
                }
            };

            let error = if !exists && request.operation == LfsBatchOperation::Download {
                Some(LfsObjectError {
                    code: 404,
                    message: "Object not found".to_owned(),
                })
            } else {
                None
            };

            LfsObjectResponse {
                oid: obj.oid.clone(),
                size: obj.size,
                actions,
                error,
            }
        })
        .collect();

    Ok(Json(LfsBatchResponse {
        transfer: "basic".to_owned(),
        objects,
    }))
}

// ---- LFS upload (requires Write) ----

async fn lfs_upload(
    headers: axum::http::HeaderMap,
    Path(oid): Path<String>,
    body: bytes::Bytes,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_upload", "PUT", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    commit::validate_lfs_oid(&oid)?;
    state
        .store
        .put_lfs_object(&oid, &body)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::OK)
}

// ---- LFS download (requires Read) ----

async fn lfs_download(
    headers: axum::http::HeaderMap,
    Path(oid): Path<String>,
) -> Result<(StatusCode, [(axum::http::header::HeaderName, &'static str); 1], Vec<u8>), HubApiError>
{
    shardline_metrics::record_hub_api_request("lfs_download", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let data = state
        .store
        .get_lfs_object(&oid)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::NotFound)?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        data,
    ))
}

// ---- Git HEAD reference ----

/// Serves the HEAD reference for a repository.
async fn git_head(
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<String, HubApiError> {
    let state = crate::state::get();
    let repo_id = format!("{repo_type}/{ns}/{repo}");
    let revisions = state
        .store
        .list_revisions(&repo_id)
        .map_err(|e| {
            tracing::debug!("failed to list revisions for {repo_id}: {e}");
            HubApiError::RepoNotFound
        })?;

    // Find HEAD revision — prefer explicit HEAD, then empty, then fall back to latest.
    let head_sha = revisions
        .iter()
        .find(|r| r.ref_name == "HEAD" || r.ref_name.is_empty())
        .or_else(|| revisions.first())
        .map(|r| r.sha.as_str())
        .unwrap_or("0000000000000000000000000000000000000000");

    Ok(format!("ref: refs/heads/main\n{head_sha} refs/heads/main\n"))
}

// ---- Dataset viewer endpoints ----

/// Lists parquet/data files in a dataset repository.
async fn dataset_parquet(
    headers: axum::http::HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
) -> Result<Json<DatasetParquetResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_parquet", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let parquet_files: Vec<DatasetParquetFile> = files
        .iter()
        .filter(|f| f.path.ends_with(".parquet") || f.path.ends_with(".csv") || f.path.ends_with(".jsonl"))
        .map(|f| DatasetParquetFile {
            path: f.path.clone(),
            size: f.size,
            sha: f.sha.clone(),
        })
        .collect();
    Ok(Json(DatasetParquetResponse {
        files: parquet_files,
    }))
}

/// Returns the first rows of a dataset split.
async fn dataset_first_rows(
    headers: axum::http::HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
    Query(query): Query<DatasetFirstRowsQuery>,
) -> Result<Json<DatasetFirstRowsResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_first_rows", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let data_file = find_dataset_file(&files, &query.config, &query.split)
        .ok_or_else(|| HubApiError::PathValidation("no data file found for config/split".to_owned()))?;
    let content = data_file
        .inline_content
        .as_deref()
        .ok_or_else(|| HubApiError::PathValidation("file content not available inline".to_owned()))?;
    let limit = query.limit.min(1000);
    let rows = parse_rows_from_content(content, &data_file.path, 0, limit)?;
    let columns = rows.first().map(|r| r.columns.keys().cloned().collect()).unwrap_or_default();
    Ok(Json(DatasetFirstRowsResponse { columns, rows }))
}

/// Returns rows from a dataset split with pagination.
async fn dataset_viewer(
    headers: axum::http::HeaderMap,
    Path((ns, repo, split)): Path<(String, String, String)>,
    Query(query): Query<DatasetViewerQuery>,
) -> Result<Json<DatasetViewerResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_viewer", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if entry.repo_type != HubRepoType::Dataset {
        return Err(HubApiError::PathValidation(
            "not a dataset repository".to_owned(),
        ));
    }
    let commit_sha = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let data_file = find_dataset_file(&files, &query.config, &split)
        .ok_or_else(|| HubApiError::PathValidation("no data file found for config/split".to_owned()))?;
    let content = data_file
        .inline_content
        .as_deref()
        .ok_or_else(|| HubApiError::PathValidation("file content not available inline".to_owned()))?;
    let length = query.length.min(10000);
    let rows = parse_rows_from_content(content, &data_file.path, query.offset, length)?;
    let columns = rows.first().map(|r| r.columns.keys().cloned().collect()).unwrap_or_default();
    Ok(Json(DatasetViewerResponse {
        columns,
        rows,
        num_rows_total: None,
    }))
}

/// Finds the data file for a given config and split.
fn find_dataset_file<'input>(
    files: &'input [HubFileEntry],
    config: &str,
    split: &str,
) -> Option<&'input HubFileEntry> {
    let candidates = [
        format!("{config}/{split}/data.parquet"),
        format!("{config}/{split}/data.csv"),
        format!("{config}/{split}/data.jsonl"),
        format!("data/{split}/data.parquet"),
        format!("data/{split}/data.csv"),
        format!("data/{split}/data.jsonl"),
        format!("{split}/data.parquet"),
        format!("{split}/data.csv"),
        format!("{split}/data.jsonl"),
        String::from("data.parquet"),
        String::from("data.csv"),
        String::from("data.jsonl"),
    ];
    for candidate in &candidates {
        if let Some(file) = files.iter().find(|f| f.path == *candidate) {
            return Some(file);
        }
    }
    None
}

/// Parses rows from inline file content (CSV or JSONL).
fn parse_rows_from_content(
    content: &[u8],
    path: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<DatasetRow>, HubApiError> {
    let text = std::str::from_utf8(content)
        .map_err(|e| HubApiError::PathValidation(format!("invalid UTF-8: {e}")))?;
    if path.ends_with(".jsonl") {
        parse_jsonl_rows(text, offset, limit)
    } else if path.ends_with(".csv") {
        parse_csv_rows(text, offset, limit)
    } else {
        Err(HubApiError::PathValidation(format!(
            "unsupported file format: {path}"
        )))
    }
}

/// Parses JSONL (newline-delimited JSON) rows.
#[allow(clippy::arithmetic_side_effects)]
fn parse_jsonl_rows(text: &str, offset: usize, limit: usize) -> Result<Vec<DatasetRow>, HubApiError> {
    let mut rows = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if i < offset {
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .map_err(|e| HubApiError::PathValidation(format!("invalid JSON at line {}: {e}", i + 1)))?;
        let columns = value
            .as_object()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        rows.push(DatasetRow { columns });
    }
    Ok(rows)
}

/// Parses CSV rows.
fn parse_csv_rows(text: &str, offset: usize, limit: usize) -> Result<Vec<DatasetRow>, HubApiError> {
    let mut lines = text.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| HubApiError::PathValidation("empty CSV file".to_owned()))?;
    let headers: Vec<String> = header_line
        .split(',')
        .map(|h| h.trim().trim_matches('"').to_owned())
        .collect();
    let mut rows = Vec::new();
    for (i, line) in lines.enumerate() {
        if i < offset {
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let values: Vec<&str> = line.split(',').collect();
        let columns: std::collections::BTreeMap<String, serde_json::Value> = headers
            .iter()
            .zip(values.iter())
            .map(|(h, v)| {
                let json_val = serde_json::from_str(v)
                    .unwrap_or_else(|_| serde_json::Value::String(v.trim_matches('"').to_owned()));
                (h.clone(), json_val)
            })
            .collect();
        rows.push(DatasetRow { columns });
    }
    Ok(rows)
}

// ---- Webhook endpoints ----

/// Maximum allowed webhook URL length.
const MAX_WEBHOOK_URL_LEN: usize = 2048;

/// Maximum number of events per webhook.
const MAX_WEBHOOK_EVENTS: usize = 50;

/// Validates a webhook URL to prevent SSRF attacks.
///
/// Checks:
/// - Scheme is `http` or `https`
/// - Host is present
/// - URL length does not exceed 2048 characters
/// - Host is not a private/internal IP or reserved address
fn validate_webhook_url(url: &str) -> Result<(), HubApiError> {
    if url.len() > MAX_WEBHOOK_URL_LEN {
        return Err(HubApiError::PathValidation(format!(
            "webhook URL exceeds maximum length of {MAX_WEBHOOK_URL_LEN}"
        )));
    }

    let parsed = url::Url::parse(url)
        .map_err(|e| HubApiError::PathValidation(format!("invalid webhook URL: {e}")))?;

    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(HubApiError::PathValidation(format!(
            "webhook URL scheme must be http or https, got {scheme}"
        )));
    }

    let host_str = parsed
        .host_str()
        .ok_or_else(|| HubApiError::PathValidation("webhook URL has no host".to_owned()))?;

    // Strip brackets from IPv6 addresses like [::1]
    let host = host_str
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host_str);

    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if is_private_ip(&ip) {
            return Err(HubApiError::PathValidation(
                "webhook URL must not point to a private/internal/reserved address".to_owned(),
            ));
        }
    } else if host == "localhost" {
        return Err(HubApiError::PathValidation(
            "webhook URL must not point to localhost".to_owned(),
        ));
    }

    Ok(())
}

/// Returns `true` if the IP address is private, loopback, link-local, or
/// otherwise reserved (not globally routable).
#[allow(clippy::missing_const_for_fn)]
fn is_private_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback() // 127.0.0.0/8
                || v4.is_private() // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                || v4.is_link_local() // 169.254.0.0/16
                || v4.is_unspecified() // 0.0.0.0
                || v4.is_broadcast()
                || v4.is_documentation() // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() // ::1
                || v6.is_unspecified() // ::
                || v6.is_unicast_link_local() // fe80::/10
        }
    }
}

/// Creates a webhook for a repository.
async fn webhook_create(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
    Json(request): Json<WebhookCreateRequest>,
) -> Result<(StatusCode, Json<WebhookResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_create", "POST", 201);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    if request.events.len() > MAX_WEBHOOK_EVENTS {
        return Err(HubApiError::PathValidation(format!(
            "webhook events exceeds maximum of {MAX_WEBHOOK_EVENTS}"
        )));
    }
    validate_webhook_url(&request.url)?;
    let name = format!("{ns}/{repo}");
    let _ = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let webhook = state
        .store
        .create_webhook(&name, &request.url, &request.events, request.secret.as_deref())
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(webhook_response_from_hub(&webhook))))
}

/// Lists webhooks for a repository.
async fn webhook_list(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<WebhookListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_list", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let webhooks = state
        .store
        .list_webhooks(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let response = WebhookListResponse {
        webhooks: webhooks.iter().map(webhook_response_from_hub).collect(),
    };
    Ok(Json(response))
}

/// Deletes a webhook.
async fn webhook_delete(
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, webhook_id)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_delete", "DELETE", 204);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    state
        .store
        .delete_webhook(&name, &webhook_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}
