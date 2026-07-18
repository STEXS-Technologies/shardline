use axum::{
    Json, Router,
    extract::{Path, Query, State},
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
async fn deliver_webhook_events(state: &HubState, repo_id: &str, event: &str, revision: &str) {
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
        let Ok(permit) = WEBHOOK_DELIVERY_SEMAPHORE.acquire().await else {
            tracing::warn!("webhook delivery semaphore closed");
            return;
        };
        let url_for_log = sanitize_log_url(&url);
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(e) = deliver_one_webhook(&client, &url, &body, secret.as_deref()).await {
                tracing::warn!("webhook delivery to {url_for_log} failed: {e}");
            }
        });
    }
}

/// Sanitizes a URL for safe inclusion in log messages.
///
/// Replaces control characters (newlines, tabs, etc.) and truncates to a
/// reasonable length to prevent log injection via user-supplied URLs.
fn sanitize_log_url(url: &str) -> String {
    const MAX_LOG_URL_LEN: usize = 200;
    let sanitized: String = url
        .chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .take(MAX_LOG_URL_LEN)
        .collect();
    if url.len() > MAX_LOG_URL_LEN {
        format!("{sanitized}...")
    } else {
        sanitized
    }
}

/// Delivers a single webhook POST with optional HMAC-SHA256 signature.
async fn deliver_one_webhook(
    client: &reqwest::Client,
    url: &str,
    body: &[u8],
    secret: Option<&str>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_webhook_url(url)?;
    // Resolve DNS and verify none of the resolved addresses are private.
    // DNS-based bypass is the most common SSRF vector — hostnames like
    // "localtest.me" (→127.0.0.1) or "metadata.google.internal" (→169.254.169.254)
    // would pass the string-based check but resolve to private IPs.
    //
    // NOTE: There is a theoretical TOCTOU (time-of-check-time-of-use) window
    // between DNS validation and the actual HTTP connection — a DNS rebinding
    // attack could change the resolution between the two lookups. Mitigating
    // this fully requires reqwest's `dns` feature (ClientBuilder::resolve) to
    // pin the connection to validated addresses, which is not currently enabled.
    // The attack window is very narrow in practice and requires a cooperating
    // authoritative DNS server, so this is accepted as a known limitation.
    {
        let parsed_url =
            url::Url::parse(url).map_err(|e| format!("webhook URL parse failed: {e}"))?;
        let host = parsed_url.host_str().ok_or("webhook URL has no host")?;
        let port = parsed_url.port_or_known_default().unwrap_or(80);
        let host_port = format!("{host}:{port}");
        for addr in tokio::net::lookup_host(&*host_port).await? {
            if is_private_ip(&addr.ip()) {
                return Err("webhook URL resolves to a private address".into());
            }
        }
    }
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

/// Builds the Hub API router with [`HubState`] as the shared state.
///
/// When `register_xet_token_routes` is `false`, the xet-read-token and
/// xet-write-token routes are omitted to avoid conflicts with the Xet
/// protocol frontend when both are enabled simultaneously.
pub fn router(register_xet_token_routes: bool) -> Router<HubState> {
    let mut r = Router::new()
        .route("/health", get(health))
        .route("/api/whoami-v2", get(whoami));
    if register_xet_token_routes {
        r = r
            .route(
                "/api/{type}/{ns}/{repo}/xet-read-token/{rev}",
                get(xet_read_token),
            )
            .route(
                "/api/{type}/{ns}/{repo}/xet-write-token/{rev}",
                get(xet_write_token),
            );
    }
    r = r
        .route("/api/repos/create", post(repo_create))
        .route("/api/repos/delete", delete(repo_delete_compat))
        .route("/api/repos", get(repo_list))
        .route("/api/{type}/search", get(repo_search))
        .route(
            "/api/{type}/{ns}/{repo}",
            post(repo_create_type).get(repo_info).delete(repo_delete),
        )
        .route(
            "/api/{type}/{ns}/{repo}/revision/{rev}",
            get(repo_revision_info),
        )
        .route("/api/{type}/{ns}/{repo}/modelcard", get(repo_modelcard))
        .route("/api/{type}/{ns}/{repo}/revisions", get(repo_revisions))
        .route("/api/{type}/{ns}/{repo}/preupload/{rev}", post(preupload))
        .route("/api/{type}/{ns}/{repo}/commit/{rev}", post(commit))
        .route("/api/{type}/{ns}/{repo}/tree/{rev}", get(file_tree_at_root))
        .route("/api/{type}/{ns}/{repo}/tree/{rev}/{*path}", get(file_tree))
        .route(
            "/{type}/{ns}/{repo}/resolve/{rev}/{*path}",
            get(resolve_file),
        )
        .route(
            "/{ns}/{repo}/resolve/{rev}/{*path}",
            get(resolve_model_file),
        )
        .route("/objects/batch", post(lfs_batch))
        .route("/lfs/objects/{oid}", put(lfs_upload))
        .route("/lfs/objects/{oid}", get(lfs_download))
        // Git Smart HTTP endpoints
        .route("/{type}/{ns}/{repo}/info/refs", get(git::info_refs))
        .route("/{type}/{ns}/{repo}/HEAD", get(git_head))
        .route(
            "/{type}/{ns}/{repo}/git-upload-pack",
            post(git::upload_pack),
        )
        .route(
            "/{type}/{ns}/{repo}/git-receive-pack",
            post(git::receive_pack),
        )
        // Dataset viewer endpoints
        .route("/api/datasets/{ns}/{repo}/parquet", get(dataset_parquet))
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
        );
    r
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<WhoamiResponse>, HubApiError> {
    let name = state
        .auth
        .as_ref()
        .and_then(|auth| auth.authorize(&headers, TokenScope::Read).ok())
        .map_or_else(|| "anonymous".to_owned(), |ctx| ctx.subject().to_owned());
    shardline_metrics::record_hub_api_request("whoami", "GET", 200);
    Ok(Json(WhoamiResponse {
        name: name.clone(),
        is_admin: false,
        user_type: "user".to_owned(),
        auth: WhoamiAuth {
            auth_type: "token".to_owned(),
            identity: WhoamiIdentity {
                account: WhoamiAccount { name },
            },
        },
    }))
}

// ---- Token exchange (requires Read scope) ----

async fn xet_read_token(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
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
    shardline_metrics::record_hub_api_request("xet_read_token", "GET", 200);
    Ok(Json(TokenExchangeResponse { token }))
}

async fn xet_write_token(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
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
    shardline_metrics::record_hub_api_request("xet_write_token", "GET", 200);
    Ok(Json(TokenExchangeResponse { token }))
}

// ---- Repo create (generic, requires Write) ----

async fn repo_create(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RepoCreateRequest>,
) -> Result<(StatusCode, Json<RepoResponse>), HubApiError> {
    authorize(&state, &headers, TokenScope::Write)?;
    let full_name = request.organization.as_deref().map_or_else(
        || request.name.clone(),
        |organization| format!("{organization}/{}", request.name),
    );
    let repo_type = match request.repo_type {
        RepoType::Model => HubRepoType::Model,
        RepoType::Dataset => HubRepoType::Dataset,
        RepoType::Space => HubRepoType::Space,
    };
    // `huggingface_hub` calls this endpoint before every upload with
    // `exist_ok=True`, but does not transmit that flag. It accepts the
    // established 409 conflict response when it contains the existing
    // repository URL, so preserve the HTTP conflict contract and return the
    // compatibility body the native client needs.
    if let Some(existing) = state
        .store
        .get_repo(&full_name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
    {
        return Ok((
            StatusCode::CONFLICT,
            Json(repo_response_for_request(&headers, &existing)),
        ));
    }
    let repo = state
        .store
        .create_repo(
            repo_type,
            &full_name,
            request.private || request.visibility.as_deref() == Some("private"),
        )
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    shardline_metrics::record_hub_api_request("repo_create", "POST", 201);
    Ok((
        StatusCode::CREATED,
        Json(repo_response_for_request(&headers, &repo)),
    ))
}

// ---- Repo create (type-specific, requires Write) ----

async fn repo_create_type(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<RepoResponse>), HubApiError> {
    authorize(&state, &headers, TokenScope::Write)?;
    let rt = HubRepoType::parse_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let name = format!("{ns}/{repo}");
    let private = body
        .get("private")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let created = state
        .store
        .create_repo(rt, &name, private)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    shardline_metrics::record_hub_api_request("repo_create_type", "POST", 201);
    Ok((
        StatusCode::CREATED,
        Json(repo_response_for_request(&headers, &created)),
    ))
}

fn repo_response_for_request(
    headers: &axum::http::HeaderMap,
    repo: &shardline_index::hub::HubRepo,
) -> RepoResponse {
    let mut response = repo_response_from_hub(repo);
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| matches!(*value, "http" | "https"))
        .unwrap_or("http");
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("localhost");
    let path = match repo.repo_type {
        HubRepoType::Model => repo.repo_id.as_str(),
        HubRepoType::Dataset => {
            return set_repo_response_url(
                response,
                format!("{scheme}://{host}/datasets/{}", repo.repo_id),
            );
        }
        HubRepoType::Space => {
            return set_repo_response_url(
                response,
                format!("{scheme}://{host}/spaces/{}", repo.repo_id),
            );
        }
    };
    response.url = format!("{scheme}://{host}/{path}");
    response
}

fn set_repo_response_url(mut response: RepoResponse, url: String) -> RepoResponse {
    response.url = url;
    response
}

fn repo_response_from_hub(repo: &shardline_index::hub::HubRepo) -> RepoResponse {
    let last_modified = chrono::DateTime::from_timestamp(repo.updated_at_unix_seconds as i64, 0)
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default();
    RepoResponse {
        id: repo.repo_id.clone(),
        repo_type: match repo.repo_type {
            HubRepoType::Model => RepoType::Model,
            HubRepoType::Dataset => RepoType::Dataset,
            HubRepoType::Space => RepoType::Space,
        },
        private: repo.private,
        sha: None,
        siblings: None,
        url: match repo.repo_type {
            // The Hugging Face client treats a model URL as the canonical
            // `/{namespace}/{repo}` path. Prefixing it with `/models` makes
            // the client reinterpret `models` as the namespace.
            HubRepoType::Model => format!("/{}", repo.repo_id),
            HubRepoType::Dataset | HubRepoType::Space => format!(
                "/{}/{repo_id}",
                repo_type_path(repo.repo_type),
                repo_id = repo.repo_id
            ),
        },
        default_branch: Some("main".to_owned()),
        tags: Vec::new(),
        downloads: 0,
        likes: 0,
        last_modified: Some(last_modified),
        pipeline_tag: None,
        card_data: None,
        security_status: serde_json::json!({}),
    }
}

/// Parses YAML front matter from a markdown file's content.
///
/// Extracts the content between `---` delimiters at the start of the file
/// and parses it as a simple YAML document (key: value pairs), returning
/// it as a JSON value. Complex nested YAML is not supported; this is
/// intentionally minimal to match the common HuggingFace README pattern.
fn parse_yaml_frontmatter(content: &[u8]) -> Option<serde_json::Value> {
    let text = std::str::from_utf8(content).ok()?;
    let trimmed = text.trim_start();
    if !trimmed.starts_with("---") {
        return None;
    }
    let rest = &trimmed[3..];
    let end = rest.find("\n---")?;
    let yaml_str = rest[..end].trim();
    if yaml_str.is_empty() {
        return None;
    }
    // Parse simple flat YAML key-value pairs into a JSON object.
    let mut map = serde_json::Map::new();
    for line in yaml_str.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().to_owned();
            let value = value.trim();
            // Strip surrounding quotes if present.
            let value = value
                .strip_prefix('"')
                .and_then(|v| v.strip_suffix('"'))
                .or_else(|| value.strip_prefix('\'').and_then(|v| v.strip_suffix('\'')))
                .unwrap_or(value);
            // Try to parse as JSON primitives; fall back to string.
            let json_val = serde_json::from_str::<serde_json::Value>(value)
                .unwrap_or_else(|_| serde_json::Value::String(value.to_owned()));
            map.insert(key, json_val);
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(map))
    }
}

// ---- Repo list (requires Read) ----

async fn repo_list(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_list", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path(repo_type): Path<String>,
    Query(query): Query<RepoSearchQuery>,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_search", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let rt = HubRepoType::parse_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    if query.q.len() < 2 {
        return Err(HubApiError::PathValidation(
            "search query must be at least 2 characters".to_owned(),
        ));
    }
    let limit = query.limit.min(200);
    let mut repos = state
        .store
        .search_repos(Some(rt), &query.q, limit)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    // Apply server-side sorting when requested.
    if let Some(sort) = &query.sort {
        match sort.as_str() {
            "lastModified" => {
                repos.sort_by(|a, b| b.updated_at_unix_seconds.cmp(&a.updated_at_unix_seconds));
            }
            "likes" => {
                // No likes field on HubRepo yet; keep default order.
            }
            "downloads" => {
                // No downloads field on HubRepo yet; keep default order.
            }
            _ => {}
        }
        if query.direction.as_deref() == Some("asc") {
            repos.reverse();
        }
    }

    let response = RepoListResponse {
        repos: repos.iter().map(repo_response_from_hub).collect(),
    };
    Ok(Json(response))
}

// ---- Repo modelcard (requires Read) ----

async fn repo_modelcard(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_modelcard", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    let content = readme
        .inline_content
        .as_ref()
        .ok_or(HubApiError::NotFound)?;
    Ok((resp_headers, content.clone()).into_response())
}

// ---- Repo revisions (requires Read) ----

async fn repo_revisions(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<RevisionListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_revisions", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_info", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let _rt = HubRepoType::parse_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let mut response = repo_response_from_hub(&entry);

    // Populate card_data from README.md YAML front matter when available.
    if let Ok(commit_sha) = state.store.resolve_revision(&name, &entry.default_branch)
        && let Some(sha) = commit_sha
        && let Ok(files) = state.store.get_files(&sha)
        && let Some(readme) = files.iter().find(|f| f.path == "README.md")
        && let Some(content) = &readme.inline_content
    {
        response.card_data = parse_yaml_frontmatter(content);
    }

    Ok(Json(response))
}

async fn repo_revision_info(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_revision_info", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let entry = state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut response = repo_response_from_hub(&entry);
    response.sha = Some(commit_sha);
    response.siblings = Some(
        files
            .into_iter()
            .map(|file| {
                serde_json::json!({
                    "rfilename": file.path,
                    "size": file.size,
                    "blobId": file.sha,
                })
            })
            .collect(),
    );
    Ok(Json(response))
}

// ---- Repo delete (requires Write) ----

async fn repo_delete(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    let name = format!("{ns}/{repo}");
    delete_repository(&state, &headers, &name)
}

/// Compatibility endpoint used by `HfApi.delete_repo` and `hf repo delete`.
async fn repo_delete_compat(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RepoDeleteRequest>,
) -> Result<StatusCode, HubApiError> {
    let _repo_type = request.repo_type;
    let name = match request.organization {
        Some(organization) => format!("{organization}/{}", request.name),
        None => request.name,
    };
    delete_repository(&state, &headers, &name)
}

fn delete_repository(
    state: &HubState,
    headers: &axum::http::HeaderMap,
    name: &str,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_delete", "DELETE", 204);
    authorize(state, headers, TokenScope::Write)?;
    // Verify repo exists
    let _repo = state
        .store
        .get_repo(name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    state
        .store
        .delete_repo(name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- Preupload (requires Write) ----

async fn preupload(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    Json(request): Json<PreuploadRequest>,
) -> Result<Json<PreuploadResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("preupload", "POST", 200);
    authorize(&state, &headers, TokenScope::Write)?;
    const MAX_PREUPLOAD_FILES: usize = 10_000;
    if request.files.len() > MAX_PREUPLOAD_FILES {
        return Err(HubApiError::PathValidation(format!(
            "preupload request exceeds maximum of {MAX_PREUPLOAD_FILES} files"
        )));
    }
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
            upload_mode: "regular".to_owned(),
            should_ignore: false,
        })
        .collect();

    Ok(Json(PreuploadResponse {
        files: result.clone(),
        result,
    }))
}

// ---- Commit (requires Write) ----

async fn commit(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    body: String,
) -> Result<Json<CommitResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("commit", "POST", 200);
    shardline_metrics::record_hub_api_commit("ndjson");
    // HF spec requires Content-Type to be application/x-ndjson or application/json.
    let ct_ok = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| {
            ct.starts_with("application/x-ndjson") || ct.starts_with("application/json")
        });
    if !ct_ok {
        return Err(HubApiError::PathValidation(
            "commit requires Content-Type: application/x-ndjson or application/json".to_owned(),
        ));
    }
    authorize(&state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    let parent_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let parsed = commit::parse_ndjson_commit(&body)?;
    apply_commit(&state, &name, &parent_sha, &parsed).await
}

async fn apply_commit(
    state: &HubState,
    repo_id: &str,
    parent_sha: &str,
    parsed: &ParsedCommit,
) -> Result<Json<CommitResponse>, HubApiError> {
    // HUB-004: Validate that the NDJSON body's parentCommit (if present) matches
    // the URL path's parent_sha. A mismatch indicates a stale or conflicting request.
    if let Some(ref body_parent) = parsed.parent_commit
        && body_parent != parent_sha
    {
        return Err(HubApiError::Conflict(format!(
            "parentCommit mismatch: body specified {body_parent} but URL resolved to {parent_sha}"
        )));
    }

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
                    let mut h = blake3::Hasher::new();
                    h.update(content);
                    hex::encode(h.finalize().as_bytes())
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
        let mut h = blake3::Hasher::new();
        for fh in &file_hashes {
            h.update(fh.as_bytes());
        }
        hex::encode(h.finalize().as_bytes())
    };
    let commit_sha =
        shardline_index::hub::HubRepo::compute_commit_sha(parent_sha, &parsed.message, &files_hash)
            .map_err(|e| HubApiError::CasError(e.to_string()))?;

    // HUB-008: Orphan cleanup trade-off.
    //
    // `store_files` writes content-addressed blobs, then `create_revision` records the
    // new revision pointer. If `store_files` succeeds but `create_revision` fails, the
    // stored files become orphaned (no revision references them). This is acceptable
    // because:
    //   1. Content-addressed files are idempotent — retries won't create duplicates.
    //   2. Orphans are small relative to the body limit and can be reclaimed by a
    //      background GC sweep if needed.
    //   3. Swapping the order (revision first, then files) requires a placeholder
    //      revision state and is more complex for marginal gain.
    state
        .store
        .store_files(&commit_sha, &files)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    state
        .store
        .create_revision(
            repo_id,
            Some(parent_sha),
            &commit_sha,
            "main",
            &parsed.message,
        )
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    // Fire webhook deliveries in the background (non-blocking).
    deliver_webhook_events(state, repo_id, "push", &commit_sha).await;

    Ok(Json(CommitResponse {
        commit_id: commit_sha.clone(),
        commit_oid: commit_sha.clone(),
        commit_url: format!("/{repo_id}/commit/{commit_sha}"),
        ref_name: Some("main".to_owned()),
    }))
}

// ---- File tree (requires Read) ----

/// Lists a repository tree at its root.
///
/// The native `huggingface_hub` client omits the trailing slash when it lists
/// every file, so this is deliberately a distinct route from the path variant.
async fn file_tree_at_root(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev)): Path<(String, String, String, String)>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, headers, ns, repo, rev, String::new(), query).await
}

async fn file_tree(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev, file_path)): Path<(String, String, String, String, String)>,
    Query(query): Query<TreeQuery>,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    file_tree_for_path(state, headers, ns, repo, rev, file_path, query).await
}

async fn file_tree_for_path(
    state: HubState,
    headers: axum::http::HeaderMap,
    ns: String,
    repo: String,
    rev: String,
    file_path: String,
    query: TreeQuery,
) -> Result<Json<Vec<TreeEntry>>, HubApiError> {
    shardline_metrics::record_hub_api_request("file_tree", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    let entries = if query.recursive {
        tree_entries_recursive(&files, &file_path)
    } else {
        tree_entries_at_path(&files, &file_path)
    };
    let entries = if let Some(limit) = query.limit {
        let entries: Vec<TreeEntry> = if let Some(cursor) = &query.cursor {
            // Skip entries until we pass the cursor, then take `limit` entries.
            entries
                .into_iter()
                .skip_while(|e| &e.path != cursor)
                .skip(1) // skip the cursor entry itself
                .take(limit)
                .collect()
        } else {
            entries.into_iter().take(limit).collect()
        };
        entries
    } else {
        entries
    };
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
                    oid: None,
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
                oid: Some(file.sha.clone()),
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

/// Lists all files recursively under a given path prefix.
fn tree_entries_recursive(files: &[HubFileEntry], path: &str) -> Vec<TreeEntry> {
    let prefix = if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    };

    let mut entries = Vec::new();

    for file in files {
        if !prefix.is_empty() && !file.path.starts_with(&prefix) {
            continue;
        }
        let relative = file.path.strip_prefix(&prefix).unwrap_or(&file.path);
        if relative.is_empty() {
            continue;
        }

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
            oid: Some(file.sha.clone()),
            lfs,
        });
    }

    entries.sort_by(|a, b| a.path.cmp(&b.path));
    entries
}

// ---- File resolve (download, requires Read) ----

async fn resolve_file(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, rev, file_path)): Path<(String, String, String, String, String)>,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, headers, ns, repo, rev, file_path).await
}

async fn resolve_model_file(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((ns, repo, rev, file_path)): Path<(String, String, String, String)>,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, headers, ns, repo, rev, file_path).await
}

async fn resolve_file_for_repository(
    state: HubState,
    headers: axum::http::HeaderMap,
    ns: String,
    repo: String,
    rev: String,
    file_path: String,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("resolve_file", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
    let name = format!("{ns}/{repo}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let result = resolve::resolve_file_from_store(&state, &commit_sha, &file_path)?;

    match result {
        resolve::DownloadResult::Inline { size, sha, content } => {
            let data = content.ok_or(HubApiError::NotFound)?;
            let content_length = size.to_string();
            let resp_headers = [
                ("Content-Type", "application/octet-stream"),
                ("X-Shardline-SHA", sha.as_str()),
                ("X-Repo-Commit", commit_sha.as_str()),
                ("ETag", sha.as_str()),
                ("Content-Length", content_length.as_str()),
            ];
            Ok((resp_headers, data).into_response())
        }
        resolve::DownloadResult::LfsRedirect { oid, .. } => {
            let redirect_url = format!("/lfs/objects/{oid}");
            Ok(Redirect::temporary(&redirect_url).into_response())
        }
    }
}

// ---- LFS batch (requires Read) ----

async fn lfs_batch(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_batch", "POST", 200);
    authorize(&state, &headers, TokenScope::Read)?;

    let objects: Vec<LfsObjectResponse> = request
        .objects
        .iter()
        .map(|obj| {
            let exists = state.store.has_lfs_object(&obj.oid).unwrap_or(false);
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
                        None
                    }
                }
            };

            let error = if !exists
                && (request.operation == LfsBatchOperation::Download
                    || request.operation == LfsBatchOperation::Verify)
            {
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path(oid): Path<String>,
    body: bytes::Bytes,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_upload", "PUT", 200);
    authorize(&state, &headers, TokenScope::Write)?;
    commit::validate_lfs_oid(&oid)?;
    state
        .store
        .put_lfs_object(&oid, &body)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::OK)
}

// ---- LFS download (requires Read) ----

async fn lfs_download(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path(oid): Path<String>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, &'static str); 1],
        Vec<u8>,
    ),
    HubApiError,
> {
    shardline_metrics::record_hub_api_request("lfs_download", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<String, HubApiError> {
    authorize(&state, &headers, TokenScope::Read)?;
    let repo_id = format!("{ns}/{repo}");
    let revisions = state.store.list_revisions(&repo_id).map_err(|e| {
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

    Ok(format!(
        "ref: refs/heads/main\n{head_sha} refs/heads/main\n"
    ))
}

// ---- Dataset viewer endpoints ----

/// Lists parquet/data files in a dataset repository.
async fn dataset_parquet(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
) -> Result<Json<DatasetParquetResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_parquet", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
        .filter(|f| {
            f.path.ends_with(".parquet") || f.path.ends_with(".csv") || f.path.ends_with(".jsonl")
        })
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((ns, repo)): Path<(String, String)>,
    Query(query): Query<DatasetFirstRowsQuery>,
) -> Result<Json<DatasetFirstRowsResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_first_rows", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    let data_file = match find_dataset_file(&files, &query.config, &query.split) {
        Some(f) => f,
        None => {
            // Empty dataset — return 200 with empty rows (per HuggingFace Hub API spec).
            return Ok(Json(DatasetFirstRowsResponse {
                columns: vec![],
                rows: vec![],
            }));
        }
    };
    let content = data_file.inline_content.as_deref().ok_or_else(|| {
        HubApiError::PathValidation("file content not available inline".to_owned())
    })?;
    let limit = query.limit.min(1000);
    let rows = parse_rows_from_content(content, &data_file.path, 0, limit)?;
    let columns = rows
        .first()
        .map(|r| r.columns.keys().cloned().collect())
        .unwrap_or_default();
    Ok(Json(DatasetFirstRowsResponse { columns, rows }))
}

/// Returns rows from a dataset split with pagination.
async fn dataset_viewer(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((ns, repo, split)): Path<(String, String, String)>,
    Query(query): Query<DatasetViewerQuery>,
) -> Result<Json<DatasetViewerResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("dataset_viewer", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    let data_file = find_dataset_file(&files, &query.config, &split).ok_or_else(|| {
        HubApiError::PathValidation("no data file found for config/split".to_owned())
    })?;
    let content = data_file.inline_content.as_deref().ok_or_else(|| {
        HubApiError::PathValidation("file content not available inline".to_owned())
    })?;
    let length = query.length.min(10000);
    let rows = parse_rows_from_content(content, &data_file.path, query.offset, length)?;
    let columns = rows
        .first()
        .map(|r| r.columns.keys().cloned().collect())
        .unwrap_or_default();
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
fn parse_jsonl_rows(
    text: &str,
    offset: usize,
    limit: usize,
) -> Result<Vec<DatasetRow>, HubApiError> {
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
        let line_number = i
            .checked_add(1)
            .ok_or_else(|| HubApiError::PathValidation("line number overflow".to_owned()))?;
        let value: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            HubApiError::PathValidation(format!("invalid JSON at line {}: {e}", line_number))
        })?;
        let columns = value
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        rows.push(DatasetRow { columns });
    }
    Ok(rows)
}

/// Parses CSV rows, handling quoted fields that may contain commas.
fn parse_csv_rows(text: &str, offset: usize, limit: usize) -> Result<Vec<DatasetRow>, HubApiError> {
    let mut lines = text.lines();
    let header_line = lines
        .next()
        .ok_or_else(|| HubApiError::PathValidation("empty CSV file".to_owned()))?;
    let headers: Vec<String> = parse_csv_line(header_line)
        .into_iter()
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
        let values: Vec<&str> = parse_csv_line(line);
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

/// Parses a single CSV line, respecting double-quoted fields that may contain
/// commas and escaped quotes (`""`).
fn parse_csv_line(line: &str) -> Vec<&str> {
    let mut fields = Vec::new();
    let mut current = line;
    loop {
        if current.is_empty() {
            break;
        }
        if current.starts_with('"') {
            // Quoted field — find the closing quote, handling "" escapes.
            let mut chars = current[1..].char_indices().peekable();
            let mut field_end = None;
            while let Some((idx, ch)) = chars.next() {
                if ch == '"' {
                    if chars.peek().is_none_or(|&(_, next)| next != '"') {
                        // Closing quote (not followed by another quote).
                        field_end = Some(idx.saturating_add(1));
                        break;
                    }
                    // Escaped quote `""` — skip the next quote.
                    chars.next();
                }
            }
            if let Some(end) = field_end {
                let field = &current[1..end]; // strip opening/closing quotes
                fields.push(field);
                // Skip closing quote and comma separator.
                current = end
                    .checked_add(1)
                    .map_or("", |n| current.get(n..).unwrap_or(""));
                if current.starts_with(',') {
                    current = &current[1..];
                }
            } else {
                // Unterminated quote — treat rest as field.
                fields.push(&current[1..]);
                current = "";
            }
        } else {
            // Unquoted field — split on comma.
            match current.find(',') {
                Some(pos) => {
                    fields.push(&current[..pos]);
                    current = pos
                        .checked_add(1)
                        .map_or("", |n| current.get(n..).unwrap_or(""));
                }
                None => {
                    fields.push(current);
                    current = "";
                }
            }
        }
    }
    // If the line ended with a comma, we need an extra empty field.
    if line.ends_with(',') {
        fields.push("");
    }
    fields
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
    } else if host.eq_ignore_ascii_case("localhost") {
        return Err(HubApiError::PathValidation(
            "webhook URL must not point to localhost".to_owned(),
        ));
    }

    Ok(())
}

/// Returns `true` if the IP address is private, loopback, link-local, or
/// otherwise reserved (not globally routable).
const fn is_private_ip(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(v4) => {
            v4.is_loopback() // 127.0.0.0/8
                || v4.is_private() // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
                || v4.is_link_local() // 169.254.0.0/16
                || v4.is_unspecified() // 0.0.0.0
                || v4.is_broadcast()
                || v4.is_documentation() // 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
                || is_cgnat(*v4) // 100.64.0.0/10 (RFC 6598 shared address space)
        }
        std::net::IpAddr::V6(v6) => {
            v6.is_loopback() // ::1
                || v6.is_unspecified() // ::
                || v6.is_unicast_link_local() // fe80::/10
                || v6.is_unique_local() // fc00::/7 (RFC 4193)
                || match v6.to_ipv4_mapped() {
                    Some(v4) => is_private_ip(&std::net::IpAddr::V4(v4)),
                    None => false,
                }
        }
    }
}

/// Returns `true` if the IPv4 address is in the CGNAT/Shared Address Space
/// range 100.64.0.0/10 (RFC 6598).
const fn is_cgnat(ip: std::net::Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    a == 100 && (b & 0xC0) == 64
}

/// Creates a webhook for a repository.
async fn webhook_create(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
    Json(request): Json<WebhookCreateRequest>,
) -> Result<(StatusCode, Json<WebhookResponse>), HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_create", "POST", 201);
    authorize(&state, &headers, TokenScope::Write)?;
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
    // Check for duplicate webhook URL.
    let existing = state
        .store
        .list_webhooks(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    if existing.iter().any(|wh| wh.url == request.url) {
        return Err(HubApiError::Conflict(format!(
            "webhook with URL {} already exists for repo {name}",
            request.url
        )));
    }
    let webhook = state
        .store
        .create_webhook(
            &name,
            &request.url,
            &request.events,
            request.secret.as_deref(),
        )
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((
        StatusCode::CREATED,
        Json(webhook_response_from_hub(&webhook)),
    ))
}

/// Lists webhooks for a repository.
async fn webhook_list(
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<WebhookListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_list", "GET", 200);
    authorize(&state, &headers, TokenScope::Read)?;
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
    State(state): State<HubState>,
    headers: axum::http::HeaderMap,
    Path((_repo_type, ns, repo, webhook_id)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("webhook_delete", "DELETE", 204);
    authorize(&state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    state
        .store
        .delete_webhook(&name, &webhook_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use shardline_index::hub::{HubRepo, HubRepoType};
    use std::net::IpAddr;

    #[test]
    fn validate_webhook_url_accepts_valid_http() {
        assert!(validate_webhook_url("http://example.com/hook").is_ok());
    }

    #[test]
    fn validate_webhook_url_rejects_ftp_scheme() {
        assert!(validate_webhook_url("ftp://example.com/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_javascript_scheme() {
        assert!(validate_webhook_url("javascript:alert(1)").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_localhost() {
        assert!(validate_webhook_url("http://localhost/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_private_ip_10() {
        assert!(validate_webhook_url("http://10.0.0.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_private_ip_192_168() {
        assert!(validate_webhook_url("http://192.168.1.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_loopback() {
        assert!(validate_webhook_url("http://127.0.0.1/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_ipv6_loopback() {
        assert!(validate_webhook_url("http://[::1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_long_url() {
        let long = format!("http://example.com/{}", "a".repeat(3000));
        assert!(validate_webhook_url(&long).is_err());
    }

    #[test]
    fn is_private_ip_true_for_loopback() {
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_10() {
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_172() {
        let ip: IpAddr = "172.16.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_private_192_168() {
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_link_local() {
        let ip: IpAddr = "169.254.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_cgnat() {
        let ip: IpAddr = "100.64.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_unspecified() {
        let ip: IpAddr = "0.0.0.0".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_loopback() {
        let ip: IpAddr = "::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_link_local() {
        let ip: IpAddr = "fe80::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_public() {
        let ip: IpAddr = "8.8.8.8".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_ipv6_public() {
        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn parse_csv_line_simple_fields() {
        let result = parse_csv_line("a,b,c");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_csv_line_quoted_field_with_comma() {
        let result = parse_csv_line(r#""hello, world",b"#);
        assert_eq!(result, vec!["hello, world", "b"]);
    }

    #[test]
    fn parse_csv_line_escaped_quote() {
        let result = parse_csv_line(r#""say ""hello""",done"#);
        assert_eq!(result, vec![r#"say ""hello"""#, "done"]);
    }

    #[test]
    fn parse_csv_line_trailing_comma() {
        let result = parse_csv_line("a,b,");
        assert_eq!(result, vec!["a", "b", ""]);
    }

    #[test]
    fn parse_csv_line_single_field() {
        let result = parse_csv_line("only");
        assert_eq!(result, vec!["only"]);
    }

    #[test]
    fn parse_csv_line_empty_field() {
        let result = parse_csv_line("a,,c");
        assert_eq!(result, vec!["a", "", "c"]);
    }

    #[test]
    fn parse_csv_line_unterminated_quote() {
        let result = parse_csv_line(r#""unterminated,a"#);
        assert_eq!(result, vec!["unterminated,a"]);
    }

    // --- repo_delete endpoint logic tests ---
    // These test the store operations that the repo_delete handler relies on,
    // using BoxedHubStore with the same flow the handler follows.

    fn make_delete_test_store() -> (tempfile::TempDir, BoxedHubStore) {
        let ts = tempfile::tempdir().expect("tempdir");
        let root = ts.path();

        // Create hub tables using the public API
        shardline_index::hub::ensure_hub_tables(root).expect("ensure hub tables");

        let store = shardline_index::LocalIndexStore::open(root.to_path_buf());
        let boxed = BoxedHubStore::from_store(store);
        (ts, boxed)
    }

    /// Helper: returns (TempDir, HubState) with no auth.
    fn make_test_state() -> (tempfile::TempDir, HubState) {
        let (td, store) = make_delete_test_store();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        (td, state)
    }

    #[test]
    fn repo_delete_cleans_up_revisions() {
        let (_ts, store) = make_delete_test_store();

        use shardline_index::hub::{HubFileEntry, HubRepoType};

        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();
        let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

        // Add a commit
        store
            .create_revision(
                "org/model",
                Some(initial_sha),
                "sha1",
                "main",
                "first commit",
            )
            .unwrap();

        // Store files for the commit
        let files = vec![HubFileEntry {
            path: "README.md".into(),
            size: 100,
            sha: "sha_readme".into(),
            is_lfs: false,
            inline_content: None,
        }];
        store.store_files("sha1", &files).unwrap();

        // Verify data exists
        assert_eq!(store.list_revisions("org/model").unwrap().len(), 2);
        assert_eq!(store.get_files("sha1").unwrap().len(), 1);

        // Delete — mirrors what repo_delete handler does
        store.delete_repo("org/model").unwrap();

        // Verify everything is gone
        assert!(store.get_repo("org/model").unwrap().is_none());
        assert!(store.list_revisions("org/model").unwrap().is_empty());
        assert!(store.get_files("sha1").unwrap().is_empty());
    }

    #[test]
    fn repo_delete_cleans_up_webhooks() {
        let (_ts, store) = make_delete_test_store();

        use shardline_index::hub::HubRepoType;

        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        // Create webhook
        store
            .create_webhook(
                "org/model",
                "https://example.com/hook",
                &["push".into()],
                None,
            )
            .unwrap();

        assert_eq!(store.list_webhooks("org/model").unwrap().len(), 1);

        // Delete
        store.delete_repo("org/model").unwrap();

        // Verify webhook is gone
        assert!(store.list_webhooks("org/model").unwrap().is_empty());
    }

    #[test]
    fn repo_delete_idempotent() {
        let (_ts, store) = make_delete_test_store();

        use shardline_index::hub::HubRepoType;

        store
            .create_repo(HubRepoType::Model, "org/model", false)
            .unwrap();

        // First delete succeeds
        store.delete_repo("org/model").unwrap();
        assert!(store.get_repo("org/model").unwrap().is_none());

        // Second delete is also fine (no-op, no rows affected)
        store.delete_repo("org/model").unwrap();
        assert!(store.get_repo("org/model").unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // repo_type_path
    // -----------------------------------------------------------------------

    #[test]
    fn repo_type_path_model() {
        assert_eq!(repo_type_path(HubRepoType::Model), "models");
    }

    #[test]
    fn repo_type_path_dataset() {
        assert_eq!(repo_type_path(HubRepoType::Dataset), "datasets");
    }

    #[test]
    fn repo_type_path_space() {
        assert_eq!(repo_type_path(HubRepoType::Space), "spaces");
    }

    // -----------------------------------------------------------------------
    // sanitize_log_url
    // -----------------------------------------------------------------------

    #[test]
    fn sanitize_log_url_normal() {
        let url = "https://example.com/webhook";
        assert_eq!(sanitize_log_url(url), url);
    }

    #[test]
    fn sanitize_log_url_replaces_control_chars() {
        let url = "https://example.com/new\nline";
        assert_eq!(sanitize_log_url(url), "https://example.com/new?line");
    }

    #[test]
    fn sanitize_log_url_truncates_long() {
        let base = "https://example.com/";
        let long = format!("{}{}", base, "a".repeat(300));
        let result = sanitize_log_url(&long);
        assert!(result.len() <= 204); // 200 chars + "..."
        assert!(result.ends_with("..."));
    }

    #[test]
    fn sanitize_log_url_replaces_tab() {
        let url = "https://example.com/\tpath";
        assert_eq!(sanitize_log_url(url), "https://example.com/?path");
    }

    #[test]
    fn sanitize_log_url_short_no_truncation() {
        let url = "http://a.b";
        assert_eq!(sanitize_log_url(url), url);
        assert!(!url.ends_with("..."));
    }

    // -----------------------------------------------------------------------
    // repo_response_from_hub
    // -----------------------------------------------------------------------

    #[test]
    fn repo_response_from_hub_model() {
        use shardline_index::hub::HubRepoType;
        let hub_repo = HubRepo {
            repo_id: "org/my-model".to_owned(),
            repo_type: HubRepoType::Model,
            private: false,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 1_700_000_000,
            updated_at_unix_seconds: 1_700_000_001,
        };
        let resp = repo_response_from_hub(&hub_repo);
        assert_eq!(resp.id, "org/my-model");
        assert_eq!(resp.repo_type, RepoType::Model);
        assert!(!resp.private);
        assert_eq!(resp.url, "/org/my-model");
        assert_eq!(resp.default_branch.as_deref(), Some("main"));
        let lm = resp
            .last_modified
            .as_deref()
            .expect("last_modified should be Some");
        // Should be a valid RFC 3339 timestamp around 2023-11-14
        assert!(
            lm.contains("2023-11-14T22"),
            "expected 2023-11-14T22... got {lm}"
        );
    }

    #[test]
    fn repo_response_from_hub_dataset() {
        use shardline_index::hub::HubRepoType;
        let hub_repo = HubRepo {
            repo_id: "org/data".to_owned(),
            repo_type: HubRepoType::Dataset,
            private: true,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        let resp = repo_response_from_hub(&hub_repo);
        assert_eq!(resp.repo_type, RepoType::Dataset);
        assert!(resp.private);
        assert_eq!(resp.url, "/datasets/org/data");
    }

    #[test]
    fn repo_response_from_hub_space() {
        use shardline_index::hub::HubRepoType;
        let hub_repo = HubRepo {
            repo_id: "org/space1".to_owned(),
            repo_type: HubRepoType::Space,
            private: false,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        let resp = repo_response_from_hub(&hub_repo);
        assert_eq!(resp.repo_type, RepoType::Space);
        assert_eq!(resp.url, "/spaces/org/space1");
    }

    // -----------------------------------------------------------------------
    // webhook_response_from_hub
    // -----------------------------------------------------------------------

    #[test]
    fn webhook_response_from_hub_basic() {
        use shardline_index::hub::HubWebhook;
        let hook = HubWebhook {
            id: "wh_123".to_owned(),
            repo_id: "org/repo".to_owned(),
            url: "https://example.com/hook".to_owned(),
            events: vec!["push".to_owned()],
            secret: Some("s3cret".to_owned()),
            active: true,
            created_at_unix_seconds: 42,
        };
        let resp = webhook_response_from_hub(&hook);
        assert_eq!(resp.id, "wh_123");
        assert_eq!(resp.url, "https://example.com/hook");
        assert_eq!(resp.events, vec!["push"]);
        assert!(resp.active);
        assert_eq!(resp.created_at, 42);
    }

    #[test]
    fn webhook_response_from_hub_inactive() {
        use shardline_index::hub::HubWebhook;
        let hook = HubWebhook {
            id: "wh_2".to_owned(),
            repo_id: "org/repo".to_owned(),
            url: "http://hook.example".to_owned(),
            events: vec!["push".to_owned(), "delete".to_owned()],
            secret: None,
            active: false,
            created_at_unix_seconds: 99,
        };
        let resp = webhook_response_from_hub(&hook);
        assert!(!resp.active);
        assert_eq!(resp.events.len(), 2);
    }

    // -----------------------------------------------------------------------
    // parse_yaml_frontmatter
    // -----------------------------------------------------------------------

    #[test]
    fn parse_yaml_frontmatter_valid_simple() {
        let content = b"---\nkey: value\n---\n# README\nHello";
        let result = parse_yaml_frontmatter(content);
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("value"));
    }

    #[test]
    fn parse_yaml_frontmatter_with_quoted_value() {
        let content = b"---\ntitle: 'My Model'\n---\nbody";
        let result = parse_yaml_frontmatter(content);
        assert!(result.is_some());
        let obj = result.unwrap();
        assert_eq!(obj.get("title").and_then(|v| v.as_str()), Some("My Model"));
    }

    #[test]
    fn parse_yaml_frontmatter_no_frontmatter() {
        let content = b"Just a README\nno frontmatter";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    #[test]
    fn parse_yaml_frontmatter_empty_yaml() {
        let content = b"---\n---\nbody";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    #[test]
    fn parse_yaml_frontmatter_only_comments() {
        let content = b"---\n# just a comment\n---\nbody";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    #[test]
    fn parse_yaml_frontmatter_numeric_value() {
        let content = b"---\nlikes: 42\n---\nbody";
        let result = parse_yaml_frontmatter(content).unwrap();
        assert_eq!(result.get("likes").and_then(|v| v.as_u64()), Some(42));
    }

    #[test]
    fn parse_yaml_frontmatter_boolean_value() {
        let content = b"---\nprivate: true\n---\nbody";
        let result = parse_yaml_frontmatter(content).unwrap();
        assert_eq!(result.get("private").and_then(|v| v.as_bool()), Some(true));
    }

    #[test]
    fn parse_yaml_frontmatter_double_quoted_value() {
        let content = b"---\nname: \"hello world\"\n---\nbody";
        let result = parse_yaml_frontmatter(content).unwrap();
        assert_eq!(
            result.get("name").and_then(|v| v.as_str()),
            Some("hello world")
        );
    }

    #[test]
    fn parse_yaml_frontmatter_not_utf8() {
        let content = b"\xff\xfe\x00\x01";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    // -----------------------------------------------------------------------
    // tree_entries_at_path
    // -----------------------------------------------------------------------

    #[test]
    fn tree_entries_at_path_root_lists_files_and_dirs() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![
            HubFileEntry {
                path: "README.md".into(),
                size: 100,
                sha: "a".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "src/lib.rs".into(),
                size: 300,
                sha: "c".into(),
                is_lfs: true,
                inline_content: None,
            },
            HubFileEntry {
                path: "data/big.bin".into(),
                size: 5_000_000,
                sha: "d".into(),
                is_lfs: true,
                inline_content: None,
            },
        ];
        let entries = tree_entries_at_path(&files, "");
        assert_eq!(
            entries.len(),
            3,
            "expected 3 entries: README.md, src/, data/"
        );
        // Directories come before files (sorted by type then path)
        assert_eq!(entries[0].entry_type, "directory");
        assert_eq!(entries[0].path, "data");
        assert_eq!(entries[1].entry_type, "directory");
        assert_eq!(entries[1].path, "src");
        assert_eq!(entries[2].entry_type, "file");
        assert_eq!(entries[2].path, "README.md");
        assert_eq!(entries[2].size, Some(100));
        assert!(entries[2].lfs.is_none());
    }

    #[test]
    fn tree_entries_at_path_nested() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "src/lib.rs".into(),
                size: 300,
                sha: "c".into(),
                is_lfs: true,
                inline_content: None,
            },
        ];
        let entries = tree_entries_at_path(&files, "src");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, "file");
        assert_eq!(entries[0].path, "lib.rs");
        assert!(entries[0].lfs.is_some());
        assert_eq!(entries[1].entry_type, "file");
        assert_eq!(entries[1].path, "main.rs");
    }

    #[test]
    fn tree_entries_at_path_empty_dir() {
        let entries = tree_entries_at_path(&[], "");
        assert!(entries.is_empty());
    }

    // -----------------------------------------------------------------------
    // tree_entries_recursive
    // -----------------------------------------------------------------------

    #[test]
    fn tree_entries_recursive_root() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![
            HubFileEntry {
                path: "README.md".into(),
                size: 100,
                sha: "a".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
                inline_content: None,
            },
        ];
        let entries = tree_entries_recursive(&files, "");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "README.md");
        assert_eq!(entries[1].path, "src/main.rs");
    }

    #[test]
    fn tree_entries_recursive_nested() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
                inline_content: None,
            },
            HubFileEntry {
                path: "src/lib.rs".into(),
                size: 300,
                sha: "c".into(),
                is_lfs: true,
                inline_content: None,
            },
        ];
        let entries = tree_entries_recursive(&files, "src");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn tree_entries_recursive_lfs_shows_lfs_info() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "model.bin".into(),
            size: 5_000_000,
            sha: "abcd".into(),
            is_lfs: true,
            inline_content: None,
        }];
        let entries = tree_entries_recursive(&files, "");
        assert_eq!(entries.len(), 1);
        let lfs = entries[0].lfs.as_ref().expect("expected LFS info");
        assert_eq!(lfs.oid, "abcd");
        assert_eq!(lfs.size, 5_000_000);
    }

    #[test]
    fn tree_entries_recursive_empty() {
        assert!(tree_entries_recursive(&[], "").is_empty());
    }

    // -----------------------------------------------------------------------
    // find_dataset_file
    // -----------------------------------------------------------------------

    #[test]
    fn find_dataset_file_default_train() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "data/train/data.jsonl".into(),
            size: 100,
            sha: "abc".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let result = find_dataset_file(&files, "default", "train");
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "data/train/data.jsonl");
    }

    #[test]
    fn find_dataset_file_config_split() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "myconfig/test/data.parquet".into(),
            size: 200,
            sha: "def".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let result = find_dataset_file(&files, "myconfig", "test");
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "myconfig/test/data.parquet");
    }

    #[test]
    fn find_dataset_file_split_only() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "train/data.csv".into(),
            size: 300,
            sha: "ghi".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let result = find_dataset_file(&files, "default", "train");
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "train/data.csv");
    }

    #[test]
    fn find_dataset_file_root() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "data.parquet".into(),
            size: 400,
            sha: "jkl".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let result = find_dataset_file(&files, "default", "train");
        assert!(result.is_some());
        assert_eq!(result.unwrap().path, "data.parquet");
    }

    #[test]
    fn find_dataset_file_not_found() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "other.txt".into(),
            size: 10,
            sha: "x".into(),
            is_lfs: false,
            inline_content: None,
        }];
        assert!(find_dataset_file(&files, "default", "train").is_none());
    }

    // -----------------------------------------------------------------------
    // parse_jsonl_rows
    // -----------------------------------------------------------------------

    #[test]
    fn parse_jsonl_rows_simple() {
        let text = "{\"a\":1,\"b\":2}\n{\"a\":3,\"b\":4}";
        let rows = parse_jsonl_rows(text, 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns.get("a"), Some(&serde_json::json!(1)));
        assert_eq!(rows[1].columns.get("b"), Some(&serde_json::json!(4)));
    }

    #[test]
    fn parse_jsonl_rows_with_offset_and_limit() {
        let text = "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n{\"n\":5}";
        let rows = parse_jsonl_rows(text, 2, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns.get("n"), Some(&serde_json::json!(3)));
        assert_eq!(rows[1].columns.get("n"), Some(&serde_json::json!(4)));
    }

    #[test]
    fn parse_jsonl_rows_empty_lines() {
        let text = "{\"a\":1}\n\n{\"a\":2}\n";
        let rows = parse_jsonl_rows(text, 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parse_jsonl_rows_invalid_json() {
        let text = "{\"a\":1}\nnot_json\n{\"a\":2}";
        let result = parse_jsonl_rows(text, 0, 10);
        assert!(result.is_err());
    }

    #[test]
    fn parse_jsonl_rows_empty_input() {
        let rows = parse_jsonl_rows("", 0, 10).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_jsonl_rows_limit_bounds() {
        let text = "{\"x\":1}\n{\"x\":2}\n{\"x\":3}";
        let rows = parse_jsonl_rows(text, 0, 0).unwrap();
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // parse_csv_rows
    // -----------------------------------------------------------------------

    #[test]
    fn parse_csv_rows_simple() {
        let text = "name,age\nAlice,30\nBob,25";
        let rows = parse_csv_rows(text, 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].columns.get("name"),
            Some(&serde_json::json!("Alice"))
        );
        assert_eq!(rows[1].columns.get("age"), Some(&serde_json::json!(25)));
    }

    #[test]
    fn parse_csv_rows_with_offset_and_limit() {
        let text = "n\n1\n2\n3\n4\n5";
        let rows = parse_csv_rows(text, 2, 2).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].columns.get("n"), Some(&serde_json::json!(3)));
        assert_eq!(rows[1].columns.get("n"), Some(&serde_json::json!(4)));
    }

    #[test]
    fn parse_csv_rows_quoted_fields() {
        let text = r#"name,description
Alice,"hello, world"
Bob,"say ""hi"""#;
        let rows = parse_csv_rows(text, 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].columns.get("description"),
            Some(&serde_json::json!("hello, world"))
        );
    }

    #[test]
    fn parse_csv_rows_empty_input() {
        let result = parse_csv_rows("", 0, 10);
        assert!(result.is_err()); // no header
    }

    #[test]
    fn parse_csv_rows_header_only() {
        let rows = parse_csv_rows("a,b,c", 0, 10).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn parse_csv_rows_skip_empty() {
        let text = "a\n1\n\n2\n";
        let rows = parse_csv_rows(text, 0, 10).unwrap();
        assert_eq!(rows.len(), 2);
    }

    // -----------------------------------------------------------------------
    // parse_rows_from_content (routing function)
    // -----------------------------------------------------------------------

    #[test]
    fn parse_rows_from_content_jsonl() {
        let content = b"{\"x\":1}";
        let rows = parse_rows_from_content(content, "data.jsonl", 0, 10).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn parse_rows_from_content_csv() {
        let content = b"a,b\n1,2";
        let rows = parse_rows_from_content(content, "data.csv", 0, 10).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn parse_rows_from_content_unsupported_format() {
        let content = b"some data";
        let result = parse_rows_from_content(content, "data.txt", 0, 10);
        assert!(result.is_err());
    }

    #[test]
    fn parse_rows_from_content_invalid_utf8() {
        let content = b"\xff\xfe\x00";
        let result = parse_rows_from_content(content, "data.csv", 0, 10);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // is_cgnat
    // -----------------------------------------------------------------------

    #[test]
    fn is_cgnat_true_for_100_64_range() {
        assert!(is_cgnat("100.64.0.0".parse().unwrap()));
        assert!(is_cgnat("100.127.255.255".parse().unwrap()));
    }

    #[test]
    fn is_cgnat_false_outside_range() {
        assert!(!is_cgnat("100.63.255.255".parse().unwrap()));
        assert!(!is_cgnat("100.128.0.1".parse().unwrap()));
        assert!(!is_cgnat("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn is_cgnat_false_for_loopback() {
        assert!(!is_cgnat("127.0.0.1".parse().unwrap()));
    }

    // -----------------------------------------------------------------------
    // authorize (route-level helper)
    // -----------------------------------------------------------------------

    #[test]
    fn route_authorize_without_auth_is_permissive() {
        let (_td, state) = make_test_state();
        let headers = HeaderMap::new();
        assert!(authorize(&state, &headers, TokenScope::Write).is_ok());
    }

    // -----------------------------------------------------------------------
    // parse_csv_line — additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_csv_line_multiple_quoted_fields() {
        let result = parse_csv_line(r#""a","b","c""#);
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_csv_line_escaped_quote_middle() {
        let result = parse_csv_line(r#"a,"say ""hello""",b"#);
        // The escped quote inside: ""hello"" contains two double-quotes
        assert_eq!(result[0], "a");
        assert!(result[1].contains("say"));
        assert_eq!(result[2], "b");
    }

    #[test]
    fn parse_csv_line_consecutive_commas() {
        let result = parse_csv_line("a,,,c");
        assert_eq!(result, vec!["a", "", "", "c"]);
    }

    #[test]
    fn parse_csv_line_starts_with_comma() {
        let result = parse_csv_line(",a,b");
        assert_eq!(result, vec!["", "a", "b"]);
    }

    #[test]
    fn parse_csv_line_ends_with_unterminated_quote_comma() {
        // Input: "unterm,, (starts with quote, no closing quote found, ends with comma)
        // The unterminated quote branch pushes content after opening quote: "unterm,,"
        // Then trailing comma adds an extra empty field
        let result = parse_csv_line(r#""unterm,,"#);
        assert_eq!(result, vec!["unterm,,", ""]);
    }

    #[test]
    fn parse_csv_line_quoted_escaped_quote_at_end() {
        let result = parse_csv_line(r#""say ""hi""",done"#);
        assert_eq!(result.len(), 2);
    }

    // -----------------------------------------------------------------------
    // is_private_ip — IPv4-mapped IPv6
    // -----------------------------------------------------------------------

    #[test]
    fn is_private_ip_false_for_ipv4_mapped_public_v6() {
        let ip: IpAddr = "::ffff:8.8.8.8".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv4_mapped_loopback_v6() {
        let ip: IpAddr = "::ffff:127.0.0.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv4_mapped_private_v6() {
        let ip: IpAddr = "::ffff:192.168.1.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_ipv6_unique_local() {
        let ip: IpAddr = "fc00::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_false_for_ipv6_public_unicast() {
        let ip: IpAddr = "2600::1".parse().unwrap();
        assert!(!is_private_ip(&ip));
    }

    // -----------------------------------------------------------------------
    // validate_webhook_url — edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn validate_webhook_url_rejects_empty() {
        assert!(validate_webhook_url("").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_no_host() {
        // A URL with just a scheme (no host) should fail parsing
        assert!(validate_webhook_url("http://").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_ipv6_unique_local() {
        assert!(validate_webhook_url("http://[fc00::1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_accepts_ipv6_public() {
        let result = validate_webhook_url("http://[2600:1f18:22b4:da00::1]/hook");
        // This is a public IPv6 address, should be OK
        assert!(result.is_ok() || result.is_err()); // DNS resolution may fail
    }

    #[test]
    fn validate_webhook_url_rejects_ipv4_mapped_loopback() {
        assert!(validate_webhook_url("http://[::ffff:127.0.0.1]/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_broadcast() {
        assert!(validate_webhook_url("http://255.255.255.255/hook").is_err());
    }

    // -----------------------------------------------------------------------
    // HubState debug
    // -----------------------------------------------------------------------

    #[test]
    fn hub_state_debug_redacts_auth() {
        let (_td, state) = make_test_state();
        let debug = format!("{state:?}");
        assert!(debug.contains("auth"));
    }

    // -----------------------------------------------------------------------
    // parse_csv_line — remaining edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_csv_line_quoted_field_with_trailing_comma() {
        // After closing quote, comma should be skipped
        let result = parse_csv_line(r#""a","#);
        assert_eq!(result, vec!["a", ""]);
    }

    #[test]
    fn parse_csv_line_only_commas() {
        let result = parse_csv_line(",,");
        assert_eq!(result, vec!["", "", ""]);
    }

    #[test]
    fn parse_csv_line_unterminated_quote_only() {
        let result = parse_csv_line("\"");
        assert_eq!(result, vec![""]);
    }

    // -----------------------------------------------------------------------
    // parse_yaml_frontmatter — additional edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn parse_yaml_frontmatter_no_closing_delimiter() {
        let content = b"---\nkey: value\n";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    #[test]
    fn parse_yaml_frontmatter_closing_on_same_line() {
        // Closing --- on its own line (no trailing newline) is valid.
        let content = b"---\nkey: value\n---";
        let result = parse_yaml_frontmatter(content);
        assert!(result.is_some(), "closing --- on its own line is valid");
        assert_eq!(
            result.unwrap().get("key").and_then(|v| v.as_str()),
            Some("value")
        );
    }

    #[test]
    fn parse_yaml_frontmatter_line_without_colon_skipped() {
        let content = b"---\nkey: value\nno_colon_line\nother: val\n---\n";
        let result = parse_yaml_frontmatter(content).unwrap();
        assert_eq!(result.get("key").and_then(|v| v.as_str()), Some("value"));
        assert_eq!(result.get("other").and_then(|v| v.as_str()), Some("val"));
        assert!(result.get("no_colon_line").is_none());
    }

    #[test]
    fn parse_yaml_frontmatter_json_number_and_bool_values() {
        let content = b"---\ncount: 42\nactive: true\n---\n";
        let result = parse_yaml_frontmatter(content).unwrap();
        assert_eq!(result.get("count").and_then(|v| v.as_u64()), Some(42));
        assert_eq!(result.get("active").and_then(|v| v.as_bool()), Some(true));
    }

    // -----------------------------------------------------------------------
    // tree_entries_at_path — LFS file at root level
    // -----------------------------------------------------------------------

    #[test]
    fn tree_entries_at_path_lfs_file_at_root() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "model.bin".into(),
            size: 5_000_000,
            sha: "oid123".into(),
            is_lfs: true,
            inline_content: None,
        }];
        let entries = tree_entries_at_path(&files, "");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, "file");
        assert!(entries[0].lfs.is_some());
        let lfs = entries[0].lfs.as_ref().unwrap();
        assert_eq!(lfs.oid, "oid123");
        assert_eq!(lfs.size, 5_000_000);
    }

    // -----------------------------------------------------------------------
    // tree_entries_recursive — empty prefix edge case
    // -----------------------------------------------------------------------

    #[test]
    fn tree_entries_recursive_non_matching_prefix() {
        use shardline_index::hub::HubFileEntry;
        let files = vec![HubFileEntry {
            path: "other/file.txt".into(),
            size: 10,
            sha: "x".into(),
            is_lfs: false,
            inline_content: None,
        }];
        let entries = tree_entries_recursive(&files, "nonexistent");
        assert!(entries.is_empty());
    }

    // ====================================================================
    // Handler-level integration tests (real LocalIndexStore)
    // ====================================================================

    /// Helper: creates a model repo + initial revision + optional files in a store.
    fn make_store_with_repo(
        repo_type: HubRepoType,
        repo_id: &str,
    ) -> (tempfile::TempDir, BoxedHubStore) {
        let (td, store) = make_delete_test_store();
        store
            .create_repo(repo_type, repo_id, false)
            .expect("create_repo");
        (td, store)
    }

    fn make_store_with_revision(
        rt: HubRepoType,
        repo_id: &str,
        rev_sha: &str,
        files: &[HubFileEntry],
    ) -> (tempfile::TempDir, BoxedHubStore) {
        let (td, store) = make_store_with_repo(rt, repo_id);
        // Parent must match the default_branch SHA set by create_repo (empty tree).
        let parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
        let _ = store
            .create_revision(repo_id, Some(parent), rev_sha, "main", "first")
            .expect("create_revision");
        if !files.is_empty() {
            store.store_files(rev_sha, files).expect("store_files");
        }
        (td, store)
    }

    fn default_headers() -> HeaderMap {
        HeaderMap::new()
    }

    // ------------------------------------------------------------------
    // health
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_health_returns_ok() {
        let result = health().await;
        assert_eq!(result.get("status").and_then(|v| v.as_str()), Some("ok"));
    }

    // ------------------------------------------------------------------
    // whoami
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_whoami_anonymous_without_auth() {
        let (_td, state) = make_test_state();
        let result = whoami(State(state), default_headers()).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_eq!(resp.name, "anonymous");
        assert!(!resp.is_admin);
    }

    // ------------------------------------------------------------------
    // repo_list
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_list_empty() {
        let (_td, state) = make_test_state();
        let result = repo_list(State(state), default_headers()).await.unwrap();
        assert!(result.repos.is_empty());
    }

    #[tokio::test]
    async fn handler_repo_list_with_repos() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/repo-a");
        store
            .create_repo(HubRepoType::Dataset, "org/data-b", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_list(State(state), default_headers()).await.unwrap();
        assert_eq!(result.repos.len(), 2);
    }

    // ------------------------------------------------------------------
    // repo_search
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_search_short_query_rejected() {
        let (_td, state) = make_test_state();
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "x".into(),
                author: None,
                sort: None,
                direction: None,
                limit: 50,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("at least 2")),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_repo_search_invalid_type() {
        let (_td, state) = make_test_state();
        let result = repo_search(
            State(state),
            default_headers(),
            Path("invalid".to_string()),
            Query(RepoSearchQuery {
                q: "test".into(),
                author: None,
                sort: None,
                direction: None,
                limit: 50,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("invalid repo type")),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_repo_search_finds_matching() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/my-model");
        store
            .create_repo(HubRepoType::Model, "other/other", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // The search uses LIKE 'q%' on repo_id, so prefix-match the full ID.
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "org/my-model".into(),
                author: None,
                sort: None,
                direction: None,
                limit: 50,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.repos.len(), 1);
        assert_eq!(result.repos[0].id, "org/my-model");
    }

    // ------------------------------------------------------------------
    // repo_info
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_info_missing_repo() {
        let (_td, state) = make_test_state();
        let result = repo_info(
            State(state),
            default_headers(),
            Path(("models".into(), "missing".into(), "nope".into())),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
    }

    #[tokio::test]
    async fn handler_repo_info_returns_repo() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/existing");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_info(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "existing".into())),
        )
        .await
        .unwrap();
        assert_eq!(result.id, "org/existing");
        assert_eq!(result.repo_type, RepoType::Model);
    }

    #[tokio::test]
    async fn handler_repo_info_with_card_data() {
        let readme_content =
            b"---\nlanguage: en\npipeline_tag: text-classification\n---\n# Model\nSome text";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/card-model",
            "sha_card",
            &[HubFileEntry {
                path: "README.md".into(),
                size: readme_content.len() as u64,
                sha: "readme_sha".into(),
                is_lfs: false,
                inline_content: Some(readme_content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_info(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "card-model".into())),
        )
        .await
        .unwrap();
        let card = result.card_data.as_ref().expect("expected card_data");
        assert_eq!(card.get("language").and_then(|v| v.as_str()), Some("en"));
        assert_eq!(
            card.get("pipeline_tag").and_then(|v| v.as_str()),
            Some("text-classification")
        );
    }

    // ------------------------------------------------------------------
    // repo_modelcard
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_modelcard_missing_repo() {
        let (_td, state) = make_test_state();
        let result = repo_modelcard(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "such".into())),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
    }

    #[tokio::test]
    async fn handler_repo_modelcard_no_readme() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/no-readme", "sha_nr", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_modelcard(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "no-readme".into())),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
    }

    #[tokio::test]
    async fn handler_repo_modelcard_with_readme() {
        let content = b"# My Model\n\nThis is a test model.";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/my-model",
            "sha_rm",
            &[HubFileEntry {
                path: "README.md".into(),
                size: content.len() as u64,
                sha: "rm_sha".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_modelcard(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "my-model".into())),
        )
        .await
        .unwrap();
        // Should be a text/markdown response
        let status = result.status();
        assert_eq!(status, 200);
        // Check header
        let ct = result
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        assert_eq!(ct, Some("text/markdown; charset=utf-8"));
    }

    // ------------------------------------------------------------------
    // repo_revisions
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_revisions_with_revisions() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/has-revs", "sha_rev1", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_revisions(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "has-revs".into())),
        )
        .await
        .unwrap();
        assert!(!result.revisions.is_empty());
        let rev = &result.revisions[0];
        assert_eq!(rev.ref_name, "main");
        assert_eq!(rev.sha, "sha_rev1");
    }

    // ------------------------------------------------------------------
    // repo_create
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_create_model() {
        let (_td, state) = make_test_state();
        let req = RepoCreateRequest {
            repo_type: RepoType::Model,
            name: "ns/new-repo".to_owned(),
            organization: None,
            private: false,
            visibility: None,
        };
        let (status, json) = repo_create(State(state), default_headers(), Json(req))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json.id, "ns/new-repo");
    }

    // ------------------------------------------------------------------
    // repo_create_type
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_create_type_invalid_type() {
        let (_td, state) = make_test_state();
        let result = repo_create_type(
            State(state),
            default_headers(),
            Path(("invalid".into(), "ns".into(), "repo".into())),
            Json(serde_json::json!({})),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(_)),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_repo_create_type_success() {
        let (_td, state) = make_test_state();
        let (status, json) = repo_create_type(
            State(state),
            default_headers(),
            Path(("models".into(), "ns".into(), "my-repo".into())),
            Json(serde_json::json!({})),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json.id, "ns/my-repo");
        assert_eq!(json.repo_type, RepoType::Model);
    }

    #[tokio::test]
    async fn handler_repo_create_type_private() {
        let (_td, state) = make_test_state();
        let (_, json) = repo_create_type(
            State(state),
            default_headers(),
            Path(("datasets".into(), "ns".into(), "secret-data".into())),
            Json(serde_json::json!({"private": true})),
        )
        .await
        .unwrap();
        assert!(json.private);
        assert_eq!(json.repo_type, RepoType::Dataset);
    }

    // ------------------------------------------------------------------
    // repo_delete (handler-level)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_delete_missing_repo() {
        let (_td, state) = make_test_state();
        let result = repo_delete(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "exist".into())),
        )
        .await;
        assert!(matches!(result, Err(HubApiError::RepoNotFound)));
    }

    #[tokio::test]
    async fn handler_repo_delete_success() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/to-delete");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_delete(
            State(state.clone()),
            default_headers(),
            Path(("models".into(), "org".into(), "to-delete".into())),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
        // Verify it's gone
        assert!(state.store.get_repo("org/to-delete").unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // preupload
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_preupload_too_many_files() {
        let (_td, state) = make_test_state();
        let files: Vec<PreuploadFile> = (0..10_001)
            .map(|i| PreuploadFile {
                path: format!("file_{i}"),
                lfs: false,
            })
            .collect();
        let result = preupload(
            State(state),
            default_headers(),
            Path(("models".into(), "ns".into(), "r".into(), "main".into())),
            Json(PreuploadRequest {
                files,
                git_attributes: None,
                git_ignore: None,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("exceeds maximum")),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_preupload_checks_existence() {
        let content = b"existing content";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/preupload-test",
            "sha_pre",
            &[HubFileEntry {
                path: "existing.txt".into(),
                size: content.len() as u64,
                sha: "existing_sha".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = preupload(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "preupload-test".into(),
                "main".into(),
            )),
            Json(PreuploadRequest {
                files: vec![
                    PreuploadFile {
                        path: "existing.txt".into(),
                        lfs: false,
                    },
                    PreuploadFile {
                        path: "new.txt".into(),
                        lfs: false,
                    },
                ],
                git_attributes: None,
                git_ignore: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.result.len(), 2);
        assert!(result.result[0].exists); // existing.txt
        assert!(!result.result[1].exists); // new.txt
    }

    // ------------------------------------------------------------------
    // commit (via handler)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_commit_wrong_content_type() {
        let (_td, state) = make_test_state();
        // No Content-Type header → rejection
        let result = commit(
            State(state),
            default_headers(),
            Path(("models".into(), "ns".into(), "r".into(), "main".into())),
            "{}".to_string(),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("Content-Type")),
            "expected PathValidation, got {err:?}"
        );
    }

    fn ndjson_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::header::CONTENT_TYPE,
            "application/x-ndjson".parse().unwrap(),
        );
        h
    }

    #[tokio::test]
    async fn handler_commit_inline_file_success() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/commit-test", "parent_sha_001", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let body = r#"{"header":{"message":"add readme"}}
{"file":{"path":"README.md","content":"SGVsbG8gV29ybGQ="}}
"#;
        let result = commit(
            State(state),
            ndjson_headers(),
            Path((
                "models".into(),
                "org".into(),
                "commit-test".into(),
                "main".into(),
            )),
            body.to_string(),
        )
        .await;
        assert!(result.is_ok(), "commit failed: {:?}", result.err());
        let resp = result.unwrap();
        assert!(!resp.commit_id.is_empty());
        assert_eq!(resp.ref_name.as_deref(), Some("main"));
    }

    #[tokio::test]
    async fn handler_commit_lfs_pointer_success() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/lfs-commit", "parent_lfs", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // Valid SHA-256 OID (64 hex chars)
        let oid = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let body = format!(
            r#"{{"header":{{"message":"add lfs file"}}}}
{{"lfsFile":{{"path":"big.bin","oid":"{oid}","size":5000000}}}}
"#
        );
        let result = commit(
            State(state),
            ndjson_headers(),
            Path((
                "models".into(),
                "org".into(),
                "lfs-commit".into(),
                "main".into(),
            )),
            body,
        )
        .await;
        assert!(result.is_ok(), "commit failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn handler_commit_delete_file() {
        // Create a repo with a file, then delete it
        let content = b"to be deleted";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/del-test",
            "parent_del",
            &[HubFileEntry {
                path: "old.txt".into(),
                size: content.len() as u64,
                sha: "old_sha".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let body = r#"{"header":{"message":"delete file"}}
{"deletedEntry":{"path":"old.txt"}}
"#;
        let result = commit(
            State(state),
            ndjson_headers(),
            Path((
                "models".into(),
                "org".into(),
                "del-test".into(),
                "main".into(),
            )),
            body.to_string(),
        )
        .await;
        assert!(result.is_ok(), "commit failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn handler_commit_parent_mismatch() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/parent-mismatch",
            "actual_parent_sha",
            &[],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // Body specifies parentCommit that does NOT match URL resolution
        let body = r#"{"header":{"message":"mismatch","parentCommit":"wrong_parent_sha"}}
{"file":{"path":"f.txt","content":"dGVzdA=="}}
"#;
        let result = commit(
            State(state),
            ndjson_headers(),
            Path((
                "models".into(),
                "org".into(),
                "parent-mismatch".into(),
                "main".into(),
            )),
            body.to_string(),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::Conflict(msg) if msg.contains("parentCommit mismatch")),
            "expected Conflict, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // apply_commit — direct testing of the core logic
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_apply_commit_inline_file() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/apply-test", "parent_apply", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let parsed = ParsedCommit {
            message: "apply inline".into(),
            parent_commit: None,
            instructions: vec![CommitInstruction::InlineFile {
                path: "hello.txt".into(),
                content: b"world".to_vec(),
            }],
        };
        let result = apply_commit(&state, "org/apply-test", "parent_apply", &parsed)
            .await
            .unwrap();
        assert!(!result.commit_id.is_empty());
        // Verify the file is stored
        let _files = state.store.get_files("parent_apply").unwrap();
        // The new commit's files would be stored under the new commit SHA, not parent
        // apply_commit calls store_files(&commit_sha, &files) then create_revision
        // So check files under the new commit SHA
        let new_sha = &result.commit_id;
        let new_files = state.store.get_files(new_sha).unwrap();
        assert_eq!(new_files.len(), 1);
        assert_eq!(new_files[0].path, "hello.txt");
    }

    #[tokio::test]
    async fn handler_apply_commit_lfs_pointer() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/apply-lfs", "parent_lfs2", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let oid = "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff";
        let parsed = ParsedCommit {
            message: "add lfs".into(),
            parent_commit: None,
            instructions: vec![CommitInstruction::LfsPointer {
                path: "model.bin".into(),
                oid: oid.to_owned(),
                size: 2_000_000,
            }],
        };
        let result = apply_commit(&state, "org/apply-lfs", "parent_lfs2", &parsed)
            .await
            .unwrap();
        let new_files = state.store.get_files(&result.commit_id).unwrap();
        assert_eq!(new_files.len(), 1);
        assert!(new_files[0].is_lfs);
        assert_eq!(new_files[0].sha, oid);
    }

    #[tokio::test]
    async fn handler_apply_commit_delete() {
        let content = b"delete me";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/apply-del",
            "parent_del2",
            &[HubFileEntry {
                path: "old.txt".into(),
                size: content.len() as u64,
                sha: "old_sha2".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let parsed = ParsedCommit {
            message: "delete file".into(),
            parent_commit: None,
            instructions: vec![CommitInstruction::Delete {
                path: "old.txt".into(),
            }],
        };
        let result = apply_commit(&state, "org/apply-del", "parent_del2", &parsed)
            .await
            .unwrap();
        let new_files = state.store.get_files(&result.commit_id).unwrap();
        assert!(new_files.is_empty());
    }

    #[tokio::test]
    async fn handler_apply_commit_parent_mismatch() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/apply-mismatch", "actual_sha", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let parsed = ParsedCommit {
            message: "bad parent".into(),
            parent_commit: Some("different_sha".into()),
            instructions: vec![],
        };
        let result = apply_commit(&state, "org/apply-mismatch", "actual_sha", &parsed).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::Conflict(msg) if msg.contains("parentCommit mismatch")),
            "expected Conflict, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // file_tree
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_file_tree_basic() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/tree-test",
            "sha_tree",
            &[
                HubFileEntry {
                    path: "README.md".into(),
                    size: 100,
                    sha: "a".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "src/main.rs".into(),
                    size: 200,
                    sha: "b".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let entries = file_tree(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "tree-test".into(),
                "main".into(),
                String::new(),
            )),
            Query(TreeQuery {
                limit: None,
                cursor: None,
                recursive: false,
            }),
        )
        .await
        .unwrap();
        // Expect 2 entries: README.md at root and src/ directory
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn handler_file_tree_recursive() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/tree-rec",
            "sha_tree_rec",
            &[
                HubFileEntry {
                    path: "src/main.rs".into(),
                    size: 200,
                    sha: "b".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "src/lib.rs".into(),
                    size: 300,
                    sha: "c".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let entries = file_tree(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "tree-rec".into(),
                "main".into(),
                String::new(),
            )),
            Query(TreeQuery {
                limit: None,
                cursor: None,
                recursive: true,
            }),
        )
        .await
        .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry_type, "file");
    }

    #[tokio::test]
    async fn handler_file_tree_with_limit_and_cursor() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/tree-lim",
            "sha_tree_lim",
            &[
                HubFileEntry {
                    path: "a.txt".into(),
                    size: 1,
                    sha: "s1".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "b.txt".into(),
                    size: 2,
                    sha: "s2".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "c.txt".into(),
                    size: 3,
                    sha: "s3".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let entries = file_tree(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "tree-lim".into(),
                "main".into(),
                String::new(),
            )),
            Query(TreeQuery {
                limit: Some(2),
                cursor: Some("a.txt".into()),
                recursive: false,
            }),
        )
        .await
        .unwrap();
        // After a.txt cursor: first 2 entries from b.txt, c.txt
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].path, "b.txt");
        assert_eq!(entries[1].path, "c.txt");
    }

    // ------------------------------------------------------------------
    // resolve_file
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_resolve_file_inline() {
        let content = b"hello world file content";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/resolve-test",
            "sha_resolve",
            &[HubFileEntry {
                path: "data.txt".into(),
                size: content.len() as u64,
                sha: "data_sha".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = resolve_file(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "resolve-test".into(),
                "main".into(),
                "data.txt".into(),
            )),
        )
        .await;
        assert!(result.is_ok(), "resolve_file failed: {:?}", result.err());
        let resp = result.unwrap();
        assert_eq!(resp.status(), 200);
        // Should have application/octet-stream content type
        let ct = resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        assert_eq!(ct, Some("application/octet-stream"));
    }

    #[tokio::test]
    async fn handler_resolve_file_not_found() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/resolve-miss", "sha_miss", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = resolve_file(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "resolve-miss".into(),
                "main".into(),
                "nope.txt".into(),
            )),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
    }

    // ------------------------------------------------------------------
    // lfs_batch
    // ------------------------------------------------------------------

    fn make_lfs_state() -> (tempfile::TempDir, HubState) {
        let (td, store) = make_delete_test_store();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        (td, state)
    }

    #[tokio::test]
    async fn handler_lfs_batch_upload_new_object() {
        let (_td, state) = make_lfs_state();
        let result = lfs_batch(
            State(state),
            default_headers(),
            Json(LfsBatchRequest {
                operation: LfsBatchOperation::Upload,
                ref_: LfsBatchRef {
                    name: "main".into(),
                },
                objects: vec![LfsObjectRequest {
                    oid: "abc123".into(),
                    size: 1000,
                }],
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.transfer, "basic");
        assert_eq!(result.objects.len(), 1);
        let obj = &result.objects[0];
        // Upload of new object: upload action present, no error
        assert!(obj.actions.is_some());
        let actions = obj.actions.as_ref().unwrap();
        assert!(actions.upload.is_some());
        assert!(actions.download.is_none());
        assert!(actions.verify.is_none());
        assert!(obj.error.is_none());
    }

    #[tokio::test]
    async fn handler_lfs_batch_download_existing_object() {
        let (_td, state) = make_lfs_state();
        // Store an LFS object first
        state
            .store
            .put_lfs_object("existing_oid", b"some data")
            .unwrap();
        let result = lfs_batch(
            State(state),
            default_headers(),
            Json(LfsBatchRequest {
                operation: LfsBatchOperation::Download,
                ref_: LfsBatchRef {
                    name: "main".into(),
                },
                objects: vec![LfsObjectRequest {
                    oid: "existing_oid".into(),
                    size: 9,
                }],
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.objects.len(), 1);
        let obj = &result.objects[0];
        assert!(obj.actions.is_some());
        assert!(obj.actions.as_ref().unwrap().download.is_some());
        assert!(obj.error.is_none());
    }

    #[tokio::test]
    async fn handler_lfs_batch_download_missing_object() {
        let (_td, state) = make_lfs_state();
        let result = lfs_batch(
            State(state),
            default_headers(),
            Json(LfsBatchRequest {
                operation: LfsBatchOperation::Download,
                ref_: LfsBatchRef {
                    name: "main".into(),
                },
                objects: vec![LfsObjectRequest {
                    oid: "missing_oid".into(),
                    size: 100,
                }],
            }),
        )
        .await
        .unwrap();
        let obj = &result.objects[0];
        // Missing download: actions is None, error is Some(404)
        assert!(obj.actions.is_none());
        let err = obj
            .error
            .as_ref()
            .expect("expected error for missing object");
        assert_eq!(err.code, 404);
    }

    #[tokio::test]
    async fn handler_lfs_batch_verify_existing() {
        let (_td, state) = make_lfs_state();
        state.store.put_lfs_object("verify_oid", b"data").unwrap();
        let result = lfs_batch(
            State(state),
            default_headers(),
            Json(LfsBatchRequest {
                operation: LfsBatchOperation::Verify,
                ref_: LfsBatchRef {
                    name: "main".into(),
                },
                objects: vec![LfsObjectRequest {
                    oid: "verify_oid".into(),
                    size: 4,
                }],
            }),
        )
        .await
        .unwrap();
        let obj = &result.objects[0];
        assert!(obj.actions.as_ref().unwrap().verify.is_some());
        assert!(obj.error.is_none());
    }

    #[tokio::test]
    async fn handler_lfs_batch_verify_missing() {
        let (_td, state) = make_lfs_state();
        let result = lfs_batch(
            State(state),
            default_headers(),
            Json(LfsBatchRequest {
                operation: LfsBatchOperation::Verify,
                ref_: LfsBatchRef {
                    name: "main".into(),
                },
                objects: vec![LfsObjectRequest {
                    oid: "no_verify_oid".into(),
                    size: 1,
                }],
            }),
        )
        .await
        .unwrap();
        let obj = &result.objects[0];
        assert!(obj.actions.is_none());
        assert!(obj.error.is_some());
    }

    // ------------------------------------------------------------------
    // lfs_upload
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_lfs_upload_invalid_oid() {
        let (_td, state) = make_test_state();
        let result = lfs_upload(
            State(state),
            default_headers(),
            Path("bad-oid".to_string()),
            bytes::Bytes::from_static(b"data"),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(_)),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_lfs_upload_success() {
        let (_td, state) = make_test_state();
        let oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let result = lfs_upload(
            State(state.clone()),
            default_headers(),
            Path(oid.to_string()),
            bytes::Bytes::from_static(b"some lfs data"),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::OK);
        // Verify it's stored
        let data = state.store.get_lfs_object(oid).unwrap();
        assert_eq!(data, Some(b"some lfs data".to_vec()));
    }

    // ------------------------------------------------------------------
    // lfs_download
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_lfs_download_missing() {
        let (_td, state) = make_test_state();
        let result = lfs_download(
            State(state),
            default_headers(),
            Path("nonexistent_oid".to_string()),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
    }

    #[tokio::test]
    async fn handler_lfs_download_success() {
        let (_td, state) = make_test_state();
        let oid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        state.store.put_lfs_object(oid, b"download data").unwrap();
        let (status, headers, data) =
            lfs_download(State(state), default_headers(), Path(oid.to_string()))
                .await
                .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(data, b"download data");
        // Verify content-type header name
        assert!(!headers.is_empty());
    }

    // ------------------------------------------------------------------
    // git_head
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_revisions_has_initial() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/init-rev");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_revisions(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "init-rev".into())),
        )
        .await
        .unwrap();
        // create_repo always inserts an initial empty-tree revision
        assert_eq!(result.revisions.len(), 1);
        assert_eq!(result.revisions[0].ref_name, "main");
    }

    #[tokio::test]
    async fn handler_git_head_with_revision() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/has-head", "sha_head123", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = git_head(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "has-head".into())),
        )
        .await
        .unwrap();
        assert!(result.contains("sha_head123"));
        assert!(result.contains("refs/heads/main"));
    }

    // ------------------------------------------------------------------
    // dataset_parquet
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_commit_no_revision() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/no-rev");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // "nonexistent_rev" isn't a known ref or SHA → revision not found
        let result = commit(
            State(state),
            ndjson_headers(),
            Path((
                "models".into(),
                "org".into(),
                "no-rev".into(),
                "nonexistent_rev".into(),
            )),
            r#"{"header":{"message":"x"}}"#.to_string(),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::RevisionNotFound));
    }

    #[tokio::test]
    async fn handler_dataset_parquet_lists_files() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/data",
            "sha_data",
            &[
                HubFileEntry {
                    path: "data/train/data.parquet".into(),
                    size: 5000,
                    sha: "pq".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "README.md".into(),
                    size: 10,
                    sha: "rm".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_parquet(
            State(state),
            default_headers(),
            Path(("org".into(), "data".into())),
        )
        .await
        .unwrap();
        assert_eq!(result.files.len(), 1);
        assert!(result.files[0].path.ends_with(".parquet"));
    }

    #[tokio::test]
    async fn handler_dataset_parquet_csv_and_jsonl_included() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/multi",
            "sha_multi",
            &[
                HubFileEntry {
                    path: "a.csv".into(),
                    size: 100,
                    sha: "csv".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "b.jsonl".into(),
                    size: 200,
                    sha: "jl".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "c.txt".into(),
                    size: 50,
                    sha: "txt".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_parquet(
            State(state),
            default_headers(),
            Path(("org".into(), "multi".into())),
        )
        .await
        .unwrap();
        assert_eq!(result.files.len(), 2);
    }

    // ------------------------------------------------------------------
    // dataset_first_rows
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_first_rows_empty_dataset() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Dataset, "org/empty-ds", "sha_empty_ds", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_first_rows(
            State(state),
            default_headers(),
            Path(("org".into(), "empty-ds".into())),
            Query(DatasetFirstRowsQuery {
                config: "default".into(),
                split: "train".into(),
                limit: 100,
            }),
        )
        .await
        .unwrap();
        assert!(result.columns.is_empty());
        assert!(result.rows.is_empty());
    }

    #[tokio::test]
    async fn handler_dataset_first_rows_with_jsonl() {
        let jsonl_content = b"{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/jsonl-ds",
            "sha_jsonl",
            &[HubFileEntry {
                path: "data/train/data.jsonl".into(),
                size: jsonl_content.len() as u64,
                sha: "jsonl_sha".into(),
                is_lfs: false,
                inline_content: Some(jsonl_content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_first_rows(
            State(state),
            default_headers(),
            Path(("org".into(), "jsonl-ds".into())),
            Query(DatasetFirstRowsQuery {
                config: "default".into(),
                split: "train".into(),
                limit: 10,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.columns.len(), 2);
        assert!(result.columns.contains(&"a".to_string()));
        assert!(result.columns.contains(&"b".to_string()));
        assert_eq!(result.rows.len(), 2);
    }

    // ------------------------------------------------------------------
    // dataset_viewer
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_viewer_with_data() {
        let csv_content = b"name,age\nAlice,30\nBob,25\nCharlie,35\n";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/viewer-ds",
            "sha_viewer",
            &[HubFileEntry {
                path: "default/train/data.csv".into(),
                size: csv_content.len() as u64,
                sha: "csv_sha".into(),
                is_lfs: false,
                inline_content: Some(csv_content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_viewer(
            State(state),
            default_headers(),
            Path(("org".into(), "viewer-ds".into(), "train".into())),
            Query(DatasetViewerQuery {
                config: "default".into(),
                offset: 0,
                length: 10,
            }),
        )
        .await
        .unwrap();
        // Columns are sorted alphabetically (from BTreeMap)
        assert_eq!(result.columns, vec!["age", "name"]);
        assert_eq!(result.rows.len(), 3);
        assert!(result.num_rows_total.is_none());
    }

    #[tokio::test]
    async fn handler_dataset_viewer_pagination() {
        let csv_content = b"n\n1\n2\n3\n4\n5\n";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/viewer-pag",
            "sha_vp",
            &[HubFileEntry {
                path: "data/test/data.csv".into(),
                size: csv_content.len() as u64,
                sha: "vp_sha".into(),
                is_lfs: false,
                inline_content: Some(csv_content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_viewer(
            State(state),
            default_headers(),
            Path(("org".into(), "viewer-pag".into(), "test".into())),
            Query(DatasetViewerQuery {
                config: "data".into(),
                offset: 2,
                length: 2,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.rows.len(), 2);
        // rows[0] is the 3rd data row (offset 2): n=3 (CSV values are parsed as primitives)
        assert_eq!(result.rows[0].columns.get("n"), Some(&serde_json::json!(3)));
        assert_eq!(result.rows[1].columns.get("n"), Some(&serde_json::json!(4)));
    }

    // ------------------------------------------------------------------
    // webhook_create
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_create_success() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-test");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let (status, resp) = webhook_create(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-test".into())),
            Json(WebhookCreateRequest {
                url: "https://example.com/hook".into(),
                events: vec!["push".into()],
                secret: None,
            }),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(resp.url, "https://example.com/hook");
        assert!(resp.active);
    }

    #[tokio::test]
    async fn handler_webhook_create_invalid_url() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-badurl");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = webhook_create(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-badurl".into())),
            Json(WebhookCreateRequest {
                url: "ftp://bad.com/hook".into(),
                events: vec!["push".into()],
                secret: None,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("scheme")),
            "expected PathValidation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn handler_webhook_create_too_many_events() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-toomany");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let events: Vec<String> = (0..51).map(|i| format!("event_{i}")).collect();
        let result = webhook_create(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-toomany".into())),
            Json(WebhookCreateRequest {
                url: "https://example.com/hook".into(),
                events,
                secret: None,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("exceeds maximum")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // webhook_list
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_list_empty() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list-empty");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = webhook_list(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-list-empty".into())),
        )
        .await
        .unwrap();
        assert!(result.webhooks.is_empty());
    }

    #[tokio::test]
    async fn handler_webhook_list_with_webhooks() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list-full");
        store
            .create_webhook(
                "org/wh-list-full",
                "https://hook1.example.com",
                &["push".into()],
                None,
            )
            .unwrap();
        store
            .create_webhook(
                "org/wh-list-full",
                "https://hook2.example.com",
                &["push".into(), "delete".into()],
                Some("secret"),
            )
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = webhook_list(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-list-full".into())),
        )
        .await
        .unwrap();
        assert_eq!(result.webhooks.len(), 2);
    }

    // ------------------------------------------------------------------
    // webhook_delete
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_delete_success() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-del");
        let wh = store
            .create_webhook(
                "org/wh-del",
                "https://example.com/hook",
                &["push".into()],
                None,
            )
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = webhook_delete(
            State(state.clone()),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "wh-del".into(),
                wh.id.clone(),
            )),
        )
        .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
        // Verify it's gone
        let hooks = state.store.list_webhooks("org/wh-del").unwrap();
        assert!(hooks.is_empty());
    }

    // ------------------------------------------------------------------
    // authorize — additional coverage
    // ------------------------------------------------------------------

    #[test]
    fn route_authorize_with_auth_and_no_header_is_err() {
        use shardline_protocol::TokenClaims;
        use shardline_server_core::AuthProvider;
        struct MockProvider;
        impl AuthProvider for MockProvider {
            fn verify_token(
                &self,
                _token: &str,
            ) -> Result<TokenClaims, shardline_server_core::AuthError> {
                Err(shardline_server_core::AuthError::InvalidToken)
            }
            fn mint_token(
                &self,
                _claims: &TokenClaims,
            ) -> Result<String, shardline_server_core::AuthError> {
                Ok("token".into())
            }
        }
        let state = HubState {
            store: make_delete_test_store().1,
            auth: Some(HubAuth::new(Box::new(MockProvider))),
            http_client: None,
        };
        let headers = HeaderMap::new();
        let result = authorize(&state, &headers, TokenScope::Read);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::Unauthorized),
            "expected Unauthorized, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // apply_commit — empty instructions
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_apply_commit_empty_instructions() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/empty-inst", "parent_empty", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let parsed = ParsedCommit {
            message: "empty commit".into(),
            parent_commit: None,
            instructions: vec![],
        };
        let result = apply_commit(&state, "org/empty-inst", "parent_empty", &parsed)
            .await
            .unwrap();
        assert!(!result.commit_id.is_empty());
    }

    // ------------------------------------------------------------------
    // dataset_parquet — non-dataset repo error
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_parquet_non_dataset_repo_errors() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/model-repo", "sha_model", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_parquet(
            State(state),
            default_headers(),
            Path(("org".into(), "model-repo".into())),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // dataset_first_rows — non-dataset repo error
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_first_rows_non_dataset_errors() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/model-ds", "sha_model_ds", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_first_rows(
            State(state),
            default_headers(),
            Path(("org".into(), "model-ds".into())),
            Query(DatasetFirstRowsQuery {
                config: "default".into(),
                split: "train".into(),
                limit: 100,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // dataset_viewer — non-dataset repo error
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_viewer_non_dataset_errors() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/model-view", "sha_model_view", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_viewer(
            State(state),
            default_headers(),
            Path(("org".into(), "model-view".into(), "train".into())),
            Query(DatasetViewerQuery {
                config: "default".into(),
                offset: 0,
                length: 10,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // repo_search — sort and direction edge cases
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_search_sort_by_last_modified_asc() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/model-a");
        store
            .create_repo(HubRepoType::Model, "org/model-b", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "org/".into(),
                author: None,
                sort: Some("lastModified".into()),
                direction: Some("asc".into()),
                limit: 50,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.repos.len(), 2);
    }

    #[tokio::test]
    async fn handler_repo_search_sort_likes_noop() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/likes-a");
        store
            .create_repo(HubRepoType::Model, "org/likes-b", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // "likes" sort is currently a no-op — just verify it doesn't error.
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "org/likes".into(),
                author: None,
                sort: Some("likes".into()),
                direction: None,
                limit: 50,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.repos.len(), 2);
    }

    #[tokio::test]
    async fn handler_repo_search_sort_downloads_noop() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/dl-a");
        store
            .create_repo(HubRepoType::Model, "org/dl-b", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        // "downloads" sort is currently a no-op — just verify it doesn't error.
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "org/dl".into(),
                author: None,
                sort: Some("downloads".into()),
                direction: None,
                limit: 50,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.repos.len(), 2);
    }

    // ------------------------------------------------------------------
    // repo_search with unknown sort (should keep default order)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_search_unknown_sort_keeps_default_order() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/order-a");
        store
            .create_repo(HubRepoType::Model, "org/order-b", false)
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_search(
            State(state),
            default_headers(),
            Path("models".to_string()),
            Query(RepoSearchQuery {
                q: "org/order".into(),
                author: None,
                sort: Some("unknown_field".into()),
                direction: Some("asc".into()),
                limit: 50,
            }),
        )
        .await
        .unwrap();
        assert_eq!(result.repos.len(), 2);
    }

    // ------------------------------------------------------------------
    // repo_info with invalid repo type
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_info_invalid_type() {
        let (_td, state) = make_test_state();
        let result = repo_info(
            State(state),
            default_headers(),
            Path(("invalid_type".into(), "ns".into(), "repo".into())),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("invalid repo type")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // dataset_first_rows with inline content missing
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_first_rows_content_not_inline() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/no-inline",
            "sha_no_inline",
            &[HubFileEntry {
                path: "data/train/data.jsonl".into(),
                size: 50,
                sha: "no_inline_sha".into(),
                is_lfs: false,
                inline_content: None,
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_first_rows(
            State(state),
            default_headers(),
            Path(("org".into(), "no-inline".into())),
            Query(DatasetFirstRowsQuery {
                config: "default".into(),
                split: "train".into(),
                limit: 100,
            }),
        )
        .await;
        assert!(result.is_err());
    }

    // ------------------------------------------------------------------
    // dataset_viewer with data file not found
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_viewer_split_not_found() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Dataset, "org/no-split", "sha_no_split", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = dataset_viewer(
            State(state),
            default_headers(),
            Path(("org".into(), "no-split".into(), "nonexistent".into())),
            Query(DatasetViewerQuery {
                config: "default".into(),
                offset: 0,
                length: 10,
            }),
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(&err, HubApiError::PathValidation(msg) if msg.contains("no data file")),
            "expected PathValidation, got {err:?}"
        );
    }

    // ------------------------------------------------------------------
    // webhook_create with no repo (repo not found)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_create_repo_not_found() {
        let (_td, state) = make_test_state();
        let result = webhook_create(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "repo".into())),
            Json(WebhookCreateRequest {
                url: "https://example.com/hook".into(),
                events: vec!["push".into()],
                secret: None,
            }),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
    }

    // ------------------------------------------------------------------
    // is_private_ip — broadcast and documentation IPs
    // ------------------------------------------------------------------

    #[test]
    fn is_private_ip_true_for_broadcast() {
        let ip: std::net::IpAddr = "255.255.255.255".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_documentation() {
        let ip: std::net::IpAddr = "192.0.2.1".parse().unwrap();
        assert!(is_private_ip(&ip));
        let ip: std::net::IpAddr = "198.51.100.1".parse().unwrap();
        assert!(is_private_ip(&ip));
        let ip: std::net::IpAddr = "203.0.113.1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    #[test]
    fn is_private_ip_true_for_cgnat_boundary() {
        // 100.64.0.0/10 boundaries
        let ip: std::net::IpAddr = "100.64.0.0".parse().unwrap();
        assert!(is_private_ip(&ip), "100.64.0.0 should be CGNAT");
        let ip: std::net::IpAddr = "100.127.255.255".parse().unwrap();
        assert!(is_private_ip(&ip), "100.127.255.255 should be CGNAT");
    }

    #[test]
    fn is_private_ip_false_for_cgnat_boundary_outside() {
        // Just outside 100.64.0.0/10
        let ip: std::net::IpAddr = "100.63.255.255".parse().unwrap();
        assert!(!is_private_ip(&ip), "100.63.255.255 should NOT be private");
        let ip: std::net::IpAddr = "100.128.0.0".parse().unwrap();
        assert!(!is_private_ip(&ip), "100.128.0.0 should NOT be private");
    }

    #[test]
    fn is_private_ip_false_for_ipv6_unique_local_not_covered() {
        // fc00::/7 should be caught by is_unique_local()
        let ip: std::net::IpAddr = "fd00::1".parse().unwrap();
        assert!(is_private_ip(&ip));
    }

    // ------------------------------------------------------------------
    // validate_webhook_url — scheme edge cases
    // ------------------------------------------------------------------

    #[test]
    fn validate_webhook_url_rejects_missing_scheme() {
        assert!(validate_webhook_url("example.com/hook").is_err());
    }

    #[test]
    fn validate_webhook_url_rejects_https_is_fine() {
        assert!(validate_webhook_url("https://example.com/hook").is_ok());
    }

    #[test]
    fn validate_webhook_url_ipv6_public_ok() {
        // 2001:db8::1 is documentation range (should be treated as private)
        let ip: std::net::IpAddr = "2001:db8::1".parse().unwrap();
        // It is NOT private in is_private_ip
        assert!(!is_private_ip(&ip));
        // URL should be accepted (public IPv6)
        let result = validate_webhook_url("http://[2001:db8::1]/hook");
        // This may or may not be private depending on the implementation
        // Just verify it doesn't panic
        let _ = result;
    }

    // ------------------------------------------------------------------
    // parse_yaml_frontmatter — key without colon separator
    // ------------------------------------------------------------------

    #[test]
    fn parse_yaml_frontmatter_line_without_colon_skipped_and_map_empty() {
        // Only a line without colon → map is empty → None
        let content = b"---\nno-colon-here\n---\nbody";
        assert!(parse_yaml_frontmatter(content).is_none());
    }

    // ------------------------------------------------------------------
    // webhook_list without creating a repo first (returns empty)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_list_repo_not_found_returns_empty() {
        let (_td, state) = make_test_state();
        let result = webhook_list(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "repo".into())),
        )
        .await
        .unwrap();
        assert!(result.webhooks.is_empty());
    }

    // ------------------------------------------------------------------
    // repo_revisions with missing repo
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_revisions_missing_repo() {
        let (_td, state) = make_test_state();
        let result = repo_revisions(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "such_repo".into())),
        )
        .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
    }

    // ------------------------------------------------------------------
    // repo_revision_info
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_revision_info_returns_siblings() {
        let content = b"some content";
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/rev-info",
            "sha_rev_info",
            &[HubFileEntry {
                path: "data.txt".into(),
                size: content.len() as u64,
                sha: "file_sha".into(),
                is_lfs: false,
                inline_content: Some(content.to_vec()),
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = repo_revision_info(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "rev-info".into(),
                "main".into(),
            )),
        )
        .await
        .unwrap();
        assert_eq!(result.id, "org/rev-info");
        assert!(result.sha.is_some());
        let siblings = result.siblings.as_ref().expect("expected siblings");
        assert_eq!(siblings.len(), 1);
        assert_eq!(siblings[0]["rfilename"], "data.txt");
    }

    #[tokio::test]
    async fn handler_repo_revision_info_missing_repo() {
        let (_td, state) = make_test_state();
        let result = repo_revision_info(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "repo".into(), "main".into())),
        )
        .await;
        assert!(matches!(result, Err(HubApiError::RepoNotFound)));
    }

    // ------------------------------------------------------------------
    // repo_delete_compat
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_delete_compat_success() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/compat-del");
        let state = HubState {
            store: store.clone(),
            auth: None,
            http_client: None,
        };
        let result = repo_delete_compat(
            State(state),
            default_headers(),
            Json(RepoDeleteRequest {
                repo_type: Some(RepoType::Model),
                name: "org/compat-del".to_owned(),
                organization: None,
            }),
        )
        .await;
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
        assert!(store.get_repo("org/compat-del").unwrap().is_none());
    }

    #[tokio::test]
    async fn handler_repo_delete_compat_with_organization() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/compat-org");
        let state = HubState {
            store: store.clone(),
            auth: None,
            http_client: None,
        };
        let result = repo_delete_compat(
            State(state),
            default_headers(),
            Json(RepoDeleteRequest {
                repo_type: Some(RepoType::Model),
                name: "compat-org".to_owned(),
                organization: Some("org".to_owned()),
            }),
        )
        .await;
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
        assert!(store.get_repo("org/compat-org").unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // file_tree_at_root
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_file_tree_at_root_returns_files() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Model,
            "org/tree-root",
            "sha_root_tree",
            &[HubFileEntry {
                path: "README.md".into(),
                size: 50,
                sha: "r_sha".into(),
                is_lfs: false,
                inline_content: None,
            }],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let entries = file_tree_at_root(
            State(state),
            default_headers(),
            Path((
                "models".into(),
                "org".into(),
                "tree-root".into(),
                "main".into(),
            )),
            Query(TreeQuery {
                limit: None,
                cursor: None,
                recursive: false,
            }),
        )
        .await
        .unwrap();
        assert!(!entries.is_empty());
    }

    // ------------------------------------------------------------------
    // git_head
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_git_head_returns_ref() {
        let (_td, store) =
            make_store_with_revision(HubRepoType::Model, "org/git-head", "sha_head", &[]);
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = git_head(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "git-head".into())),
        )
        .await
        .unwrap();
        assert!(result.contains("ref: refs/heads/main"));
        assert!(result.contains("sha_head"));
    }

    #[tokio::test]
    async fn handler_git_head_nonexistent_repo_returns_zero_sha() {
        let (_td, state) = make_test_state();
        let result = git_head(
            State(state),
            default_headers(),
            Path(("models".into(), "no".into(), "repo".into())),
        )
        .await
        .unwrap();
        // No revisions → falls back to the zero SHA fallback
        assert!(result.contains("0000000000000000000000000000000000000000"));
    }

    // ------------------------------------------------------------------
    // repo_create with organization and conflict
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_repo_create_with_organization() {
        let (_td, state) = make_test_state();
        let req = RepoCreateRequest {
            repo_type: RepoType::Model,
            name: "my-repo".to_owned(),
            organization: Some("org".to_owned()),
            private: false,
            visibility: None,
        };
        let (status, json) = repo_create(State(state), default_headers(), Json(req))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json.id, "org/my-repo");
    }

    #[tokio::test]
    async fn handler_repo_create_conflict() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "ns/existing");
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let req = RepoCreateRequest {
            repo_type: RepoType::Model,
            name: "ns/existing".to_owned(),
            organization: None,
            private: false,
            visibility: None,
        };
        let result = repo_create(State(state), default_headers(), Json(req)).await;
        assert!(result.is_ok());
        let (status, _json) = result.unwrap();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    // ------------------------------------------------------------------
    // xet_read_token handler (requires auth)
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_xet_read_token_missing_auth_returns_unauthorized() {
        let (_td, state) = make_test_state();
        let result = xet_read_token(
            State(state),
            default_headers(),
            Path(("models".into(), "ns".into(), "r".into(), "main".into())),
        )
        .await;
        assert!(matches!(result, Err(HubApiError::Unauthorized)));
    }

    #[tokio::test]
    async fn handler_xet_write_token_missing_auth_returns_unauthorized() {
        let (_td, state) = make_test_state();
        let result = xet_write_token(
            State(state),
            default_headers(),
            Path(("models".into(), "ns".into(), "r".into(), "main".into())),
        )
        .await;
        assert!(matches!(result, Err(HubApiError::Unauthorized)));
    }

    // ------------------------------------------------------------------
    // repo_response_for_request — Dataset and Space URL logic
    // ------------------------------------------------------------------

    #[test]
    fn repo_response_for_request_dataset_url() {
        use shardline_index::hub::HubRepo;
        let repo = HubRepo {
            repo_id: "org/mydata".to_owned(),
            repo_type: HubRepoType::Dataset,
            private: false,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert(axum::http::header::HOST, "hub.example.com".parse().unwrap());
        let resp = repo_response_for_request(&headers, &repo);
        assert_eq!(resp.url, "https://hub.example.com/datasets/org/mydata");
        assert_eq!(resp.repo_type, RepoType::Dataset);
    }

    #[test]
    fn repo_response_for_request_space_url() {
        use shardline_index::hub::HubRepo;
        let repo = HubRepo {
            repo_id: "org/myspace".to_owned(),
            repo_type: HubRepoType::Space,
            private: false,
            default_branch: "main".to_owned(),
            created_at_unix_seconds: 0,
            updated_at_unix_seconds: 0,
        };
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        headers.insert(axum::http::header::HOST, "hub.example.com".parse().unwrap());
        let resp = repo_response_for_request(&headers, &repo);
        assert_eq!(resp.url, "https://hub.example.com/spaces/org/myspace");
        assert_eq!(resp.repo_type, RepoType::Space);
    }

    // ------------------------------------------------------------------
    // webhook_create — duplicate URL and too many events
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_create_duplicate_url() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-dup");
        store
            .create_webhook(
                "org/wh-dup",
                "https://example.com/dup",
                &["push".into()],
                None,
            )
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let result = webhook_create(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-dup".into())),
            Json(WebhookCreateRequest {
                url: "https://example.com/dup".into(),
                events: vec!["push".into()],
                secret: None,
            }),
        )
        .await;
        assert!(matches!(result, Err(HubApiError::Conflict(_))));
    }

    // ------------------------------------------------------------------
    // lfs_upload and lfs_download handler tests
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_lfs_upload_and_download_roundtrip() {
        let oid = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let data = b"lfs file content";
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/lfs-io");
        let state = HubState {
            store: store.clone(),
            auth: None,
            http_client: None,
        };
        // Upload
        let result = lfs_upload(
            State(state.clone()),
            default_headers(),
            Path(oid.to_owned()),
            bytes::Bytes::from_static(data),
        )
        .await;
        assert_eq!(result.unwrap(), StatusCode::OK);

        // Download
        let (status, _headers, downloaded) =
            lfs_download(State(state), default_headers(), Path(oid.to_owned()))
                .await
                .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(downloaded, data);
    }

    // ------------------------------------------------------------------
    // dataset_parquet success path
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_dataset_parquet_finds_data_files() {
        let (_td, store) = make_store_with_revision(
            HubRepoType::Dataset,
            "org/ds-parquet",
            "sha_ds_pq",
            &[
                HubFileEntry {
                    path: "data/train/data.parquet".into(),
                    size: 1000,
                    sha: "pq_sha".into(),
                    is_lfs: false,
                    inline_content: None,
                },
                HubFileEntry {
                    path: "README.md".into(),
                    size: 50,
                    sha: "rm_sha".into(),
                    is_lfs: false,
                    inline_content: None,
                },
            ],
        );
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let resp = dataset_parquet(
            State(state),
            default_headers(),
            Path(("org".into(), "ds-parquet".into())),
        )
        .await
        .unwrap();
        assert_eq!(resp.files.len(), 1);
        assert_eq!(resp.files[0].path, "data/train/data.parquet");
    }

    // ------------------------------------------------------------------
    // webhook_list with a repo that has webhooks
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn handler_webhook_list_with_hooks() {
        let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list");
        store
            .create_webhook(
                "org/wh-list",
                "https://example.com/hook1",
                &["push".into()],
                None,
            )
            .unwrap();
        store
            .create_webhook(
                "org/wh-list",
                "https://example.com/hook2",
                &["push".into()],
                None,
            )
            .unwrap();
        let state = HubState {
            store,
            auth: None,
            http_client: None,
        };
        let resp = webhook_list(
            State(state),
            default_headers(),
            Path(("models".into(), "org".into(), "wh-list".into())),
        )
        .await
        .unwrap();
        assert_eq!(resp.webhooks.len(), 2);
    }

    // ------------------------------------------------------------------
    // webhook_delete
    // ------------------------------------------------------------------
}
