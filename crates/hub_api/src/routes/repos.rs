use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};

use crate::error::HubApiError;
use crate::models::*;
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;

use super::{HubState, authorize, repo_type_path};

// ---- Repo create (generic, requires Write) ----

pub(crate) async fn repo_create(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn repo_create_type(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) fn repo_response_for_request(
    headers: &HeaderMap,
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

pub(crate) fn repo_response_from_hub(repo: &shardline_index::hub::HubRepo) -> RepoResponse {
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
pub(crate) fn parse_yaml_frontmatter(content: &[u8]) -> Option<serde_json::Value> {
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

pub(crate) async fn repo_list(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn repo_search(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn repo_modelcard(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<axum::response::Response, HubApiError> {
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

pub(crate) async fn repo_revisions(
    State(state): State<HubState>,
    headers: HeaderMap,
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

// ---- Repo info (requires Read) ----

pub(crate) async fn repo_info(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn repo_revision_info(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn repo_delete(
    State(state): State<HubState>,
    headers: HeaderMap,
    Path((_repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    let name = format!("{ns}/{repo}");
    delete_repository(&state, &headers, &name)
}

/// Compatibility endpoint used by `HfApi.delete_repo` and `hf repo delete`.
pub(crate) async fn repo_delete_compat(
    State(state): State<HubState>,
    headers: HeaderMap,
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
    headers: &HeaderMap,
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
