use std::str::FromStr;

use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};

use shardline_protocol::ByteRange;
use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectStore;

use crate::{
    error::HubApiError,
    models::*,
    types::{HubSortField, SortDirection},
};
use shardline_index::hub::HubRepoType;
use shardline_protocol::TokenScope;

use super::{HubRepository, HubState, authorize, lfs_object_key, repo_type_path};

// ---- Repo create (generic, requires Write) ----

pub(crate) async fn repo_create(
    State(state): State<HubState>,
    headers: HeaderMap,
    repo: HubRepository<true>,
    Json(request): Json<RepoCreateRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), HubApiError> {
    let full_name = request.organization.as_deref().map_or_else(
        || request.name.clone(),
        |organization| format!("{organization}/{}", request.name),
    );
    // Repository creation is deliberately global: a Write-scoped caller may
    // create a repository under any namespace. The C1 cross-tenant boundary is
    // enforced on ACCESS (read/write/delete/commit/resolve all require the
    // token's repository scope to match the URL repo); a freshly created empty
    // repository grants no access to existing tenants' content. The extractor
    // has already required Write scope (and, on this pathless route, performed
    // no repository binding).
    let repo_type: HubRepoType = request.repo_type.into();
    if let Some(existing) = state
        .store
        .get_repo(&full_name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
    {
        // `huggingface_hub` calls this endpoint before every upload with
        // `exist_ok=True`, but does not transmit that flag. It accepts the
        // established 409 conflict response when it contains the existing
        // repository URL, so the compatibility body is preserved for the
        // caller's OWN repository. For any other repository it must NEVER be
        // returned: the full RepoResponse would disclose the victim's
        // id/private/url/last_modified to a caller with no access, the same
        // cross-tenant privacy boundary enforced on repo_list/repo_search
        // (`repo_visible_to_owner`). A caller that does not own the existing
        // repository gets a minimal conflict body with no repository metadata.
        let caller_repo_id = repo.capability().claims().map(|claims| {
            let r = claims.repository();
            format!("{}/{}", r.owner(), r.name())
        });
        if caller_repo_id.as_deref() == Some(existing.repo_id.as_str()) {
            return Ok((
                StatusCode::CONFLICT,
                Json(serde_json::to_value(repo_response_for_request(
                    &headers, &existing,
                ))?),
            ));
        }
        return Ok((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "name": full_name,
                "message": "repository already exists",
            })),
        ));
    }
    let created = state
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
        Json(serde_json::to_value(repo_response_for_request(
            &headers, &created,
        ))?),
    ))
}

// ---- Repo create (type-specific, requires Write) ----

pub(crate) async fn repo_create_type(
    State(state): State<HubState>,
    headers: HeaderMap,
    _repo: HubRepository<true, false>,
    Path((repo_type, _ns, _repo_name)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<RepoResponse>), HubApiError> {
    // Repository creation is deliberately global (same rationale as repo_create),
    // so the extractor performs the Write-scope check but skips the
    // token→repository binding.
    let rt = RepoType::from_api_str(&repo_type)
        .map(Into::into)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let name = format!("{_ns}/{_repo_name}");
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
        .filter(|value| !value.is_empty() && !value.contains('/') && !value.contains('\n'))
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
        repo_type: repo.repo_type.into(),
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
                .and_then(|s| s.strip_suffix('"'))
                .or_else(|| value.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')))
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
    repo: HubRepository,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_list", "GET", 200);
    // Intentionally global: lists repositories across the whole instance, not a
    // single repo, so there is no repository binding to enforce. The caller's
    // identity comes from the capability's claims.
    let caller_repo_id = repo.capability().claims().map(|claims| {
        let r = claims.repository();
        format!("{}/{}", r.owner(), r.name())
    });
    let repos = state
        .store
        .list_repos()
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    // Cross-tenant privacy: a caller must never see another tenant's private
    // repositories, even though the list endpoint is global. Public repos and
    // the caller's own private repos (exact `owner/name` match) remain visible.
    let visible: Vec<_> = repos
        .into_iter()
        .filter(|r| repo_visible_to_owner(r, caller_repo_id.as_deref()))
        .collect();
    let response = RepoListResponse {
        repos: visible.iter().map(repo_response_from_hub).collect(),
    };
    Ok(Json(response))
}

// ---- Repo search (requires Read) ----

pub(crate) async fn repo_search(
    State(state): State<HubState>,
    repo: HubRepository,
    Path(repo_type): Path<String>,
    Query(query): Query<RepoSearchQuery>,
) -> Result<Json<RepoListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_search", "GET", 200);
    // Intentionally global: searches across all repositories, not a single repo,
    // so there is no repository binding to enforce. The caller's identity comes
    // from the capability's claims.
    let caller_repo_id = repo.capability().claims().map(|claims| {
        let r = claims.repository();
        format!("{}/{}", r.owner(), r.name())
    });
    let rt = RepoType::from_api_str(&repo_type)
        .map(Into::into)
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
        // Unknown sort fields parse to `None` and fall through to the default
        // (unsorted) order, preserving the previous `_ => {}` behavior.
        match HubSortField::from_str(sort).ok() {
            Some(HubSortField::LastModified) => {
                repos.sort_by_key(|b| std::cmp::Reverse(b.updated_at_unix_seconds));
            }
            Some(HubSortField::Likes) => {
                // No likes field on HubRepo yet; keep default order.
            }
            Some(HubSortField::Downloads) => {
                // No downloads field on HubRepo yet; keep default order.
            }
            None => {
                // Unknown sort field; keep default order.
            }
        }
        if query
            .direction
            .as_deref()
            .and_then(|d| SortDirection::from_str(d).ok())
            == Some(SortDirection::Asc)
        {
            repos.reverse();
        }
    }

    // Cross-tenant privacy: hide other tenants' private repositories from the
    // search results, mirroring `repo_list`.
    let visible: Vec<_> = repos
        .into_iter()
        .filter(|r| repo_visible_to_owner(r, caller_repo_id.as_deref()))
        .collect();
    let response = RepoListResponse {
        repos: visible.iter().map(repo_response_from_hub).collect(),
    };
    Ok(Json(response))
}

/// Returns whether a repository is visible to a caller identified by the full
/// `owner/name` repository identity from their scoped token.
///
/// In permissive mode (`caller_repo_id` is `None`) every repository is visible,
/// matching the historical behavior when no auth is configured. When the caller
/// has an authenticated identity, a private repository is hidden unless it is
/// the caller's *own* repository (an exact `owner/name` match of `repo_id`);
/// public repositories remain visible.
///
/// The comparison uses the full repository identity rather than only the owner
/// namespace to prevent same-namespace private-repo leaks: under OIDC every
/// subject is scoped to a single owner, and a Local token scoped to one repo in
/// a namespace must not reveal other private repos in that same namespace.
fn repo_visible_to_owner(
    repo: &shardline_index::hub::HubRepo,
    caller_repo_id: Option<&str>,
) -> bool {
    let Some(caller_repo_id) = caller_repo_id else {
        return true;
    };
    if !repo.private {
        return true;
    }
    repo.repo_id == caller_repo_id
}

// ---- Validate YAML (requires Read) ----

pub(crate) async fn validate_yaml(
    State(state): State<HubState>,
    headers: HeaderMap,
    Json(_body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, HubApiError> {
    shardline_metrics::record_hub_api_request("validate_yaml", "POST", 200);
    authorize(&state, &headers, TokenScope::Read)?;

    // NOTE: This is an intentionally unimplemented stub for HuggingFace Hub API
    // compatibility. Clients should perform their own YAML validation. This endpoint
    // returns empty warnings/errors to indicate "validation passed" without actually
    // validating the content.
    let response = serde_json::json!({
        "warnings": [],
        "errors": []
    });
    Ok(Json(response))
}

// ---- Repo modelcard (requires Read) ----

pub(crate) async fn repo_modelcard(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((_repo_type, ns, repo_name)): Path<(String, String, String)>,
) -> Result<axum::response::Response, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_modelcard", "GET", 200);
    let name = format!("{ns}/{repo_name}");
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
    let content = read_file_from_object_store(&state, &readme.sha, repo.capability())
        .ok_or(HubApiError::NotFound)?;
    Ok((resp_headers, content).into_response())
}

// ---- Repo revisions (requires Read) ----

pub(crate) async fn repo_revisions(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_repo_type, ns, repo_name)): Path<(String, String, String)>,
) -> Result<Json<RevisionListResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_revisions", "GET", 200);
    let name = format!("{ns}/{repo_name}");
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
    repo: HubRepository,
    Path((repo_type, ns, repo_name)): Path<(String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_info", "GET", 200);
    RepoType::from_api_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let name = format!("{ns}/{repo_name}");
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
        && let Some(content) = read_file_from_object_store(&state, &readme.sha, repo.capability())
    {
        response.card_data = parse_yaml_frontmatter(&content);
    }

    Ok(Json(response))
}

pub(crate) async fn repo_revision_info(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_repo_type, ns, repo_name, rev)): Path<(String, String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_revision_info", "GET", 200);
    let name = format!("{ns}/{repo_name}");
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
    repo: HubRepository<true>,
    Path((_repo_type, ns, repo_name)): Path<(String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    let name = format!("{ns}/{repo_name}");
    delete_repository(&state, &repo, &name)
}

/// Compatibility endpoint used by `HfApi.delete_repo` and `hf repo delete`.
pub(crate) async fn repo_delete_compat(
    State(state): State<HubState>,
    repo: HubRepository<true>,
    Json(request): Json<RepoDeleteRequest>,
) -> Result<StatusCode, HubApiError> {
    let _repo_type = request.repo_type;
    let name = match request.organization {
        Some(organization) => format!("{organization}/{}", request.name),
        None => request.name,
    };
    delete_repository(&state, &repo, &name)
}

/// Reads a file from the ObjectStore by its SHA, using the repository-scoped
/// LFS key so reads find the namespaced writes made by `apply_commit`.
fn read_file_from_object_store(
    state: &HubState,
    sha: &str,
    auth: &AuthorizedRepository,
) -> Option<Vec<u8>> {
    let key = lfs_object_key(sha, auth).ok()?;
    let size = state.object_store.metadata(&key).ok()??.length();
    let range_end = size.checked_sub(1)?;
    let range = ByteRange::new(0, range_end).ok()?;
    state.object_store.read_range(&key, range).ok()
}

/// Splits a `owner/name` repository identifier into `(owner, name)`. When no
/// separator is present, the owner is treated as empty (which can never match a
/// scoped token's non-empty owner, so repo-scoped binding will deny it).
fn split_repo_name(full_name: &str) -> (&str, &str) {
    match full_name.split_once('/') {
        Some((owner, name)) => (owner, name),
        None => ("", full_name),
    }
}

fn delete_repository(
    state: &HubState,
    repo: &HubRepository<true>,
    name: &str,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_delete", "DELETE", 204);
    let (owner, repo_name) = split_repo_name(name);
    // The extractor has already required Write scope and bound the URL
    // `(ns, repo)`; the compat endpoint resolves its repository identity from
    // the request body, so re-check the binding against that identity too.
    repo.require_binding(owner, repo_name)?;
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
