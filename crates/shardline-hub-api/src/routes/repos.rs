use std::str::FromStr;

use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};

use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectStore;

use crate::{
    error::HubApiError,
    models::*,
    types::{HubSortField, SortDirection},
};
use shardline_index::hub::{HubRepoSearchOptions, HubRepoSearchOrder, HubRepoType};
use shardline_protocol::TokenScope;

use super::{
    HubRepository, HubState, authorize, read_object_prefix, repo_type_path, stream_object,
};

// Include both delimiters as well as the full accepted metadata budget.
const MAX_README_FRONT_MATTER_BYTES: usize = super::yaml_validation::MAX_CARD_BYTES + 16;

// ---- Repo create (generic, requires Write) ----

pub(crate) async fn repo_create(
    State(state): State<HubState>,
    _headers: HeaderMap,
    repo: HubRepository<true>,
    Json(request): Json<RepoCreateRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), HubApiError> {
    let full_name = request.organization.as_deref().map_or_else(
        || request.name.clone(),
        |organization| format!("{organization}/{}", request.name),
    );
    // This route has no repository in its path, so bind the requested identity
    // from the body before checking whether it already exists. A repository
    // scoped write token may create/retry only that exact repository unless
    // its issuer explicitly grants creation in the same owner namespace.
    let (namespace, repository) = split_repo_name(&full_name);
    repo.require_creation_binding(namespace, repository)?;
    let repo_type: HubRepoType = request.repo_type.into();
    let result = super::repo_creation::create(
        &state,
        repo.capability(),
        repo_type,
        &full_name,
        super::repo_creation::private_visibility(request.private, request.visibility.as_deref())?,
    )?;
    shardline_metrics::record_hub_api_request("repo_create", "POST", result.0.as_u16());
    Ok(result)
}

// ---- Repo create (type-specific, requires Write) ----

pub(crate) async fn repo_create_type(
    State(state): State<HubState>,
    _headers: HeaderMap,
    repo: HubRepository<true, false>,
    Path((repo_type, _ns, _repo_name)): Path<(String, String, String)>,
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), HubApiError> {
    // The path provides the requested identity, so bind it to the exact
    // repository in the write token before reading or creating metadata.
    repo.require_creation_binding(&_ns, &_repo_name)?;
    let rt = RepoType::from_api_str(&repo_type)
        .map(Into::into)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let name = format!("{_ns}/{_repo_name}");
    if !body.is_object() {
        return Err(HubApiError::BadRequest(
            "repository creation options must be an object".to_owned(),
        ));
    }
    let options: super::repo_creation::CreationVisibility =
        serde_json::from_value(body).map_err(|_parse_error| {
            HubApiError::BadRequest("invalid repository creation options".to_owned())
        })?;
    let private =
        super::repo_creation::private_visibility(options.private, options.visibility.as_deref())?;
    let result = super::repo_creation::create(&state, repo.capability(), rt, &name, private)?;
    shardline_metrics::record_hub_api_request("repo_create_type", "POST", result.0.as_u16());
    Ok(result)
}

pub(crate) fn repo_response_for_public_base(
    public_base_url: &str,
    repo: &shardline_index::hub::HubRepo,
) -> RepoResponse {
    let mut response = repo_response_from_hub(repo);
    // The configured public origin is trusted deployment state. Host and
    // forwarding headers are request input and must not select client targets.
    response.url = format!("{}{}", public_base_url.trim_end_matches('/'), response.url);
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
/// Uses the same bounded YAML parser as card validation. Invalid metadata is
/// omitted without preventing clients from retrieving the original README.
pub(crate) fn parse_yaml_frontmatter(content: &[u8]) -> Option<serde_json::Value> {
    let text = std::str::from_utf8(content).ok()?;
    super::yaml_validation::parse_metadata(text).ok().flatten()
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
    // SECURITY NOTE: This endpoint is intentionally global — it searches across
    // all repositories on the instance, not scoped to the caller's token.
    // The HubRepository extractor provides authentication (Read scope) but the
    // repo binding is a no-op for this pathless route. Cross-tenant private repo
    // data is protected by the `repo_visible_to_owner` filter below. This matches
    // HF Hub behavior where any authenticated user can search public repos.
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
    if query.q.len() > 200 {
        return Err(HubApiError::PathValidation(
            "search query must not exceed 200 characters".to_owned(),
        ));
    }
    let limit = query.limit.min(200);
    let ascending = query
        .direction
        .as_deref()
        .and_then(|d| SortDirection::from_str(d).ok())
        == Some(SortDirection::Asc);
    let order = match query
        .sort
        .as_deref()
        .and_then(|sort| HubSortField::from_str(sort).ok())
    {
        Some(HubSortField::LastModified) if ascending => HubRepoSearchOrder::LastModifiedAsc,
        Some(HubSortField::LastModified) => HubRepoSearchOrder::LastModifiedDesc,
        _ if query.sort.is_some() && ascending => HubRepoSearchOrder::RepoIdDesc,
        _ => HubRepoSearchOrder::RepoIdAsc,
    };
    let visible = state
        .store
        .search_repos_with_options(
            Some(rt),
            &query.q,
            limit,
            &HubRepoSearchOptions {
                caller_repo_id,
                author: query.author,
                order,
            },
        )
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
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
    Json(body): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<serde_json::Value>), HubApiError> {
    authorize(&state, &headers, TokenScope::Read)?;
    let (status, errors) = match super::yaml_validation::validate(&body) {
        Ok(()) => (StatusCode::OK, Vec::new()),
        Err(message) => (StatusCode::BAD_REQUEST, vec![message]),
    };
    shardline_metrics::record_hub_api_request("validate_yaml", "POST", status.as_u16());
    Ok((
        status,
        Json(serde_json::json!({"warnings": [], "errors": errors})),
    ))
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
    crate::revision_access::authorize_read(&state, &name, "main", &commit_sha, repo.capability())
        .await?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let readme = files
        .iter()
        .find(|f| f.path == "README.md")
        .ok_or(HubApiError::NotFound)?;
    let key = crate::git::smart_http::file_key_at_revision(
        &state,
        &name,
        "main",
        &commit_sha,
        "README.md",
        repo.capability(),
    )
    .await?;
    let length = state
        .object_store
        .metadata(&key)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .ok_or(HubApiError::NotFound)?
        .length();
    if length != readme.size {
        return Err(HubApiError::CasError(
            "model card length differs from revision metadata".to_owned(),
        ));
    }
    let mut response = stream_object(&state.object_store, key, length).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "text/markdown; charset=utf-8"
            .parse()
            .map_err(|_error| HubApiError::NotFound)?,
    );
    response.headers_mut().insert(
        "x-shardline-sha",
        readme.sha.parse().map_err(|_error| HubApiError::NotFound)?,
    );
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_LENGTH, length.into());
    Ok(response)
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
    let visible =
        crate::revision_access::visible_commits(&state, &name, _repo.capability()).await?;
    let revisions = if let Some(allowed) = &visible {
        let mut revisions = Vec::with_capacity(allowed.len());
        for sha in allowed {
            if let Some(revision) = state
                .store
                .get_revision(&name, sha)
                .map_err(|error| HubApiError::CasError(error.to_string()))?
            {
                revisions.push(revision);
            }
        }
        revisions.sort_by(|left, right| {
            right
                .created_at_unix_seconds
                .cmp(&left.created_at_unix_seconds)
                .then_with(|| right.sha.cmp(&left.sha))
        });
        revisions
    } else {
        state
            .store
            .list_revisions(&name)
            .map_err(|error| HubApiError::CasError(error.to_string()))?
    };
    let response = RevisionListResponse {
        revisions: revisions
            .iter()
            .map(|r| RevisionResponse {
                // A reachable commit can originate on a different branch.
                // Expose it as this credential's history without leaking that name.
                ref_name: _repo
                    .capability()
                    .repository()
                    .and_then(|scope| scope.revision())
                    .unwrap_or(&r.ref_name)
                    .to_owned(),
                sha: r.sha.clone(),
            })
            .collect(),
    };
    Ok(Json(response))
}

// ---- Repo info (requires Read) ----

/// Hugging Face's SDK uses `/refs`, not Shardline's historical `/revisions`.
/// Read live references so deleted branches never reappear from commit history.
pub(crate) async fn repo_refs(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_repo_type, ns, repo_name)): Path<(String, String, String)>,
) -> Result<Json<serde_json::Value>, HubApiError> {
    let name = format!("{ns}/{repo_name}");
    state
        .store
        .get_repo(&name)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let refs = state
        .store
        .list_refs_bounded(&name, 10_000)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut branches = Vec::new();
    let mut tags = Vec::new();
    let mut converts = Vec::new();
    let mut pull_requests = Vec::new();
    let allowed_refs = crate::revision_access::visible_refs(&state, &name, _repo.capability())?;
    for reference in refs {
        if allowed_refs.as_ref().is_some_and(|allowed| {
            !allowed.contains(shardline_index::hub::canonical_ref_name(
                &reference.ref_name,
            ))
        }) {
            continue;
        }
        let (items, short_name, full_name) =
            if let Some(short) = reference.ref_name.strip_prefix("refs/tags/") {
                (&mut tags, short.to_owned(), reference.ref_name.clone())
            } else if let Some(short) = reference.ref_name.strip_prefix("refs/heads/") {
                (&mut branches, short.to_owned(), reference.ref_name.clone())
            } else if let Some(short) = reference.ref_name.strip_prefix("refs/pr/") {
                (
                    &mut pull_requests,
                    short.to_owned(),
                    reference.ref_name.clone(),
                )
            } else if let Some(short) = reference.ref_name.strip_prefix("refs/convert/") {
                (&mut converts, short.to_owned(), reference.ref_name.clone())
            } else if !reference.ref_name.starts_with("refs/") {
                (
                    &mut branches,
                    reference.ref_name.clone(),
                    format!("refs/heads/{}", reference.ref_name),
                )
            } else {
                continue;
            };
        items.push(serde_json::json!({
            "name": short_name,
            "ref": full_name,
            "targetCommit": crate::git::smart_http::public_commit_id(&state, &name, &reference.sha, _repo.capability()).await?,
        }));
    }
    Ok(Json(serde_json::json!({
        "branches": branches, "tags": tags, "converts": converts,
        "pullRequests": pull_requests,
    })))
}

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

    // SDK repo_info() needs the same current commit and siblings as the
    // explicit revision endpoint, rather than an apparently empty repository.
    if let Some(sha) = state
        .store
        .resolve_revision(&name, &entry.default_branch)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
    {
        crate::revision_access::authorize_read(&state, &name, "main", &sha, repo.capability())
            .await?;
        let files = state
            .store
            .get_files(&sha)
            .map_err(|e| HubApiError::CasError(e.to_string()))?;
        if files.iter().any(|file| file.path == "README.md")
            && let Some(content) =
                read_file_prefix_from_object_store(&state, &name, "main", &sha, repo.capability())
                    .await
        {
            response.card_data = parse_yaml_frontmatter(&content);
        }
        response.sha = Some(
            crate::git::smart_http::public_commit_id(&state, &name, &sha, repo.capability())
                .await?,
        );
        response.siblings = Some(
            files
                .into_iter()
                .map(|file| {
                    serde_json::json!({
                        "rfilename": file.path, "size": file.size, "blobId": file.sha,
                    })
                })
                .collect(),
        );
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
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(&state, &name, &rev, _repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(&state, &name, &rev, &commit_sha, _repo.capability())
        .await?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut response = repo_response_from_hub(&entry);
    response.sha = Some(
        crate::git::smart_http::public_commit_id(&state, &name, &commit_sha, _repo.capability())
            .await?,
    );
    if files.iter().any(|file| file.path == "README.md")
        && let Some(content) =
            read_file_prefix_from_object_store(&state, &name, &rev, &commit_sha, _repo.capability())
                .await
    {
        response.card_data = parse_yaml_frontmatter(&content);
    }
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

/// Read bounded README metadata through authorized immutable file membership,
/// independently of the credential storage namespace used by its writer.
async fn read_file_prefix_from_object_store(
    state: &HubState,
    repo: &str,
    requested: &str,
    commit: &str,
    auth: &AuthorizedRepository,
) -> Option<Vec<u8>> {
    let key = crate::git::smart_http::file_key_at_revision(
        state,
        repo,
        requested,
        commit,
        "README.md",
        auth,
    )
    .await
    .ok()?;
    let size = state.object_store.metadata(&key).ok()??.length();
    read_object_prefix(
        &state.object_store,
        &key,
        size,
        MAX_README_FRONT_MATTER_BYTES,
    )
    .ok()
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
    crate::revision_access::authorize_repository_wide(name, repo.capability())?;
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
