use axum::{
    Json, Router,
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Redirect, Response},
    routing::{get, post, put},
};

use crate::auth::HubAuth;
use crate::commit::{self, CommitInstruction, ParsedCommit};
use crate::error::HubApiError;
use crate::git;
use crate::models::*;
use crate::resolve;
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepoType};
use shardline_protocol::TokenScope;

/// Shared Hub API state.
#[derive(Clone)]
pub struct HubState {
    pub store: BoxedHubStore,
    pub auth: Option<HubAuth>,
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
#[must_use]
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
        .route("/api/{type}/{ns}/{repo}", post(repo_create_type))
        .route("/api/{type}/{ns}/{repo}", get(repo_info))
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
fn repo_type_path(rt: HubRepoType) -> &'static str {
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
    let name = if let Some(auth) = &state.auth {
        match auth.authorize(&headers, TokenScope::Read) {
            Ok(ctx) => ctx.subject().to_owned(),
            Err(_) => "anonymous".to_owned(),
        }
    } else {
        "anonymous".to_owned()
    };
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
    authorize(state, &headers, TokenScope::Read)?;
    Ok(Json(TokenExchangeResponse {
        token: "hub-api-placeholder-token".to_owned(),
    }))
}

async fn xet_write_token(
    headers: axum::http::HeaderMap,
    Path((_repo_type, _ns, _repo, _rev)): Path<(String, String, String, String)>,
) -> Result<Json<TokenExchangeResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("xet_write_token", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    Ok(Json(TokenExchangeResponse {
        token: "hub-api-placeholder-token".to_owned(),
    }))
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
    let rt = HubRepoType::from_str(&repo_type).ok_or_else(|| {
        HubApiError::PathValidation(format!("invalid repo type: {repo_type}"))
    })?;
    let name = format!("{ns}/{repo}");
    let private = body
        .get("private")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let repo = state
        .store
        .create_repo(rt, &name, private)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((StatusCode::CREATED, Json(repo_response_from_hub(&repo))))
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

// ---- Repo info (requires Read) ----

async fn repo_info(
    headers: axum::http::HeaderMap,
    Path((repo_type, ns, repo)): Path<(String, String, String)>,
) -> Result<Json<RepoResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("repo_info", "GET", 200);
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Read)?;
    let _rt = HubRepoType::from_str(&repo_type).ok_or_else(|| {
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
    let state = crate::state::get();
    authorize(state, &headers, TokenScope::Write)?;
    let name = format!("{ns}/{repo}");
    let _commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;

    let result: Vec<PreuploadResult> = request
        .files
        .into_iter()
        .map(|f| PreuploadResult {
            path: f.path,
            exists: false,
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
        resolve::DownloadResult::Inline { size, sha } => {
            let headers = [
                ("Content-Type", "application/octet-stream"),
                ("X-Shardline-SHA", sha.as_str()),
            ];
            Ok((headers, format!("inline file {size} bytes")).into_response())
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
        .map_err(|_| HubApiError::RepoNotFound)?;

    // Find HEAD revision — prefer explicit HEAD, then empty, then fall back to latest.
    let head_sha = revisions
        .iter()
        .find(|r| r.ref_name == "HEAD" || r.ref_name.is_empty())
        .or_else(|| revisions.first())
        .map(|r| r.sha.as_str())
        .unwrap_or("0000000000000000000000000000000000000000");

    Ok(format!("ref: refs/heads/main\n{head_sha} refs/heads/main\n"))
}
