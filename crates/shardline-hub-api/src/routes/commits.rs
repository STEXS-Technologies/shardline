use axum::body::{Body, to_bytes};
use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
};
use tracing;

use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

use crate::{
    commit::{self, CommitInstruction, ParsedCommit},
    error::HubApiError,
    models::*,
};
use shardline_index::hub::HubFileEntry;
use shardline_server_core::AuthorizedRepository;

use super::{HubRepository, HubState, deliver_webhook_events, lfs_object_key};

/// Commit NDJSON is control metadata; inline files larger than this should use
/// the LFS flow. Keeping the request bounded prevents a giant JSON envelope
/// from becoming a heap-sized upload.
const MAX_COMMIT_REQUEST_BYTES: usize = 16 * 1024 * 1024;

// ---- Preupload (requires Write) ----

pub(crate) async fn preupload(
    State(state): State<HubState>,
    _repo: HubRepository<true>,
    Path((_repo_type, ns, repo_name, rev)): Path<(String, String, String, String)>,
    Json(request): Json<PreuploadRequest>,
) -> Result<Json<PreuploadResponse>, HubApiError> {
    const MAX_PREUPLOAD_FILES: usize = 10_000;
    if request.files.len() > MAX_PREUPLOAD_FILES {
        return Err(HubApiError::PathValidation(format!(
            "preupload request exceeds maximum of {MAX_PREUPLOAD_FILES} files"
        )));
    }
    let name = format!("{ns}/{repo_name}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;

    let existing_files = state
        .store
        .get_files(&commit_sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;

    let existing_paths: std::collections::HashSet<&str> = existing_files
        .iter()
        .map(|file| file.path.as_str())
        .collect();

    let result: Vec<PreuploadResult> = request
        .files
        .into_iter()
        .map(|f| PreuploadResult {
            exists: existing_paths.contains(f.path.as_str()),
            path: f.path,
            upload_mode: "regular".to_owned(),
            should_ignore: false,
        })
        .collect();

    shardline_metrics::record_hub_api_request("preupload", "POST", 200);
    Ok(Json(PreuploadResponse {
        files: result.clone(),
        result,
    }))
}

// ---- Commit (requires Write) ----

pub(crate) async fn commit(
    State(state): State<HubState>,
    headers: HeaderMap,
    repo: HubRepository<true>,
    Path((_repo_type, ns, repo_name, rev)): Path<(String, String, String, String)>,
    body: Body,
) -> Result<Json<CommitResponse>, HubApiError> {
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
    let body = to_bytes(body, MAX_COMMIT_REQUEST_BYTES)
        .await
        .map_err(|error| HubApiError::PathValidation(format!("commit body too large: {error}")))?;
    let body = std::str::from_utf8(&body).map_err(|error| {
        HubApiError::PathValidation(format!("commit body is not UTF-8: {error}"))
    })?;
    let name = format!("{ns}/{repo_name}");
    let parent_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| {
            tracing::error!(error = %e, repo = %name, rev = %rev, "resolve_revision failed");
            HubApiError::CasError(e.to_string())
        })?
        .ok_or(HubApiError::RevisionNotFound)?;
    let parsed = match commit::parse_ndjson_commit(body) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, repo = %name, "parse_ndjson_commit failed");
            return Err(e);
        }
    };
    // The extractor minted a Write-scoped capability; its repository scope
    // namespaces the object-store writes so cross-tenant content stays isolated.
    match apply_commit(&state, &name, &parent_sha, &parsed, repo.capability()).await {
        Ok(response) => {
            shardline_metrics::record_hub_api_request("commit", "POST", 200);
            shardline_metrics::record_hub_api_commit("ndjson");
            Ok(response)
        }
        Err(e) => {
            tracing::error!(error = %e, repo = %name, parent = %parent_sha, "commit failed");
            shardline_metrics::record_hub_api_request("commit", "POST", 500);
            Err(e)
        }
    }
}

pub(crate) async fn apply_commit(
    state: &HubState,
    repo_id: &str,
    parent_sha: &str,
    parsed: &ParsedCommit,
    auth: &AuthorizedRepository,
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

    let existing_files: Vec<HubFileEntry> = state.store.get_files(parent_sha).map_err(|e| {
        tracing::error!(error = %e, parent_sha, "get_files failed during commit");
        HubApiError::CasError(e.to_string())
    })?;
    // Built-in metadata stores enforce unique paths. Apply instructions in
    // order without scanning the entire parent tree for every replacement or
    // deletion; canonical ordering is restored before computing identity.
    let mut files: std::collections::HashMap<String, HubFileEntry> = existing_files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect();

    for instruction in &parsed.instructions {
        match instruction {
            CommitInstruction::InlineFile { path, content } => {
                let sha = {
                    let mut h = blake3::Hasher::new();
                    h.update(content);
                    hex::encode(h.finalize().as_bytes())
                };
                let size = content.len() as u64;

                // Store in ObjectStore, namespaced by repository so the same
                // content in different repos maps to different storage objects.
                let key = lfs_object_key(&sha, auth)?;
                let body = ObjectBody::from_slice(content);
                let integrity = ObjectIntegrity::new(
                    shardline_protocol::ShardlineHash::from_bytes(
                        *blake3::hash(content).as_bytes(),
                    ),
                    size,
                );
                state
                    .object_store
                    .put_if_absent(&key, body, &integrity)
                    .map_err(|e| HubApiError::CasError(e.to_string()))?;

                files.insert(
                    path.clone(),
                    HubFileEntry {
                        path: path.clone(),
                        size,
                        sha: sha.clone(),
                        is_lfs: false,
                    },
                );
            }
            CommitInstruction::LfsPointer { path, oid, size } => {
                commit::validate_lfs_oid(oid).map_err(|e| {
                    tracing::error!(error = %e, oid, path, "invalid LFS OID");
                    HubApiError::PathValidation(format!("invalid LFS OID: {e}"))
                })?;
                files.insert(
                    path.clone(),
                    HubFileEntry {
                        path: path.clone(),
                        size: *size,
                        sha: oid.clone(),
                        is_lfs: true,
                    },
                );
            }
            CommitInstruction::Delete { path } => {
                files.remove(path);
            }
        }
    }

    let mut files: Vec<HubFileEntry> = files.into_values().collect();

    // Do not acknowledge a revision that the existing metadata read ceiling
    // would immediately make unreadable. Check the final tree so a delete/add
    // replacement at the ceiling remains valid.
    if files.len() > shardline_index::hub::HUB_TREE_READ_CEILING {
        return Err(HubApiError::PathValidation(format!(
            "Hub tree exceeds the {}-entry metadata read ceiling",
            shardline_index::hub::HUB_TREE_READ_CEILING,
        )));
    }

    // A Hub revision must also be representable as a Git tree. Check every
    // ancestor against the complete final path set: adjacent sorted paths are
    // insufficient when a sibling such as x-foo lies between x and x/a.
    let paths: std::collections::HashSet<&str> =
        files.iter().map(|file| file.path.as_str()).collect();
    for file in &files {
        for (separator, _) in file.path.match_indices('/') {
            let ancestor = file.path.get(..separator).ok_or_else(|| {
                HubApiError::PathValidation("invalid file path boundary".to_owned())
            })?;
            if paths.contains(ancestor) {
                return Err(HubApiError::PathValidation(format!(
                    "file/directory path conflict: {ancestor} is an ancestor of {}",
                    file.path,
                )));
            }
        }
    }

    // File metadata is indexed globally by commit SHA. Bind that identity to
    // the repository and the complete resulting tree, including paths and
    // deletions, rather than just the contents supplied by this request.
    // Canonical ordering and JSON field boundaries make equivalent trees
    // stable without allowing concatenation or instruction-order collisions.
    files.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    let tree: Vec<_> = files
        .iter()
        .map(|file| (&file.path, file.size, &file.sha, file.is_lfs))
        .collect();
    let identity = serde_json::to_vec(&(
        "shardline-hub-ndjson-commit-v2",
        repo_id,
        parent_sha,
        &parsed.message,
        tree,
    ))?;
    let commit_sha = blake3::hash(&identity).to_hex().to_string();

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
    state.store.store_files(&commit_sha, &files).map_err(|e| {
        tracing::error!(error = %e, commit_sha, "store_files failed");
        HubApiError::CasError(e.to_string())
    })?;
    state
        .store
        .create_revision(
            repo_id,
            Some(parent_sha),
            &commit_sha,
            "main",
            &parsed.message,
        )
        .map_err(|e| {
            tracing::error!(error = %e, commit_sha, repo_id, "create_revision failed");
            HubApiError::CasError(e.to_string())
        })?;

    // Fire webhook deliveries in the background (non-blocking).
    deliver_webhook_events(state, repo_id, "push", &commit_sha).await;

    Ok(Json(CommitResponse {
        commit_id: commit_sha.clone(),
        commit_oid: commit_sha.clone(),
        commit_url: format!("/{repo_id}/commit/{commit_sha}"),
        ref_name: Some("main".to_owned()),
    }))
}
