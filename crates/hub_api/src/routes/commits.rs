use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
};

use crate::{commit::{self, CommitInstruction, ParsedCommit}, error::HubApiError, models::*};
use shardline_index::hub::HubFileEntry;
use shardline_protocol::TokenScope;

use super::{HubState, authorize, deliver_webhook_events};

// ---- Preupload (requires Write) ----

pub(crate) async fn preupload(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn commit(
    State(state): State<HubState>,
    headers: HeaderMap,
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

pub(crate) async fn apply_commit(
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
