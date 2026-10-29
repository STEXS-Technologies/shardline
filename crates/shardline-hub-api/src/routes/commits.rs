use axum::body::Body;
use axum::http::HeaderMap;
use axum::{
    Json,
    extract::{Path, State},
};
use tracing;

use crate::{commit::ParsedCommit, error::HubApiError, models::*};
use shardline_index::hub::{HubFileEntry, HubRefUpdateOutcome, canonical_ref_name};
use shardline_server_core::AuthorizedRepository;

use super::{HubRepository, HubState, deliver_webhook_events, lfs_object_key};

mod publication;
mod stream;

/// Keep inline commits comfortably below the bounded NDJSON request limit.
/// Larger Hub files use the existing LFS object flow.
const MAX_INLINE_HUB_FILE_BYTES: u64 = 8 * 1024 * 1024;

// ---- Preupload (requires Write) ----

pub(crate) async fn preupload(
    State(state): State<HubState>,
    repo: HubRepository<true>,
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
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(&state, &name, &rev, repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(&state, &name, &rev, &commit_sha, repo.capability())
        .await?;

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
            upload_mode: if f.lfs || f.size > MAX_INLINE_HUB_FILE_BYTES {
                "lfs".to_owned()
            } else {
                "regular".to_owned()
            },
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
    let name = format!("{ns}/{repo_name}");
    let parent_sha =
        crate::git::smart_http::resolve_commit_id(&state, &name, &rev, repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    // The extractor minted a Write-scoped capability; its repository scope
    // namespaces the object-store writes so cross-tenant content stays isolated.
    // Retain the legacy explicit-parent commit URL as a conditional main
    // update. Named revisions must publish to that branch, never to main.
    let explicit_commit = rev == parent_sha
        || (rev.len() == 40
            && rev.bytes().all(|byte| byte.is_ascii_hexdigit())
            && rev
                == crate::git::smart_http::public_commit_id(
                    &state,
                    &name,
                    &parent_sha,
                    repo.capability(),
                )
                .await?);
    let target_ref = if explicit_commit || rev == "HEAD" {
        "main"
    } else {
        canonical_ref_name(&rev)
    };
    if target_ref.starts_with("refs/")
        || !crate::git::smart_http::ref_advertisement::is_valid_refname(&format!(
            "refs/heads/{target_ref}"
        ))
    {
        return Err(HubApiError::BadRequest(
            "commits require a branch reference".to_owned(),
        ));
    }
    crate::revision_access::authorize_write(&name, target_ref, repo.capability())?;
    // Resolving a readable revision can find a tag. Require an actual branch
    // before consuming uploads; retrying publication must not turn tag writes
    // into missing-branch errors or accidentally create a branch from a tag.
    if !explicit_commit
        && state
            .store
            .resolve_revision(&name, &format!("refs/heads/{target_ref}"))
            .map_err(|error| HubApiError::CasError(error.to_string()))?
            .is_none()
    {
        return Err(HubApiError::BadRequest(
            "commits require a branch reference".to_owned(),
        ));
    }
    let (parsed, instructions) =
        match stream::parse(body, state.clone(), repo.capability().clone()).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, repo = %name, "parse_ndjson_commit failed");
                return Err(e);
            }
        };
    match publication::publish(
        &state,
        &name,
        publication::CommitTarget {
            parent_sha: &parent_sha,
            ref_name: target_ref,
            retry_if_moved: parsed.parent_commit.is_none() && !explicit_commit,
        },
        &parsed,
        &instructions,
        repo.capability(),
    )
    .await
    {
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

#[cfg(test)]
pub(crate) async fn apply_commit(
    state: &HubState,
    repo_id: &str,
    parent_sha: &str,
    parsed: &ParsedCommit,
    auth: &AuthorizedRepository,
) -> Result<Json<CommitResponse>, HubApiError> {
    try_apply_commit_to_ref(
        state,
        repo_id,
        parent_sha,
        "main",
        parsed,
        parsed
            .instructions
            .iter()
            .map(|instruction| stream::prepare(state, auth, instruction)),
        auth,
    )
    .await?
    .ok_or_else(branch_changed)
}

fn branch_changed() -> HubApiError {
    HubApiError::Conflict("branch moved or was deleted".to_owned())
}

async fn try_apply_commit_to_ref(
    state: &HubState,
    repo_id: &str,
    parent_sha: &str,
    target_ref: &str,
    parsed: &ParsedCommit,
    instructions: impl Iterator<Item = Result<stream::PreparedInstruction, HubApiError>>,
    auth: &AuthorizedRepository,
) -> Result<Option<Json<CommitResponse>>, HubApiError> {
    // HUB-004: Validate that the NDJSON body's parentCommit (if present) matches
    // the URL path's parent_sha. A mismatch indicates a stale or conflicting request.
    if let Some(ref body_parent) = parsed.parent_commit {
        let resolved_parent =
            crate::git::smart_http::resolve_commit_id(state, repo_id, body_parent, auth)?;
        if resolved_parent.as_deref() != Some(parent_sha) {
            return Err(HubApiError::Conflict(format!(
                "parentCommit mismatch: body specified {body_parent} but URL resolved to {parent_sha}"
            )));
        }
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

    for instruction in instructions {
        match instruction? {
            stream::PreparedInstruction::File(file) => {
                files.insert(file.path.clone(), file);
            }
            stream::PreparedInstruction::Delete(path) => {
                if path.ends_with('/') {
                    files.retain(|candidate, _| !candidate.starts_with(&path));
                } else {
                    files.remove(&path);
                }
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
        crate::git::smart_http::validate_projected_path(&file.path)?;
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

    // HUB-008: Immutable payloads may remain unpublished after failed requests.
    // Prepare Git evidence before storing the final file-entry rows, then
    // publish the ref last. A rejected projection never writes those rows.
    let created_at_unix_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .as_secs();
    let prepared = crate::git::smart_http::prepare_publication(
        state,
        shardline_index::hub::HubRevision {
            repo_id: repo_id.to_owned(),
            ref_name: target_ref.to_owned(),
            sha: commit_sha.clone(),
            parent_sha: Some(parent_sha.to_owned()),
            message: Some(parsed.message.clone()),
            created_at_unix_seconds,
        },
        files,
        auth,
    )
    .await?;
    // Every payload read, Git limit check and archive write has succeeded.
    // Only the existing metadata/evidence transaction may now advance the ref.
    match state
        .store
        .publish_prepared_revision(&prepared.revision)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
    {
        HubRefUpdateOutcome::Updated(_) => {}
        HubRefUpdateOutcome::Conflict => {
            return Ok(None);
        }
        HubRefUpdateOutcome::Unsupported => {
            return Err(HubApiError::CasError(
                "store does not support prepared atomic branch publication".to_owned(),
            ));
        }
    }

    // Fire webhook deliveries in the background (non-blocking).
    deliver_webhook_events(state, repo_id, "push", &commit_sha).await;

    let public_sha = prepared.public_id;
    Ok(Some(Json(CommitResponse {
        commit_id: commit_sha.clone(),
        commit_oid: public_sha.clone(),
        commit_url: format!("/{repo_id}/commit/{public_sha}"),
        ref_name: Some(target_ref.to_owned()),
    })))
}
