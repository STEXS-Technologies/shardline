//! Hugging Face SDK branch and tag operations over authoritative Hub references.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::Deserialize;
use shardline_index::hub::HubRefCreateOutcome;

use super::{HubRepository, HubState};
use crate::error::HubApiError;

#[derive(Deserialize)]
pub(crate) struct BranchRequest {
    #[serde(rename = "startingPoint")]
    starting_point: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TagRequest {
    tag: String,
    message: Option<String>,
}

fn checked_ref(name: &str, tag: bool) -> Result<String, HubApiError> {
    let namespace = if tag { "tags" } else { "heads" };
    let full = format!("refs/{namespace}/{name}");
    if name.is_empty()
        || (!tag && (name == "HEAD" || name.starts_with("refs/")))
        || full.len() > 1024
        || !crate::git::smart_http::ref_advertisement::is_valid_refname(&full)
    {
        return Err(HubApiError::BadRequest("invalid reference name".to_owned()));
    }
    Ok(if tag { full } else { name.to_owned() })
}

async fn create_ref(
    state: &HubState,
    auth: &shardline_server_core::AuthorizedRepository,
    repo_id: &str,
    name: &str,
    target: &str,
    message: &str,
) -> Result<StatusCode, HubApiError> {
    crate::revision_access::authorize_write(repo_id, name, auth)?;
    state
        .store
        .get_repo(repo_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let sha = crate::git::smart_http::resolve_commit_id(state, repo_id, target, auth)?
        .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(state, repo_id, target, &sha, auth).await?;
    // Rebind an existing immutable revision. Never manufacture a new commit or
    // replace an existing ref after a read-before-write race.
    match state
        .store
        .create_revision_if_absent(repo_id, Some(&sha), &sha, name, message)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
    {
        HubRefCreateOutcome::Created(_) => Ok(StatusCode::CREATED),
        HubRefCreateOutcome::AlreadyExists => {
            Err(HubApiError::Conflict("reference already exists".to_owned()))
        }
        HubRefCreateOutcome::Unsupported => Err(HubApiError::CasError(
            "store does not support atomic reference creation".to_owned(),
        )),
    }
}

fn remove_ref(state: &HubState, repo_id: &str, name: &str) -> Result<StatusCode, HubApiError> {
    state
        .store
        .get_repo(repo_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if name == "main" || name == "HEAD" {
        return Err(HubApiError::Forbidden);
    }
    // Resolving a revision can also resolve an immutable commit ID. Deletion
    // must match a live ref, then use the store's compare-and-delete operation.
    let refs = state
        .store
        .list_refs_bounded(repo_id, 10_000)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let current = refs
        .iter()
        .find(|reference| reference.ref_name == name)
        .ok_or(HubApiError::RevisionNotFound)?;
    state
        .store
        .delete_ref(repo_id, name, &current.sha)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

pub(crate) async fn branch_create(
    State(state): State<HubState>,
    _repo: HubRepository<true>,
    Path((_kind, ns, repo, name)): Path<(String, String, String, String)>,
    Json(request): Json<BranchRequest>,
) -> Result<StatusCode, HubApiError> {
    let name = checked_ref(&name, false)?;
    create_ref(
        &state,
        _repo.capability(),
        &format!("{ns}/{repo}"),
        &name,
        request.starting_point.as_deref().unwrap_or("main"),
        "Create branch",
    )
    .await
}

pub(crate) async fn branch_delete(
    State(state): State<HubState>,
    _repo: HubRepository<true>,
    Path((_kind, ns, repo, name)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    let name = checked_ref(&name, false)?;
    crate::revision_access::authorize_write(&format!("{ns}/{repo}"), &name, _repo.capability())?;
    remove_ref(&state, &format!("{ns}/{repo}"), &name)
}

pub(crate) async fn tag_create(
    State(state): State<HubState>,
    _repo: HubRepository<true>,
    Path((_kind, ns, repo, revision)): Path<(String, String, String, String)>,
    Json(request): Json<TagRequest>,
) -> Result<StatusCode, HubApiError> {
    let name = checked_ref(&request.tag, true)?;
    create_ref(
        &state,
        _repo.capability(),
        &format!("{ns}/{repo}"),
        &name,
        &revision,
        request.message.as_deref().unwrap_or("Create tag"),
    )
    .await
}

pub(crate) async fn tag_delete(
    State(state): State<HubState>,
    _repo: HubRepository<true>,
    Path((_kind, ns, repo, name)): Path<(String, String, String, String)>,
) -> Result<StatusCode, HubApiError> {
    let name = checked_ref(&name, true)?;
    crate::revision_access::authorize_write(&format!("{ns}/{repo}"), &name, _repo.capability())?;
    remove_ref(&state, &format!("{ns}/{repo}"), &name)
}
