//! SDK commit-history projection from immutable repository-scoped Hub revisions.

use std::collections::{HashMap, HashSet};

use axum::{
    Json,
    extract::{Path, State},
};

use super::{HubRepository, HubState};
use crate::error::HubApiError;

const MAX_HISTORY_REVISIONS: usize = 10_000;

pub(crate) async fn repo_commits(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((_kind, ns, repo, revision_name)): Path<(String, String, String, String)>,
) -> Result<Json<Vec<serde_json::Value>>, HubApiError> {
    let repo_id = format!("{ns}/{repo}");
    state
        .store
        .get_repo(&repo_id)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    let head = crate::git::smart_http::resolve_commit_id(
        &state,
        &repo_id,
        &revision_name,
        _repo.capability(),
    )?
    .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(
        &state,
        &repo_id,
        &revision_name,
        &head,
        _repo.capability(),
    )
    .await?;
    let snapshot = crate::revision_access::snapshot_commit(&state, &repo_id, _repo.capability())?;
    if snapshot.as_deref().is_some_and(|allowed| allowed != head) {
        return Err(HubApiError::Forbidden);
    }
    let revisions = if snapshot.is_some() {
        vec![
            state
                .store
                .get_revision(&repo_id, &head)
                .map_err(|e| HubApiError::CasError(e.to_string()))?
                .ok_or(HubApiError::RevisionNotFound)?,
        ]
    } else {
        state
            .store
            .list_revisions_bounded(&repo_id, MAX_HISTORY_REVISIONS)
            .map_err(|e| HubApiError::CasError(e.to_string()))?
    };
    let mut by_sha: HashMap<_, _> = revisions
        .into_iter()
        .map(|revision| (revision.sha.clone(), revision))
        .collect();
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    let mut next = Some(head);
    while let Some(sha) = next {
        if !visited.insert(sha.clone()) {
            return Err(HubApiError::CasError(
                "cyclic repository history".to_owned(),
            ));
        }
        let revision = by_sha
            .remove(&sha)
            .ok_or_else(|| HubApiError::CasError("incomplete repository history".to_owned()))?;
        let seconds = i64::try_from(revision.created_at_unix_seconds)
            .map_err(|e| HubApiError::CasError(e.to_string()))?;
        let date = chrono::DateTime::from_timestamp(seconds, 0)
            .ok_or_else(|| HubApiError::CasError("invalid commit timestamp".to_owned()))?
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string();
        let message = revision.message.as_deref().unwrap_or_default();
        let (title, description) = message.split_once('\n').unwrap_or((message, ""));
        result.push(serde_json::json!({
            "id": crate::git::smart_http::public_commit_id(&state, &repo_id, &sha, _repo.capability()).await?,
            // Legacy Hub metadata has no author field. Preserve unknown
            // authorship rather than attribute old commits to the requester.
            "authors": [],
            "date": date,
            "title": title,
            "message": description,
        }));
        next = if snapshot.is_some() {
            None
        } else {
            revision.parent_sha
        };
    }
    Ok(Json(result))
}
