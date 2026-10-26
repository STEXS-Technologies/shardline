//! SDK commit-history projection from immutable repository-scoped Hub revisions.

use std::collections::HashSet;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, header::LINK},
};

use super::{HubRepository, HubState};
use crate::error::HubApiError;

#[derive(serde::Deserialize)]
pub(crate) struct HistoryQuery {
    limit: Option<usize>,
}

pub(crate) async fn repo_commits(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((kind, ns, repo, revision_name)): Path<(String, String, String, String)>,
    Query(query): Query<HistoryQuery>,
) -> Result<(HeaderMap, Json<Vec<serde_json::Value>>), HubApiError> {
    let limit = query.limit.unwrap_or(100);
    if !(1..=100).contains(&limit) {
        return Err(HubApiError::BadRequest(
            "history limit must be between 1 and 100".to_owned(),
        ));
    }
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
    let mut visited = HashSet::new();
    let mut result = Vec::new();
    let mut next = Some(head);
    while let Some(sha) = next.take() {
        if !visited.insert(sha.clone()) {
            return Err(HubApiError::CasError(
                "cyclic repository history".to_owned(),
            ));
        }
        let revision = state
            .store
            .get_revision(&repo_id, &sha)
            .map_err(|error| HubApiError::CasError(error.to_string()))?
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
        if next.as_ref().is_some_and(|parent| visited.contains(parent)) {
            return Err(HubApiError::CasError(
                "cyclic repository history".to_owned(),
            ));
        }
        if result.len() == limit {
            break;
        }
    }
    let mut headers = HeaderMap::new();
    if let Some(parent) = next {
        // Continue from immutable ancestry, never resolve the moving branch
        // again. Every page still passes normal revision authorization.
        let public =
            crate::git::smart_http::public_commit_id(&state, &repo_id, &parent, _repo.capability())
                .await?;
        let mut url = url::Url::parse(&state.public_base_url).map_err(|_error| {
            HubApiError::CasError("invalid configured public Hub URL".to_owned())
        })?;
        url.set_query(None);
        url.set_fragment(None);
        url.path_segments_mut()
            .map_err(|()| HubApiError::CasError("invalid history pagination URL".to_owned()))?
            .pop_if_empty()
            .extend(["api", &kind, &ns, &repo, "commits", &public]);
        url.query_pairs_mut()
            .append_pair("limit", &limit.to_string());
        headers.insert(
            LINK,
            HeaderValue::from_str(&format!("<{url}>; rel=\"next\"")).map_err(|_error| {
                HubApiError::CasError("invalid history pagination URL".to_owned())
            })?,
        );
    }
    Ok((headers, Json(result)))
}
