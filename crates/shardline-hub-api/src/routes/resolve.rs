use axum::{
    extract::{Path, State},
    response::{IntoResponse, Redirect, Response},
};

use crate::{error::HubApiError, resolve};

use super::{HubRepository, HubState};

// ---- File resolve (download, requires Read) ----

pub(crate) async fn resolve_file(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((_repo_type, ns, repo_name, rev, file_path)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, ns, repo_name, rev, file_path).await
}

pub(crate) async fn resolve_model_file(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name, rev, file_path)): Path<(String, String, String, String)>,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, ns, repo_name, rev, file_path).await
}

async fn resolve_file_for_repository(
    state: HubState,
    repo: HubRepository,
    ns: String,
    repo_name: String,
    rev: String,
    file_path: String,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("resolve_file", "GET", 200);
    // The extractor has already authorized the request and minted the
    // capability; its repository scope namespaces the object-store reads.
    let name = format!("{ns}/{repo_name}");
    let commit_sha = state
        .store
        .resolve_revision(&name, &rev)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::RevisionNotFound)?;
    let result = resolve::resolve_file_from_store_scoped(
        &state,
        &commit_sha,
        &file_path,
        repo.capability(),
    )?;

    match result {
        resolve::DownloadResult::Inline { size, sha, content } => {
            let data = content.ok_or(HubApiError::NotFound)?;
            let content_length = size.to_string();
            let resp_headers = [
                ("Content-Type", "application/octet-stream"),
                ("X-Shardline-SHA", sha.as_str()),
                ("X-Repo-Commit", commit_sha.as_str()),
                ("ETag", sha.as_str()),
                ("Content-Length", content_length.as_str()),
            ];
            Ok((resp_headers, data).into_response())
        }
        resolve::DownloadResult::LfsRedirect { oid, .. } => {
            let redirect_url = format!("/lfs/objects/{oid}");
            Ok(Redirect::temporary(&redirect_url).into_response())
        }
    }
}
