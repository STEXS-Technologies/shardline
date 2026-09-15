use axum::{
    extract::{Path, State},
    response::{IntoResponse, Redirect, Response},
};

use crate::{error::HubApiError, resolve};
use shardline_storage::ObjectStore;

use super::{HubRepository, HubState, lfs_object_key, stream_object};

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
        resolve::DownloadResult::Inline { size, sha, .. } => {
            let key = lfs_object_key(&sha, repo.capability())
                .map_err(|error| HubApiError::PathValidation(error.to_string()))?;
            let actual_size = state
                .object_store
                .metadata(&key)
                .map_err(|error| HubApiError::CasError(error.to_string()))?
                .ok_or(HubApiError::NotFound)?
                .length();
            if actual_size != size {
                return Err(HubApiError::CasError(
                    "stored file length did not match revision metadata".to_owned(),
                ));
            }
            let content_length = size.to_string();
            let mut response = stream_object(&state.object_store, key, size).into_response();
            for (name, value) in [
                ("content-type", "application/octet-stream"),
                ("x-shardline-sha", sha.as_str()),
                ("x-repo-commit", commit_sha.as_str()),
                ("etag", sha.as_str()),
                ("content-length", content_length.as_str()),
            ] {
                response
                    .headers_mut()
                    .insert(name, value.parse().map_err(|_| HubApiError::NotFound)?);
            }
            Ok(response)
        }
        resolve::DownloadResult::LfsRedirect { oid, .. } => {
            let redirect_url = format!("/lfs/objects/{oid}");
            Ok(Redirect::temporary(&redirect_url).into_response())
        }
    }
}
