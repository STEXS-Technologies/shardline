use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Redirect, Response},
};

use crate::{error::HubApiError, resolve};
use shardline_storage::ObjectStore;

use super::{HubRepository, HubState, stream_object};

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
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, (ns, repo_name, rev, file_path), headers, false).await
}

pub(crate) async fn resolve_file_head(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((_repo_type, ns, repo_name, rev, file_path)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, (ns, repo_name, rev, file_path), headers, true).await
}

pub(crate) async fn resolve_model_file(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name, rev, file_path)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, (ns, repo_name, rev, file_path), headers, false).await
}

pub(crate) async fn resolve_model_file_head(
    State(state): State<HubState>,
    repo: HubRepository,
    Path((ns, repo_name, rev, file_path)): Path<(String, String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, HubApiError> {
    resolve_file_for_repository(state, repo, (ns, repo_name, rev, file_path), headers, true).await
}

async fn resolve_file_for_repository(
    state: HubState,
    repo: HubRepository,
    (ns, repo_name, rev, file_path): (String, String, String, String),
    headers: HeaderMap,
    head_only: bool,
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("resolve_file", "GET", 200);
    // The extractor binds repository identity. Authorize the requested
    // revision before exposing file metadata or issuing a download URL.
    let name = format!("{ns}/{repo_name}");
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(&state, &name, &rev, repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(&state, &name, &rev, &commit_sha, repo.capability())
        .await?;
    let result = resolve::resolve_file_from_store_scoped(
        &state,
        &commit_sha,
        &file_path,
        repo.capability(),
    )?;
    let internal_commit = commit_sha.clone();
    let commit_sha =
        crate::git::smart_http::public_commit_id(&state, &name, &commit_sha, repo.capability())
            .await?;

    match result {
        resolve::DownloadResult::Inline { size, sha, .. } => {
            let key = crate::git::smart_http::file_key_at_revision(
                &state,
                &name,
                &rev,
                &internal_commit,
                &file_path,
                repo.capability(),
            )
            .await?;
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
            let etag = format!("\"{sha}\"");
            if let Some(mut response) =
                super::download_conditions::precondition_response(&headers, &etag)?
            {
                response.headers_mut().insert(
                    "x-repo-commit",
                    commit_sha.parse().map_err(|_error| HubApiError::NotFound)?,
                );
                return Ok(response);
            }
            let selection = if head_only {
                super::lfs::LfsRangeSelection::Full
            } else {
                super::lfs::parse_lfs_range(&headers, size, &etag)
            };
            let mut response = if head_only {
                Response::new(Body::empty())
            } else {
                match &selection {
                    super::lfs::LfsRangeSelection::Full => {
                        stream_object(&state.object_store, key, size).into_response()
                    }
                    super::lfs::LfsRangeSelection::Partial(range) => {
                        let range_length = range
                            .end_inclusive()
                            .checked_sub(range.start())
                            .and_then(|distance| distance.checked_add(1))
                            .ok_or_else(|| {
                                HubApiError::CasError("invalid file range".to_owned())
                            })?;
                        super::object_io::stream_object_range(
                            &state.object_store,
                            key,
                            size,
                            range.start(),
                            range_length,
                        )
                        .map_err(|error| HubApiError::CasError(error.to_string()))?
                        .into_response()
                    }
                    super::lfs::LfsRangeSelection::Unsatisfiable => Response::new(Body::empty()),
                }
            };
            set_file_metadata_headers(&mut response, &etag, &commit_sha, size)?;
            response.headers_mut().insert(
                "x-shardline-sha",
                sha.parse().map_err(|_error| HubApiError::NotFound)?,
            );
            apply_range_headers(&mut response, &selection, size)?;
            if head_only {
                *response.body_mut() = Body::empty();
            }
            Ok(response)
        }
        resolve::DownloadResult::LfsRedirect { oid, size } => {
            if let Some(mut response) =
                super::download_conditions::precondition_response(&headers, &format!("\"{oid}\""))?
            {
                response.headers_mut().insert(
                    "x-repo-commit",
                    commit_sha.parse().map_err(|_error| HubApiError::NotFound)?,
                );
                return Ok(response);
            }
            let xet_hash = crate::git::smart_http::xet_file_hash(
                state.clone(),
                repo.capability().clone(),
                oid.clone(),
                size,
            )
            .await?;
            let xet_info = xet_hash.is_some() && accepts_xet_fileinfo(&headers);
            let mut response = if xet_info {
                let mut response = Response::new(if head_only {
                    Body::empty()
                } else {
                    Body::from("{}")
                });
                response.headers_mut().insert(
                    "content-type",
                    HeaderValue::from_static("application/vnd.xet-fileinfo+json"),
                );
                response
                    .headers_mut()
                    .insert("content-length", HeaderValue::from_static("2"));
                response.headers_mut().insert(
                    "cache-control",
                    HeaderValue::from_static("private, no-store"),
                );
                response
            } else if head_only {
                let mut response = Response::new(Body::empty());
                *response.status_mut() = StatusCode::OK;
                set_file_metadata_headers(&mut response, &format!("\"{oid}\""), &commit_sha, size)?;
                response
            } else {
                let redirect_url = format!(
                    "{}/lfs/objects/{oid}",
                    state.public_base_url.trim_end_matches('/')
                );
                Redirect::temporary(&redirect_url).into_response()
            };
            response
                .headers_mut()
                .insert("vary", HeaderValue::from_static("Accept"));
            let linked_size = size.to_string();
            for (header_name, value) in [
                ("x-repo-commit", commit_sha.as_str()),
                ("x-linked-size", linked_size.as_str()),
                ("x-linked-etag", oid.as_str()),
            ] {
                response.headers_mut().insert(
                    header_name,
                    value.parse().map_err(|_error| HubApiError::NotFound)?,
                );
            }
            if let Some(hash) = xet_hash {
                let stored_repo = state
                    .store
                    .get_repo(&name)
                    .map_err(|error| HubApiError::CasError(error.to_string()))?
                    .ok_or(HubApiError::RepoNotFound)?;
                let mut refresh = url::Url::parse(&state.public_base_url)
                    .map_err(|error| HubApiError::CasError(error.to_string()))?;
                refresh
                    .path_segments_mut()
                    .map_err(|()| {
                        HubApiError::CasError("public URL cannot contain path segments".to_owned())
                    })?
                    .pop_if_empty()
                    .extend([
                        "api",
                        super::repo_type_path(stored_repo.repo_type),
                        &ns,
                        &repo_name,
                        "xet-read-token",
                        &rev,
                    ]);
                let mut reconstruction = url::Url::parse(&state.public_base_url)
                    .map_err(|error| HubApiError::CasError(error.to_string()))?;
                reconstruction
                    .path_segments_mut()
                    .map_err(|()| HubApiError::CasError("invalid public URL".to_owned()))?
                    .pop_if_empty()
                    .extend(["v1", "reconstructions", &hash]);
                for (key, value) in [
                    ("x-xet-hash", hash),
                    ("x-xet-refresh-route", refresh.to_string()),
                    (
                        "link",
                        format!(
                            "<{refresh}>; rel=\"xet-auth\", <{reconstruction}>; rel=\"xet-reconstruction-info\""
                        ),
                    ),
                ] {
                    response
                        .headers_mut()
                        .insert(key, value.parse().map_err(|_error| HubApiError::NotFound)?);
                }
            }
            Ok(response)
        }
    }
}

fn accepts_xet_fileinfo(headers: &HeaderMap) -> bool {
    headers
        .get_all(axum::http::header::ACCEPT)
        .iter()
        .any(|value| {
            value.to_str().is_ok_and(|value| {
                value.split(',').any(|item| {
                    let mut fields = item.split(';');
                    fields.next().is_some_and(|media| {
                        media
                            .trim()
                            .eq_ignore_ascii_case("application/vnd.xet-fileinfo+json")
                    }) && fields.all(|field| {
                        let Some((name, parameter)) = field.trim().split_once('=') else {
                            return true;
                        };
                        !name.trim().eq_ignore_ascii_case("q")
                            || parameter
                                .trim()
                                .parse::<f64>()
                                .is_ok_and(|q| q > 0.0 && q <= 1.0)
                    })
                })
            })
        })
}

#[cfg(test)]
mod negotiation_tests;

fn set_file_metadata_headers(
    response: &mut Response,
    etag: &str,
    commit_sha: &str,
    size: u64,
) -> Result<(), HubApiError> {
    for (name, value) in [
        ("content-type", "application/octet-stream".to_owned()),
        ("x-repo-commit", commit_sha.to_owned()),
        ("etag", etag.to_owned()),
        ("content-length", size.to_string()),
    ] {
        response
            .headers_mut()
            .insert(name, value.parse().map_err(|_error| HubApiError::NotFound)?);
    }
    Ok(())
}

fn apply_range_headers(
    response: &mut Response,
    selection: &super::lfs::LfsRangeSelection,
    total_length: u64,
) -> Result<(), HubApiError> {
    response.headers_mut().insert(
        axum::http::header::ACCEPT_RANGES,
        "bytes".parse().map_err(|_error| HubApiError::NotFound)?,
    );
    match selection {
        super::lfs::LfsRangeSelection::Full => {}
        super::lfs::LfsRangeSelection::Partial(range) => {
            let length = range
                .end_inclusive()
                .checked_sub(range.start())
                .and_then(|distance| distance.checked_add(1))
                .ok_or_else(|| HubApiError::CasError("invalid file range".to_owned()))?;
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
            response.headers_mut().insert(
                axum::http::header::CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{total_length}",
                    range.start(),
                    range.end_inclusive()
                )
                .parse()
                .map_err(|_error| HubApiError::NotFound)?,
            );
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_LENGTH, length.into());
        }
        super::lfs::LfsRangeSelection::Unsatisfiable => {
            *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            response.headers_mut().insert(
                axum::http::header::CONTENT_RANGE,
                format!("bytes */{total_length}")
                    .parse()
                    .map_err(|_error| HubApiError::NotFound)?,
            );
            response
                .headers_mut()
                .insert(axum::http::header::CONTENT_LENGTH, 0_u64.into());
        }
    }
    Ok(())
}
