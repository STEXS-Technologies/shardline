use axum::{
    Json,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderMap, header::CONTENT_TYPE},
};
use serde::Deserialize;

use crate::{commit::validate_commit_path, error::HubApiError, models::RepoType};

use super::{HubRepository, HubState};

const MAX_PATHS_INFO_REQUEST: usize = 1_000;
const MAX_PATHS_INFO_BODY_BYTES: usize = 64 * 1024;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum RequestedPaths {
    Many(Vec<String>),
    One(String),
}

#[derive(Debug, Deserialize)]
pub(crate) struct PathsInfoRequest {
    paths: RequestedPaths,
    #[serde(default)]
    expand: bool,
}

/// Returns file and directory metadata for explicitly requested repository paths.
pub(crate) async fn paths_info(
    State(state): State<HubState>,
    _repo: HubRepository,
    Path((repo_type, namespace, repo_name, revision)): Path<(String, String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<Vec<serde_json::Value>>, HubApiError> {
    let request = parse_paths_info_request(&headers, body).await?;
    let requested_type = RepoType::from_api_str(&repo_type)
        .ok_or_else(|| HubApiError::PathValidation(format!("invalid repo type: {repo_type}")))?;
    let requested = match request.paths {
        RequestedPaths::Many(paths) => paths,
        RequestedPaths::One(path) => vec![path],
    };
    if requested.len() > MAX_PATHS_INFO_REQUEST {
        return Err(HubApiError::PathValidation(format!(
            "paths-info accepts at most {MAX_PATHS_INFO_REQUEST} paths"
        )));
    }
    for path in &requested {
        validate_commit_path(path)?;
    }

    let repo_id = format!("{namespace}/{repo_name}");
    let stored_repo = state
        .store
        .get_repo(&repo_id)
        .map_err(|error| HubApiError::CasError(error.to_string()))?
        .ok_or(HubApiError::RepoNotFound)?;
    if RepoType::from(stored_repo.repo_type) != requested_type {
        return Err(HubApiError::RepoNotFound);
    }
    let commit_sha =
        crate::git::smart_http::resolve_commit_id(&state, &repo_id, &revision, _repo.capability())?
            .ok_or(HubApiError::RevisionNotFound)?;
    crate::revision_access::authorize_read(
        &state,
        &repo_id,
        &revision,
        &commit_sha,
        _repo.capability(),
    )
    .await?;
    let files = state
        .store
        .get_files(&commit_sha)
        .map_err(|error| HubApiError::CasError(error.to_string()))?;
    let oids = crate::git::smart_http::path_oids(
        state.clone(),
        repo_id.clone(),
        commit_sha.clone(),
        files.clone(),
        requested.clone(),
        _repo.capability().clone(),
    )
    .await?;
    let last_commits = if request.expand {
        super::path_history::last_path_commits(
            &state,
            &repo_id,
            &commit_sha,
            &files,
            &requested,
            _repo.capability(),
        )
        .await?
    } else {
        std::collections::HashMap::new()
    };
    let by_path: std::collections::HashMap<_, _> = files
        .into_iter()
        .map(|file| (file.path.clone(), file))
        .collect();

    let mut response = Vec::with_capacity(requested.len());
    for path in requested {
        let Some(metadata) = oids.get(&path) else {
            continue;
        };
        if let Some(file) = by_path.get(&path) {
            response.push(serde_json::json!({
                "type": "file",
                "path": file.path,
                "size": file.size,
                "oid": metadata.oid,
                "xetHash": metadata.xet_hash,
                "lastCommit": last_commits.get(&path),
                "lfs": file.is_lfs.then(|| serde_json::json!({
                    "oid": file.sha,
                    "size": file.size,
                    "pointerSize": lfs_pointer_size(&file.sha, file.size),
                })),
            }));
            continue;
        }

        response.push(serde_json::json!({
            "type": "directory",
            "path": path,
            "oid": metadata.oid,
            "lastCommit": last_commits.get(&path),
        }));
    }
    shardline_metrics::record_hub_api_request("paths_info", "POST", 200);
    Ok(Json(response))
}

async fn parse_paths_info_request(
    headers: &HeaderMap,
    body: Body,
) -> Result<PathsInfoRequest, HubApiError> {
    let bytes = to_bytes(body, MAX_PATHS_INFO_BODY_BYTES)
        .await
        .map_err(|error| HubApiError::BadRequest(format!("paths-info request body: {error}")))?;
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim();
    match content_type {
        "application/json" => serde_json::from_slice(&bytes)
            .map_err(|error| HubApiError::BadRequest(format!("invalid paths-info JSON: {error}"))),
        "application/x-www-form-urlencoded" => {
            let mut paths = Vec::new();
            let mut expand = false;
            for (key, value) in url::form_urlencoded::parse(&bytes) {
                match key.as_ref() {
                    "paths" => paths.push(value.into_owned()),
                    "expand" => {
                        expand = match value.as_ref() {
                            "true" | "1" => true,
                            "false" | "0" | "" => false,
                            _ => {
                                return Err(HubApiError::BadRequest(
                                    "paths-info expand must be true or false".to_owned(),
                                ));
                            }
                        };
                    }
                    _ => {}
                }
            }
            let request = PathsInfoRequest {
                paths: RequestedPaths::Many(paths),
                expand,
            };
            Ok(request)
        }
        _ => Err(HubApiError::BadRequest(
            "paths-info requires application/json or application/x-www-form-urlencoded".to_owned(),
        )),
    }
}

fn lfs_pointer_size(oid: &str, size: u64) -> usize {
    format!("version https://git-lfs.github.com/spec/v1\noid sha256:{oid}\nsize {size}\n").len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, header::CONTENT_TYPE};

    #[tokio::test]
    async fn accepts_the_official_form_encoding_and_json_compatibility_shape() {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let request = parse_paths_info_request(
            &headers,
            Body::from("paths=one.txt&paths=two.txt&expand=true"),
        )
        .await
        .expect("parse official form request");
        assert!(request.expand);
        assert!(matches!(
            request.paths,
            RequestedPaths::Many(paths) if paths == ["one.txt", "two.txt"]
        ));

        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let request = parse_paths_info_request(
            &headers,
            Body::from(r#"{"paths":"one.txt","expand":false}"#),
        )
        .await
        .expect("parse JSON compatibility request");
        assert!(!request.expand);
        assert!(matches!(request.paths, RequestedPaths::One(path) if path == "one.txt"));
    }
}
