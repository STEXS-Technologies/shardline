use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use bytes::Bytes;

use crate::{commit, error::HubApiError, models::*};
use shardline_storage::ObjectStore;

use super::{HubRepository, HubState, lfs_object_key};

// ---- LFS batch (requires Read) ----

pub(crate) async fn lfs_batch(
    State(state): State<HubState>,
    repo: HubRepository,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Json<LfsBatchResponse>, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_batch", "POST", 200);
    // These LFS routes carry no `ns/repo` in the URL path; the token itself
    // identifies the repository, so the capability's claims both authorize the
    // request and namespace the LFS object keys.
    let capability = repo.capability();

    // Determine the transfer adapter. Prefer "xet" when the client supports it
    // and the server has an auth provider to mint CAS tokens. Fall back to "basic".
    let supports_xet = request.transfers.iter().any(|t| t == "xet");
    let supports_basic = request.transfers.iter().any(|t| t == "basic");
    let use_xet = supports_xet && state.auth.is_some() && capability.claims().is_some();
    let transfer = if use_xet {
        "xet"
    } else if request.transfers.is_empty() || supports_basic {
        "basic"
    } else {
        // Client advertised xet-only but server can't negotiate it; fall back
        // to basic so git-lfs still functions (it will use the basic transfer).
        "basic"
    };

    let objects: Vec<LfsObjectResponse> = request
        .objects
        .iter()
        .map(|obj| {
            let key = match lfs_object_key(&obj.oid, capability) {
                Ok(k) => k,
                Err(e) => {
                    return LfsObjectResponse {
                        oid: obj.oid.clone(),
                        size: obj.size,
                        actions: None,
                        error: Some(LfsObjectError {
                            code: 400,
                            message: format!("invalid oid: {e}"),
                        }),
                    };
                }
            };
            let exists = state.object_store.contains(&key).unwrap_or(false);
            let actions = match request.operation {
                LfsBatchOperation::Download => {
                    if exists {
                        Some(LfsObjectActions {
                            download: Some(LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                            upload: None,
                            verify: None,
                        })
                    } else {
                        None
                    }
                }
                LfsBatchOperation::Upload => {
                    if exists {
                        Some(LfsObjectActions {
                            download: None,
                            upload: None,
                            verify: Some(LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                        })
                    } else {
                        Some(LfsObjectActions {
                            download: None,
                            upload: Some(LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                            verify: None,
                        })
                    }
                }
                LfsBatchOperation::Verify => {
                    if exists {
                        Some(LfsObjectActions {
                            download: None,
                            upload: None,
                            verify: Some(LfsObjectAction {
                                href: format!("/lfs/objects/{}", obj.oid),
                                header: None,
                                ssh: None,
                            }),
                        })
                    } else {
                        None
                    }
                }
            };

            let error = if !exists
                && (request.operation == LfsBatchOperation::Download
                    || request.operation == LfsBatchOperation::Verify)
            {
                Some(LfsObjectError {
                    code: 404,
                    message: "Object not found".to_owned(),
                })
            } else {
                None
            };

            LfsObjectResponse {
                oid: obj.oid.clone(),
                size: obj.size,
                actions,
                error,
            }
        })
        .collect();

    Ok(Json(LfsBatchResponse {
        transfer: transfer.to_owned(),
        objects,
    }))
}

// ---- LFS upload (requires Write) ----

pub(crate) async fn lfs_upload(
    State(state): State<HubState>,
    repo: HubRepository<true>,
    Path(oid): Path<String>,
    body: Bytes,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_upload", "PUT", 200);
    shardline_metrics::record_hub_api_file_upload();
    commit::validate_lfs_oid(&oid)?;

    use shardline_storage::{ObjectBody, ObjectIntegrity};
    let key = lfs_object_key(&oid, repo.capability())?;
    let object_body = ObjectBody::from_slice(&body);
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&body).as_bytes()),
        body.len() as u64,
    );
    state
        .object_store
        .put_if_absent(&key, object_body, &integrity)
        .map_err(|e: shardline_server_core::ServerObjectStoreError| {
            HubApiError::CasError(e.to_string())
        })?;
    Ok(StatusCode::OK)
}

// ---- LFS download (requires Read) ----

pub(crate) async fn lfs_download(
    State(state): State<HubState>,
    repo: HubRepository,
    Path(oid): Path<String>,
) -> Result<
    (
        StatusCode,
        [(axum::http::header::HeaderName, &'static str); 1],
        Vec<u8>,
    ),
    HubApiError,
> {
    shardline_metrics::record_hub_api_request("lfs_download", "GET", 200);
    shardline_metrics::record_hub_api_file_download();
    let key = lfs_object_key(&oid, repo.capability())?;
    let meta = state
        .object_store
        .metadata(&key)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::NotFound)?;
    let range_end = meta.length().checked_sub(1).ok_or(HubApiError::NotFound)?;
    let range = shardline_protocol::ByteRange::new(0, range_end)
        .map_err(|_range_err| HubApiError::NotFound)?;
    let data = state
        .object_store
        .read_range(&key, range)
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
        data,
    ))
}
