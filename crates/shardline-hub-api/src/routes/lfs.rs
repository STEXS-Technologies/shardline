use std::collections::BTreeMap;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
};
use futures_util::StreamExt;
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectIntegrity, ObjectStore};

use crate::{commit, error::HubApiError, models::*};

use super::{HubRepository, HubState, lfs_object_key, stream_object};

/// Maximum number of objects allowed in a single batch request.
const MAX_LFS_BATCH_OBJECTS: usize = 1024;
/// Maximum body size for a single LFS upload (64 MiB, matches server-side default).
const MAX_LFS_UPLOAD_BYTES: usize = 64 * 1024 * 1024;

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

    // Validate batch size.
    if request.objects.len() > MAX_LFS_BATCH_OBJECTS {
        return Err(HubApiError::BadRequest(
            "too many objects in batch request".to_owned(),
        ));
    }

    // Validate hash algorithm.
    if let Some(hash_algo) = request.hash_algo.as_deref()
        && hash_algo != "sha256"
    {
        return Err(HubApiError::BadRequest(
            "unsupported hash algorithm".to_owned(),
        ));
    }

    // Determine the transfer adapter. Prefer "xet" when the client supports it
    // and the server has an auth provider to mint CAS tokens. Fall back to "basic".
    let supports_xet = request.transfers.iter().any(|t| t == "xet");
    let use_xet = supports_xet && state.auth.is_some() && capability.claims().is_some();
    let transfer = if use_xet {
        "xet"
    } else {
        // Fall back to basic: either the client supports basic, or we
        // degrade gracefully when xet-only was requested but unsupported.
        "basic"
    };

    // Mint a CAS token when using xet transfer. The existing claims are
    // re-signed so git-xet receives a scoped token for the CAS layer.
    let cas_token = if use_xet {
        capability.claims().and_then(|claims| {
            state
                .auth
                .as_ref()
                .and_then(|server_auth| server_auth.provider().mint_token(claims).ok())
        })
    } else {
        None
    };
    let cas_header: Option<BTreeMap<String, String>> = cas_token.map(|token| {
        let mut h = BTreeMap::new();
        h.insert(
            "X-Xet-Content-CAS-URL".to_owned(),
            state.public_base_url.trim_end_matches('/').to_owned(),
        );
        h.insert("X-Xet-Content-CAS-Access".to_owned(), token);
        h.insert(
            "X-Xet-Content-CAS-Token-Expiration".to_owned(),
            "0".to_owned(),
        );
        h
    });

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
                                header: cas_header.clone(),
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
                                header: cas_header.clone(),
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
        hash_algo: Some("sha256".to_owned()),
    }))
}

// ---- LFS upload (requires Write) ----

pub(crate) async fn lfs_upload(
    State(state): State<HubState>,
    repo: HubRepository<true>,
    Path(oid): Path<String>,
    body: Body,
) -> Result<StatusCode, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_upload", "PUT", 200);
    shardline_metrics::record_hub_api_file_upload();
    commit::validate_lfs_oid(&oid)?;

    let temporary =
        tempfile::NamedTempFile::new().map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut output = tokio::fs::File::create(temporary.path())
        .await
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let mut length = 0_usize;
    let mut hasher = blake3::Hasher::new();
    let mut stream = body.into_data_stream();
    while let Some(frame) = stream.next().await {
        let bytes = frame.map_err(|e| HubApiError::CasError(e.to_string()))?;
        length = length
            .checked_add(bytes.len())
            .ok_or_else(|| HubApiError::BadRequest("upload too large".to_owned()))?;
        if length > MAX_LFS_UPLOAD_BYTES {
            return Err(HubApiError::BadRequest(format!(
                "upload body exceeds maximum size of {} bytes",
                MAX_LFS_UPLOAD_BYTES
            )));
        }
        hasher.update(&bytes);
        tokio::io::AsyncWriteExt::write_all(&mut output, &bytes)
            .await
            .map_err(|e| HubApiError::CasError(e.to_string()))?;
    }
    tokio::io::AsyncWriteExt::flush(&mut output)
        .await
        .map_err(|e| HubApiError::CasError(e.to_string()))?;
    let key = lfs_object_key(&oid, repo.capability())?;
    let integrity = ObjectIntegrity::new(
        ShardlineHash::from_bytes(*hasher.finalize().as_bytes()),
        length as u64,
    );
    state
        .object_store
        .put_content_addressed_file(&key, temporary.path(), &integrity)
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
) -> Result<Response, HubApiError> {
    shardline_metrics::record_hub_api_request("lfs_download", "GET", 200);
    shardline_metrics::record_hub_api_file_download();
    let key = lfs_object_key(&oid, repo.capability())?;
    let meta = state
        .object_store
        .metadata(&key)
        .map_err(|e| HubApiError::CasError(e.to_string()))?
        .ok_or(HubApiError::NotFound)?;
    let length = meta.length();
    let mut response = stream_object(&state.object_store, key, length).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/octet-stream"
            .parse()
            .map_err(|_| HubApiError::NotFound)?,
    );
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_LENGTH, length.into());
    Ok((StatusCode::OK, response).into_response())
}
