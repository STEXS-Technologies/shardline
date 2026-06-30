use std::sync::Arc;

use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, StatusCode,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use serde_json::json;
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError,
    LfsObjectResponse, lfs_object_key,
    upload_ingest::RequestBodyReader,
};

use super::{
    AppState,
    MAX_LFS_BATCH_OBJECTS,
    authorize,
    direct_object_response,
    scope_from_auth,
};

#[tracing::instrument(skip(state, headers, request))]
pub(crate) async fn lfs_batch(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<LfsBatchRequest>,
) -> Result<Response, ServerError> {
    let requested_scope = match request.operation.as_str() {
        "download" => TokenScope::Read,
        "upload" => TokenScope::Write,
        _ => return Err(ServerError::InvalidManifestReference),
    };
    let auth = authorize(&state, &headers, requested_scope)?;
    if request.objects.len() > MAX_LFS_BATCH_OBJECTS {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "too many objects in batch request" })),
        )
            .into_response());
    }
    if let Some(hash_algo) = request.hash_algo.as_deref()
        && hash_algo != "sha256"
    {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported hash algorithm" })),
        )
            .into_response());
    }

    let scope = auth.as_ref().map(scope_from_auth);
    let transfer = if request.transfers.is_empty()
        || request.transfers.iter().any(|transfer| transfer == "basic")
    {
        "basic".to_owned()
    } else {
        return Ok((
            StatusCode::UNPROCESSABLE_ENTITY,
            [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
            Json(json!({ "message": "unsupported transfer adapter" })),
        )
            .into_response());
    };
    let mut objects = Vec::with_capacity(request.objects.len());
    for object in request.objects {
        let object_key = lfs_object_key(&object.oid, scope)?;
        let object_length = state.backend.object_length(&object_key).await;
        match request.operation.as_str() {
            "download" => match object_length {
                Ok(length) => {
                    let actions = json!({
                        "download": {
                            "href": format!(
                                "{}/v1/lfs/objects/{}",
                                state.config.public_base_url().trim_end_matches('/'),
                                object.oid
                            )
                        }
                    });
                    objects.push(LfsObjectResponse {
                        oid: object.oid,
                        size: length,
                        authenticated: Some(auth.is_some()),
                        actions: Some(actions),
                        error: None,
                    });
                }
                Err(ServerError::NotFound) => objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size: object.size,
                    authenticated: None,
                    actions: None,
                    error: Some(LfsObjectError {
                        code: 404,
                        message: "Object does not exist".to_owned(),
                    }),
                }),
                Err(error) => return Err(error),
            },
            "upload" => {
                let (size, actions) = match object_length {
                    Ok(length) => (length, None),
                    Err(ServerError::NotFound) => (
                        object.size,
                        Some(json!({
                            "upload": {
                                "href": format!(
                                    "{}/v1/lfs/objects/{}",
                                    state.config.public_base_url().trim_end_matches('/'),
                                    object.oid
                                )
                            }
                        })),
                    ),
                    Err(error) => return Err(error),
                };
                objects.push(LfsObjectResponse {
                    oid: object.oid,
                    size,
                    authenticated: Some(auth.is_some()),
                    actions,
                    error: None,
                });
            }
            _ => return Err(ServerError::InvalidManifestReference),
        }
    }

    Ok((
        StatusCode::OK,
        [(CONTENT_TYPE, LFS_CONTENT_TYPE)],
        Json(LfsBatchResponse {
            transfer,
            objects,
            hash_algo: "sha256",
        }),
    )
        .into_response())
}

#[tracing::instrument(skip(state, headers), fields(oid))]
pub(crate) async fn lfs_get_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        Some(format!("sha256:{oid}")),
    )
    .await
}

#[tracing::instrument(skip(state, headers), fields(oid))]
pub(crate) async fn lfs_head_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((StatusCode::OK, [(CONTENT_LENGTH, total_length.to_string())]))
}

#[tracing::instrument(skip(state, headers, body), fields(oid))]
pub(crate) async fn lfs_put_object(
    State(state): State<Arc<AppState>>,
    Path(oid): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = lfs_object_key(&oid, auth.as_ref().map(scope_from_auth))?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &oid, body)
        .await?;
    Ok(StatusCode::OK)
}
