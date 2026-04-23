use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, Uri},
    response::IntoResponse,
};
use serde::Deserialize;
use shardline_protocol::TokenScope;

use crate::{
    ServerError, model::BatchReconstructionResponse,
    reconstruction::build_batch_reconstruction_response,
};

use super::{
    AppState, authorize,
    reconstruction_helpers::{
        load_reconstruction_response, load_reconstruction_v2_response,
        parse_batch_reconstruction_file_ids, parse_reconstruction_request_range,
        validate_optional_content_hash, validate_xet_hash_path,
    },
    scope_from_auth,
};

#[derive(Debug, Deserialize)]
pub(super) struct FileVersionQuery {
    content_hash: Option<String>,
}

pub(super) async fn reconstruction(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<FileVersionQuery>,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_xet_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    Ok(Json(
        load_reconstruction_response(
            &state,
            &file_id,
            query.content_hash.as_deref(),
            requested_range,
            auth.as_ref().map(scope_from_auth),
        )
        .await?,
    ))
}

pub(super) async fn reconstruction_v2(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<FileVersionQuery>,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_xet_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    Ok(Json(
        load_reconstruction_v2_response(
            &state,
            &file_id,
            query.content_hash.as_deref(),
            requested_range,
            auth.as_ref().map(scope_from_auth),
        )
        .await?,
    ))
}

pub(super) async fn batch_reconstruction(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<BatchReconstructionResponse>, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let repository_scope = auth.as_ref().map(scope_from_auth);
    let file_ids = parse_batch_reconstruction_file_ids(&uri)?;
    let mut responses = Vec::new();

    for file_id in file_ids {
        match load_reconstruction_response(&state, &file_id, None, None, repository_scope).await {
            Ok(response) => responses.push((file_id, response)),
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }

    Ok(Json(build_batch_reconstruction_response(responses)))
}
