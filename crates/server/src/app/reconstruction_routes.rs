use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, Uri},
    response::IntoResponse,
};
use serde::Deserialize;
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    xet_adapter::{
        BatchReconstructionResponse, build_batch_reconstruction_response, validate_hash_path,
        validate_optional_content_hash,
    },
};

use super::{
    AppState, authorize,
    reconstruction_helpers::{
        load_reconstruction_response, load_reconstruction_v2_response,
        parse_batch_reconstruction_file_ids, parse_reconstruction_request_range,
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
    validate_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    let start = Instant::now();
    let result = load_reconstruction_response(
        &state,
        &file_id,
        query.content_hash.as_deref(),
        requested_range,
        auth.as_ref().map(scope_from_auth),
    )
    .await;
    let elapsed = start.elapsed();
    match &result {
        Ok(response) => {
            let chunks = u64::try_from(response.terms.len()).unwrap_or(0);
            shardline_metrics::record_reconstruction(true, elapsed, chunks);
            shardline_metrics::metrics().xet.record_reconstruction(true, elapsed, chunks);
        }
        Err(_) => {
            shardline_metrics::record_reconstruction(false, elapsed, 0);
        }
    }
    Ok(Json(result?))
}

pub(super) async fn reconstruction_v2(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<FileVersionQuery>,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    validate_hash_path(&file_id)?;
    validate_optional_content_hash(query.content_hash.as_deref())?;
    let requested_range = parse_reconstruction_request_range(
        &state,
        &headers,
        &file_id,
        query.content_hash.as_deref(),
        auth.as_ref().map(scope_from_auth),
    )
    .await?;
    let start = Instant::now();
    let result = load_reconstruction_v2_response(
        &state,
        &file_id,
        query.content_hash.as_deref(),
        requested_range,
        auth.as_ref().map(scope_from_auth),
    )
    .await;
    let elapsed = start.elapsed();
    match &result {
        Ok(response) => {
            let chunks = u64::try_from(response.terms.len()).unwrap_or(0);
            shardline_metrics::record_reconstruction(true, elapsed, chunks);
            shardline_metrics::metrics().xet.record_reconstruction(true, elapsed, chunks);
        }
        Err(_) => {
            shardline_metrics::record_reconstruction(false, elapsed, 0);
        }
    }
    Ok(Json(result?))
}

pub(super) async fn batch_reconstruction(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    uri: Uri,
) -> Result<Json<BatchReconstructionResponse>, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let repository_scope = auth.as_ref().map(scope_from_auth);
    let file_ids = parse_batch_reconstruction_file_ids(&uri)?;
    let start = Instant::now();
    let mut responses = Vec::new();

    for file_id in file_ids {
        match load_reconstruction_response(&state, &file_id, None, None, repository_scope).await {
            Ok(response) => responses.push((file_id, response)),
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }

    let elapsed = start.elapsed();
    let total_chunks: u64 = responses
        .iter()
        .map(|(_, r)| u64::try_from(r.terms.len()).unwrap_or(0))
        .sum();
    shardline_metrics::record_reconstruction(true, elapsed, total_chunks);
    shardline_metrics::metrics().xet.record_reconstruction(true, elapsed, total_chunks);

    Ok(Json(build_batch_reconstruction_response(responses)))
}
