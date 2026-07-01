use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use shardline_protocol::TokenScope;

use crate::{
    BazelCacheKind, ServerError, bazel_cache_object_key,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{AppState, authorize, direct_object_response, scope_from_auth};

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_ac(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let _stored = state
        .backend
        .put_object_bytes_if_absent(&object_key, bytes)?;
    Ok(StatusCode::NO_CONTENT)
}

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &object_key,
        "application/octet-stream",
        None,
    )
    .await
}

#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put_cas(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<impl IntoResponse, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Write)?;
    let object_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let _stored = state
        .backend
        .put_sha256_addressed_object_stream_if_absent(&object_key, &hash, body)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
