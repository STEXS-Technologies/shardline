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

// ---------------------------------------------------------------------------
// AC (Action Cache) handlers — /v1/bazel/cache/ac/{hash}
// ---------------------------------------------------------------------------

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
        "bazel",
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
pub(crate) async fn bazel_head_ac(
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
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (axum::http::header::CONTENT_TYPE, "application/octet-stream".to_owned()),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// CAS (Content-Addressable Storage) handlers — /v1/bazel/cache/cas/{hash}
// ---------------------------------------------------------------------------

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
        "bazel",
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

#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head_cas(
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
    let total_length = state.backend.object_length(&object_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (axum::http::header::CONTENT_TYPE, "application/octet-stream".to_owned()),
        ],
    )
        .into_response())
}

// ---------------------------------------------------------------------------
// Flat routes — /v1/bazel/{hash}
// Bazel remote cache spec uses a flat URL namespace: GET/PUT/HEAD /{hash}.
// These routes try both AC and CAS for backward compatibility with stock
// Bazel clients that configure --remote_cache=http://host:port/v1/bazel.
// ---------------------------------------------------------------------------

/// GET /v1/bazel/{hash} — Download blob (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_get(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) {
        if state.backend.object_length(&ac_key).await.is_ok() {
            return direct_object_response(
                &state,
                &headers,
                &ac_key,
                "application/octet-stream",
                None,
                "bazel",
            )
            .await;
        }
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    direct_object_response(
        &state,
        &headers,
        &cas_key,
        "application/octet-stream",
        None,
        "bazel",
    )
    .await
}

/// PUT /v1/bazel/{hash} — Upload blob to CAS.
#[tracing::instrument(skip(state, headers, body), fields(hash))]
pub(crate) async fn bazel_put(
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

/// HEAD /v1/bazel/{hash} — Check existence (tries AC first, falls back to CAS).
#[tracing::instrument(skip(state, headers), fields(hash))]
pub(crate) async fn bazel_head(
    State(state): State<Arc<AppState>>,
    Path(hash): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    let auth = authorize(&state, &headers, TokenScope::Read)?;

    // Try AC first, then CAS.
    if let Ok(ac_key) = bazel_cache_object_key(
        BazelCacheKind::Ac,
        &hash,
        auth.as_ref().map(scope_from_auth),
    ) {
        if let Ok(total_length) = state.backend.object_length(&ac_key).await {
            return Ok((
                StatusCode::OK,
                [
                    (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
                    (axum::http::header::CONTENT_TYPE, "application/octet-stream".to_owned()),
                ],
            )
                .into_response());
        }
    }

    let cas_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &hash,
        auth.as_ref().map(scope_from_auth),
    )?;
    let total_length = state.backend.object_length(&cas_key).await?;
    Ok((
        StatusCode::OK,
        [
            (axum::http::header::CONTENT_LENGTH, total_length.to_string()),
            (axum::http::header::CONTENT_TYPE, "application/octet-stream".to_owned()),
        ],
    )
        .into_response())
}
