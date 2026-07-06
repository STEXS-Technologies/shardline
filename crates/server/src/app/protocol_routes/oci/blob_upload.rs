use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode, Uri,
        header::{CONTENT_RANGE, LOCATION, RANGE},
    },
    response::Response,
};
use shardline_protocol::TokenScope;

use crate::{
    ServerError,
    oci_adapter::{
        abort_s3_multipart_upload_session, append_s3_multipart_upload_bytes, append_upload_bytes,
        create_upload_session, delete_upload_session, finalize_s3_multipart_upload_session,
        lock_upload_sessions, oci_blob_key, oci_blob_location, read_upload_session,
        touch_upload_session, upload_body_integrity, upload_body_path_for_session, upload_length,
        upload_session_length, upload_session_location, validate_repository,
    },
    protocol_support::{parse_sha256_digest, scope_namespace, validate_oci_repository_scope},
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::super::{
    AppState, ensure_upload_growth_within_limit, parse_query_map, parse_upload_content_range,
    scope_from_auth,
};
use super::tags::oci_created_response;
use super::token::oci_authorize;

#[tracing::instrument(skip(state, headers, uri, body), fields(repository))]
pub(crate) async fn oci_post_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repository: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_repository(repository)?;
    let query = parse_query_map(uri)?;
    if let Some(mount_digest) = query.get("mount") {
        let digest_hex = parse_sha256_digest(mount_digest)?;
        let from = query.get("from").map(String::as_str).unwrap_or(repository);
        let source_key = oci_blob_key(from, &digest_hex, scope)?;
        let target_key = oci_blob_key(repository, &digest_hex, scope)?;
        match state
            .backend
            .copy_object_if_absent(&source_key, &target_key)
        {
            Ok(_stored) => {
                return oci_created_response(
                    &oci_blob_location(repository, &digest_hex),
                    Some(&digest_hex),
                );
            }
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
    }

    if let Some(digest) = query.get("digest") {
        let digest_hex = parse_sha256_digest(digest)?;
        let body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
        if body.expected_total_bytes() != Some(0) {
            let object_key = oci_blob_key(repository, &digest_hex, scope)?;
            let _stored = state
                .backend
                .put_sha256_addressed_object_stream_if_absent(&object_key, &digest_hex, body)
                .await?;
            return oci_created_response(
                &oci_blob_location(repository, &digest_hex),
                Some(&digest_hex),
            );
        }
    }

    let session_id = create_upload_session(
        state.config.root_dir(),
        Some(&state.backend),
        repository,
        scope,
        state.config.oci_upload_session_ttl_seconds(),
        state.config.oci_upload_max_active_sessions(),
        state.backend.uses_s3_object_store(),
    )
    .await?;
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(LOCATION, upload_session_location(repository, &session_id))
        .header(RANGE, "0-0")
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

#[tracing::instrument(
    skip(state, auth_headers, headers, body),
    fields(repository, session_id)
)]
pub(crate) async fn oci_patch_blob_upload(
    state: &Arc<AppState>,
    auth_headers: &HeaderMap,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, auth_headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let bytes = read_body_to_bytes(&mut body).await?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let current_length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    if let Some(content_range) = headers.get(CONTENT_RANGE) {
        let content_range = content_range
            .to_str()
            .map_err(|_error| ServerError::InvalidRangeHeader)?;
        let expected_range = parse_upload_content_range(content_range)?;
        if expected_range.start() != current_length {
            return Err(ServerError::RangeNotSatisfiable);
        }
        let observed_end = expected_range
            .start()
            .checked_add(u64::try_from(bytes.len())?)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        if observed_end != expected_range.end_inclusive() {
            return Err(ServerError::RangeNotSatisfiable);
        }
    }
    ensure_upload_growth_within_limit(state, current_length, bytes.len())?;
    let new_length = if session.use_s3_multipart {
        let (_session, new_length) = append_s3_multipart_upload_bytes(
            state.config.root_dir(),
            &state.backend,
            session_id,
            session,
            &bytes,
        )
        .await?;
        new_length
    } else {
        let new_length = append_upload_bytes(state.config.root_dir(), session_id, &bytes).await?;
        touch_upload_session(state.config.root_dir(), session_id, session).await?;
        new_length
    };
    let last = new_length.saturating_sub(1);
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .header(LOCATION, upload_session_location(repository, session_id))
        .header(RANGE, format!("0-{last}"))
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

#[tracing::instrument(skip(state, headers, uri, body), fields(repository, session_id))]
pub(crate) async fn oci_put_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    uri: &Uri,
    repository: &str,
    session_id: &str,
    body: Body,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let query = parse_query_map(uri)?;
    let digest = query.get("digest").ok_or(ServerError::InvalidDigest)?;
    let digest_hex = parse_sha256_digest(digest)?;
    let mut body = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())?;
    let final_bytes = read_body_to_bytes(&mut body).await?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let current_length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    if let Some(content_range) = headers.get(CONTENT_RANGE) {
        let content_range = content_range
            .to_str()
            .map_err(|_error| ServerError::InvalidRangeHeader)?;
        let expected_range = parse_upload_content_range(content_range)?;
        if expected_range.start() != current_length {
            return Err(ServerError::RangeNotSatisfiable);
        }
        let observed_end = expected_range
            .start()
            .checked_add(u64::try_from(final_bytes.len())?)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        if observed_end != expected_range.end_inclusive() {
            return Err(ServerError::RangeNotSatisfiable);
        }
    }
    ensure_upload_growth_within_limit(state, current_length, final_bytes.len())?;
    let object_key = oci_blob_key(repository, &digest_hex, scope)?;
    if session.use_s3_multipart {
        let _stored = finalize_s3_multipart_upload_session(
            state.config.root_dir(),
            &state.backend,
            session_id,
            session,
            &object_key,
            &digest_hex,
            &final_bytes,
        )
        .await?;
        delete_upload_session(state.config.root_dir(), session_id).await?;
        return oci_created_response(
            &oci_blob_location(repository, &digest_hex),
            Some(&digest_hex),
        );
    }
    if !final_bytes.is_empty() {
        let _new_length =
            append_upload_bytes(state.config.root_dir(), session_id, &final_bytes).await?;
    }
    let (observed, integrity) = upload_body_integrity(state.config.root_dir(), session_id).await?;
    if observed != digest_hex {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    let upload_path = upload_body_path_for_session(state.config.root_dir(), session_id)?;
    let _stored = state.backend.put_sha256_addressed_object_file(
        &object_key,
        &digest_hex,
        &upload_path,
        &integrity,
    )?;
    delete_upload_session(state.config.root_dir(), session_id).await?;
    oci_created_response(
        &oci_blob_location(repository, &digest_hex),
        Some(&digest_hex),
    )
}

#[tracing::instrument(skip(state, headers), fields(repository, session_id))]
pub(crate) async fn oci_get_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    let length = if let Some(length) = upload_session_length(&session) {
        length
    } else {
        upload_length(state.config.root_dir(), session_id).await?
    };
    touch_upload_session(state.config.root_dir(), session_id, session).await?;
    let last = length.saturating_sub(1);
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .header(LOCATION, upload_session_location(repository, session_id))
        .header(RANGE, format!("0-{last}"))
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}

#[tracing::instrument(skip(state, headers), fields(repository, session_id))]
pub(crate) async fn oci_delete_blob_upload(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    repository: &str,
    session_id: &str,
) -> Result<Response, ServerError> {
    let auth = oci_authorize(state, headers, Some(repository), TokenScope::Write)?;
    let scope = auth.as_ref().map(scope_from_auth);
    validate_oci_repository_scope(repository, scope)?;
    let _lock = lock_upload_sessions(state.config.root_dir()).await?;
    let session = read_upload_session(
        state.config.root_dir(),
        session_id,
        state.config.oci_upload_session_ttl_seconds(),
    )
    .await?;
    if session.repository != repository || session.scope_namespace != scope_namespace(scope) {
        return Err(ServerError::NotFound);
    }
    if session.use_s3_multipart {
        abort_s3_multipart_upload_session(&state.backend, &session).await?;
    }
    delete_upload_session(state.config.root_dir(), session_id).await?;
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .map_err(|_error| ServerError::Overflow)
}
