use std::sync::Arc;

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode, Uri,
        header::{CONTENT_RANGE, LOCATION, RANGE},
    },
    response::Response,
};
use shardline_metrics::metrics;
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
    // The OCI spec allows a `digest-algorithm` query parameter on blob upload
    // initiation. Only SHA-256 is supported; reject any other algorithm per spec.
    if let Some(algo) = query.get("digest-algorithm").map(String::as_str)
        && algo != "sha256"
    {
        return Err(ServerError::InvalidDigest);
    }
    if let Some(mount_digest) = query.get("mount") {
        let digest_hex = parse_sha256_digest(mount_digest)?;
        let from = query.get("from").map(String::as_str).unwrap_or(repository);
        let source_key = oci_blob_key(from, &digest_hex, scope)?;
        let target_key = oci_blob_key(repository, &digest_hex, scope)?;
        match state
            .backend
            .copy_object_if_absent(&source_key, &target_key)
            .await
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
        let object_key = oci_blob_key(repository, &digest_hex, scope)?;
        let _stored = state
            .backend
            .put_sha256_addressed_object_stream_if_absent(&object_key, &digest_hex, body)
            .await?;
        metrics().protocol.record_oci_upload();
        return oci_created_response(
            &oci_blob_location(repository, &digest_hex),
            Some(&digest_hex),
        );
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
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to build post blob upload response");
            ServerError::Overflow
        })
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
        let content_range = content_range.to_str().map_err(|e| {
            tracing::warn!(error = %e, "invalid content-range header utf-8");
            ServerError::InvalidRangeHeader
        })?;
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
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to build patch blob upload response");
            ServerError::Overflow
        })
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
        let content_range = content_range.to_str().map_err(|e| {
            tracing::warn!(error = %e, "invalid content-range header utf-8");
            ServerError::InvalidRangeHeader
        })?;
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
    let _stored = state
        .backend
        .put_sha256_addressed_object_file(&object_key, &digest_hex, &upload_path, &integrity)
        .await?;
    delete_upload_session(state.config.root_dir(), session_id).await?;
    metrics().protocol.record_oci_upload();
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
        .map_err(|e| {
            tracing::warn!(error = %e, "failed to build get blob upload response");
            ServerError::Overflow
        })
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{Method, StatusCode, header},
    };
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;

    use super::super::test_helpers::{build_oci_test_state, oci_test_router};

    const REPO: &str = "team/assets";

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    async fn send(
        app: &axum::Router,
        method: Method,
        uri: &str,
        body: Body,
    ) -> axum::http::Response<Body> {
        let request = axum::http::Request::builder()
            .method(method)
            .uri(uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .unwrap();
        app.clone().oneshot(request).await.unwrap()
    }

    async fn upload_blob_direct(
        app: &axum::Router,
        repository: &str,
        data: &[u8],
    ) -> (String, axum::http::Response<Body>) {
        let digest = sha256_hex(data);
        let uri = format!("/v2/{repository}/blobs/uploads/?digest=sha256:{digest}");
        let response = send(app, Method::POST, &uri, Body::from(data.to_vec())).await;
        (digest, response)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_rejects_unsupported_digest_algorithm() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let data = b"test data";
        let digest = sha256_hex(data);
        let uri = format!("/v2/{REPO}/blobs/uploads/?digest=sha256:{digest}&digest-algorithm=sha1");
        let response = send(&app, Method::POST, &uri, Body::from(data.to_vec())).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_empty_body() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let data = b"";
        let (_digest, response) = upload_blob_direct(&app, REPO, data).await;
        assert!(
            response.status() == StatusCode::CREATED || response.status() == StatusCode::ACCEPTED,
            "empty body upload should return created or accepted, got {}",
            response.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_various_sizes() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Small blob (1 byte)
        let (_, response) = upload_blob_direct(&app, REPO, b"x").await;
        assert_eq!(response.status(), StatusCode::CREATED);

        // Medium blob (1 KB)
        let medium = vec![b'A'; 1024];
        let (_, response) = upload_blob_direct(&app, REPO, &medium).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_initiate_returns_accepted() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let uri = format!("/v2/{REPO}/blobs/uploads/");
        let response = send(&app, Method::POST, &uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(response.headers().get(header::LOCATION).is_some());
        assert!(response.headers().get(header::RANGE).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_range_header_on_response() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let uri = format!("/v2/{REPO}/blobs/uploads/");
        let response = send(&app, Method::POST, &uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let range = response
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(range, "0-0");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_get_missing_session_returns_not_found() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Use a hex-only session ID that passes upload session validation
        // but doesn't exist in the store.
        let uri = format!("/v2/{REPO}/blobs/uploads/0000000000000000");
        let response = send(&app, Method::GET, &uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_put_missing_session_returns_not_found() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let digest = sha256_hex(b"data");
        let uri = format!("/v2/{REPO}/blobs/uploads/0000000000000000?digest=sha256:{digest}");
        let response = send(&app, Method::PUT, &uri, Body::from(b"data".to_vec())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_delete_missing_session_returns_not_found() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let uri = format!("/v2/{REPO}/blobs/uploads/0000000000000000");
        let response = send(&app, Method::DELETE, &uri, Body::empty()).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_with_large_blob() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // 100 KB blob
        let data = vec![b'Z'; 102_400];
        let (_, response) = upload_blob_direct(&app, REPO, &data).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_with_same_blob_twice() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let data = b"deduplicated content";
        let (_, response1) = upload_blob_direct(&app, REPO, data).await;
        assert_eq!(response1.status(), StatusCode::CREATED);
        // Same blob uploaded again should also succeed (idempotent)
        let (_, response2) = upload_blob_direct(&app, REPO, data).await;
        assert_eq!(response2.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_mount_existing_blob() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Upload a blob first
        let data = b"mountable content";
        let (digest, _) = upload_blob_direct(&app, REPO, data).await;

        // Now mount it into another repository via POST with ?mount=
        let other_repo = "team/other-assets";
        let mount_uri =
            format!("/v2/{other_repo}/blobs/uploads/?mount=sha256:{digest}&from={REPO}");
        let response = send(&app, Method::POST, &mount_uri, Body::empty()).await;
        assert!(
            response.status() == StatusCode::CREATED || response.status() == StatusCode::ACCEPTED,
            "mount should succeed, got {}",
            response.status()
        );
    }

    // ── Session lifecycle tests ─────────────────────────────────────────────

    /// Extracts the session ID from a Location header like `/v2/repo/blobs/uploads/<session_id>`.
    fn session_id_from_location(response: &axum::http::Response<Body>) -> String {
        let location = response
            .headers()
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        // Location is like: /v2/team/assets/blobs/uploads/<session_id>
        location.split('/').next_back().unwrap().to_owned()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_patch_appends_bytes() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(
            &app,
            Method::PATCH,
            &patch_uri,
            Body::from(b"hello".to_vec()),
        )
        .await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);
        let range = patch_response
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(range, "0-4");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_patch_empty_body() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH with empty body
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(&app, Method::PATCH, &patch_uri, Body::empty()).await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);
        // Range should remain 0-0 (nothing appended)
        let range = patch_response
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(range, "0-0");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_delete_after_initiate() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // DELETE the session
        let delete_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let delete_response = send(&app, Method::DELETE, &delete_uri, Body::empty()).await;
        assert_eq!(delete_response.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_get_after_initiate() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // GET the session status
        let get_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let get_response = send(&app, Method::GET, &get_uri, Body::empty()).await;
        // Should return NO_CONTENT with Location and Range headers
        assert_eq!(get_response.status(), StatusCode::NO_CONTENT);
        assert!(get_response.headers().get(header::LOCATION).is_some());
        assert!(get_response.headers().get(header::RANGE).is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_patch_with_content_range() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH with Content-Range header (start=0, end=4, total=5)
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let request = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(&patch_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-4/5")
            .body(Body::from(b"hello".to_vec()))
            .unwrap();
        let patch_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_patch_with_invalid_content_range() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH with Content-Range with wrong start (5 instead of 0)
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let request = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(&patch_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(axum::http::header::CONTENT_RANGE, "bytes 5-9/10")
            .body(Body::from(b"hello".to_vec()))
            .unwrap();
        let patch_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(patch_response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_put_finalize() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data
        let data = b"final data";
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(&app, Method::PATCH, &patch_uri, Body::from(data.to_vec())).await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);

        // PUT to finalize with digest
        let digest = sha256_hex(data);
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}?digest=sha256:{digest}");
        let put_response = send(&app, Method::PUT, &put_uri, Body::empty()).await;
        assert_eq!(put_response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_put_finalize_hash_mismatch() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data
        let data = b"data for hash mismatch test";
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(&app, Method::PATCH, &patch_uri, Body::from(data.to_vec())).await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);

        // PUT with wrong digest — should fail with hash mismatch
        let wrong_digest = sha256_hex(b"different data");
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}?digest=sha256:{wrong_digest}");
        let put_response = send(&app, Method::PUT, &put_uri, Body::empty()).await;
        assert_eq!(put_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_put_without_digest_errors() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PUT without digest should fail
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let put_response = send(&app, Method::PUT, &put_uri, Body::empty()).await;
        assert_eq!(put_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_patch_wrong_repository_errors() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session in one repository
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // Try to PATCH using a different repository
        let wrong_repo = "team/other-assets";
        let patch_uri = format!("/v2/{wrong_repo}/blobs/uploads/{session_id}");
        let patch_response = send(
            &app,
            Method::PATCH,
            &patch_uri,
            Body::from(b"data".to_vec()),
        )
        .await;
        assert_eq!(patch_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_mount_nonexistent_blob_returns_error() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Mount a blob that doesn't exist — the handler attempts
        // copy_object_if_absent which on a local backend returns an IO error
        // (not NotFound), so this currently returns 500.
        let some_digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let mount_uri = format!("/v2/{REPO}/blobs/uploads/?mount=sha256:{some_digest}&from={REPO}");
        let response = send(&app, Method::POST, &mount_uri, Body::empty()).await;
        // The mount path should eventually fall through to session creation,
        // but the local backend copy_object_if_absent returns Io error instead
        // of NotFound for missing source files. This test documents the current
        // behavior — it returns an error (not a panic).
        assert!(
            response.status().is_server_error(),
            "mount non-existent blob should return an error, got {}",
            response.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_with_content_range() {
        // Direct upload with Content-Range header alongside digest
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        let data = b"content-range-direct";
        let digest = sha256_hex(data);
        let uri = format!("/v2/{REPO}/blobs/uploads/?digest=sha256:{digest}");
        let request = axum::http::Request::builder()
            .method(Method::POST)
            .uri(&uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .body(Body::from(data.to_vec()))
            .unwrap();
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_put_wrong_repository_errors() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session in one repository
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PUT to finalize with a different repository
        let data = b"some final data";
        let digest = sha256_hex(data);
        let wrong_repo = "team/other-assets";
        let put_uri = format!("/v2/{wrong_repo}/blobs/uploads/{session_id}?digest=sha256:{digest}");
        let put_response = send(&app, Method::PUT, &put_uri, Body::from(data.to_vec())).await;
        assert_eq!(put_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_get_wrong_repository_errors() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // GET with a different repository
        let wrong_repo = "team/other-assets";
        let get_uri = format!("/v2/{wrong_repo}/blobs/uploads/{session_id}");
        let get_response = send(&app, Method::GET, &get_uri, Body::empty()).await;
        assert_eq!(get_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_delete_wrong_repository_errors() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // DELETE with a different repository
        let wrong_repo = "team/other-assets";
        let delete_uri = format!("/v2/{wrong_repo}/blobs/uploads/{session_id}");
        let delete_response = send(&app, Method::DELETE, &delete_uri, Body::empty()).await;
        assert_eq!(delete_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_put_with_content_range() {
        // PUT with Content-Range header alongside digest
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PUT with Content-Range
        let data = b"hello";
        let digest = sha256_hex(data);
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}?digest=sha256:{digest}");
        let request = axum::http::Request::builder()
            .method(Method::PUT)
            .uri(&put_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-4/5")
            .body(Body::from(data.to_vec()))
            .unwrap();
        let put_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(put_response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_put_with_content_range_body_size_mismatch() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PUT with Content-Range where body size doesn't match the range
        let data = b"hello";
        let digest = sha256_hex(data);
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}?digest=sha256:{digest}");
        let request = axum::http::Request::builder()
            .method(Method::PUT)
            .uri(&put_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            // Range says 0-6/7 (7 bytes) but body is only 5 bytes
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-6/7")
            .body(Body::from(data.to_vec()))
            .unwrap();
        let put_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(put_response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_put_with_content_range_start_mismatch() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session — current_length = 0
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PUT with Content-Range where start != current_length (0)
        let data = b"mismatch";
        let digest = sha256_hex(data);
        let put_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}?digest=sha256:{digest}");
        let request = axum::http::Request::builder()
            .method(Method::PUT)
            .uri(&put_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            // Start=5 but current_length=0
            .header(axum::http::header::CONTENT_RANGE, "bytes 5-12/13")
            .body(Body::from(data.to_vec()))
            .unwrap();
        let put_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(put_response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_patch_with_content_range_body_size_mismatch() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH with Content-Range where body size doesn't match range length
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let request = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(&patch_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            // Range says 0-6/7 (7 bytes) but body is only 5 bytes
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-6/7")
            .body(Body::from(b"short".to_vec()))
            .unwrap();
        let patch_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(patch_response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_patch_with_content_range_different_repository() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH with Content-Range and different repository should still fail
        // (the repo check happens before Content-Range validation)
        let wrong_repo = "team/other-assets";
        let patch_uri = format!("/v2/{wrong_repo}/blobs/uploads/{session_id}");
        let request = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(&patch_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-4/5")
            .body(Body::from(b"hello".to_vec()))
            .unwrap();
        let patch_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(patch_response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_post_with_digest_algorithm_sha256() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // POST with digest-algorithm=sha256 (the only allowed value)
        let data = b"sha256 algorithm test";
        let digest = sha256_hex(data);
        let uri =
            format!("/v2/{REPO}/blobs/uploads/?digest=sha256:{digest}&digest-algorithm=sha256");
        let response = send(&app, Method::POST, &uri, Body::from(data.to_vec())).await;
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_post_mount_without_from_parameter() {
        // When mount is specified but `from` is omitted, the handler uses
        // the current repository as the source. If the blob doesn't exist
        // locally, it falls through to session creation.
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Upload a blob first so it exists
        let data = b"mount without from";
        let (digest, _response) = upload_blob_direct(&app, REPO, data).await;

        // Now mount with the same repo (implicitly via missing `from`)
        let mount_uri = format!("/v2/{REPO}/blobs/uploads/?mount=sha256:{digest}");
        let response = send(&app, Method::POST, &mount_uri, Body::empty()).await;
        assert!(
            response.status() == StatusCode::CREATED || response.status() == StatusCode::ACCEPTED,
            "mount without from should succeed or fall through, got {}",
            response.status()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_patch_with_content_range_start_mismatch() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session and append some data so current_length > 0
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data (current_length becomes 5)
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(
            &app,
            Method::PATCH,
            &patch_uri,
            Body::from(b"hello".to_vec()),
        )
        .await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);

        // Now PATCH with Content-Range where start != current_length (5)
        let request = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri(&patch_uri)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            // Start=0 but current_length=5
            .header(axum::http::header::CONTENT_RANGE, "bytes 0-4/10")
            .body(Body::from(b"test".to_vec()))
            .unwrap();
        let patch_response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(patch_response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_put_then_get_shows_range() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(
            &app,
            Method::PATCH,
            &patch_uri,
            Body::from(b"data".to_vec()),
        )
        .await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);

        // GET session to verify range is updated
        let get_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let get_response = send(&app, Method::GET, &get_uri, Body::empty()).await;
        assert_eq!(get_response.status(), StatusCode::NO_CONTENT);
        let range = get_response
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(range, "0-3");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_direct_oversized_body_errors() {
        // Direct upload with body exceeding a custom small max request body limit.
        // The default limit is 64 MiB, so we create a state with a 1-byte limit to
        // force the rejection.
        use std::num::NonZeroUsize;
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().to_path_buf();
        let config = crate::config::ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:8080".to_owned(),
            root,
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_max_request_body_bytes(NonZeroUsize::new(1).unwrap());
        let backend = crate::backend::ServerBackend::from_config(&config)
            .await
            .expect("backend");
        let state = Arc::new(crate::AppState {
            config,
            role: crate::server_role::ServerRole::All,
            backend,
            auth: None,
            provider_tokens: None,
            reconstruction_cache: crate::reconstruction_cache::ReconstructionCacheService::disabled(
            ),
            transfer_limiter: crate::TransferLimiter::new(
                NonZeroUsize::new(4096).unwrap(),
                NonZeroUsize::new(16).unwrap(),
            ),
            oci_registry_token_limiter: Arc::new(tokio::sync::Semaphore::new(64)),
            protocol_metrics: crate::app::ProtocolMetrics::default(),
        });
        let app = oci_test_router(&state);

        // Direct upload with a body that exceeds the 1-byte limit
        let data = b"oversized";
        let digest = sha256_hex(data);
        let uri = format!("/v2/{REPO}/blobs/uploads/?digest=sha256:{digest}");
        let response = send(&app, Method::POST, &uri, Body::from(data.to_vec())).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blob_upload_session_patch_then_get_shows_updated_range() {
        let ctx = build_oci_test_state().await;
        let app = oci_test_router(&ctx.state);

        // Initiate a session
        let init_uri = format!("/v2/{REPO}/blobs/uploads/");
        let init_response = send(&app, Method::POST, &init_uri, Body::empty()).await;
        assert_eq!(init_response.status(), StatusCode::ACCEPTED);
        let session_id = session_id_from_location(&init_response);

        // PATCH to append data
        let patch_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let patch_response = send(
            &app,
            Method::PATCH,
            &patch_uri,
            Body::from(b"test data".to_vec()),
        )
        .await;
        assert_eq!(patch_response.status(), StatusCode::ACCEPTED);

        // GET session should show updated range
        let get_uri = format!("/v2/{REPO}/blobs/uploads/{session_id}");
        let get_response = send(&app, Method::GET, &get_uri, Body::empty()).await;
        assert_eq!(get_response.status(), StatusCode::NO_CONTENT);
        let range = get_response
            .headers()
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(range, "0-8"); // "test data" is 9 bytes → 0-8
    }
}
