//! S3 multipart upload handlers (Lane 4): `CreateMultipartUpload`,
//! `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`.
//!
//! These are query-param dispatches on the existing `/{bucket}/{*key}` routes
//! (`POST ?uploads`, `PUT ?partNumber&uploadId`, `POST ?uploadId`,
//! `DELETE ?uploadId`) — see `object.rs`. Part bodies stream to per-part files
//! under the session directory; `CompleteMultipartUpload` feeds every part
//! file through ONE CDC ingest pass (`RequestBodyReader::from_reader_chain` +
//! `put_s3_object_stream`), producing a single `FileRecord` whose BLAKE3 root
//! content hash equals a single `PutObject` of the same bytes.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use axum::{
    body::Body,
    http::{HeaderValue, StatusCode, header::ETAG},
    response::{IntoResponse, Response},
};
use shardline_index::S3ObjectEntry;
use shardline_s3_adapter::{
    CompleteMultipartUploadResult, InitiateMultipartUploadResult, S3Error, create_session,
    delete_session, parse_complete_multipart_parts, part_file_path, read_session, store_part,
};
use tokio::io::AsyncWriteExt;

use crate::{
    ServerError,
    app::AppState,
    metrics,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{S3ObjectContext, s3_xml_content_type};

/// Maps a local I/O failure to the S3 internal-error envelope.
fn io_to_s3(error: std::io::Error) -> S3Error {
    S3Error::from(ServerError::Io(error))
}

/// The `413 EntityTooLarge` envelope for an oversized part body.
fn entity_too_large() -> S3Error {
    S3Error {
        code: "EntityTooLarge",
        message: "Your proposed upload exceeds the maximum allowed part size".to_owned(),
        status: StatusCode::PAYLOAD_TOO_LARGE,
    }
}

/// `POST /{bucket}/{*key}?uploads` — `CreateMultipartUpload`.
///
/// Creates a disk-persisted session and responds `200` with the
/// `InitiateMultipartUploadResult` XML envelope carrying the opaque upload id.
pub(super) async fn s3_create_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
) -> Result<Response, S3Error> {
    let upload_id = create_session(
        state.config.root_dir(),
        &context.bucket,
        &context.key,
        &context.scope_namespace,
        state.config.s3_upload_session_ttl_seconds(),
        state.config.s3_upload_max_active_sessions(),
    )
    .await?;
    let xml = InitiateMultipartUploadResult {
        bucket: context.bucket.clone(),
        key: context.key.clone(),
        upload_id,
    }
    .to_xml();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// `PUT /{bucket}/{*key}?partNumber=N&uploadId=U` — `UploadPart`.
///
/// Streams the part body to the session's `part-{N}` file (overwrite: the
/// last upload of a part number wins) and responds `200` with an opaque
/// per-part ETag (`"<upload_id>-<N>"`) the client echoes back in Complete.
pub(super) async fn s3_upload_part(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
    part_number: u32,
    upload_id: &str,
    body: Body,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // The session must exist, be unexpired, and belong to this bucket/key.
    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }

    // Parts larger than SHARDLINE_S3_MAX_PART_BYTES are rejected.
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let mut body = match RequestBodyReader::from_body(body, max_bytes) {
        Ok(reader) => reader,
        Err(ServerError::RequestBodyTooLarge) => return Err(entity_too_large()),
        Err(error) => return Err(S3Error::from(error)),
    };

    // Stream the body to the part file (overwrite semantics).
    let part_path = part_file_path(root, upload_id, part_number)?;
    let mut file = tokio::fs::File::create(&part_path)
        .await
        .map_err(io_to_s3)?;
    let mut total_bytes = 0_u64;
    while let Some(chunk) = body.next_bytes().await? {
        total_bytes = total_bytes
            .checked_add(u64::try_from(chunk.len()).map_err(ServerError::from)?)
            .ok_or(ServerError::Overflow)?;
        file.write_all(&chunk).await.map_err(io_to_s3)?;
    }
    file.flush().await.map_err(io_to_s3)?;

    store_part(root, upload_id, part_number, total_bytes, ttl).await?;

    // Opaque per-part ETag (documented deviation: the client echoes it back in
    // Complete; we ignore the echoed value).
    let etag = format!("\"{upload_id}-{part_number}\"");
    let mut response = StatusCode::OK.into_response();
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&etag).map_err(|_error| S3Error::internal())?,
    );
    Ok(response)
}

/// `POST /{bucket}/{*key}?uploadId=U` — `CompleteMultipartUpload`.
///
/// Validates that every part `1..=N` was uploaded, then streams the part files
/// in order through ONE `FileUploadIngestor` pass (whole-object dedup), removes
/// the session, upserts the S3 listing-index row, and responds `200` with the
/// `CompleteMultipartUploadResult` XML envelope (`ETag` = the BLAKE3 root
/// content hash — identical to a single `PutObject` of the same bytes).
pub(super) async fn s3_complete_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
    upload_id: &str,
    body: Body,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }

    // Parse the Complete request body minimally (the client echoes the part
    // numbers/etags it uploaded).
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    let bytes = read_body_to_bytes(&mut reader)
        .await
        .map_err(S3Error::from)?;
    let body_str = std::str::from_utf8(&bytes).map_err(|_error| S3Error::invalid_part())?;
    let mut requested_parts = parse_complete_multipart_parts(body_str)?;

    // The client's part list must exactly match the uploaded parts.
    requested_parts.sort_unstable();
    let uploaded_parts: Vec<u32> = session.parts.keys().copied().collect();
    if requested_parts != uploaded_parts {
        return Err(S3Error::invalid_part());
    }

    // Every part 1..=N must be present (missing → InvalidPart).
    let Some((&max_part, _)) = session.parts.last_key_value() else {
        return Err(S3Error::invalid_part());
    };

    // Build one continuous stream from the part files, in order.
    let chunk_size = state.config.chunk_size().get();
    let mut part_files = Vec::with_capacity(session.parts.len());
    for part in 1..=max_part {
        if !session.parts.contains_key(&part) {
            return Err(S3Error::invalid_part());
        }
        let path = part_file_path(root, upload_id, part)?;
        let file = tokio::fs::File::open(&path).await.map_err(io_to_s3)?;
        part_files.push(file);
    }
    let parts_reader = RequestBodyReader::from_reader_chain(part_files, chunk_size);

    // S3 overwrite semantics (same as PutObject): delete-then-upload.
    match state.backend.object_length(&context.object_key).await {
        Ok(_length) => {
            let _existing = state
                .backend
                .delete_object_if_present(&context.object_key)
                .await?;
        }
        Err(ServerError::NotFound) => {}
        Err(error) => return Err(S3Error::from(error)),
    }

    let start = Instant::now();
    let uploaded = state
        .backend
        .put_s3_object_stream(&context.object_key, parts_reader)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("s3", uploaded.total_bytes, elapsed, true);

    // The session is consumed by the completion; a failed cleanup is swept at
    // startup or on the next session creation.
    let _ignored = delete_session(root, upload_id).await;

    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_error| S3Error::internal())?;
    state
        .backend
        .upsert_s3_object(&S3ObjectEntry {
            scope_namespace: context.scope_namespace.clone(),
            object_key: context.key.clone(),
            file_id: uploaded.file_id,
            size_bytes: uploaded.total_bytes,
            content_hash: uploaded.content_hash.clone(),
            updated_at_unix_seconds: now,
        })
        .await?;

    let xml = CompleteMultipartUploadResult {
        bucket: context.bucket.clone(),
        key: context.key.clone(),
        etag: uploaded.content_hash,
    }
    .to_xml();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// `DELETE /{bucket}/{*key}?uploadId=U` — `AbortMultipartUpload`.
///
/// Removes the session directory and all part files (no object or index row
/// exists yet) and responds `204`. Unknown upload ids are `404 NoSuchUpload`.
pub(super) async fn s3_abort_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
    upload_id: &str,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();
    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    delete_session(root, upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
