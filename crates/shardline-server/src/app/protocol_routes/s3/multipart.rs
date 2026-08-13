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
//!
//! Every mutating operation holds the adapter's global session lock for its
//! whole duration — the same pattern the OCI frontend uses — so a concurrent
//! session sweep (which also takes that lock) can never remove the session
//! directory mid-write, and concurrent `UploadPart`/`Complete`/`Abort` calls
//! for the same session serialize. The adapter's `store_part_locked` /
//! `delete_session_locked` variants are used to avoid re-acquiring the lock.

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header::ETAG},
    response::{IntoResponse, Response},
};
use shardline_index::S3ObjectEntry;
use shardline_s3_adapter::{
    CompleteMultipartUploadResult, InitiateMultipartUploadResult, S3Error, create_session,
    delete_session_locked, lock_upload_sessions, parse_complete_multipart_parts, part_file_path,
    read_session, store_part_locked,
};
use tokio::io::AsyncWriteExt;

use crate::{
    ServerError,
    app::AppState,
    metrics,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{S3ObjectContext, acquire_object_upload_lock, aws_chunked, s3_xml_content_type};

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
        state.config.s3_upload_total_max_bytes(),
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
/// The per-session and aggregate byte quotas are enforced under the session
/// lock; the S3 5 MiB minimum is enforced for non-final parts at Complete
/// (matching S3, which validates at completion).
pub(super) async fn s3_upload_part(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
    part_number: u32,
    upload_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // Hold the global session lock for the whole mutation so a concurrent
    // sweep cannot remove the session directory mid-write (F-s3-4).
    let _session_lock = lock_upload_sessions(root).await?;

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

    // Real clients stream multipart parts with AWS chunked encoding; decode
    // the framing so the part file holds the actual payload. The decoded size
    // is enforced by the decoder against the part ceiling.
    if aws_chunked::is_aws_chunked(headers) {
        let max_bytes_u64 = u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?;
        if let Some(decoded) = aws_chunked::declared_decoded_content_length(headers)
            && decoded > max_bytes_u64
        {
            return Err(entity_too_large());
        }
        body = RequestBodyReader::from_stream(aws_chunked::decode_aws_chunked(
            body,
            u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?,
        ));
    }

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

    if let Err(error) = store_part_locked(
        root,
        upload_id,
        part_number,
        total_bytes,
        ttl,
        state.config.s3_upload_session_max_bytes(),
        state.config.s3_upload_total_max_bytes(),
    )
    .await
    {
        // A quota rejection must not leave an orphaned part file behind.
        let _ignored = tokio::fs::remove_file(&part_path).await;
        return Err(S3Error::from(error));
    }

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
/// Validates the echoed part list against the session, enforces S3's 5 MiB
/// minimum for every non-final part, then streams the part files in order
/// through ONE `FileUploadIngestor` pass (whole-object dedup), removes the
/// session, and atomically swaps the object (upload-then-swap, like
/// `PutObject`): the new record is streamed first, then the listing-index row
/// is upserted and any stale direct object dropped. Responds `200` with the
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

    // Hold the global session lock so a concurrent UploadPart cannot write
    // into a part file while completion is ingesting it, and a concurrent
    // sweep cannot remove the session mid-completion.
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }

    // Parse the Complete request body (the client echoes the part
    // numbers/etags it uploaded; ETags are opaque and ignored).
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    let bytes = read_body_to_bytes(&mut reader)
        .await
        .map_err(S3Error::from)?;
    let body_str = std::str::from_utf8(&bytes).map_err(|_error| S3Error::invalid_part())?;
    let requested = parse_complete_multipart_parts(body_str)?;

    // The client's part set must exactly match the uploaded parts.
    let uploaded_parts: std::collections::BTreeSet<u32> = session.parts.keys().copied().collect();
    if requested.part_numbers() != &uploaded_parts {
        return Err(S3Error::invalid_part());
    }

    // Every part 1..=N must be present (missing → InvalidPart).
    let Some(&max_part) = session.parts.keys().last() else {
        return Err(S3Error::invalid_part());
    };

    // S3's 5 MiB minimum applies to every part except the final one.
    let min_part_bytes = state.config.s3_min_part_bytes().get();
    for (part_number, part) in &session.parts {
        if *part_number < max_part && part.size_bytes < min_part_bytes {
            return Err(S3Error::entity_too_small());
        }
    }

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

    // Serialize concurrent overwrites of the target object key.
    let object_lock = acquire_object_upload_lock(context.object_key.as_str());
    let _object_guard = object_lock.lock().await;

    // Atomic overwrite (same as PutObject): stream the new record FIRST (a
    // failure commits nothing and the old object stays readable), then swap
    // the index row and drop any stale direct object.
    let start = Instant::now();
    let uploaded = state
        .backend
        .put_s3_object_stream(&context.object_key, parts_reader)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("s3", uploaded.total_bytes, elapsed, true);

    // The session is consumed by the completion; a failed cleanup is swept at
    // startup or on the next session creation.
    let _ignored = delete_session_locked(root, upload_id).await;

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
    let _stale_direct = state
        .backend
        .delete_direct_object_if_present(&context.object_key)
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

    // Hold the global session lock so the session directory is not deleted
    // while a concurrent UploadPart/Complete is writing to it (and vice versa).
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    delete_session_locked(root, upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
