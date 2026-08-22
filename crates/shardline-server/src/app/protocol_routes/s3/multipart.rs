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
//! Locking: the adapter's process-global session lock ([`lock_upload_sessions`])
//! is held only for session validation and metadata/quota mutations — never
//! across a network body stream, so a slow `UploadPart` or `Complete` cannot
//! stall other tenants' session operations (F-10). Part-file writes and reads
//! are instead serialized with the expiry sweep (which deletes session
//! directories) and with each other by a per-session lock keyed by the upload
//! id ([`acquire_session_part_lock`]): concurrent `UploadPart`s for the same
//! session serialize there, `CompleteMultipartUpload` reads the part files
//! under it, and the adapter's sweep takes it before removing a session
//! directory. The adapter's `store_part_locked` / `delete_session_locked`
//! variants are used to avoid re-acquiring the global lock for metadata
//! mutations.

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    body::{Body, HttpBody},
    http::{HeaderMap, HeaderValue, StatusCode, header::ETAG},
    response::{IntoResponse, Response},
};
use md5::{Digest, Md5};
use shardline_index::S3ObjectEntry;
use shardline_s3_adapter::{
    CompleteMultipartUploadResult, InitiateMultipartUploadResult, S3Error, S3SessionError,
    acquire_session_part_lock, create_session, delete_session_locked, lock_session_parts,
    lock_upload_sessions, parse_complete_multipart_parts, part_file_path, read_session,
    store_part_locked, validate_part_quota_locked,
};
use tokio::io::AsyncWriteExt;

use crate::{
    ServerError,
    app::AppState,
    metrics,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{
    S3ObjectContext, acquire_object_upload_lock, aws_chunked, object, s3_xml_content_type,
};

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

/// Translates an adapter session-store failure into the S3 error envelope.
///
/// The global active-part-file cap is a server resource limit with no S3
/// protocol code; it is surfaced as a `429 TooManyParts` envelope carrying the
/// [`ServerError::S3UploadTooManyParts`] message. Every other adapter error
/// keeps its existing S3 translation.
fn store_error_to_s3(error: S3SessionError) -> S3Error {
    match error {
        S3SessionError::TooManyPartFiles => S3Error {
            code: "TooManyParts",
            message: ServerError::S3UploadTooManyParts.to_string(),
            status: StatusCode::TOO_MANY_REQUESTS,
        },
        other @ S3SessionError::Io(_)
        | other @ S3SessionError::Json(_)
        | other @ S3SessionError::NotFound
        | other @ S3SessionError::InvalidUploadId
        | other @ S3SessionError::InvalidPartNumber
        | other @ S3SessionError::MissingPart(_)
        | other @ S3SessionError::TooManySessions
        | other @ S3SessionError::SessionQuotaExceeded
        | other @ S3SessionError::AggregateQuotaExceeded
        | other @ S3SessionError::Overflow
        | other @ S3SessionError::BlockingTask(_) => S3Error::from(other),
    }
}

/// `POST /{bucket}/{*key}?uploads` — `CreateMultipartUpload`.
///
/// Creates a disk-persisted session and responds `200` with the
/// `InitiateMultipartUploadResult` XML envelope carrying the opaque upload id.
pub(super) async fn s3_create_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    // S3 user metadata is supplied at CreateMultipartUpload and applied to the
    // completed object.
    let user_metadata = object::capture_user_metadata(headers);
    let upload_id = create_session(
        state.config.root_dir(),
        &context.bucket,
        &context.key,
        &context.scope_namespace,
        state.config.s3_upload_session_ttl_seconds(),
        state.config.s3_upload_max_active_sessions(),
        state.config.s3_upload_total_max_bytes(),
        user_metadata,
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
/// UploadPart accepts ANY body size for any part number `1..=MAX_S3_PART_NUMBER`
/// (matching S3: the 5 MiB minimum is enforced only at CompleteMultipartUpload
/// for every part except the last). The per-session and aggregate byte quotas
/// and the global active-part-file cap are enforced under the session lock:
/// against the declared part length BEFORE the file is written (no
/// write-then-delete) and again against the streamed size at
/// `store_part_locked`. The expiry sweep remains as belt-and-braces.
pub(super) async fn s3_upload_part(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    part_number: u32,
    upload_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();
    let session_quota = state.config.s3_upload_session_max_bytes();
    let total_quota = state.config.s3_upload_total_max_bytes();
    let part_file_cap = state.config.s3_upload_max_active_part_files();

    // Global session lock: session validation and the metadata/quota mutation
    // (`store_part_locked`) below only. The lock is NOT held across the body
    // stream — a slow part body must not stall other tenants' session
    // operations (F-10); the part-file write is serialized per-session
    // instead.
    let _session_lock = lock_upload_sessions(root).await?;

    // The session must exist, be unexpired, and belong to this bucket/key.
    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }

    // The part's current contribution to the session total (an overwrite
    // replaces the old size), used for the pre-write quota projection and the
    // undeclared-length body ceiling.
    let previous_size = session
        .parts
        .get(&part_number)
        .map_or(0_u64, |part| part.size_bytes);
    let session_total = session
        .parts
        .values()
        .fold(0_u64, |total, part| total.saturating_add(part.size_bytes));
    let session_remaining = session_quota
        .get()
        .saturating_sub(session_total.saturating_sub(previous_size));

    // The declared decoded part length, when the client provides one
    // (aws-chunked framing or a `Content-Length`/framed size hint). `None`
    // means the length is unknown until the stream is drained.
    let expected_len: Option<u64> = if aws_chunked::is_aws_chunked(headers) {
        aws_chunked::declared_decoded_content_length(headers)
    } else {
        let size_hint = body.size_hint();
        size_hint.exact().or_else(|| size_hint.upper())
    };

    // F-19: enforce the per-session and aggregate byte quotas and the global
    // active-part-file cap against the declared part length BEFORE any bytes
    // are written, so an over-quota/over-cap part never materializes a file.
    // Runs under the global session lock, exactly like `store_part_locked`'s
    // accounting; the quotas are re-checked against the streamed size after
    // the write. A cap rejection is surfaced as a clean 429.
    if let Some(length) = expected_len {
        validate_part_quota_locked(
            root,
            upload_id,
            part_number,
            length,
            ttl,
            session_quota,
            total_quota,
            part_file_cap,
        )
        .await
        .map_err(store_error_to_s3)?;
    }

    // Parts larger than SHARDLINE_S3_MAX_PART_BYTES are rejected. For
    // undeclared-length bodies the reader ceiling is additionally clamped to
    // the remaining session quota, so an over-quota chunked stream aborts
    // mid-stream instead of fully materializing on disk (F-19b).
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let body_ceiling = if expected_len.is_some() {
        max_bytes
    } else {
        let remaining = usize::try_from(session_remaining).map_err(|_error| S3Error::internal())?;
        let clamped = remaining.min(max_bytes.get());
        NonZeroUsize::new(clamped).ok_or_else(entity_too_large)?
    };
    let mut body = match RequestBodyReader::from_body(body, body_ceiling) {
        Ok(reader) => reader,
        Err(ServerError::RequestBodyTooLarge) => return Err(entity_too_large()),
        Err(error) => return Err(S3Error::from(error)),
    };

    // Real clients stream multipart parts with AWS chunked encoding; decode
    // the framing so the part file holds the actual payload. The decoded size
    // is enforced by the decoder against the part ceiling.
    if aws_chunked::is_aws_chunked(headers) {
        let max_bytes_u64 =
            u64::try_from(body_ceiling.get()).map_err(|_error| S3Error::internal())?;
        if let Some(decoded) = aws_chunked::declared_decoded_content_length(headers)
            && decoded > max_bytes_u64
        {
            return Err(entity_too_large());
        }
        body = RequestBodyReader::from_stream(aws_chunked::decode_aws_chunked(body, max_bytes_u64));
    }

    // Take the per-session lock while still holding the global lock (the
    // sweep takes them in the same order), then drop the global lock before
    // streaming: the part-file write below is protected from the sweep and
    // from a concurrent Complete by the per-session lock alone, so other
    // tenants' session operations are never blocked on this body (F-10).
    let part_lock = acquire_session_part_lock(upload_id);
    let _part_guard = part_lock.lock().await;
    let part_file_guard = lock_session_parts(root, upload_id).await?;
    drop(_session_lock);

    // Stream the body to the part file (overwrite semantics). A mid-stream
    // abort (over-quota ceiling or over-size) removes the partial file, so a
    // rejected part never materializes.
    let part_path = part_file_path(root, upload_id, part_number)?;
    let mut file = match tokio::fs::File::create(&part_path).await {
        Ok(file) => file,
        // The session directory was removed (sweep/Complete) after
        // validation; report the session as gone rather than a 500.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(S3Error::no_such_upload());
        }
        Err(error) => return Err(io_to_s3(error)),
    };
    let streamed: Result<u64, ServerError> = async {
        let mut total_bytes = 0_u64;
        while let Some(chunk) = body.next_bytes().await? {
            total_bytes = total_bytes
                .checked_add(u64::try_from(chunk.len()).map_err(ServerError::from)?)
                .ok_or(ServerError::Overflow)?;
            file.write_all(&chunk).await.map_err(ServerError::from)?;
        }
        file.flush().await.map_err(ServerError::from)?;
        Ok(total_bytes)
    }
    .await;
    let total_bytes = match streamed {
        Ok(total_bytes) => total_bytes,
        Err(error) => {
            // A mid-stream quota/size abort must not leave a partial file.
            let _ignored = tokio::fs::remove_file(&part_path).await;
            return Err(S3Error::from(error));
        }
    };

    // The file is fully written; release the per-session lock (never await
    // the global lock while holding it) and do the metadata + quota
    // accounting back under the global lock. The quotas (and the global
    // active-part-file cap) are re-checked against the actually-streamed size
    // here; a rejection must not leave an orphaned part file behind.
    // Release BOTH layers of the per-session lock before waiting for the
    // global lock. Keeping the cross-process file lock here lets a sweep take
    // the global lock and then wait for this file lock while this request
    // waits for the global lock: a lock-order cycle within one process.
    drop(part_file_guard);
    drop(_part_guard);
    let _global_lock = lock_upload_sessions(root).await?;
    if let Err(error) = store_part_locked(
        root,
        upload_id,
        part_number,
        total_bytes,
        ttl,
        state.config.s3_upload_session_max_bytes(),
        state.config.s3_upload_total_max_bytes(),
        part_file_cap,
    )
    .await
    {
        // A quota/cap rejection must not leave an orphaned part file behind.
        let _ignored = tokio::fs::remove_file(&part_path).await;
        return Err(store_error_to_s3(error));
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
/// minimum for every part EXCEPT the last (the part with the highest part
/// number in the submitted list — the client's part set must exactly match
/// the uploaded parts, so the highest uploaded number IS the last part),
/// then streams the part files in order through ONE `FileUploadIngestor`
/// pass (whole-object dedup), removes the session, and atomically swaps the
/// object (upload-then-swap, like `PutObject`): the new record is streamed
/// first, then the listing-index row is upserted and any stale direct object
/// dropped. Responds `200` with the `CompleteMultipartUploadResult` XML
/// envelope (`ETag` = the BLAKE3 root content hash — identical to a single
/// `PutObject` of the same bytes).
pub(super) async fn s3_complete_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    upload_id: &str,
    body: Body,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // Global session lock: session validation, request parsing, and the
    // part-set checks below (all bounded work). The part-file ingest itself is
    // NOT run under this lock — it is serialized per-session so a slow
    // completion cannot stall other tenants' session operations (F-10).
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    // User metadata was captured at CreateMultipartUpload; apply it now.
    let user_metadata = session.user_metadata.clone();

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

    // S3's 5 MiB minimum applies to every part except the LAST one — the part
    // with the highest part number in the submitted parts list (a small part
    // is exempt no matter its number, and a small non-final part is rejected
    // with EntityTooSmall). The request part set was verified to exactly
    // match `session.parts` above, so `max_part` IS the last part.
    let min_part_bytes = state.config.s3_min_part_bytes().get();
    for (part_number, part) in &session.parts {
        if *part_number < max_part && part.size_bytes < min_part_bytes {
            return Err(S3Error::entity_too_small());
        }
    }

    // Take the per-session lock while still holding the global lock (the
    // sweep takes them in the same order), then drop the global lock: a
    // concurrent UploadPart for this session (and the expiry sweep) serialize
    // on the per-session lock while we open and ingest the part files, so the
    // ingest below cannot race a part write or a directory delete (F-10).
    let part_lock = acquire_session_part_lock(upload_id);
    let _part_guard = part_lock.lock().await;
    let part_file_guard = lock_session_parts(root, upload_id).await?;
    drop(_session_lock);

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
    // the index row and drop any stale direct object. The MD5 tee computes the
    // S3 ETag (hex MD5 of the assembled object) as the parts stream.
    let hasher = Arc::new(Mutex::new(Md5::new()));
    let parts_reader = parts_reader.with_md5_tee(hasher.clone());
    let start = Instant::now();
    let uploaded = state
        .backend
        .put_s3_object_stream(&context.object_key, parts_reader)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("s3", uploaded.total_bytes, elapsed, true);
    let etag = object::md5_hasher_hex(&hasher);

    // The ingest is done; release the per-session lock (never await the
    // global lock while holding it) and consume the session under the global
    // lock.
    // The sweep's order is global -> process-local part -> part file. Release
    // both per-session layers before reacquiring global to preserve that order.
    drop(part_file_guard);
    drop(_part_guard);
    let _global_lock = lock_upload_sessions(root).await?;

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
            etag: etag.clone(),
            user_metadata,
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
        etag,
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
    context: &S3ObjectContext<'_>,
    upload_id: &str,
) -> Result<Response, S3Error> {
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // Hold the global session lock for validation and the delete, plus the
    // per-session lock so the session directory is not removed while a
    // concurrent UploadPart is mid-write into a part file (and vice versa);
    // the sweep takes both locks in the same order.
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    let part_lock = acquire_session_part_lock(upload_id);
    let _part_guard = part_lock.lock().await;
    let _part_file_guard = lock_session_parts(root, upload_id).await?;
    delete_session_locked(root, upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
