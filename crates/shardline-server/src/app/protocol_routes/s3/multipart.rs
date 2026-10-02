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
//! Locking: the adapter's per-root session lock ([`lock_upload_sessions`]) is
//! held only for session validation and metadata/quota mutations — never
//! across a network body stream, so a slow `UploadPart` or `Complete` cannot
//! stall other tenants' session operations (F-10). Part-file writes and reads
//! are instead serialized with the expiry sweep (which deletes session
//! directories) and with each other by a per-session lock scoped to the
//! deployment root ([`acquire_session_part_lock_for_root`]): concurrent
//! `UploadPart`s for the same session serialize there,
//! `CompleteMultipartUpload` reads the part files under it, and the adapter's
//! sweep takes it before removing a session directory. The adapter's
//! `store_part_locked` / `delete_session_locked` variants are used to avoid
//! re-acquiring the per-root lock for metadata
//! mutations.

use std::{
    num::{NonZeroU64, NonZeroUsize},
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    body::{Body, HttpBody},
    http::{HeaderMap, HeaderValue, StatusCode, header::ETAG},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use shardline_index::{
    CreateResumableSessionOutcome, PublishResumablePartOutcome, ResourceLockKey, ResumableSession,
    ResumableSessionProtocol, ResumableSessionState, S3ObjectEntry, S3PublishCondition,
};
use shardline_s3_adapter::{
    CompleteMultipartUploadResult, InitiateMultipartUploadResult, MultipartPart, PartQuotaLimits,
    S3Error, S3SessionError, acquire_session_part_lock_for_root, create_session,
    delete_session_locked, lock_session_parts, lock_upload_sessions, new_upload_id,
    parse_complete_multipart_parts, parse_content_md5, read_conditional_headers,
    read_session_locked, session_dir, store_versioned_part_locked, stored_part_file_path,
    validate_part_quota_for_session_locked, versioned_part_file_name,
};
use shardline_storage::ObjectKey;
use tokio::io::AsyncWriteExt;

use crate::{
    ServerError,
    app::AppState,
    metrics,
    object_store::{
        local_s3_resumable_parts_reader, materialize_object_to_file, s3_resumable_parts_reader,
        stage_reader_content_addressed_s3,
    },
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

use super::{
    S3ObjectContext, acquire_object_upload_lock_for_root, aws_chunked, object, s3_xml_content_type,
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
        | other @ S3SessionError::Reliability(_)
        | other @ S3SessionError::BlockingTask(_) => S3Error::from(other),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableS3SessionAttributes {
    bucket: String,
    user_metadata: Vec<(String, String)>,
}

const fn durable_sessions_enabled(state: &AppState) -> bool {
    state.backend.supports_fenced_s3_publication()
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
    if durable_sessions_enabled(state) {
        let upload_id = new_upload_id();
        let expires_at = state
            .backend
            .postgres_now()
            .await?
            .checked_add(std::time::Duration::from_secs(
                state.config.s3_upload_session_ttl_seconds().get(),
            ))
            .ok_or_else(S3Error::internal)?;
        let attributes = serde_json::to_string(&DurableS3SessionAttributes {
            bucket: context.bucket.clone(),
            user_metadata,
        })
        .map_err(|_error| S3Error::internal())?;
        let session = ResumableSession::new(
            upload_id.clone(),
            ResumableSessionProtocol::S3Multipart,
            context.scope_namespace.clone(),
            context.key.clone(),
            expires_at,
        )
        .with_attributes_json(attributes)
        .map_err(|_error| S3Error::internal())?;
        match state
            .backend
            .create_resumable_session_bounded(
                &session,
                state.config.s3_upload_max_active_sessions().get(),
            )
            .await?
        {
            CreateResumableSessionOutcome::Created => {}
            CreateResumableSessionOutcome::AlreadyExists => return Err(S3Error::internal()),
            CreateResumableSessionOutcome::TooManyActive => {
                return Err(S3Error::from(S3SessionError::TooManySessions));
            }
        }
        let xml = InitiateMultipartUploadResult {
            bucket: context.bucket.clone(),
            key: context.key.clone(),
            upload_id,
        }
        .to_xml();
        return Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
            xml,
        )
            .into_response());
    }
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
async fn validate_local_part_file(
    path: &std::path::Path,
    expected_size: u64,
    expected_hash: &str,
) -> Result<(), S3Error> {
    let mut reader = tokio::fs::File::open(path).await.map_err(io_to_s3)?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 65536];
    loop {
        let count = tokio::io::AsyncReadExt::read(&mut reader, &mut buffer)
            .await
            .map_err(io_to_s3)?;
        if count == 0 {
            break;
        }
        let bytes = buffer.get(..count).ok_or_else(S3Error::internal)?;
        size = size
            .checked_add(u64::try_from(count).map_err(|_error| S3Error::internal())?)
            .ok_or_else(S3Error::internal)?;
        if size > expected_size {
            return Err(S3Error::internal());
        }
        hasher.update(bytes);
    }
    if size != expected_size || hasher.finalize().to_hex().as_str() != expected_hash {
        return Err(S3Error::internal());
    }
    Ok(())
}

async fn local_part_md5_etag(
    path: &std::path::Path,
    expected_size: u64,
) -> Result<String, S3Error> {
    let mut reader = tokio::fs::File::open(path).await.map_err(io_to_s3)?;
    let mut hasher = Md5::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 65536];
    loop {
        let count = tokio::io::AsyncReadExt::read(&mut reader, &mut buffer)
            .await
            .map_err(io_to_s3)?;
        if count == 0 {
            break;
        }
        let bytes = buffer.get(..count).ok_or_else(S3Error::internal)?;
        size = size
            .checked_add(u64::try_from(count).map_err(|_error| S3Error::internal())?)
            .ok_or_else(S3Error::internal)?;
        if size > expected_size {
            return Err(S3Error::invalid_part());
        }
        hasher.update(bytes);
    }
    if size != expected_size {
        return Err(S3Error::invalid_part());
    }
    Ok(shardline_s3_adapter::etag_header(&hex::encode(
        hasher.finalize(),
    )))
}

pub(super) async fn s3_upload_part(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    part_number: u32,
    upload_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    if durable_sessions_enabled(state) {
        return durable_s3_upload_part(state, context, part_number, upload_id, headers, body).await;
    }
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
    let session = read_session_locked(root, upload_id, ttl).await?;
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
        validate_part_quota_for_session_locked(
            root,
            &session,
            part_number,
            length,
            ttl,
            PartQuotaLimits {
                session_max_bytes: session_quota,
                total_max_bytes: total_quota,
                max_active_part_files: part_file_cap,
            },
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

    if let Some(expected) = parse_content_md5(headers)? {
        body = body.with_expected_md5(expected);
    }
    let md5 = Arc::new(Mutex::new(Md5::new()));
    body = body.with_md5_tee(md5.clone());

    // Take the per-session lock while still holding the global lock (the
    // sweep takes them in the same order), then drop the global lock before
    // streaming: the part-file write below is protected from the sweep and
    // from a concurrent Complete by the per-session lock alone, so other
    // tenants' session operations are never blocked on this body (F-10).
    let part_lock = acquire_session_part_lock_for_root(root, upload_id);
    let _part_guard = part_lock.lock().await;
    let part_file_guard = lock_session_parts(root, upload_id).await?;
    drop(_session_lock);

    // New bytes are private until the session metadata points to their
    // immutable version. Interrupted or rejected overwrites preserve the
    // previously acknowledged part and its metadata.
    let directory = session_dir(root, upload_id)?;
    let temporary = tempfile::NamedTempFile::new_in(&directory).map_err(io_to_s3)?;
    let mut file = tokio::fs::File::from_std(temporary.reopen().map_err(io_to_s3)?);
    let mut content_hasher = blake3::Hasher::new();
    let mut total_bytes = 0_u64;
    while let Some(chunk) = body.next_bytes().await.map_err(S3Error::from)? {
        total_bytes = total_bytes
            .checked_add(u64::try_from(chunk.len()).map_err(|_error| S3Error::internal())?)
            .ok_or_else(S3Error::internal)?;
        content_hasher.update(&chunk);
        file.write_all(&chunk).await.map_err(io_to_s3)?;
    }
    file.flush().await.map_err(io_to_s3)?;
    file.sync_all().await.map_err(io_to_s3)?;
    drop(file);
    let digest = content_hasher.finalize().to_hex().to_string();
    let file_name = versioned_part_file_name(part_number, &digest)?;
    let etag = shardline_s3_adapter::etag_header(&object::md5_hasher_hex(&md5));
    let new_part = MultipartPart {
        size_bytes: total_bytes,
        file_name,
        etag: Some(etag.clone()),
    };

    // Reacquire in global -> per-session -> OS-file-lock order. Publication
    // and Complete/Sweep now observe one metadata/file version coherently.
    drop(part_file_guard);
    drop(_part_guard);
    let _global_lock = lock_upload_sessions(root).await?;
    let current = read_session_locked(root, upload_id, ttl).await?;
    if current.key != context.key || current.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    let publication_lock = acquire_session_part_lock_for_root(root, upload_id);
    let _publication_guard = publication_lock.lock().await;
    let _part_file_guard = lock_session_parts(root, upload_id).await?;
    validate_part_quota_for_session_locked(
        root,
        &current,
        part_number,
        total_bytes,
        ttl,
        PartQuotaLimits {
            session_max_bytes: session_quota,
            total_max_bytes: total_quota,
            max_active_part_files: part_file_cap,
        },
    )
    .await
    .map_err(store_error_to_s3)?;
    let final_path = stored_part_file_path(root, upload_id, part_number, &new_part)?;
    let newly_created = match temporary.persist_noclobber(&final_path) {
        Ok(_file) => true,
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            // The same content can already be referenced by the previous part.
            // Validate existing bytes before reusing the content-addressed file.
            validate_local_part_file(&final_path, total_bytes, &digest).await?;
            false
        }
        Err(error) => return Err(io_to_s3(error.error)),
    };
    let publication = store_versioned_part_locked(
        root,
        upload_id,
        part_number,
        new_part.clone(),
        &context.scope_namespace,
        &context.key,
        ttl,
        session_quota,
        total_quota,
        part_file_cap,
    )
    .await;
    let previous = match publication {
        Ok(previous) => previous,
        Err(error) => {
            if newly_created
                && read_session_locked(root, upload_id, ttl)
                    .await
                    .is_ok_and(|observed_session| {
                        !observed_session
                            .parts
                            .values()
                            .any(|part| part.file_name == new_part.file_name)
                    })
            {
                let _ignored = tokio::fs::remove_file(&final_path).await;
            }
            return Err(store_error_to_s3(error));
        }
    };
    if let Some(previous) = previous.filter(|part| part.file_name != new_part.file_name) {
        let old_path = stored_part_file_path(root, upload_id, part_number, &previous)?;
        let _ignored = tokio::fs::remove_file(old_path).await;
    }

    let mut response = StatusCode::OK.into_response();
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&etag).map_err(|_error| S3Error::internal())?,
    );
    Ok(response)
}

async fn durable_s3_upload_part(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    part_number: u32,
    upload_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let session = state
        .backend
        .resumable_session_by_id(upload_id)
        .await?
        .filter(|session| {
            session.protocol() == ResumableSessionProtocol::S3Multipart
                && session.state() == ResumableSessionState::Active
                && session.scope_namespace() == context.scope_namespace
                && session.target_key() == context.key
        })
        .ok_or_else(S3Error::no_such_upload)?;
    let _session = session;
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let mut reader = RequestBodyReader::from_body(body, max_bytes).map_err(|error| {
        if matches!(&error, ServerError::RequestBodyTooLarge) {
            entity_too_large()
        } else {
            S3Error::from(error)
        }
    })?;
    if aws_chunked::is_aws_chunked(headers) {
        let ceiling = u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?;
        if aws_chunked::declared_decoded_content_length(headers).is_some_and(|len| len > ceiling) {
            return Err(entity_too_large());
        }
        reader = RequestBodyReader::from_stream(aws_chunked::decode_aws_chunked(reader, ceiling));
    }

    if let Some(expected) = parse_content_md5(headers)? {
        reader = reader.with_expected_md5(expected);
    }
    let md5 = Arc::new(Mutex::new(Md5::new()));
    reader = reader.with_md5_tee(md5.clone());

    let store = state.backend.object_store();
    let (staging_key, size_bytes) = match &store {
        shardline_server_core::ServerObjectStore::S3(s3_store) => {
            let prefix = format!("staging/resumable/s3/{upload_id}/{part_number}");
            let (key, integrity) =
                stage_reader_content_addressed_s3(s3_store, &prefix, &mut reader)
                    .await
                    .map_err(S3Error::from)?;
            (key, integrity.length())
        }
        shardline_server_core::ServerObjectStore::Local(_)
        | shardline_server_core::ServerObjectStore::Blackhole => {
            let temporary = tempfile::NamedTempFile::new().map_err(io_to_s3)?;
            let mut file = tokio::fs::File::from_std(temporary.reopen().map_err(io_to_s3)?);
            let mut hasher = blake3::Hasher::new();
            let mut size_bytes = 0_u64;
            while let Some(chunk) = reader.next_bytes().await.map_err(S3Error::from)? {
                size_bytes = size_bytes
                    .checked_add(u64::try_from(chunk.len()).map_err(|_error| S3Error::internal())?)
                    .ok_or_else(S3Error::internal)?;
                hasher.update(&chunk);
                file.write_all(&chunk).await.map_err(io_to_s3)?;
            }
            file.flush().await.map_err(io_to_s3)?;
            drop(file);

            let digest = hasher.finalize();
            let staging_key = ObjectKey::parse(&format!(
                "staging/resumable/s3/{upload_id}/{part_number}/{}",
                hex::encode(digest.as_bytes())
            ))
            .map_err(|_error| S3Error::internal())?;
            let integrity = shardline_storage::ObjectIntegrity::new(
                shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes()),
                size_bytes,
            );
            let path = temporary.path().to_path_buf();
            let durable_key = staging_key.clone();
            tokio::task::spawn_blocking(move || {
                store.put_content_addressed_file(&durable_key, &path, &integrity)
            })
            .await
            .map_err(|error| io_to_s3(std::io::Error::other(error)))?
            .map_err(ServerError::from)?;
            (staging_key, size_bytes)
        }
    };

    let part_number = NonZeroU64::new(u64::from(part_number)).ok_or_else(S3Error::invalid_part)?;
    let etag = shardline_s3_adapter::etag_header(&object::md5_hasher_hex(&md5));
    match state
        .backend
        .publish_resumable_part_bounded(
            upload_id,
            part_number,
            staging_key.as_str(),
            size_bytes,
            Some(&etag),
            None,
            state.config.s3_upload_session_max_bytes().get(),
            state.config.s3_upload_total_max_bytes().get(),
            state.config.s3_upload_max_active_part_files().get(),
        )
        .await?
    {
        PublishResumablePartOutcome::Published(_) => {}
        PublishResumablePartOutcome::SessionUnavailable => {
            return Err(S3Error::no_such_upload());
        }
        PublishResumablePartOutcome::SessionQuotaExceeded => {
            return Err(S3Error::from(S3SessionError::SessionQuotaExceeded));
        }
        PublishResumablePartOutcome::AggregateQuotaExceeded => {
            return Err(S3Error::from(S3SessionError::AggregateQuotaExceeded));
        }
        PublishResumablePartOutcome::TooManyParts => {
            return Err(store_error_to_s3(S3SessionError::TooManyPartFiles));
        }
    }
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
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    // Parse all supplied safeguards before advancing any session or writing data.
    read_conditional_headers(headers)
        .map_err(|_error| S3Error::invalid_argument("Invalid conditional entity-tag header"))?;
    if durable_sessions_enabled(state) {
        return durable_s3_complete_multipart_upload(state, context, upload_id, headers, body)
            .await;
    }
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // Global session lock: session validation, request parsing, and the
    // part-set checks below (all bounded work). The part-file ingest itself is
    // NOT run under this lock — it is serialized per-session so a slow
    // completion cannot stall other tenants' session operations (F-10).
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session_locked(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    // User metadata was captured at CreateMultipartUpload; apply it now.
    let user_metadata = session.user_metadata.clone();

    // Parse the Complete request body (the client echoes the part
    // numbers/etags it uploaded; ETags are opaque and ignored).
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    if let Some(expected) = parse_content_md5(headers)? {
        reader = reader.with_expected_md5(expected);
    }
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
    let part_lock = acquire_session_part_lock_for_root(root, upload_id);
    let _part_guard = part_lock.lock().await;
    let part_file_guard = lock_session_parts(root, upload_id).await?;
    drop(_session_lock);

    // Resolve immutable filenames from the locked session snapshot, and
    // validate the client's selected part identity before publishing anything.
    let chunk_size = state.config.chunk_size().get();
    let mut part_readers = Vec::with_capacity(session.parts.len());
    for part_number in 1..=max_part {
        let part = session
            .parts
            .get(&part_number)
            .ok_or_else(S3Error::invalid_part)?;
        let path = stored_part_file_path(root, upload_id, part_number, part)?;
        let expected_etag = match &part.etag {
            Some(etag) => etag.clone(),
            None => local_part_md5_etag(&path, part.size_bytes).await?,
        };
        if requested.etag(part_number) != Some(expected_etag.as_str()) {
            return Err(S3Error::invalid_part());
        }
        let file = tokio::fs::File::open(&path).await.map_err(io_to_s3)?;
        let raw_etag = expected_etag
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .ok_or_else(S3Error::invalid_part)?;
        let mut expected_md5 = [0_u8; 16];
        hex::decode_to_slice(raw_etag, &mut expected_md5)
            .map_err(|_error| S3Error::invalid_part())?;
        part_readers.push(
            RequestBodyReader::from_reader_chain(vec![file], chunk_size)
                .with_expected_md5(expected_md5),
        );
    }
    let part_stream = stream::iter(part_readers).flat_map(|part_reader| {
        stream::unfold(part_reader, |mut part_reader| async move {
            match part_reader.next_bytes().await {
                Ok(Some(part_bytes)) => Some((Ok(part_bytes), part_reader)),
                Err(error) => Some((Err(error), part_reader)),
                Ok(None) => None,
            }
        })
    });
    let parts_reader = RequestBodyReader::from_stream(part_stream);

    // Reuse PutObject's lock, conditional recheck, compare-and-swap and
    // conditional-loser cleanup before consuming the multipart session.
    let hasher = Arc::new(Mutex::new(Md5::new()));
    let parts_reader = parts_reader.with_md5_tee(hasher.clone());
    let (_uploaded, etag) = object::s3_upload_object_body(
        state,
        context,
        parts_reader,
        user_metadata,
        hasher,
        Some(headers),
    )
    .await?;

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

async fn durable_s3_complete_multipart_upload(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    upload_id: &str,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    // Validate the opaque upload identity before claiming completion.  A
    // caller may know a valid upload ID but present it under another bucket or
    // key; that request must be observationally rejected and must not advance
    // the session fence or move it to `completing`.
    let candidate = state
        .backend
        .resumable_session_by_id(upload_id)
        .await?
        .filter(|session| {
            session.protocol() == ResumableSessionProtocol::S3Multipart
                && matches!(
                    session.state(),
                    ResumableSessionState::Active | ResumableSessionState::Completing
                )
                && session.scope_namespace() == context.scope_namespace
                && session.target_key() == context.key
        })
        .ok_or_else(S3Error::no_such_upload)?;
    let candidate_attributes: DurableS3SessionAttributes =
        serde_json::from_str(candidate.attributes_json()).map_err(|_error| S3Error::internal())?;
    if candidate_attributes.bucket != context.bucket {
        return Err(S3Error::no_such_upload());
    }

    let mut request_reader =
        RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
            .map_err(S3Error::from)?;
    if let Some(expected) = parse_content_md5(headers)? {
        request_reader = request_reader.with_expected_md5(expected);
    }
    let request_bytes = read_body_to_bytes(&mut request_reader)
        .await
        .map_err(S3Error::from)?;
    let request_text =
        std::str::from_utf8(&request_bytes).map_err(|_error| S3Error::invalid_part())?;
    let requested = parse_complete_multipart_parts(request_text)?;

    let (session, parts) = state
        .backend
        .begin_resumable_completion(upload_id)
        .await?
        .ok_or_else(S3Error::no_such_upload)?;
    let completion = async {
        let attributes = candidate_attributes;

        let uploaded_numbers = parts
            .iter()
            .map(|part| u32::try_from(part.part_number().get()))
            .collect::<Result<std::collections::BTreeSet<_>, _>>()
            .map_err(|_error| S3Error::invalid_part())?;
        if requested.part_numbers() != &uploaded_numbers {
            return Err(S3Error::invalid_part());
        }
        for part in &parts {
            let number = u32::try_from(part.part_number().get())
                .map_err(|_error| S3Error::invalid_part())?;
            let Some(stored_etag) = part.etag() else {
                return Err(S3Error::invalid_part());
            };
            let raw_etag = stored_etag
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .ok_or_else(S3Error::invalid_part)?;
            if raw_etag.len() != 32
                || !raw_etag
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || requested.etag(number) != Some(stored_etag)
            {
                return Err(S3Error::invalid_part());
            }
        }
        let Some(last_part) = parts.last() else {
            return Err(S3Error::invalid_part());
        };
        for part in &parts {
            if part.part_number() != last_part.part_number()
                && part.size_bytes() < state.config.s3_min_part_bytes().get()
            {
                return Err(S3Error::entity_too_small());
            }
        }

        let hasher = Arc::new(Mutex::new(Md5::new()));
        let (reader, _temporary) = if let Some(reader) =
            s3_resumable_parts_reader(&state.backend.object_store(), &parts).await?
        {
            (reader.with_md5_tee(hasher.clone()), None)
        } else {
            let temporary = tempfile::tempdir().map_err(io_to_s3)?;
            let mut files = Vec::with_capacity(parts.len());
            for part in &parts {
                let key =
                    ObjectKey::parse(part.staging_key()).map_err(|_error| S3Error::internal())?;
                let destination = temporary
                    .path()
                    .join(format!("part-{}", part.part_number()));
                materialize_object_to_file(
                    &state.backend.object_store(),
                    &key,
                    part.size_bytes(),
                    &destination,
                )
                .await?;
                files.push(tokio::fs::File::open(destination).await.map_err(io_to_s3)?);
            }
            let chunk_size = state.config.chunk_size().get();
            (
                local_s3_resumable_parts_reader(files, &parts, chunk_size)?
                    .with_md5_tee(hasher.clone()),
                Some(temporary),
            )
        };

        let object_lock = acquire_object_upload_lock_for_root(
            state.config.root_dir(),
            context.object_key.as_str(),
        );
        let _object_guard = object_lock.lock().await;
        let mut resource_guard = state
            .backend
            .acquire_resource_write_lock(
                state.config.root_dir(),
                &ResourceLockKey::s3_object(&context.scope_namespace, &context.key),
            )
            .await?;
        let conditions = read_conditional_headers(headers)
            .map_err(|_error| S3Error::invalid_argument("Invalid conditional entity-tag header"))?;
        let condition = if conditions.is_empty() {
            S3PublishCondition::Unconditional
        } else {
            let existing = object::s3_object_entry(state, context).await?;
            object::check_precondition(
                existing.as_ref().map(|entry| entry.etag.as_str()),
                headers,
                &context.key,
            )?;
            S3PublishCondition::IfUnchanged(existing)
        };
        let start = Instant::now();
        let prepared = state
            .backend
            .prepare_s3_object_stream(&context.object_key, reader)
            .await?;
        metrics::record_upload(
            "s3",
            prepared.response.total_bytes,
            start.elapsed().as_secs_f64(),
            true,
        );
        let etag = object::md5_hasher_hex(&hasher);
        let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
            .map_err(|_error| S3Error::internal())?;
        let entry = S3ObjectEntry {
            scope_namespace: context.scope_namespace.clone(),
            object_key: context.key.clone(),
            file_id: prepared.response.file_id.clone(),
            size_bytes: prepared.response.total_bytes,
            content_hash: prepared.response.content_hash.clone(),
            etag: etag.clone(),
            user_metadata: attributes.user_metadata,
            updated_at_unix_seconds: now,
        };
        let published = state
            .backend
            .publish_s3_object_locked(
                &mut resource_guard,
                &prepared,
                &entry,
                &condition,
                Some(&session.completion_fence()),
            )
            .await?;
        if !published {
            let still_owned = state
                .backend
                .resumable_session_by_id(upload_id)
                .await?
                .is_some_and(|current| {
                    current.state() == ResumableSessionState::Completing
                        && current.fence_epoch() == session.fence_epoch()
                });
            if still_owned && !conditions.is_empty() {
                return Err(S3Error::precondition_failed());
            }
            return Err(S3Error::no_such_upload());
        }
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
    .await;
    if completion.is_err() {
        // Only this reservation's fence may reopen the session. A concurrent
        // completion owner or a committed publication must remain untouched.
        state
            .backend
            .reopen_resumable_session_after_failed_completion(upload_id, session.fence_epoch())
            .await?;
    }
    completion
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
    if durable_sessions_enabled(state) {
        let session = state
            .backend
            .resumable_session_by_id(upload_id)
            .await?
            .filter(|session| {
                session.protocol() == ResumableSessionProtocol::S3Multipart
                    && matches!(
                        session.state(),
                        ResumableSessionState::Active | ResumableSessionState::Completing
                    )
                    && session.scope_namespace() == context.scope_namespace
                    && session.target_key() == context.key
            })
            .ok_or_else(S3Error::no_such_upload)?;
        if !state
            .backend
            .transition_resumable_session(
                upload_id,
                session.state(),
                session.fence_epoch(),
                ResumableSessionState::Aborted,
            )
            .await?
        {
            return Err(S3Error::no_such_upload());
        }
        return Ok(StatusCode::NO_CONTENT.into_response());
    }
    let root = state.config.root_dir();
    let ttl = state.config.s3_upload_session_ttl_seconds();

    // Hold the global session lock for validation and the delete, plus the
    // per-session lock so the session directory is not removed while a
    // concurrent UploadPart is mid-write into a part file (and vice versa);
    // the sweep takes both locks in the same order.
    let _session_lock = lock_upload_sessions(root).await?;

    let session = read_session_locked(root, upload_id, ttl).await?;
    if session.key != context.key || session.scope_namespace != context.scope_namespace {
        return Err(S3Error::no_such_upload());
    }
    let part_lock = acquire_session_part_lock_for_root(root, upload_id);
    let _part_guard = part_lock.lock().await;
    let _part_file_guard = lock_session_parts(root, upload_id).await?;
    delete_session_locked(root, upload_id).await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}
