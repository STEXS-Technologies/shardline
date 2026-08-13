//! S3 object-data routes: `PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`.
//!
//! `PutObject` streams through the shared CDC ingestor under the protocol
//! object's deterministic file id and refreshes the S3 listing index row.
//! `GetObject` serves through the shared record-reconstruction path with
//! `206`/`416` range semantics. `HeadObject` resolves size + ETag through the
//! authoritative record. `DeleteObject` drops the listing row first, then the
//! record + direct object (crash-safe ordering).

use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderMap, HeaderValue, StatusCode, Uri,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE},
    },
    response::{IntoResponse, Response},
};
use shardline_index::S3ObjectEntry;
use shardline_protocol::TokenScope;
use shardline_s3_adapter::{S3Error, S3SubResource, classify, etag_header, parse_s3_range};

use crate::{
    ServerError,
    app::{AppState, reconstruction_helpers, scope_from_auth},
    metrics,
    upload_ingest::RequestBodyReader,
};

use super::{
    S3ObjectContext, acquire_object_upload_lock, authorize_s3, aws_chunked, format_http_date,
    has_sub_resource, multipart, parse_s3_query, require_s3_object_context,
};

/// `PUT /{bucket}/{*key}` — stream the body through the CDC ingestor.
///
/// Overwrite semantics are **atomic upload-then-swap**: the new body is
/// streamed to a new record version first (the protocol file id is
/// deterministic, so a fresh version lands on top without removing the old
/// one), then the listing-index row is swapped and any stale direct object is
/// dropped. A mid-stream failure (chunked/lying Content-Length, client
/// disconnect) commits nothing — the old record version, index row, and direct
/// object remain intact and readers never observe a transient 404. A per-key
/// upload lock serializes concurrent overwrites of the same key.
#[tracing::instrument(skip(state, headers, body), fields(bucket, key))]
pub(crate) async fn s3_put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let context = require_s3_object_context(claims, &bucket, &key)?;

    // `?partNumber=N&uploadId=U` dispatches to UploadPart; other sub-resources
    // (multipart create/completion and out-of-scope ops) are handled below or
    // rejected as 501.
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    let part_number = resources.iter().find_map(|resource| {
        if let S3SubResource::PartNumber(number) = resource {
            Some(*number)
        } else {
            None
        }
    });
    let upload_id = resources.iter().find_map(|resource| {
        if let S3SubResource::UploadId(id) = resource {
            Some(id.as_str())
        } else {
            None
        }
    });
    if let (Some(part_number), Some(upload_id)) = (part_number, upload_id) {
        return multipart::s3_upload_part(&state, &context, part_number, upload_id, &headers, body)
            .await;
    }
    if !resources.is_empty() {
        return Err(S3Error::not_implemented());
    }

    // Bodies larger than SHARDLINE_S3_MAX_PART_BYTES must use multipart.
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let body = match RequestBodyReader::from_body(body, max_bytes) {
        Ok(reader) => reader,
        Err(ServerError::RequestBodyTooLarge) => {
            return Err(S3Error {
                code: "EntityTooLarge",
                message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
                status: StatusCode::PAYLOAD_TOO_LARGE,
            });
        }
        Err(error) => return Err(S3Error::from(error)),
    };

    // Real clients (mc, AWS SDKs, pyarrow) stream bodies with AWS chunked
    // encoding; decode the framing so the CDC ingestor stores the actual
    // payload (and size). The decoded size is enforced against the part
    // ceiling by the decoder.
    let body = if aws_chunked::is_aws_chunked(&headers) {
        let max_bytes_u64 = u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?;
        if let Some(decoded) = aws_chunked::declared_decoded_content_length(&headers)
            && decoded > max_bytes_u64
        {
            return Err(S3Error {
                code: "EntityTooLarge",
                message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
                status: StatusCode::PAYLOAD_TOO_LARGE,
            });
        }
        RequestBodyReader::from_stream(aws_chunked::decode_aws_chunked(
            body,
            u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?,
        ))
    } else {
        body
    };

    // Serialize concurrent overwrites of the same key; the swap below (index
    // upsert + stale-direct drop) is atomic with respect to other overwrites.
    let object_lock = acquire_object_upload_lock(context.object_key.as_str());
    let _object_guard = object_lock.lock().await;

    // Stream the new body FIRST. On failure nothing was committed (the record
    // commit only happens at ingest finish), so the old record version remains
    // the latest and the index row still points at it.
    let start = Instant::now();
    let uploaded = match state
        .backend
        .put_s3_object_stream(&context.object_key, body)
        .await
    {
        Ok(uploaded) => uploaded,
        // A chunked body with a lying/absent Content-Length can exceed the
        // limit mid-stream; surface it as the S3 EntityTooLarge envelope.
        Err(ServerError::RequestBodyTooLarge) => {
            return Err(S3Error {
                code: "EntityTooLarge",
                message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
                status: StatusCode::PAYLOAD_TOO_LARGE,
            });
        }
        Err(error) => return Err(S3Error::from(error)),
    };
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("s3", uploaded.total_bytes, elapsed, true);

    // Swap: point the index at the new record version, then drop any stale
    // direct object that would shadow the record (the old record version is
    // left for GC — record stores are versioned).
    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_error| S3Error::internal())?;
    state
        .backend
        .upsert_s3_object(&S3ObjectEntry {
            scope_namespace: context.scope_namespace,
            object_key: context.key,
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

    let etag = etag_header(&uploaded.content_hash);
    let mut response = StatusCode::OK.into_response();
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&etag).map_err(|_error| S3Error::internal())?,
    );
    Ok(response)
}

/// `GET /{bucket}/{*key}` — full or ranged read through the shared
/// reconstruction path.
///
/// Serves `200` with the full body when no `Range` header is present, `206`
/// with `Content-Range` for a satisfiable range, `416 InvalidRange` for an
/// unsatisfiable one, and `404 NoSuchKey` when the object does not exist.
#[tracing::instrument(skip(state, headers), fields(bucket, key))]
pub(crate) async fn s3_get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Read)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let context = require_s3_object_context(claims, &bucket, &key)?;

    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
        return Err(S3Error::not_implemented());
    }

    // Resolve the object's length and record version in ONE atomic snapshot:
    // the stream below is pinned to the same version, so a concurrent
    // overwrite can never yield a torn read (old length, new stream) or a
    // transient 404 — the old version stays readable until the new one is
    // fully durable and the index row has moved.
    let snapshot = match state
        .backend
        .s3_object_read_snapshot(&context.object_key)
        .await
    {
        Ok(snapshot) => snapshot,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
        Err(error) => return Err(S3Error::from(error)),
    };
    let total_length = snapshot.total_bytes;
    let range_header = headers.get(RANGE).and_then(|value| value.to_str().ok());
    let range = match range_header {
        Some(header) => {
            // An explicit range on an empty object is unsatisfiable.
            if total_length == 0 {
                return Err(S3Error::invalid_range());
            }
            Some(parse_s3_range(Some(header), total_length)?)
        }
        None => None,
    };

    let byte_stream = state
        .backend
        .read_object_stream_pinned(
            &context.object_key,
            total_length,
            range,
            snapshot.record_content_hash.as_deref(),
        )
        .await?;
    let mut response = if let Some(range) = range {
        metrics::record_range_request();
        let transfer_length = range.len().ok_or(ServerError::Overflow)?;
        reconstruction_helpers::byte_range_stream_response(
            byte_stream,
            state.transfer_limiter.clone(),
            range,
            total_length,
            transfer_length,
        )
    } else {
        reconstruction_helpers::full_byte_stream_response(
            byte_stream,
            state.transfer_limiter.clone(),
            total_length,
        )
    };
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    // Real clients (mc, the AWS SDKs) parse `Last-Modified` on GetObject
    // responses; serve it from the listing-index snapshot like HeadObject.
    let last_modified = last_modified_for(&state, &context).await?;
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&last_modified).map_err(|_error| S3Error::internal())?,
    );
    metrics::record_download("s3", total_length, 0.0, true);
    Ok(response)
}

/// `HEAD /{bucket}/{*key}` — size + ETag + Last-Modified through the
/// authoritative record.
#[tracing::instrument(skip(state, headers), fields(bucket, key))]
pub(crate) async fn s3_head_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Read)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let context = require_s3_object_context(claims, &bucket, &key)?;

    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
        return Err(S3Error::not_implemented());
    }

    let (size, content_hash) = match state.backend.s3_object_metadata(&context.object_key).await {
        Ok(metadata) => metadata,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
        Err(error) => return Err(S3Error::from(error)),
    };
    let last_modified = last_modified_for(&state, &context).await?;

    let mut response = StatusCode::OK.into_response();
    response
        .headers_mut()
        .insert(CONTENT_LENGTH, HeaderValue::from(size));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    // Clients (pyarrow, the AWS SDKs) use `Accept-Ranges` on HeadObject to
    // decide the object supports ranged (seekable) access; without it pyarrow
    // opens a non-seekable stream and parquet reads fail.
    response
        .headers_mut()
        .insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if !content_hash.is_empty() {
        let etag = etag_header(&content_hash);
        response.headers_mut().insert(
            ETAG,
            HeaderValue::from_str(&etag).map_err(|_error| S3Error::internal())?,
        );
    }
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&last_modified).map_err(|_error| S3Error::internal())?,
    );
    Ok(response)
}

/// `DELETE /{bucket}/{*key}` — idempotent object removal (`204`).
///
/// Crash-safe ordering per the design: the listing-index row is dropped first
/// (the snapshot is GC-inert and deleting it never touches chunks or records),
/// then `delete_object_if_present` removes the direct object and record.
#[tracing::instrument(skip(state, headers), fields(bucket, key))]
pub(crate) async fn s3_delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let context = require_s3_object_context(claims, &bucket, &key)?;

    // `?uploadId` dispatches to AbortMultipartUpload; other sub-resources are
    // out of scope.
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if let Some(S3SubResource::UploadId(upload_id)) = resources
        .iter()
        .find(|resource| matches!(resource, S3SubResource::UploadId(_)))
    {
        return multipart::s3_abort_multipart_upload(&state, &context, upload_id).await;
    }
    if !resources.is_empty() {
        return Err(S3Error::not_implemented());
    }

    let _row_deleted = state
        .backend
        .delete_s3_object(&context.scope_namespace, &context.key)
        .await?;
    let _outcome = state
        .backend
        .delete_object_if_present(&context.object_key)
        .await?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

/// `POST /{bucket}/{*key}` — `CreateMultipartUpload`/`UploadPart` are Lane 4
/// work; `PostObject` is out of scope. Everything is `501 NotImplemented`
/// today.
#[tracing::instrument(skip(state, headers), fields(bucket, key))]
pub(crate) async fn s3_post_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let context = require_s3_object_context(claims, &bucket, &key)?;

    // `?uploads` → CreateMultipartUpload, `?uploadId` → CompleteMultipartUpload;
    // anything else (PostObject) is out of scope.
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if resources
        .iter()
        .any(|resource| matches!(resource, S3SubResource::Uploads))
    {
        return multipart::s3_create_multipart_upload(&state, &context).await;
    }
    if let Some(S3SubResource::UploadId(upload_id)) = resources
        .iter()
        .find(|resource| matches!(resource, S3SubResource::UploadId(_)))
    {
        return multipart::s3_complete_multipart_upload(&state, &context, upload_id, body).await;
    }
    Err(S3Error::not_implemented())
}

/// Resolves the `Last-Modified` header value from the S3 listing-index row,
/// falling back to the Unix epoch when no row exists yet.
///
/// # Errors
///
/// Returns [`S3Error`] when the index scan fails.
async fn last_modified_for(
    state: &Arc<AppState>,
    context: &S3ObjectContext,
) -> Result<String, S3Error> {
    let rows = state
        .backend
        .scan_s3_objects(&context.scope_namespace, &context.key, None, 1)
        .await?;
    let updated_at = rows
        .first()
        .map(|row| row.updated_at_unix_seconds)
        .unwrap_or(0);
    Ok(format_http_date(updated_at))
}
