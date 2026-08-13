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
        header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE},
    },
    response::{IntoResponse, Response},
};
use shardline_index::S3ObjectEntry;
use shardline_protocol::TokenScope;
use shardline_s3_adapter::{S3Error, etag_header, parse_s3_range};

use crate::{
    ServerError,
    app::{AppState, reconstruction_helpers, scope_from_auth},
    metrics,
    upload_ingest::RequestBodyReader,
};

use super::{
    S3ObjectContext, authorize_s3, format_http_date, has_sub_resource, parse_s3_query,
    require_s3_object_context,
};

/// `PUT /{bucket}/{*key}` — stream the body through the CDC ingestor.
///
/// Overwrite semantics are delete-then-upload: when the key already resolves
/// (direct object or record), it is removed before the replacement body is
/// streamed. On success the S3 listing-index row is upserted with the new
/// record's file id, size, and BLAKE3 root content hash.
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

    // Multipart (`?uploads`, `?uploadId`, `?partNumber`) and out-of-scope
    // sub-resources are later-lane / 501 work.
    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
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

    // S3 overwrite semantics: delete-then-upload.
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
        .put_s3_object_stream(&context.object_key, body)
        .await?;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::record_upload("s3", uploaded.total_bytes, elapsed, true);

    // Refresh the S3 listing-index snapshot.
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

    let total_length = match state.backend.object_length(&context.object_key).await {
        Ok(length) => length,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
        Err(error) => return Err(S3Error::from(error)),
    };
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
        .read_object_stream(&context.object_key, total_length, range)
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

    // `?uploadId` (AbortMultipartUpload) is Lane 4 work.
    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
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
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let _context = require_s3_object_context(claims, &bucket, &key)?;
    let _query = parse_s3_query(&uri)?;
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
