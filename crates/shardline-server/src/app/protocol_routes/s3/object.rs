//! S3 object-data routes: `PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`.
//!
//! `PutObject` streams through the shared CDC ingestor under the protocol
//! object's deterministic file id and refreshes the S3 listing index row.
//! `GetObject` serves through the shared record-reconstruction path with
//! `206`/`416` range semantics. `HeadObject` resolves size + ETag through the
//! authoritative record. `DeleteObject` drops the listing row first, then the
//! record + direct object (crash-safe ordering).

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    body::{Body, Bytes},
    extract::{Path, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, StatusCode, Uri,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_TYPE, ETAG, LAST_MODIFIED, RANGE},
    },
    response::{IntoResponse, Response},
};
use futures_util::{Stream, StreamExt, stream};
use md5::{Digest, Md5};
use shardline_index::S3ObjectEntry;
use shardline_s3_adapter::{
    CopyObjectResult, S3Error, S3SubResource, classify, etag_header, format_iso8601,
    parse_copy_source, parse_s3_range, read_conditional_headers, require_s3_bucket_binding,
};
use shardline_server_core::AuthorizedRepository;

use crate::{
    ServerByteStream, ServerError,
    app::{AppState, reconstruction_helpers},
    metrics,
    overflow::checked_add,
    protocol_support::scope_namespace,
    upload_ingest::RequestBodyReader,
};

use super::{
    S3ObjectContext, S3Repository, acquire_object_upload_lock, aws_chunked, format_http_date,
    has_sub_resource, multipart, parse_s3_query, require_s3_object_context, s3_xml_content_type,
};

/// The `x-amz-copy-source` request header (not in axum's header constants).
const COPY_SOURCE: axum::http::header::HeaderName =
    axum::http::header::HeaderName::from_static("x-amz-copy-source");

/// The `x-amz-metadata-directive` request header (CopyObject semantics).
const METADATA_DIRECTIVE: axum::http::header::HeaderName =
    axum::http::header::HeaderName::from_static("x-amz-metadata-directive");

/// S3 user-metadata request header prefix.
const META_PREFIX: &str = "x-amz-meta-";

// ---------------------------------------------------------------------------
// User metadata, ETag (MD5) helpers.
// ---------------------------------------------------------------------------

/// Captures `x-amz-meta-*` request headers as sorted `(name, value)` pairs,
/// stripped of the prefix with names lowercased (S3 canonicalization).
pub(super) fn capture_user_metadata(headers: &HeaderMap) -> Vec<(String, String)> {
    let mut metadata: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let suffix = name.as_str().strip_prefix(META_PREFIX)?;
            let value = value.to_str().ok()?.to_owned();
            Some((suffix.to_ascii_lowercase(), value))
        })
        .collect();
    metadata.sort();
    metadata
}

/// The CopyObject `x-amz-metadata-directive` value.
enum MetadataDirective {
    /// Propagate the source object's user metadata (S3 default).
    Copy,
    /// Overwrite the destination's metadata with `x-amz-meta-*` headers.
    Replace,
}

/// Resolves the CopyObject metadata directive; anything other than `REPLACE`
/// (case-insensitive) is treated as `COPY`.
fn metadata_directive(headers: &HeaderMap) -> MetadataDirective {
    match headers
        .get(&METADATA_DIRECTIVE)
        .and_then(|value| value.to_str().ok())
    {
        Some(directive) if directive.eq_ignore_ascii_case("REPLACE") => MetadataDirective::Replace,
        _ => MetadataDirective::Copy,
    }
}

/// Inserts stored user metadata as `x-amz-meta-*` response headers.
fn insert_user_metadata(response: &mut Response, metadata: &[(String, String)]) {
    for (name, value) in metadata {
        let Ok(value) = HeaderValue::from_str(value) else {
            continue;
        };
        let Ok(header) = HeaderName::try_from(format!("{META_PREFIX}{name}")) else {
            continue;
        };
        response.headers_mut().insert(header, value);
    }
}

/// Resolves the S3 listing-index row for an object (`None` when absent).
async fn s3_object_entry(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
) -> Result<Option<S3ObjectEntry>, S3Error> {
    // The namespace is derived from the capability the context carries — the
    // same derivation every other storage call got at context construction.
    //
    // Exact-key lookup, NOT the prefix scan: `scan_s3_objects` matches
    // `object_key` as a string PREFIX for listing pagination, so when the
    // exact key is absent a longer sibling key with it as a string prefix
    // (e.g. `a` vs `a/b`) would be returned as the object — breaking
    // conditional semantics (F-33): If-None-Match:* would spuriously 412 a
    // create-if-absent PUT against the sibling's ETag. The table's unique
    // `(scope_namespace, object_key)` primary key makes the exact lookup hit
    // the index directly.
    state
        .backend
        .scan_s3_object_exact(&scope_namespace(context.auth.namespace()), &context.key)
        .await
        .map_err(S3Error::from)
}

/// Finalizes a shared MD5 tee hasher into an S3 ETag hex string.
pub(super) fn md5_hasher_hex(hasher: &Arc<Mutex<Md5>>) -> String {
    let mut guard = hasher
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let digest = guard.finalize_reset();
    format!("{digest:x}")
}

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
#[tracing::instrument(skip(auth, state, headers, body), fields(bucket, key))]
pub(crate) async fn s3_put_object(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path((_bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let context = require_s3_object_context(auth.capability(), &key)?;

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

    // `CopyObject` is a PUT with the `x-amz-copy-source` header (S3's COPY is
    // not a separate method): read the source within the caller's bucket and
    // write it to this key.
    if let Some(copy_source) = headers
        .get(COPY_SOURCE)
        .and_then(|value| value.to_str().ok())
    {
        return s3_copy_object(&state, auth.capability(), &context, copy_source, &headers).await;
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

    // Conditional requests (If-Match / If-None-Match) are evaluated against
    // the CURRENT object BEFORE the body is read — a fast-path rejection. The
    // authoritative re-check happens under the per-key lock in
    // `s3_upload_object_body`, immediately before the index swap, so a
    // concurrent conditional writer cannot both pass the check (check-then-act
    // TOCTOU) — see the comment there.
    check_put_precondition(&state, &context, &headers).await?;

    // Capture S3 user metadata (x-amz-meta-*) and compute the ETag (hex MD5 of
    // the object bytes) while the body streams — standard S3 semantics that
    // checksum-verifying clients (s3cmd, the AWS SDKs) depend on.
    let user_metadata = capture_user_metadata(&headers);
    let hasher = Arc::new(Mutex::new(Md5::new()));
    let body = body.with_md5_tee(hasher.clone());

    // Stream the new body FIRST (atomic upload-then-swap under the per-key
    // lock). On failure nothing was committed, so the old record version
    // remains the latest and the index row still points at it.
    let (_uploaded, etag) = s3_upload_object_body(
        &state,
        &context,
        body,
        user_metadata,
        hasher,
        Some(&headers),
    )
    .await?;

    let mut response = StatusCode::OK.into_response();
    response.headers_mut().insert(
        ETAG,
        HeaderValue::from_str(&etag_header(&etag)).map_err(|_error| S3Error::internal())?,
    );
    Ok(response)
}

/// `CopyObject` — `PUT /{bucket}/{*key}` with `x-amz-copy-source`.
///
/// Reads the source object (which must be in the caller's bound bucket) via
/// the snapshot + read path and writes it to the destination key with the same
/// atomic upload-then-swap as `PutObject`. The destination gets a fresh ETag
/// equal to its content hash — identical content yields the identical ETag.
/// Responds `200` with a `CopyObjectResult` envelope.
async fn s3_copy_object(
    state: &Arc<AppState>,
    capability: &AuthorizedRepository,
    destination: &S3ObjectContext<'_>,
    copy_source: &str,
    headers: &HeaderMap,
) -> Result<Response, S3Error> {
    let source = parse_copy_source(copy_source)
        .map_err(|_error| S3Error::invalid_argument("Invalid x-amz-copy-source header"))?;
    // The source must be inside the caller's bound bucket (which must equal the
    // destination bucket under the C1 repo-binding model). The destination was
    // bound by the S3Repository extractor; the source bucket lives in the
    // copy-source header, so it is bound here against the capability.
    require_s3_bucket_binding(capability.repository(), &source.bucket)?;
    let source_context = require_s3_object_context(capability, &source.key)?;

    // Conditional requests apply to the destination (create-if-absent /
    // replace-if-matching semantics) BEFORE any write. Like PutObject this is
    // only a fast-path rejection: the authoritative re-check happens under the
    // per-key lock in `s3_upload_object_body`, right before the index swap.
    check_put_precondition(state, destination, headers).await?;

    // The copy is subject to the SAME per-request byte ceiling as PutObject
    // (SHARDLINE_S3_MAX_PART_BYTES): resolve the source's length AND metadata
    // in ONE pinned snapshot and reject an over-cap source with EntityTooLarge
    // BEFORE any bytes are read or written — exactly like a direct over-cap
    // PUT. The snapshot is the source's single commit point (F-70/F-79): its
    // user metadata, size, and record version are paired in one resolution, so
    // a concurrent source overwrite can never pair OLD-row metadata with
    // NEW-record bytes (a PUT commits its new record before swapping the row).
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let max_bytes_u64 = u64::try_from(max_bytes.get()).map_err(|_error| S3Error::internal())?;
    let snapshot = match state
        .backend
        .s3_object_read_snapshot(
            &source_context.scope_namespace,
            &source_context.key,
            &source_context.object_key,
        )
        .await
    {
        Ok(snapshot) => snapshot,
        Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&source.key)),
        Err(error) => return Err(S3Error::from(error)),
    };
    if snapshot.total_bytes > max_bytes_u64 {
        return Err(S3Error {
            code: "EntityTooLarge",
            message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
            status: StatusCode::PAYLOAD_TOO_LARGE,
        });
    }

    // Metadata directive: COPY (default) propagates the source's user metadata
    // — resolved from the SAME snapshot as the source bytes, so the copied
    // metadata always belongs to the copied content (F-79); REPLACE overrides
    // it with the x-amz-meta-* headers of this request.
    let user_metadata = match metadata_directive(headers) {
        MetadataDirective::Replace => capture_user_metadata(headers),
        MetadataDirective::Copy => snapshot.user_metadata,
    };

    // Stream the source through the same pinned read path as GetObject (no
    // unbounded full-object `read_object` buffer), bounded mid-stream by the
    // same ceiling so a lying record surfaces EntityTooLarge identically.
    let source_stream = state
        .backend
        .read_object_stream_pinned(
            &source_context.object_key,
            snapshot.total_bytes,
            None,
            snapshot.record_content_hash.as_deref(),
        )
        .await?;

    // The destination gets a fresh ETag: hex MD5 of its bytes computed via the
    // MD5 tee while the source streams (identical content yields the identical
    // ETag).
    let hasher = Arc::new(Mutex::new(Md5::new()));
    let body = RequestBodyReader::from_stream(bounded_byte_stream(source_stream, max_bytes_u64))
        .with_md5_tee(hasher.clone());
    let (_uploaded, etag) = s3_upload_object_body(
        state,
        destination,
        body,
        user_metadata,
        hasher,
        Some(headers),
    )
    .await?;

    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_error| S3Error::internal())?;
    let xml = CopyObjectResult {
        etag,
        last_modified_iso8601: format_iso8601(now),
    }
    .to_xml();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// Evaluates the `If-Match` / `If-None-Match` headers against the object's
/// CURRENT state, before a write mutates anything.
///
/// `If-Match` on a missing object is `404 NoSuchKey` (RFC 9110: a missing
/// resource fails `If-Match`); a mismatch is `412 PreconditionFailed`.
/// `If-None-Match` passes for a missing object (create-if-absent) and fails
/// with `412` when the stored ETag matches (or `*` and the object exists).
async fn check_put_precondition(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    headers: &HeaderMap,
) -> Result<(), S3Error> {
    let existing = match s3_object_entry(state, context).await? {
        Some(entry) => Some(entry.etag),
        None => None,
    };
    check_precondition(existing.as_deref(), headers, &context.key)
}

/// Evaluates the S3 conditional headers against a stored ETag.
///
/// `stored_etag: None` means the object does not exist.
fn check_precondition(
    stored_etag: Option<&str>,
    headers: &HeaderMap,
    key: &str,
) -> Result<(), S3Error> {
    let Some(condition) = read_conditional_headers(headers) else {
        return Ok(());
    };
    if condition.satisfied(stored_etag) {
        return Ok(());
    }
    if matches!(
        condition,
        shardline_s3_adapter::ConditionalHeader::IfMatch(_)
    ) && stored_etag.is_none()
    {
        return Err(S3Error::no_such_key(key));
    }
    Err(S3Error::precondition_failed())
}

/// Wraps a byte stream with a hard total-byte ceiling (defense-in-depth for
/// the CopyObject read path).
///
/// Once more than `max_bytes` have been delivered the stream fails with
/// [`ServerError::RequestBodyTooLarge`] — the same signal a PutObject body
/// reader emits, which `s3_upload_object_body` surfaces as S3
/// `EntityTooLarge`. The pinned read path already guarantees the stream length
/// equals the snapshot length (pre-checked against the ceiling), so this only
/// guards against a corrupt/lying record or a racing direct object.
fn bounded_byte_stream(
    stream: ServerByteStream,
    max_bytes: u64,
) -> impl Stream<Item = Result<Bytes, ServerError>> + Send + 'static {
    stream::unfold(
        (stream, 0_u64, max_bytes),
        |(mut stream, mut read, max_bytes)| async move {
            match stream.next().await {
                Some(Ok(chunk)) => {
                    let chunk_len = chunk.len() as u64;
                    match checked_add(read, chunk_len) {
                        Ok(next) => {
                            read = next;
                            if read > max_bytes {
                                return Some((
                                    Err(ServerError::RequestBodyTooLarge),
                                    (stream, read, max_bytes),
                                ));
                            }
                            Some((Ok(chunk), (stream, read, max_bytes)))
                        }
                        Err(error) => Some((Err(error), (stream, read, max_bytes))),
                    }
                }
                Some(Err(error)) => Some((Err(error), (stream, read, max_bytes))),
                None => None,
            }
        },
    )
}

/// Streams a request body to an object key with the atomic upload-then-swap
/// ordering, serialized under the per-key upload lock.
///
/// The body is streamed to a new record version FIRST (a mid-stream failure
/// commits nothing), then the listing-index row is swapped and any stale
/// direct object dropped. Used by `PutObject` and `CopyObject`. The shared
/// MD5 tee hasher is finalized after the stream is drained into the S3 ETag,
/// which is stored in the index row and returned.
///
/// When `precondition` is `Some`, the `If-Match` / `If-None-Match` headers are
/// evaluated before streaming and the observed row becomes the expected value
/// of an atomic metadata compare-and-swap:
///
/// 1. BEFORE the body is streamed or any record committed (F-86): the row is
///    only mutated by holders of this same lock (PutObject / CopyObject /
///    multipart completion / DeleteObject), so a failing check here returns
///    412 with NO write side effect — the losing record is never committed,
///    never becomes the LATEST version, and its chunks are never written. The
///    handlers' early check is only a fast-path rejection.
/// 2. AFTER the body streamed, the replacement row is written only if the
///    database row still exactly matches that observed value. This is the
///    cross-replica linearization point; only one competing conditional writer
///    can win. A loser purges its just-created record when it is still latest.
///
/// The purge is F-92-guarded: it deletes the LOSER's committed version only
/// while the latest alias still points at it (see
/// `delete_file_reference_if_latest`). The process-local lock remains a useful
/// single-node optimization, but correctness comes from the database CAS.
async fn s3_upload_object_body(
    state: &Arc<AppState>,
    context: &S3ObjectContext<'_>,
    body: RequestBodyReader,
    user_metadata: Vec<(String, String)>,
    hasher: Arc<Mutex<Md5>>,
    precondition: Option<&HeaderMap>,
) -> Result<(crate::model::UploadFileResponse, String), S3Error> {
    // Serialize concurrent overwrites of the same key; the swap below (index
    // upsert + stale-direct drop) is atomic with respect to other overwrites.
    let object_lock = acquire_object_upload_lock(context.object_key.as_str());
    let _object_guard = object_lock.lock().await;

    // Capture the exact metadata row that satisfied the condition. The later
    // compare-and-swap rejects the write if any replica changes that row while
    // this request streams its body.
    let conditional_headers =
        precondition.filter(|headers| read_conditional_headers(headers).is_some());
    let expected_entry = if let Some(headers) = conditional_headers {
        let existing = s3_object_entry(state, context).await?;
        check_precondition(
            existing.as_ref().map(|entry| entry.etag.as_str()),
            headers,
            &context.key,
        )?;
        Some(existing)
    } else {
        None
    };

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

    // The body stream has been fully drained: the shared hasher now holds the
    // MD5 of the object bytes — the standard S3 ETag.
    let etag = md5_hasher_hex(&hasher);

    // Swap: point the index at the new record version, then drop any stale
    // direct object that would shadow the record (the old record version is
    // left for GC — record stores are versioned).
    let now = i64::try_from(shardline_protocol::unix_now_seconds_lossy())
        .map_err(|_error| S3Error::internal())?;
    let replacement = S3ObjectEntry {
        scope_namespace: context.scope_namespace.clone(),
        object_key: context.key.clone(),
        file_id: uploaded.file_id.clone(),
        size_bytes: uploaded.total_bytes,
        content_hash: uploaded.content_hash.clone(),
        etag: etag.clone(),
        user_metadata,
        updated_at_unix_seconds: now,
    };
    let swapped = if conditional_headers.is_some() {
        state
            .backend
            .compare_and_swap_s3_object(
                expected_entry.as_ref().and_then(Option::as_ref),
                &replacement,
            )
            .await?
    } else {
        state.backend.upsert_s3_object(&replacement).await?;
        true
    };
    if !swapped {
        let file_id = uploaded.file_id.clone();
        let content_hash = uploaded.content_hash.clone();
        match state
            .backend
            .delete_file_reference_if_latest(&file_id, &content_hash)
            .await
        {
            Ok(true) => {
                tracing::debug!(file_id, %content_hash, "purged conditional-write loser record");
            }
            Ok(false) => {
                tracing::debug!(file_id, %content_hash, "conditional-write loser record already gone or superseded by another version");
            }
            Err(purge_error) => {
                tracing::warn!(file_id, %purge_error, "failed to purge conditional-write loser record");
            }
        }
        return Err(S3Error::precondition_failed());
    }
    let _stale_direct = state
        .backend
        .delete_direct_object_if_present(&context.object_key)
        .await?;
    Ok((uploaded, etag))
}

/// `GET /{bucket}/{*key}` — full or ranged read through the shared
/// reconstruction path.
///
/// Serves `200` with the full body when no `Range` header is present, `206`
/// with `Content-Range` for a satisfiable range, `416 InvalidRange` for an
/// unsatisfiable one, and `404 NoSuchKey` when the object does not exist.
#[tracing::instrument(skip(auth, state, headers), fields(bucket, key))]
pub(crate) async fn s3_get_object(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path((_bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let context = require_s3_object_context(auth.capability(), &key)?;

    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
        return Err(S3Error::not_implemented());
    }

    // The S3 listing-index row is the object's single commit point: its ETag /
    // user-metadata / Last-Modified and its record version (content hash +
    // size) are resolved in ONE read, and the stream below is pinned to that
    // exact immutable record version. A concurrent PUT commits a new record
    // FIRST and swaps the row SECOND, so the row read before the swap pairs
    // the OLD row with the OLD record — the reader observes the pre- or
    // post-overwrite state, never a mix of old-row metadata with new-record
    // bytes (F-70). Only when no row exists is the latest record snapshot used
    // as a fallback (there is then no row metadata to pair).
    let entry = s3_object_entry(&state, &context).await?;
    // Conditional requests (If-Match / If-None-Match) evaluate against the
    // stored S3 ETag (listing-index row) before any bytes are served.
    check_precondition(
        entry.as_ref().map(|entry| entry.etag.as_str()),
        &headers,
        &context.key,
    )?;

    // Resolve the object's length and record version from the SAME row whose
    // ETag / metadata are served below: the stream is pinned to the row's
    // version, so a concurrent overwrite can never yield a torn read (old
    // length, new stream) — the old version stays readable until the new one
    // is fully durable and the index row has moved.
    let (total_length, pinned_hash) = match &entry {
        Some(row) => (row.size_bytes, Some(row.content_hash.clone())),
        None => {
            let snapshot = match state
                .backend
                .s3_object_read_snapshot(
                    &context.scope_namespace,
                    &context.key,
                    &context.object_key,
                )
                .await
            {
                Ok(snapshot) => snapshot,
                Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
                Err(error) => return Err(S3Error::from(error)),
            };
            (snapshot.total_bytes, snapshot.record_content_hash)
        }
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
        .read_object_stream_pinned(
            &context.object_key,
            total_length,
            range,
            pinned_hash.as_deref(),
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
    // responses; derive it from the entry captured above so it is never torn
    // against the ETag/metadata (fallback: Unix epoch when no row exists).
    response.headers_mut().insert(
        LAST_MODIFIED,
        HeaderValue::from_str(&last_modified_from_entry(entry.as_ref()))
            .map_err(|_error| S3Error::internal())?,
    );
    // S3 serves the ETag (hex MD5) and user metadata on GetObject too.
    if let Some(entry) = entry {
        response.headers_mut().insert(
            ETAG,
            HeaderValue::from_str(&etag_header(&entry.etag))
                .map_err(|_error| S3Error::internal())?,
        );
        insert_user_metadata(&mut response, &entry.user_metadata);
    }
    metrics::record_download("s3", total_length, 0.0, true);
    Ok(response)
}

/// `HEAD /{bucket}/{*key}` — size + ETag + Last-Modified through the
/// authoritative record.
#[tracing::instrument(skip(auth, state, headers), fields(bucket, key))]
pub(crate) async fn s3_head_object(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path((_bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let context = require_s3_object_context(auth.capability(), &key)?;

    let query = parse_s3_query(&uri)?;
    if has_sub_resource(&query) {
        return Err(S3Error::not_implemented());
    }

    // Same single-commit-point resolution as GetObject (F-70): the size comes
    // from the SAME row whose ETag / user-metadata / Last-Modified are served
    // below, so a concurrent PUT that committed a new record but has not yet
    // swapped the row is simply not visible — the headers and the size are
    // never a mix of two overwrite states. Only when no row exists is the
    // authoritative record metadata used (with no row metadata to pair).
    let entry = s3_object_entry(&state, &context).await?;
    // Conditional requests evaluate against the stored S3 ETag before the
    // headers are served.
    check_precondition(
        entry.as_ref().map(|entry| entry.etag.as_str()),
        &headers,
        &context.key,
    )?;

    let size = match &entry {
        Some(row) => row.size_bytes,
        None => {
            let (size, _content_hash) =
                match state.backend.s3_object_metadata(&context.object_key).await {
                    Ok(metadata) => metadata,
                    Err(ServerError::NotFound) => return Err(S3Error::no_such_key(&context.key)),
                    Err(error) => return Err(S3Error::from(error)),
                };
            size
        }
    };
    let last_modified = last_modified_from_entry(entry.as_ref());

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
    if let Some(entry) = entry {
        let etag = etag_header(&entry.etag);
        response.headers_mut().insert(
            ETAG,
            HeaderValue::from_str(&etag).map_err(|_error| S3Error::internal())?,
        );
        insert_user_metadata(&mut response, &entry.user_metadata);
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
#[tracing::instrument(skip(auth, state, headers), fields(bucket, key))]
pub(crate) async fn s3_delete_object(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path((_bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let context = require_s3_object_context(auth.capability(), &key)?;

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

    // Serialize DELETE with in-flight overwrites (PutObject / CopyObject /
    // multipart completion) of the same key. Without the per-key lock a DELETE
    // could interleave with a PUT's upload-then-swap and remove the record a
    // just-committed PUT points at — a phantom delete where the PUT returns 200
    // but the object is gone. Holding the same lock the writers use makes the
    // check-and-delete atomic with respect to any swap.
    let object_lock = acquire_object_upload_lock(context.object_key.as_str());
    let _object_guard = object_lock.lock().await;

    // Conditional requests evaluate against the CURRENT object; a missing
    // object fails `If-Match` (404) and passes `If-None-Match` (delete is
    // idempotent).
    let existing = match s3_object_entry(&state, &context).await? {
        Some(entry) => Some(entry.etag),
        None => None,
    };
    check_precondition(existing.as_deref(), &headers, &context.key)?;

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
#[tracing::instrument(skip(auth, state, headers, body), fields(bucket, key))]
pub(crate) async fn s3_post_object(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path((_bucket, key)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let context = require_s3_object_context(auth.capability(), &key)?;

    // `?uploads` → CreateMultipartUpload, `?uploadId` → CompleteMultipartUpload;
    // anything else (PostObject) is out of scope.
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if resources
        .iter()
        .any(|resource| matches!(resource, S3SubResource::Uploads))
    {
        return multipart::s3_create_multipart_upload(&state, &context, &headers).await;
    }
    if let Some(S3SubResource::UploadId(upload_id)) = resources
        .iter()
        .find(|resource| matches!(resource, S3SubResource::UploadId(_)))
    {
        return multipart::s3_complete_multipart_upload(&state, &context, upload_id, body).await;
    }
    Err(S3Error::not_implemented())
}

/// Formats the `Last-Modified` header from the already-fetched listing-index
/// row (falling back to the Unix epoch when no row exists).
///
/// Derived from the same `S3ObjectEntry` as the ETag/user metadata so the
/// header can never be torn against them (no separate index scan).
fn last_modified_from_entry(entry: Option<&S3ObjectEntry>) -> String {
    let updated_at = entry.map(|row| row.updated_at_unix_seconds).unwrap_or(0);
    format_http_date(updated_at)
}
