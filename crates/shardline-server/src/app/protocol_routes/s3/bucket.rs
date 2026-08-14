//! S3 bucket routes: the `/{bucket}` stubs.
//!
//! `CreateBucket` is a no-op `200` for S3A / object_store missing-bucket
//! probes, `HeadBucket` answers the connect probe (`200`, `404 NoSuchBucket`,
//! `403 AccessDenied`), `GetBucketLocation` returns the `us-east-1` stub,
//! `ListObjectsV2` (`?list-type=2`) serves the index-backed listing, and
//! `DeleteBucket` plus every other bucket sub-resource respond
//! `501 NotImplemented`.

use std::sync::Arc;

use axum::body::Body;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_s3_adapter::{
    ListBucketsResult, MAX_S3_DELETE_KEYS, S3Error, S3SubResource, classify, encode_bucket,
    parse_delete_object_keys, s3_object_key,
};

use super::{S3Repository, listing, parse_s3_query, s3_xml_content_type};
use crate::{
    app::AppState,
    protocol_support::scope_namespace,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

/// `GET /` — `ListBuckets` (service level).
///
/// The caller's capability is bound to exactly one bucket (`{owner}.{name}`
/// from its repository scope, via [`encode_bucket`]), so the response lists
/// that single bucket. A capability without a repository (permissive mode, or
/// no credentials) is `403 AccessDenied`.
#[tracing::instrument(skip(auth, _headers), fields(bucket))]
pub(crate) async fn s3_list_buckets(
    auth: S3Repository,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    let repository = auth
        .capability()
        .repository()
        .ok_or_else(S3Error::access_denied)?;
    let bucket = encode_bucket(repository.owner(), repository.name());
    let xml = ListBucketsResult {
        buckets: vec![bucket],
    }
    .to_xml();
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// `PUT /{bucket}` — `CreateBucket` stub: a no-op `200` once the bucket
/// decodes and binds to the capability (done by the [`S3Repository`]
/// extractor).
#[tracing::instrument(skip(_auth, _headers), fields(bucket))]
pub(crate) async fn s3_create_bucket(
    _auth: S3Repository,
    Path(_bucket): Path<String>,
    uri: Uri,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    let query = parse_s3_query(&uri)?;
    if !classify(&query).is_empty() {
        return Err(S3Error::not_implemented());
    }
    Ok(StatusCode::OK.into_response())
}

/// `HEAD /{bucket}` — `HeadBucket` connect probe: `200` when the bucket
/// decodes and binds to the capability (the [`S3Repository`] extractor already
/// rejected undecodable buckets with `404 NoSuchBucket` and mismatches with
/// `403 AccessDenied`).
#[tracing::instrument(skip(_auth, _headers), fields(bucket))]
pub(crate) async fn s3_head_bucket(
    _auth: S3Repository,
    Path(_bucket): Path<String>,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    Ok(StatusCode::OK.into_response())
}

/// `GET /{bucket}` — query dispatch on the bucket path.
///
/// `?location` serves the `GetBucketLocation` stub
/// (`<LocationConstraint>us-east-1</LocationConstraint>`); `?list-type=2`
/// dispatches to [`s3_list_objects_v2`](listing::s3_list_objects_v2); a bare
/// `GET /{bucket}` (no sub-resource) is the S3-default `ListObjects` v1 —
/// which `s3cmd` and other legacy clients send for `ls` — and dispatches to
/// [`s3_list_objects_v1`](listing::s3_list_objects_v1); `?list-type=1` and
/// every other bucket sub-resource respond `501 NotImplemented`.
#[tracing::instrument(skip(auth, state, headers), fields(bucket))]
pub(crate) async fn s3_get_bucket(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if resources
        .iter()
        .any(|resource| matches!(resource, S3SubResource::Location))
    {
        return Ok(get_bucket_location_response());
    }
    if resources
        .iter()
        .any(|resource| matches!(resource, S3SubResource::ListObjects))
    {
        return listing::s3_list_objects_v2(auth, State(state), Path(bucket), uri, headers).await;
    }
    if resources.is_empty() {
        return listing::s3_list_objects_v1(auth, State(state), Path(bucket), uri, headers).await;
    }
    Err(S3Error::not_implemented())
}

/// `DELETE /{bucket}` — `DeleteBucket` is explicitly out of scope:
/// `501 NotImplemented`.
#[tracing::instrument(skip(_auth, _headers), fields(bucket))]
pub(crate) async fn s3_delete_bucket(
    _auth: S3Repository,
    Path(_bucket): Path<String>,
    uri: Uri,
    _headers: HeaderMap,
) -> Result<Response, S3Error> {
    let _query = parse_s3_query(&uri)?;
    Err(S3Error::not_implemented())
}

/// `POST /{bucket}?delete=` — `DeleteObjects` (batch delete).
///
/// Real clients (mc's `rm`, the AWS SDKs) batch-delete through this endpoint
/// instead of per-object `DELETE`. The XML body lists `<Key>` elements; each
/// object is removed with the same crash-safe ordering as `DeleteObject`
/// (index row first, then record + direct object). Responds `200` with a
/// `<DeleteResult>` envelope. `POST` without `?delete=` is `501`.
///
/// The handler mirrors S3's `DeleteObjects` protocol contract:
///
/// - the request is rejected with `400 MalformedXML` before any deletion when
///   the body lists no keys or more than [`MAX_S3_DELETE_KEYS`] *distinct*
///   keys (duplicate `<Key>` entries collapse to one delete, keeping both the
///   backend work — two ops per key — and the `<DeleteResult>` response linear
///   in the protocol cap);
/// - invalid keys (leading slash, control characters, path traversal,
///   oversized) are collected into per-key `<Error>` rows instead of aborting
///   the batch, so a request never mutates state and then fails part-way; the
///   valid keys are deleted and the response is `200` with `<Deleted>` rows for
///   the successes and `<Error>` rows for the failures, in request order.
#[tracing::instrument(skip(auth, state, _headers, body), fields(bucket))]
pub(crate) async fn s3_post_bucket(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(_bucket): Path<String>,
    uri: Uri,
    _headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let query = parse_s3_query(&uri)?;
    if !classify(&query)
        .iter()
        .any(|resource| matches!(resource, S3SubResource::DeleteObjects))
    {
        return Err(S3Error::not_implemented());
    }

    let scope_namespace = scope_namespace(auth.capability().namespace());
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    let bytes = read_body_to_bytes(&mut reader)
        .await
        .map_err(S3Error::from)?;
    let body_str = std::str::from_utf8(&bytes).map_err(|_error| S3Error::internal())?;
    let keys = parse_delete_object_keys(body_str)?;
    if keys.is_empty() {
        return Err(S3Error::malformed_xml());
    }

    // Dedupe while preserving request order: duplicate `<Key>` entries collapse
    // into a single delete (and a single `<Deleted>` row).
    let mut distinct = Vec::with_capacity(keys.len());
    let mut seen = std::collections::HashSet::with_capacity(keys.len());
    for key in keys {
        if seen.insert(key.clone()) {
            distinct.push(key);
        }
    }
    if distinct.len() > MAX_S3_DELETE_KEYS {
        return Err(S3Error::malformed_xml());
    }

    // One pass, in request order: invalid keys become per-key `<Error>` rows
    // (never aborting the batch after earlier keys were deleted), valid keys
    // are deleted and become `<Deleted>` rows.
    let mut outcomes = Vec::with_capacity(distinct.len());
    for key in distinct {
        match s3_object_key(&scope_namespace, &key) {
            Ok(object_key) => {
                // Crash-safe ordering (same as DeleteObject): index row first,
                // then record + direct object.
                let _row_deleted = state
                    .backend
                    .delete_s3_object(&scope_namespace, &key)
                    .await?;
                let _outcome = state.backend.delete_object_if_present(&object_key).await?;
                outcomes.push(DeleteOutcome::Deleted(key));
            }
            Err(_error) => {
                let error = S3Error::no_such_key(&key);
                outcomes.push(DeleteOutcome::Error {
                    key,
                    code: error.code,
                    message: error.message,
                });
            }
        }
    }

    let xml = delete_result_xml(&outcomes);
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// The per-key outcome of a batch delete, in request order.
enum DeleteOutcome {
    /// The object was deleted (or already absent).
    Deleted(String),
    /// The key was invalid and could not be addressed; reported per S3's
    /// `DeleteResult` `<Error>` schema.
    Error {
        key: String,
        code: &'static str,
        message: String,
    },
}

/// Builds the `DeleteResult` XML envelope.
///
/// Rows are emitted in request order: `<Deleted>` for successes and `<Error>`
/// (with `<Key>`, `<Code>`, `<Message>`) for the per-key failures.
#[must_use]
fn delete_result_xml(outcomes: &[DeleteOutcome]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for outcome in outcomes {
        use std::fmt::Write as _;
        match outcome {
            DeleteOutcome::Deleted(key) => {
                let _result = writeln!(
                    xml,
                    "  <Deleted><Key>{}</Key></Deleted>",
                    key.replace('&', "&amp;")
                        .replace('<', "&lt;")
                        .replace('>', "&gt;")
                );
            }
            DeleteOutcome::Error { key, code, message } => {
                let _result = writeln!(
                    xml,
                    "  <Error><Key>{}</Key><Code>{code}</Code><Message>{}</Message></Error>",
                    key.replace('&', "&amp;")
                        .replace('<', "&lt;")
                        .replace('>', "&gt;"),
                    message.replace('&', "&amp;").replace('<', "&lt;")
                );
            }
        }
    }
    xml.push_str("</DeleteResult>\n");
    xml
}

/// Builds the `GetBucketLocation` XML stub response.
#[must_use]
fn get_bucket_location_response() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         us-east-1\
         </LocationConstraint>\n",
    )
        .into_response()
}
