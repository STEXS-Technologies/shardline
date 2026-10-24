//! S3 bucket routes: the `/{bucket}` stubs.
//!
//! `CreateBucket` is a no-op `200` for S3A / object_store missing-bucket
//! probes, `HeadBucket` answers the connect probe (`200`, `404 NoSuchBucket`,
//! `403 AccessDenied`), `GetBucketLocation` returns the `us-east-1` stub,
//! `GetBucketVersioning` reports the never-enabled configuration,
//! `ListObjectsV2` (`?list-type=2`) serves the index-backed listing, and
//! `GetBucketAcl` and `GetBucketOwnershipControls` report the fixed
//! bucket-owner-enforced model. ACL updates, `DeleteBucket`, and other
//! unsupported bucket sub-resources respond with explicit errors.

use std::sync::Arc;

use axum::body::Body;
use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use md5::{Digest, Md5};
use shardline_index::ResourceLockKey;
use shardline_s3_adapter::{
    ListBucketsResult, MAX_S3_DELETE_KEYS, S3Error, S3SubResource, classify, encode_bucket,
    parse_content_md5, parse_delete_object_keys, s3_object_key, xml_escape,
};

use super::{
    S3Repository, acquire_object_upload_lock_for_root, listing, object, object_headers,
    parse_s3_query, post_object::S3PostObjectForm, s3_xml_content_type,
};
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
#[tracing::instrument(skip(auth, headers), fields(bucket))]
pub(crate) async fn s3_list_buckets(
    auth: S3Repository,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    object_headers::reject_unsupported_read_headers(&headers)?;
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
#[tracing::instrument(skip(_auth, headers), fields(bucket))]
pub(crate) async fn s3_create_bucket(
    _auth: S3Repository,
    Path(_bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    object_headers::reject_unsupported_write_headers(&headers)?;
    let mut ownership_values = headers.get_all("x-amz-object-ownership").iter();
    if let Some(ownership) = ownership_values.next() {
        if ownership_values.next().is_some() {
            return Err(S3Error::invalid_argument("Repeated x-amz-object-ownership"));
        }
        let ownership = ownership
            .to_str()
            .map_err(|_error| S3Error::invalid_argument("Invalid x-amz-object-ownership"))?;
        if !ownership.eq_ignore_ascii_case("BucketOwnerEnforced") {
            return Err(S3Error::not_implemented());
        }
    }
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if resources.as_slice() == [S3SubResource::Acl] {
        return Err(S3Error::acl_not_supported());
    }
    if !resources.is_empty() {
        return Err(S3Error::not_implemented());
    }
    Ok(StatusCode::OK.into_response())
}

/// `HEAD /{bucket}` — `HeadBucket` connect probe: `200` when the bucket
/// decodes and binds to the capability (the [`S3Repository`] extractor already
/// rejected undecodable buckets with `404 NoSuchBucket` and mismatches with
/// `403 AccessDenied`).
#[tracing::instrument(skip(_auth, headers), fields(bucket))]
pub(crate) async fn s3_head_bucket(
    _auth: S3Repository,
    Path(_bucket): Path<String>,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    object_headers::reject_unsupported_read_headers(&headers)?;
    Ok(StatusCode::OK.into_response())
}

/// `GET /{bucket}` — query dispatch on the bucket path.
///
/// The handler serves location, versioning state, ACL and ownership controls,
/// `ListObjectsV2`, and multipart-upload listing. A bare bucket GET remains
/// S3's default `ListObjects` v1. Unsupported sub-resources fail with
/// `501 NotImplemented`.
#[tracing::instrument(skip(auth, state, headers), fields(bucket))]
pub(crate) async fn s3_get_bucket(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    object_headers::reject_unsupported_read_headers(&headers)?;
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if matches!(resources.as_slice(), [S3SubResource::Uploads]) {
        return super::multipart::s3_list_uploads(&state, auth.capability(), &bucket, &query).await;
    }
    if resources.as_slice() == [S3SubResource::Location] {
        return Ok(get_bucket_location_response());
    }
    if resources.as_slice() == [S3SubResource::Versioning] {
        return Ok(get_bucket_versioning_response());
    }
    if resources.as_slice() == [S3SubResource::Acl] {
        let owner_id = scope_namespace(auth.capability().namespace());
        return Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
            super::acl::access_control_policy_xml(&owner_id),
        )
            .into_response());
    }
    if resources.as_slice() == [S3SubResource::Other]
        && query
            .iter()
            .any(|(name, value)| name == "ownershipControls" && value.is_empty())
    {
        return Ok((
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
            super::acl::ownership_controls_xml(),
        )
            .into_response());
    }
    if resources.as_slice() == [S3SubResource::Other]
        && query
            .iter()
            .any(|(name, value)| name == "versions" && value.is_empty())
    {
        return listing::s3_list_object_versions(auth, State(state), Path(bucket), uri, headers)
            .await;
    }
    if resources.as_slice() == [S3SubResource::ListObjects] {
        return listing::s3_list_objects_v2(auth, State(state), Path(bucket), uri, headers).await;
    }
    if resources.is_empty() {
        return listing::s3_list_objects_v1(auth, State(state), Path(bucket), uri, headers).await;
    }
    Err(S3Error::not_implemented())
}

fn get_bucket_versioning_response() -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>",
    )
        .into_response()
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
///
/// Each key's delete pair runs under the per-key upload lock (the same
/// [`acquire_object_upload_lock`] the single-object `DeleteObject` and the
/// overwrite paths hold), so a concurrent PUT/Copy/Complete on the same key
/// cannot have its just-committed record deleted out from under it (F-18). The
/// request body is read and parsed in full before any lock is taken.
#[tracing::instrument(skip(auth, state, headers, body, post_form), fields(bucket))]
pub(crate) async fn s3_post_bucket(
    auth: S3Repository,
    State(state): State<Arc<AppState>>,
    Path(_bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    post_form: Option<Extension<S3PostObjectForm>>,
    body: Body,
) -> Result<Response, S3Error> {
    object_headers::reject_unsupported_write_headers(&headers)?;
    let query = parse_s3_query(&uri)?;
    let resources = classify(&query);
    if resources.is_empty() {
        let Some(Extension(form)) = post_form else {
            return Err(S3Error::not_implemented());
        };
        return s3_post_object(auth, &state, form, body).await;
    }
    if !resources
        .iter()
        .any(|resource| matches!(resource, S3SubResource::DeleteObjects))
    {
        return Err(S3Error::not_implemented());
    }

    let scope_namespace = scope_namespace(auth.capability().namespace());
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    if let Some(expected) = shardline_s3_adapter::parse_content_md5(&headers)? {
        reader = reader.with_expected_md5(expected);
    }
    let bytes = read_body_to_bytes(&mut reader)
        .await
        .map_err(S3Error::from)?;
    let body_str = std::str::from_utf8(&bytes).map_err(|_error| S3Error::internal())?;
    let keys = parse_delete_object_keys(body_str)?;
    if keys.is_empty() {
        return Err(S3Error::malformed_xml());
    }

    // Dedupe while preserving request order: duplicate `<Key>` entries collapse
    // into a single delete (and a single `<Deleted>` row). The parser already
    // collapsed duplicates and enforced the protocol cap DURING parsing (F-32),
    // so this list holds at most `MAX_S3_DELETE_KEYS` distinct keys; the loop
    // below remains as defense in depth for any future parser caller — never
    // materializing a key list (or `seen` set) beyond the cap (F-23).
    let dedupe_capacity = keys.len().min(MAX_S3_DELETE_KEYS + 1);
    let mut distinct = Vec::with_capacity(dedupe_capacity);
    let mut seen = std::collections::HashSet::with_capacity(dedupe_capacity);
    for key in keys {
        if seen.insert(key.clone()) {
            if distinct.len() >= MAX_S3_DELETE_KEYS {
                return Err(S3Error::malformed_xml());
            }
            distinct.push(key);
        }
    }

    // One pass, in request order: invalid keys become per-key `<Error>` rows
    // (never aborting the batch after earlier keys were deleted), valid keys
    // are deleted and become `<Deleted>` rows.
    let mut outcomes = Vec::with_capacity(distinct.len());
    for key in distinct {
        match s3_object_key(&scope_namespace, &key) {
            Ok(object_key) => {
                // Serialize the batch delete against in-flight overwrites
                // (PutObject / CopyObject / multipart completion) of the same
                // key, exactly like the single-object `DeleteObject` path: a
                // concurrent PUT's upload-then-swap must never interleave with
                // the delete pair (phantom delete / dangling index). The
                // request body was fully read and parsed above, so the lock is
                // taken per key in the delete loop — never across the body
                // read (F-18).
                //
                // Crash-safe ordering (same as DeleteObject): index row first,
                // then record + direct object.
                let object_lock = acquire_object_upload_lock_for_root(
                    state.config.root_dir(),
                    object_key.as_str(),
                );
                let _object_guard = object_lock.lock().await;
                let mut resource_guard = state
                    .backend
                    .acquire_resource_write_lock(
                        state.config.root_dir(),
                        &ResourceLockKey::s3_object(&scope_namespace, &key),
                    )
                    .await?;
                let _outcome = state
                    .backend
                    .delete_s3_object_locked(
                        &mut resource_guard,
                        &scope_namespace,
                        &key,
                        &object_key,
                    )
                    .await?;
                resource_guard.release().await?;
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
async fn s3_post_object(
    auth: S3Repository,
    state: &Arc<AppState>,
    form: S3PostObjectForm,
    body: Body,
) -> Result<Response, S3Error> {
    let context = super::require_s3_object_context(auth.capability(), &form.key)?;
    let max_bytes = usize::try_from(state.config.s3_max_part_bytes().get())
        .map_err(|_error| S3Error::internal())?;
    let max_bytes = std::num::NonZeroUsize::new(max_bytes).ok_or_else(S3Error::internal)?;
    let payload_checksum = super::checksum::parse(&form.payload_headers)?;
    let response_checksum = payload_checksum
        .as_ref()
        .map(|checksum| (checksum.header(), checksum.expected().to_owned()));
    let expected_md5 = parse_content_md5(&form.payload_headers)?;
    let mut body = match RequestBodyReader::from_body(body, max_bytes) {
        Ok(reader) => reader,
        Err(crate::ServerError::RequestBodyTooLarge) => {
            return Err(S3Error {
                code: "EntityTooLarge",
                message: "Your proposed upload exceeds the maximum allowed object size".to_owned(),
                status: StatusCode::PAYLOAD_TOO_LARGE,
            });
        }
        Err(error) => return Err(S3Error::from(error)),
    };
    if let Some(payload_checksum) = payload_checksum {
        body = super::checksum::verify_body(body, payload_checksum);
    }
    let hasher = Arc::new(std::sync::Mutex::new(Md5::new()));
    let body = body.with_md5_tee_and_expected(hasher.clone(), expected_md5);
    let mut user_metadata = form.user_metadata;
    if let Some((header, value)) = &response_checksum {
        super::checksum::attach_object_checksum(&mut user_metadata, header, value)?;
    }
    let (_uploaded, etag) = object::s3_upload_object_body(
        state,
        &context,
        body,
        user_metadata,
        hasher,
        object::ObjectUploadOptions {
            multipart_part_count: 0,
            full_object_checksum: None,
            precondition: Some(&form.payload_headers),
        },
    )
    .await?;
    let mut response = if let Some(redirect) = form.success_redirect.as_deref() {
        let location = post_object_redirect(
            redirect,
            &context.bucket,
            &form.key,
            &shardline_s3_adapter::etag_header(&etag),
        )?;
        let mut response = StatusCode::SEE_OTHER.into_response();
        response.headers_mut().insert(
            axum::http::header::LOCATION,
            axum::http::HeaderValue::from_str(&location).map_err(|_error| S3Error::internal())?,
        );
        response
    } else {
        match form.success_status {
            200 => StatusCode::OK.into_response(),
            204 => StatusCode::NO_CONTENT.into_response(),
            201 => {
                let bucket = xml_escape(&context.bucket);
                let key = xml_escape(&form.key);
                let etag = xml_escape(&shardline_s3_adapter::etag_header(&etag));
                let location = xml_escape(&post_object_location(
                    state.config.public_base_url(),
                    &context.bucket,
                    &form.key,
                )?);
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<PostResponse><Location>{location}</Location><Bucket>{bucket}</Bucket><Key>{key}</Key><ETag>{etag}</ETag></PostResponse>"
                );
                (
                    StatusCode::CREATED,
                    [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
                    xml,
                )
                    .into_response()
            }
            _ => return Err(S3Error::internal()),
        }
    };
    if let Some((header, value)) = response_checksum {
        response.headers_mut().insert(
            axum::http::HeaderName::from_bytes(header.as_bytes())
                .map_err(|_error| S3Error::internal())?,
            axum::http::HeaderValue::from_str(&value).map_err(|_error| S3Error::internal())?,
        );
    }
    Ok(response)
}

fn post_object_location(base: &str, bucket: &str, key: &str) -> Result<String, S3Error> {
    let mut location = url::Url::parse(base).map_err(|_error| S3Error::internal())?;
    {
        let mut segments = location
            .path_segments_mut()
            .map_err(|_error| S3Error::internal())?;
        segments.pop_if_empty().push(bucket);
        for segment in key.split('/') {
            segments.push(segment);
        }
    }
    Ok(location.into())
}

fn post_object_redirect(
    destination: &str,
    bucket: &str,
    key: &str,
    etag: &str,
) -> Result<String, S3Error> {
    let mut location = url::Url::parse(destination).map_err(|_error| S3Error::internal())?;
    location
        .query_pairs_mut()
        .append_pair("bucket", bucket)
        .append_pair("key", key)
        .append_pair("etag", etag);
    Ok(location.into())
}

fn delete_result_xml(outcomes: &[DeleteOutcome]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for outcome in outcomes {
        use std::fmt::Write as _;
        match outcome {
            DeleteOutcome::Deleted(key) => {
                let _result = writeln!(xml, "  <Deleted><Key>{}</Key></Deleted>", xml_escape(key));
            }
            DeleteOutcome::Error { key, code, message } => {
                let _result = writeln!(
                    xml,
                    "  <Error><Key>{}</Key><Code>{code}</Code><Message>{}</Message></Error>",
                    xml_escape(key),
                    xml_escape(message)
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
