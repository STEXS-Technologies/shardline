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
use shardline_protocol::TokenScope;
use shardline_s3_adapter::{
    S3Error, S3SubResource, classify, parse_delete_object_keys, require_s3_bucket_binding,
    s3_object_key,
};

use super::{authorize_s3, listing, parse_s3_query, s3_xml_content_type};
use crate::{
    app::{AppState, scope_from_auth},
    protocol_support::scope_namespace,
    upload_ingest::{RequestBodyReader, read_body_to_bytes},
};

/// `PUT /{bucket}` — `CreateBucket` stub: a no-op `200` once the bucket
/// decodes and binds to the token scope.
#[tracing::instrument(skip(state, headers), fields(bucket))]
pub(crate) async fn s3_create_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let query = parse_s3_query(&uri)?;
    if !classify(&query).is_empty() {
        return Err(S3Error::not_implemented());
    }
    require_s3_bucket_binding(claims, &bucket)?;
    Ok(StatusCode::OK.into_response())
}

/// `HEAD /{bucket}` — `HeadBucket` connect probe: `200` when the bucket
/// decodes and binds, `404 NoSuchBucket` when undecodable, `403 AccessDenied`
/// on claims mismatch.
#[tracing::instrument(skip(state, headers), fields(bucket))]
pub(crate) async fn s3_head_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Read)?;
    let claims = auth.as_ref().map(scope_from_auth);
    require_s3_bucket_binding(claims, &bucket)?;
    Ok(StatusCode::OK.into_response())
}

/// `GET /{bucket}` — query dispatch on the bucket path.
///
/// `?location` serves the `GetBucketLocation` stub
/// (`<LocationConstraint>us-east-1</LocationConstraint>`); `?list-type=2`
/// dispatches to [`s3_list_objects_v2`](listing::s3_list_objects_v2);
/// `?list-type=1` and every other bucket sub-resource respond
/// `501 NotImplemented`.
#[tracing::instrument(skip(state, headers), fields(bucket))]
pub(crate) async fn s3_get_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Read)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let query = parse_s3_query(&uri)?;
    require_s3_bucket_binding(claims, &bucket)?;
    if classify(&query)
        .iter()
        .any(|resource| matches!(resource, S3SubResource::Location))
    {
        return Ok(get_bucket_location_response());
    }
    if classify(&query)
        .iter()
        .any(|resource| matches!(resource, S3SubResource::ListObjects))
    {
        return listing::s3_list_objects_v2(State(state), Path(bucket), uri, headers).await;
    }
    Err(S3Error::not_implemented())
}

/// `DELETE /{bucket}` — `DeleteBucket` is explicitly out of scope:
/// `501 NotImplemented`.
#[tracing::instrument(skip(state, headers), fields(bucket))]
pub(crate) async fn s3_delete_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let _query = parse_s3_query(&uri)?;
    require_s3_bucket_binding(claims, &bucket)?;
    Err(S3Error::not_implemented())
}

/// `POST /{bucket}?delete=` — `DeleteObjects` (batch delete).
///
/// Real clients (mc's `rm`, the AWS SDKs) batch-delete through this endpoint
/// instead of per-object `DELETE`. The XML body lists `<Key>` elements; each
/// object is removed with the same crash-safe ordering as `DeleteObject`
/// (index row first, then record + direct object). Responds `200` with a
/// `<DeleteResult>` envelope. `POST` without `?delete=` is `501`.
#[tracing::instrument(skip(state, headers, body), fields(bucket))]
pub(crate) async fn s3_post_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, S3Error> {
    let auth = authorize_s3(&state, &headers, TokenScope::Write)?;
    let claims = auth.as_ref().map(scope_from_auth);
    let query = parse_s3_query(&uri)?;
    require_s3_bucket_binding(claims, &bucket)?;
    if !classify(&query)
        .iter()
        .any(|resource| matches!(resource, S3SubResource::DeleteObjects))
    {
        return Err(S3Error::not_implemented());
    }

    let scope_namespace = scope_namespace(claims);
    let mut reader = RequestBodyReader::from_body(body, state.config.max_request_body_bytes())
        .map_err(S3Error::from)?;
    let bytes = read_body_to_bytes(&mut reader)
        .await
        .map_err(S3Error::from)?;
    let body_str = std::str::from_utf8(&bytes).map_err(|_error| S3Error::internal())?;
    let keys = parse_delete_object_keys(body_str)?;
    if keys.is_empty() {
        return Err(
            S3Error::internal().with_message("DeleteObjects request listed no keys".to_owned())
        );
    }

    let mut deleted = Vec::with_capacity(keys.len());
    for key in keys {
        let object_key =
            s3_object_key(&scope_namespace, &key).map_err(|_error| S3Error::no_such_key(&key))?;
        // Crash-safe ordering (same as DeleteObject): index row first, then
        // record + direct object.
        let _row_deleted = state
            .backend
            .delete_s3_object(&scope_namespace, &key)
            .await?;
        let _outcome = state.backend.delete_object_if_present(&object_key).await?;
        deleted.push(key);
    }

    let xml = delete_result_xml(&deleted);
    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, s3_xml_content_type())],
        xml,
    )
        .into_response())
}

/// Builds the `DeleteResult` XML envelope.
#[must_use]
fn delete_result_xml(deleted: &[String]) -> String {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );
    for key in deleted {
        use std::fmt::Write as _;
        let _result = writeln!(
            xml,
            "  <Deleted><Key>{}</Key></Deleted>",
            key.replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
        );
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
