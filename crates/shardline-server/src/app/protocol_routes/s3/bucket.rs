//! S3 bucket routes: the `/{bucket}` stubs.
//!
//! `CreateBucket` is a no-op `200` for S3A / object_store missing-bucket
//! probes, `HeadBucket` answers the connect probe (`200`, `404 NoSuchBucket`,
//! `403 AccessDenied`), `GetBucketLocation` returns the `us-east-1` stub, and
//! `DeleteBucket` plus `ListObjectsV2` (Lane 5) respond `501 NotImplemented`.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use shardline_protocol::TokenScope;
use shardline_s3_adapter::{S3Error, S3SubResource, classify, require_s3_bucket_binding};

use super::{authorize_s3, parse_s3_query, s3_xml_content_type};
use crate::app::{AppState, scope_from_auth};

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
/// (`ListObjectsV2`) and every other bucket sub-resource respond
/// `501 NotImplemented` (listing is Lane 5; the rest are out of scope).
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
