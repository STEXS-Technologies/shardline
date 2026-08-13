//! S3 object-data routes (`PUT`/`GET`/`HEAD`/`DELETE /{bucket}/{*key}`) and
//! bucket stubs (`/{bucket}`) for the S3 frontend (Lane 3).
//!
//! Routing is `/{bucket}/{*key}` where `{bucket}` is the single dotted segment
//! `{owner}.{name}` of the bearer token's `RepositoryScope` and `{*key}` is the
//! arbitrary-depth S3 object key. Every handler authenticates with the
//! SigV4→bearer bridge ([`authorize_s3`]), binds the bucket to the token
//! claims, and then dispatches on the query sub-resources.

pub(super) mod bucket;
pub(super) mod listing;
pub(super) mod multipart;
pub(super) mod object;

#[cfg(test)]
mod tests;

use axum::http::{HeaderMap, HeaderValue, Uri, header::AUTHORIZATION};
use shardline_protocol::{RepositoryScope, TokenScope};
use shardline_s3_adapter::{
    QueryMap, S3Error, extract_access_key, require_s3_bucket_binding, s3_object_key,
};
use shardline_storage::ObjectKey;

use crate::{ServerError, app::AppState, auth::AuthContext, protocol_support::scope_namespace};

pub(crate) use bucket::{s3_create_bucket, s3_delete_bucket, s3_get_bucket, s3_head_bucket};
pub(crate) use object::{
    s3_delete_object, s3_get_object, s3_head_object, s3_post_object, s3_put_object,
};

/// Maximum accepted S3 query-string length (reuses the protocol query budget).
use crate::app::MAX_PROTOCOL_QUERY_BYTES;

/// The S3 XML content type.
const S3_XML_CONTENT_TYPE: &str = "application/xml";

/// Authenticates an S3 request by bridging the SigV4 access key (or a plain
/// bearer token) to the Shardline token verifier.
///
/// The client's access key **is** the bearer token, so it is extracted from the
/// `Authorization: AWS4-HMAC-SHA256 Credential=...` header (or `Bearer`/`x-amz-security-token`)
/// and verified against the configured auth provider. With no auth provider
/// configured the request is allowed through (strict mode fails closed).
///
/// # Errors
///
/// Returns [`S3Error::access_denied`] when the access key is absent or the
/// token fails verification or scope checks.
pub(super) fn authorize_s3(
    state: &AppState,
    headers: &HeaderMap,
    required_scope: TokenScope,
) -> Result<Option<AuthContext>, S3Error> {
    if let Some(auth) = &state.auth {
        let access_key = extract_access_key(headers).ok_or_else(S3Error::access_denied)?;
        let mut bearer_headers = HeaderMap::new();
        bearer_headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {access_key}"))
                .map_err(|_error| S3Error::access_denied())?,
        );
        let context = auth
            .authorize(&bearer_headers, required_scope)
            .map_err(S3Error::from)?;
        return Ok(Some(context));
    }

    // No auth provider configured.
    if state.config.deployment_mode() == crate::config::DeploymentMode::Strict {
        return Err(S3Error::internal());
    }
    Ok(None)
}

/// The scope namespace, storage object key, and client key for one S3 request.
pub(super) struct S3ObjectContext {
    /// The bucket name (`{owner}.{name}`).
    pub(super) bucket: String,
    /// sha256 repository-scope namespace keying the listing index rows.
    pub(super) scope_namespace: String,
    /// The client-facing S3 object key (no `protocols/s3/` prefix).
    pub(super) key: String,
    /// The storage object key (`protocols/s3/{scope_namespace}/{key}`).
    pub(super) object_key: ObjectKey,
}

/// Binds the bucket to the token claims and derives the storage object key.
///
/// # Errors
///
/// Returns [`S3Error::no_such_bucket`] when the bucket cannot be decoded,
/// [`S3Error::access_denied`] on claims mismatch, and
/// [`S3Error::no_such_key`] when the object key is invalid.
pub(super) fn require_s3_object_context(
    claims: Option<&RepositoryScope>,
    bucket: &str,
    key: &str,
) -> Result<S3ObjectContext, S3Error> {
    require_s3_bucket_binding(claims, bucket)?;
    let scope_namespace = scope_namespace(claims);
    let object_key =
        s3_object_key(&scope_namespace, key).map_err(|_error| S3Error::no_such_key(key))?;
    Ok(S3ObjectContext {
        bucket: bucket.to_owned(),
        scope_namespace,
        key: key.to_owned(),
        object_key,
    })
}

/// Parses the ordered, percent-decoded query pairs of a request URI.
///
/// # Errors
///
/// Returns an internal [`S3Error`] when the query exceeds the bounded metadata
/// parser budget.
pub(super) fn parse_s3_query(uri: &Uri) -> Result<QueryMap, S3Error> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };
    if query.len() > MAX_PROTOCOL_QUERY_BYTES {
        return Err(S3Error::from(ServerError::RequestQueryTooLarge));
    }
    Ok(url::form_urlencoded::parse(query.as_bytes())
        .map(|(name, value)| (name.into_owned(), value.into_owned()))
        .collect())
}

/// Formats unix seconds as an RFC 7231 IMF-fixdate HTTP date (`GMT`).
///
/// Falls back to the Unix epoch when the timestamp is out of the representable
/// `chrono` range (a timestamp of `0` yields the epoch date).
#[must_use]
pub(super) fn format_http_date(unix_seconds: i64) -> String {
    chrono::DateTime::from_timestamp(unix_seconds, 0)
        .map(|date_time| date_time.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| "Thu, 01 Jan 1970 00:00:00 GMT".to_owned())
}

/// Returns `true` when the query contains any recognized S3 sub-resource.
#[must_use]
pub(super) fn has_sub_resource(query: &QueryMap) -> bool {
    !shardline_s3_adapter::classify(query).is_empty()
}

/// The S3 XML content-type header value used by error and stub responses.
#[must_use]
pub(super) const fn s3_xml_content_type() -> &'static str {
    S3_XML_CONTENT_TYPE
}
