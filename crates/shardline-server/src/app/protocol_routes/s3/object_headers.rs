//! Persistence for standard S3 object HTTP metadata.
//!
//! These values share the existing per-object metadata column. The internal
//! keys are hidden from `x-amz-meta-*` responses and remain readable by
//! binaries that already understand the Content-Type key.

use axum::{
    http::{HeaderMap, HeaderName, HeaderValue, header::CONTENT_TYPE},
    response::Response,
};

use shardline_s3_adapter::S3Error;

const CONTENT_TYPE_KEY: &str = "shardline-internal-s3-content-type-v1";
const CACHE_CONTROL_KEY: &str = "shardline-internal-s3-cache-control-v1";
const CONTENT_DISPOSITION_KEY: &str = "shardline-internal-s3-content-disposition-v1";
const CONTENT_ENCODING_KEY: &str = "shardline-internal-s3-content-encoding-v1";
const EXPIRES_KEY: &str = "shardline-internal-s3-expires-v1";
const WEBSITE_REDIRECT_KEY: &str = "shardline-internal-s3-website-redirect-v1";
pub(super) const TAGGING_KEY: &str = "shardline-internal-s3-tagging-v1";

const PROPERTIES: [(&str, &str); 6] = [
    ("content-type", CONTENT_TYPE_KEY),
    ("cache-control", CACHE_CONTROL_KEY),
    ("content-disposition", CONTENT_DISPOSITION_KEY),
    ("content-encoding", CONTENT_ENCODING_KEY),
    ("expires", EXPIRES_KEY),
    ("x-amz-website-redirect-location", WEBSITE_REDIRECT_KEY),
];

pub(super) fn capture_headers(
    headers: &HeaderMap,
    metadata: &mut Vec<(String, String)>,
) -> Result<(), S3Error> {
    for (header_name, metadata_key) in PROPERTIES {
        let header =
            HeaderName::from_bytes(header_name.as_bytes()).map_err(|_error| S3Error::internal())?;
        let mut values = headers.get_all(&header).iter();
        let Some(value) = values.next() else {
            continue;
        };
        if values.next().is_some() {
            return Err(S3Error::invalid_argument(
                "Repeated S3 object metadata header",
            ));
        }
        let value = value
            .to_str()
            .map_err(|_error| S3Error::invalid_argument("Invalid S3 object metadata header"))?;
        attach(metadata, metadata_key, header_name, value)?;
    }
    Ok(())
}

/// Rejects S3 object options whose behavior Shardline does not persist or
/// enforce. Silently accepting these options would tell clients that a policy
/// was applied when it was not.
pub(super) fn reject_unsupported_read_headers(headers: &HeaderMap) -> Result<(), S3Error> {
    for name in headers.keys().map(HeaderName::as_str) {
        if name.starts_with("x-amz-server-side-encryption")
            || name.starts_with("x-amz-copy-source-server-side-encryption")
            || name == "x-amz-expected-bucket-owner"
            || name == "x-amz-request-payer"
        {
            return Err(S3Error::not_implemented());
        }
    }
    Ok(())
}

pub(super) fn reject_unsupported_write_headers(headers: &HeaderMap) -> Result<(), S3Error> {
    reject_unsupported_read_headers(headers)?;
    for name in headers.keys().map(HeaderName::as_str) {
        if name.starts_with("x-amz-grant-") {
            return Err(S3Error::acl_not_supported());
        }
        if name.starts_with("x-amz-object-lock-") || name == "x-amz-bypass-governance-retention" {
            return Err(S3Error::not_implemented());
        }
    }
    let mut acl_values = headers.get_all("x-amz-acl").iter();
    if let Some(acl) = acl_values.next()
        && (acl_values.next().is_some()
            || !acl
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case("bucket-owner-full-control")))
    {
        return Err(S3Error::acl_not_supported());
    }
    let mut storage_classes = headers.get_all("x-amz-storage-class").iter();
    if let Some(storage_class) = storage_classes.next() {
        if storage_classes.next().is_some() {
            return Err(S3Error::invalid_argument("Repeated x-amz-storage-class"));
        }
        let storage_class = storage_class
            .to_str()
            .map_err(|_error| S3Error::invalid_argument("Invalid x-amz-storage-class"))?;
        if !storage_class.eq_ignore_ascii_case("STANDARD") {
            return Err(S3Error::not_implemented());
        }
    }
    Ok(())
}

pub(super) fn attach_form_field(
    headers: &mut HeaderMap,
    name: &str,
    value: &str,
) -> Result<(), S3Error> {
    let canonical_name = name.to_ascii_lowercase();
    if !PROPERTIES
        .iter()
        .any(|(property_name, _)| *property_name == canonical_name)
    {
        return Err(S3Error::internal());
    }
    let header = HeaderName::from_bytes(canonical_name.as_bytes())
        .map_err(|_error| S3Error::invalid_argument("Invalid S3 form field name"))?;
    let value = HeaderValue::from_str(value)
        .map_err(|_error| S3Error::invalid_argument("Invalid S3 form field value"))?;
    headers.append(header, value);
    Ok(())
}

fn attach(
    metadata: &mut Vec<(String, String)>,
    metadata_key: &str,
    header_name: &str,
    value: &str,
) -> Result<(), S3Error> {
    HeaderValue::from_str(value)
        .map_err(|_error| S3Error::invalid_argument("Invalid S3 object metadata value"))?;
    if header_name == "x-amz-website-redirect-location"
        && (value.len() > 2048
            || !(value.starts_with('/')
                || value.starts_with("http://")
                || value.starts_with("https://")))
    {
        return Err(S3Error::invalid_argument(
            "Invalid x-amz-website-redirect-location value",
        ));
    }
    metadata.retain(|(name, _)| !name.eq_ignore_ascii_case(metadata_key));
    metadata.push((metadata_key.to_owned(), value.to_owned()));
    Ok(())
}

pub(super) fn apply_response_headers(
    response: &mut Response,
    metadata: &[(String, String)],
) -> Result<(), S3Error> {
    let content_type = read(metadata, CONTENT_TYPE_KEY)?
        .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream"));
    response.headers_mut().insert(CONTENT_TYPE, content_type);
    for (header_name, metadata_key) in PROPERTIES.into_iter().skip(1) {
        if let Some(value) = read(metadata, metadata_key)? {
            let header = HeaderName::from_bytes(header_name.as_bytes())
                .map_err(|_error| S3Error::internal())?;
            response.headers_mut().insert(header, value);
        }
    }
    Ok(())
}

fn read(metadata: &[(String, String)], metadata_key: &str) -> Result<Option<HeaderValue>, S3Error> {
    let mut values = metadata
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(metadata_key));
    let Some((_, value)) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(S3Error::internal());
    }
    HeaderValue::from_str(value)
        .map(Some)
        .map_err(|_error| S3Error::internal())
}

pub(super) const fn is_internal_metadata(name: &str) -> bool {
    name.eq_ignore_ascii_case(CONTENT_TYPE_KEY)
        || name.eq_ignore_ascii_case(CACHE_CONTROL_KEY)
        || name.eq_ignore_ascii_case(CONTENT_DISPOSITION_KEY)
        || name.eq_ignore_ascii_case(CONTENT_ENCODING_KEY)
        || name.eq_ignore_ascii_case(EXPIRES_KEY)
        || name.eq_ignore_ascii_case(WEBSITE_REDIRECT_KEY)
        || name.eq_ignore_ascii_case(TAGGING_KEY)
}

pub(super) const fn is_property_header(name: &str) -> bool {
    name.eq_ignore_ascii_case("content-type")
        || name.eq_ignore_ascii_case("cache-control")
        || name.eq_ignore_ascii_case("content-disposition")
        || name.eq_ignore_ascii_case("content-encoding")
        || name.eq_ignore_ascii_case("expires")
        || name.eq_ignore_ascii_case("x-amz-website-redirect-location")
}
