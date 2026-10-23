//! Persistence for the object's standard S3 `Content-Type` property.

use axum::http::{HeaderMap, HeaderValue, header::CONTENT_TYPE};

use shardline_s3_adapter::S3Error;

pub(super) const METADATA_KEY: &str = "shardline-internal-s3-content-type-v1";

pub(super) fn capture_header(
    headers: &HeaderMap,
    metadata: &mut Vec<(String, String)>,
) -> Result<(), S3Error> {
    let mut values = headers.get_all(CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() {
        return Err(S3Error::invalid_argument("Repeated Content-Type header"));
    }
    let value = value
        .to_str()
        .map_err(|_error| S3Error::invalid_argument("Invalid Content-Type header"))?;
    attach(metadata, value)
}

pub(super) fn attach(metadata: &mut Vec<(String, String)>, value: &str) -> Result<(), S3Error> {
    HeaderValue::from_str(value)
        .map_err(|_error| S3Error::invalid_argument("Invalid Content-Type value"))?;
    metadata.retain(|(name, _)| !name.eq_ignore_ascii_case(METADATA_KEY));
    metadata.push((METADATA_KEY.to_owned(), value.to_owned()));
    Ok(())
}

pub(super) fn read(metadata: &[(String, String)]) -> Result<Option<HeaderValue>, S3Error> {
    let mut values = metadata
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case(METADATA_KEY));
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
    name.eq_ignore_ascii_case(METADATA_KEY)
}
