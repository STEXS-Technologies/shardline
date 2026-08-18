use std::{collections::BTreeMap, sync::Arc};

use axum::{
    http::{HeaderMap, HeaderValue, header::CONTENT_TYPE},
    response::Response,
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::{ServerError, metrics};

use super::{AppState, byte_range_stream_response, full_byte_stream_response};

pub(crate) async fn direct_object_response(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    object_key: &shardline_storage::ObjectKey,
    content_type: &str,
    content_digest: Option<String>,
    protocol: &str,
    repository_scope: Option<&shardline_protocol::RepositoryScope>,
) -> Result<Response, ServerError> {
    let total_length = state
        .backend
        .object_length_scoped(object_key, repository_scope)
        .await?;
    let range = parse_optional_range(headers, total_length)?;
    let byte_stream = state
        .backend
        .read_object_stream_scoped(object_key, total_length, range, repository_scope)
        .await?;
    let mut response = if let Some(range) = range {
        metrics::record_range_request();
        let transfer_length = range.len().ok_or(ServerError::Overflow)?;
        byte_range_stream_response(
            byte_stream,
            state.transfer_limiter.clone(),
            range,
            total_length,
            transfer_length,
        )
    } else {
        full_byte_stream_response(byte_stream, state.transfer_limiter.clone(), total_length)
    };
    let content_type_value = HeaderValue::from_str(content_type)
        .map_err(|_error| ServerError::InvalidManifestReference)?;
    response
        .headers_mut()
        .insert(CONTENT_TYPE, content_type_value);
    if let Some(content_digest) = content_digest {
        let digest_value =
            HeaderValue::from_str(&content_digest).map_err(|_error| ServerError::InvalidDigest)?;
        response
            .headers_mut()
            .insert("Docker-Content-Digest", digest_value);
    }
    metrics::record_download(protocol, total_length, 0.0, true);
    Ok(response)
}

fn parse_optional_range(
    headers: &HeaderMap,
    total_length: u64,
) -> Result<Option<ByteRange>, ServerError> {
    let Some(range) = headers.get(axum::http::header::RANGE) else {
        return Ok(None);
    };
    let range = range
        .to_str()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let range = parse_http_byte_range(range, total_length).map_err(ServerError::from)?;
    Ok(Some(range))
}

pub(crate) fn parse_upload_content_range(value: &str) -> Result<ByteRange, ServerError> {
    let value = value.trim();
    let value = value.strip_prefix("bytes ").unwrap_or(value);
    let value = value.split_once('/').map_or(value, |(range, _rest)| range);
    let Some((start, end)) = value.split_once('-') else {
        return Err(ServerError::InvalidRangeHeader);
    };
    let start = start
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let end = end
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    ByteRange::new(start, end).map_err(|_error| ServerError::InvalidRangeHeader)
}

pub(crate) fn ensure_upload_growth_within_limit(
    state: &Arc<AppState>,
    current_length: u64,
    additional_bytes: usize,
) -> Result<(), ServerError> {
    let additional_bytes = u64::try_from(additional_bytes)?;
    let next_length = current_length
        .checked_add(additional_bytes)
        .ok_or(ServerError::Overflow)?;
    let max_bytes = u64::try_from(state.config.max_request_body_bytes().get())?;
    if next_length > max_bytes {
        return Err(ServerError::RequestBodyTooLarge);
    }

    Ok(())
}

pub(crate) fn parse_query_map(
    uri: &axum::http::Uri,
) -> Result<BTreeMap<String, String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(BTreeMap::new());
    };
    if query.len() > super::MAX_PROTOCOL_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }

    Ok(url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect())
}

pub(crate) fn parse_query_values(
    uri: &axum::http::Uri,
    key: &str,
) -> Result<Vec<String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };
    if query.len() > super::MAX_PROTOCOL_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }

    Ok(url::form_urlencoded::parse(query.as_bytes())
        .filter_map(|(candidate_key, value)| (candidate_key == key).then(|| value.into_owned()))
        .collect())
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue, Uri};

    use super::*;

    #[test]
    fn parse_optional_range_returns_none_when_header_absent() {
        let headers = HeaderMap::new();
        let result = parse_optional_range(&headers, 1024);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn parse_optional_range_parses_valid_range_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::RANGE,
            HeaderValue::from_static("bytes=0-9"),
        );
        let result = parse_optional_range(&headers, 100);
        let range = result.unwrap().unwrap();
        assert_eq!(range.start(), 0);
        assert_eq!(range.end_inclusive(), 9);
    }

    #[test]
    fn parse_optional_range_rejects_invalid_range_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::RANGE,
            HeaderValue::from_static("bytes=abc-def"),
        );
        let result = parse_optional_range(&headers, 100);
        assert!(matches!(result, Err(ServerError::InvalidRangeHeader)));
    }

    #[test]
    fn parse_upload_content_range_plain_range() {
        let range = parse_upload_content_range("0-9").unwrap();
        assert_eq!(range.start(), 0);
        assert_eq!(range.end_inclusive(), 9);
    }

    #[test]
    fn parse_upload_content_range_with_bytes_prefix_and_total() {
        let range = parse_upload_content_range("bytes 10-19/20").unwrap();
        assert_eq!(range.start(), 10);
        assert_eq!(range.end_inclusive(), 19);
    }

    #[test]
    fn parse_upload_content_range_with_unknown_total() {
        let range = parse_upload_content_range("bytes 20-29/*").unwrap();
        assert_eq!(range.start(), 20);
        assert_eq!(range.end_inclusive(), 29);
    }

    #[test]
    fn parse_upload_content_range_rejects_invalid_input() {
        assert!(matches!(
            parse_upload_content_range("invalid"),
            Err(ServerError::InvalidRangeHeader)
        ));
    }

    #[test]
    fn parse_upload_content_range_rejects_empty_string() {
        assert!(matches!(
            parse_upload_content_range(""),
            Err(ServerError::InvalidRangeHeader)
        ));
    }

    #[test]
    fn parse_upload_content_range_rejects_negative_start() {
        assert!(matches!(
            parse_upload_content_range("bytes -1-9"),
            Err(ServerError::InvalidRangeHeader)
        ));
    }

    #[test]
    fn parse_upload_content_range_rejects_non_numeric() {
        assert!(matches!(
            parse_upload_content_range("bytes abc-def"),
            Err(ServerError::InvalidRangeHeader)
        ));
    }

    #[test]
    fn parse_query_map_returns_empty_for_no_query() {
        let uri: Uri = "/v2/repo/blobs/uploads".parse().unwrap();
        let result = parse_query_map(&uri).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_query_map_parses_single_key_value() {
        let uri: Uri = "/v2/repo/blobs/uploads?mount=abc123".parse().unwrap();
        let result = parse_query_map(&uri).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result.get("mount").unwrap(), "abc123");
    }

    #[test]
    fn parse_query_map_parses_multiple_key_values() {
        let uri: Uri = "/v2/repo/blobs/uploads?a=1&b=2".parse().unwrap();
        let result = parse_query_map(&uri).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result.get("a").unwrap(), "1");
        assert_eq!(result.get("b").unwrap(), "2");
    }

    #[test]
    fn parse_query_map_rejects_oversized_query() {
        let long_value = "a".repeat(super::super::super::MAX_PROTOCOL_QUERY_BYTES + 1);
        let uri = Uri::builder()
            .path_and_query(format!("/v2/repo/blobs/uploads?key={long_value}"))
            .build()
            .unwrap();
        assert!(matches!(
            parse_query_map(&uri),
            Err(ServerError::RequestQueryTooLarge)
        ));
    }

    #[test]
    fn parse_query_values_returns_empty_for_no_query() {
        let uri: Uri = "/v2/repo/blobs/uploads".parse().unwrap();
        let result = parse_query_values(&uri, "scope").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_query_values_extracts_matching_key() {
        let uri: Uri = "/v2/repo/blobs/uploads?scope=repo:pull".parse().unwrap();
        let result = parse_query_values(&uri, "scope").unwrap();
        assert_eq!(result, vec!["repo:pull"]);
    }

    #[test]
    fn parse_query_values_extracts_multiple_matching_keys() {
        let uri: Uri = "/v2/repo/blobs/uploads?scope=a&scope=b".parse().unwrap();
        let result = parse_query_values(&uri, "scope").unwrap();
        assert_eq!(result, vec!["a", "b"]);
    }

    #[test]
    fn parse_query_values_returns_empty_when_key_missing() {
        let uri: Uri = "/v2/repo/blobs/uploads?other=val".parse().unwrap();
        let result = parse_query_values(&uri, "scope").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn parse_query_values_rejects_oversized_query() {
        let long_value = "a".repeat(super::super::super::MAX_PROTOCOL_QUERY_BYTES + 1);
        let uri = Uri::builder()
            .path_and_query(format!("/v2/repo/blobs/uploads?scope={long_value}"))
            .build()
            .unwrap();
        assert!(matches!(
            parse_query_values(&uri, "scope"),
            Err(ServerError::RequestQueryTooLarge)
        ));
    }
}
