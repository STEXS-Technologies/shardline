use std::{collections::BTreeMap, sync::Arc};

use axum::{
    http::{HeaderMap, HeaderValue, header::CONTENT_TYPE},
    response::Response,
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::{ServerError, metrics};

use super::super::reconstruction_helpers::single_range_header;
use super::{AppState, byte_range_stream_response, full_byte_stream_response};

pub(crate) async fn direct_object_response(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    object_key: &shardline_storage::ObjectKey,
    content_type: &str,
    content_digest: Option<String>,
    protocol: &str,
) -> Result<Response, ServerError> {
    let snapshot = state.backend.object_read_snapshot(object_key).await?;
    direct_object_response_from_snapshot(
        state,
        headers,
        object_key,
        content_type,
        content_digest,
        protocol,
        snapshot,
    )
    .await
}

pub(crate) async fn direct_object_response_from_snapshot(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    object_key: &shardline_storage::ObjectKey,
    content_type: &str,
    content_digest: Option<String>,
    protocol: &str,
    snapshot: crate::backend::ObjectReadSnapshot,
) -> Result<Response, ServerError> {
    let total_length = snapshot.total_length;
    let range = parse_optional_range(headers, total_length)?;
    let byte_stream = state
        .backend
        .read_object_stream_from_snapshot(object_key, snapshot, range)
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
    // Count the payload selected for this response before its body is polled.
    // This is not a count of bytes acknowledged by the client.
    let selected_length = range
        .map_or(Some(total_length), |range| range.len())
        .ok_or(ServerError::Overflow)?;
    metrics::record_download(protocol, selected_length, 0.0, true);
    Ok(response)
}

fn parse_optional_range(
    headers: &HeaderMap,
    total_length: u64,
) -> Result<Option<ByteRange>, ServerError> {
    // These direct-object responses expose neither ETag nor Last-Modified.
    // There is therefore no current validator that can strongly match an
    // If-Range condition. RFC 9110 section 13.1.5 requires the complete
    // representation when that condition is false, before parsing Range.
    if headers.contains_key(axum::http::header::IF_RANGE) {
        return Ok(None);
    }
    let Some(range) = single_range_header(headers, axum::http::header::RANGE)? else {
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
    // Docker upload ranges use bare start-end. Retain that form, while
    // validating the optional complete length instead of discarding it.
    let (value, complete_length) = value
        .split_once('/')
        .map_or((value, None), |(range, total)| (range, Some(total)));
    let Some((start, end)) = value.split_once('-') else {
        return Err(ServerError::InvalidRangeHeader);
    };
    let start = start
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let end = end
        .parse::<u64>()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    if let Some(total) = complete_length
        && total != "*"
    {
        if total.is_empty() || !total.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(ServerError::InvalidRangeHeader);
        }
        let total = total
            .parse::<u64>()
            .map_err(|_error| ServerError::InvalidRangeHeader)?;
        if total <= end {
            return Err(ServerError::InvalidRangeHeader);
        }
    }
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

    #[tokio::test]
    async fn direct_object_http_if_range_without_validator_returns_full_representation() {
        use std::num::NonZeroUsize;

        use axum::{
            body::Body,
            http::{Request, StatusCode, header},
        };
        use sha2::{Digest, Sha256};
        use tower::ServiceExt;

        let root = tempfile::TempDir::new().unwrap();
        let config = crate::ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://127.0.0.1:0".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(65536).unwrap(),
        )
        .with_server_frontends([crate::ServerFrontend::Lfs, crate::ServerFrontend::BazelHttp])
        .unwrap();
        let app = crate::app::router(config).await.unwrap();
        let content = b"full representation for conditional resume";
        let digest = hex::encode(Sha256::digest(content));
        for uri in [
            format!("/v1/lfs/objects/{digest}"),
            format!("/v1/bazel/cache/cas/{digest}"),
            format!("/v1/bazel/cache/ac/{digest}"),
            format!("/v1/bazel/{digest}"),
        ] {
            let uploaded = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("PUT")
                        .uri(&uri)
                        .body(Body::from(content.as_slice()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(
                uploaded.status().is_success(),
                "{uri}: {}",
                uploaded.status()
            );

            let ranged = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(&uri)
                        .header(header::RANGE, "bytes=5-9")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(ranged.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                axum::body::to_bytes(ranged.into_body(), 1024)
                    .await
                    .unwrap()
                    .as_ref(),
                &content[5..10]
            );

            for fields in [
                ["bytes=0-1", "bytes=2-3"],
                ["bytes=2-3", "bytes=0-1"],
                ["bytes=0-1", "invalid"],
                ["invalid", "bytes=0-1"],
                ["bytes=0-1", "bytes=0-1"],
            ] {
                for method in ["GET", "HEAD"] {
                    let response = app
                        .clone()
                        .oneshot(
                            Request::builder()
                                .method(method)
                                .uri(&uri)
                                .header(header::RANGE, fields[0])
                                .header(header::RANGE, fields[1])
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status(),
                        if method == "HEAD" {
                            StatusCode::OK
                        } else {
                            StatusCode::BAD_REQUEST
                        },
                        "{uri}: {method}, {fields:?}"
                    );
                    if method == "HEAD" {
                        assert_eq!(
                            response.headers()[header::CONTENT_LENGTH],
                            content.len().to_string()
                        );
                        assert!(
                            axum::body::to_bytes(response.into_body(), 1024)
                                .await
                                .unwrap()
                                .is_empty()
                        );
                    }
                }
                let fallback = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(&uri)
                            .header(header::RANGE, fields[0])
                            .header(header::RANGE, fields[1])
                            .header(header::IF_RANGE, "\"stale\"")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(fallback.status(), StatusCode::OK);
                assert_eq!(
                    axum::body::to_bytes(fallback.into_body(), 1024)
                        .await
                        .unwrap()
                        .as_ref(),
                    content
                );
            }

            for validator in [
                "\"stale-object\"",
                "W/\"stale-object\"",
                "Wed, 21 Oct 2015 07:28:00 GMT",
            ] {
                // A false If-Range also suppresses malformed/unsatisfiable
                // ranges; parsing those first would incorrectly return 416.
                for range in ["bytes=5-9", "bytes=999999-", "bytes=bad-range"] {
                    let response = app
                        .clone()
                        .oneshot(
                            Request::builder()
                                .uri(&uri)
                                .header(header::RANGE, range)
                                .header(header::IF_RANGE, validator)
                                .body(Body::empty())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status(),
                        StatusCode::OK,
                        "{uri}: {validator}, {range}"
                    );
                    assert!(!response.headers().contains_key(header::CONTENT_RANGE));
                    assert_eq!(
                        response.headers()[header::CONTENT_LENGTH],
                        content.len().to_string()
                    );
                    assert_eq!(
                        axum::body::to_bytes(response.into_body(), 1024)
                            .await
                            .unwrap()
                            .as_ref(),
                        content
                    );
                }
            }
        }
    }

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
    fn parse_upload_content_range_validates_complete_length() {
        for value in [
            "0-4",
            "bytes 0-4",
            "0-4/5",
            "bytes 0-4/5",
            "0-4/*",
            "bytes 0-4/*",
        ] {
            let range = parse_upload_content_range(value).unwrap();
            assert_eq!((range.start(), range.end_inclusive()), (0, 4), "{value}");
        }
        for value in [
            "bytes 0-4/0",
            "bytes 0-4/4",
            "bytes 0-4/",
            "bytes 0-4/not-a-number",
            "bytes 0-4/5/garbage",
            "bytes 0-4/*/garbage",
            "bytes 0-4/+5",
            "bytes 0-4/-5",
            "bytes 0-4/5.0",
            "bytes 0-4/18446744073709551616",
            "0-4/0",
            "0-4/4",
        ] {
            assert!(
                matches!(
                    parse_upload_content_range(value),
                    Err(ServerError::InvalidRangeHeader)
                ),
                "{value}"
            );
        }
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
