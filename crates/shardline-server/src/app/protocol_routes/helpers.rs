use std::{collections::BTreeMap, sync::Arc};

use axum::{
    http::{
        HeaderMap, HeaderValue,
        header::{CONTENT_LENGTH, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::{ServerError, metrics};

use super::super::reconstruction_helpers::single_range_header;
use super::{AppState, byte_range_stream_response, full_byte_stream_response};

pub(super) fn object_head_response(total_length: u64) -> Response {
    (
        [(CONTENT_LENGTH, total_length.to_string())],
        [(CONTENT_TYPE, "application/octet-stream")],
    )
        .into_response()
}

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
    let selected_length = range
        .map_or(Some(total_length), |range| range.len())
        .ok_or(ServerError::Overflow)?;
    let mut response = if let Some(range) = range {
        metrics::record_range_request();
        byte_range_stream_response(
            byte_stream,
            state.transfer_limiter.clone(),
            range,
            total_length,
            selected_length,
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

fn bounded_query_pairs(
    uri: &axum::http::Uri,
) -> Result<url::form_urlencoded::Parse<'_>, ServerError> {
    let query = uri.query().unwrap_or_default();
    if query.len() > super::MAX_PROTOCOL_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }
    Ok(url::form_urlencoded::parse(query.as_bytes()))
}

pub(crate) fn parse_query_map(
    uri: &axum::http::Uri,
) -> Result<BTreeMap<String, String>, ServerError> {
    Ok(bounded_query_pairs(uri)?.into_owned().collect())
}

pub(crate) fn parse_query_values(
    uri: &axum::http::Uri,
    key: &str,
) -> Result<Vec<String>, ServerError> {
    Ok(bounded_query_pairs(uri)?
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
    fn optional_range_distinguishes_absent_valid_and_malformed_headers() {
        let mut headers = HeaderMap::new();
        assert!(parse_optional_range(&headers, 100).unwrap().is_none());
        headers.insert(
            axum::http::header::RANGE,
            HeaderValue::from_static("bytes=0-9"),
        );
        let range = parse_optional_range(&headers, 100).unwrap().unwrap();
        assert_eq!((range.start(), range.end_inclusive()), (0, 9));
        headers.insert(
            axum::http::header::RANGE,
            HeaderValue::from_static("bytes=abc-def"),
        );
        assert!(matches!(
            parse_optional_range(&headers, 100),
            Err(ServerError::InvalidRangeHeader)
        ));
    }

    #[test]
    fn parse_upload_content_range_validates_complete_length() {
        for (value, expected) in [
            ("0-9", (0, 9)),
            ("bytes 10-19/20", (10, 19)),
            ("bytes 20-29/*", (20, 29)),
            ("0-4", (0, 4)),
            ("bytes 0-4", (0, 4)),
            ("0-4/5", (0, 4)),
            ("bytes 0-4/5", (0, 4)),
            ("0-4/*", (0, 4)),
            ("bytes 0-4/*", (0, 4)),
        ] {
            let range = parse_upload_content_range(value).unwrap();
            assert_eq!((range.start(), range.end_inclusive()), expected, "{value}");
        }
        for value in [
            "invalid",
            "",
            "bytes -1-9",
            "bytes abc-def",
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
    fn query_maps_preserve_decoded_keys_and_last_values() {
        for (uri, expected) in [
            ("/v2/repo/blobs/uploads", vec![]),
            (
                "/v2/repo/blobs/uploads?mount=abc123",
                vec![("mount", "abc123")],
            ),
            (
                "/v2/repo/blobs/uploads?a=1&b=2",
                vec![("a", "1"), ("b", "2")],
            ),
            ("/v2/repo/blobs/uploads?a=1&a=2", vec![("a", "2")]),
        ] {
            let result = parse_query_map(&uri.parse().unwrap()).unwrap();
            let pairs: Vec<_> = result
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect();
            assert_eq!(pairs, expected, "{uri}");
        }
    }

    #[test]
    fn query_values_preserve_all_matching_values_in_order() {
        for (uri, expected) in [
            ("/v2/repo/blobs/uploads", vec![]),
            ("/v2/repo/blobs/uploads?scope=repo:pull", vec!["repo:pull"]),
            ("/v2/repo/blobs/uploads?scope=a&scope=b", vec!["a", "b"]),
            ("/v2/repo/blobs/uploads?other=val", vec![]),
        ] {
            assert_eq!(
                parse_query_values(&uri.parse().unwrap(), "scope").unwrap(),
                expected,
                "{uri}"
            );
        }
    }

    #[test]
    fn query_parsers_enforce_the_same_byte_limit() {
        let limit = super::super::super::MAX_PROTOCOL_QUERY_BYTES;
        let at_limit = "a".repeat(limit);
        let uri: Uri = format!("/v2/repo/blobs/uploads?{at_limit}")
            .parse()
            .unwrap();
        assert_eq!(parse_query_map(&uri).unwrap().len(), 1);
        assert_eq!(parse_query_values(&uri, &at_limit).unwrap(), vec![""]);
        let oversized = "a".repeat(limit + 1);
        for query in [
            oversized.clone(),
            format!("key={oversized}"),
            format!("scope={oversized}"),
        ] {
            let uri: Uri = format!("/v2/repo/blobs/uploads?{query}").parse().unwrap();
            assert!(matches!(
                parse_query_map(&uri),
                Err(ServerError::RequestQueryTooLarge)
            ));
            assert!(matches!(
                parse_query_values(&uri, "scope"),
                Err(ServerError::RequestQueryTooLarge)
            ));
        }
    }
}
