//! Preconditions for immutable Hub downloads, evaluated after authorization.

use axum::{
    body::Body,
    http::{HeaderMap, StatusCode, header},
    response::Response,
};

use crate::error::HubApiError;

/// Only entity-tag validators are advertised by these immutable download routes.
/// Without a Last-Modified value, date-based conditions are not evaluated.
pub(super) fn precondition_response(
    headers: &HeaderMap,
    etag: &str,
) -> Result<Option<Response>, HubApiError> {
    let status = if headers.contains_key(header::IF_MATCH)
        && !matches_etag(headers, header::IF_MATCH, etag, false)
    {
        Some(StatusCode::PRECONDITION_FAILED)
    } else if matches_etag(headers, header::IF_NONE_MATCH, etag, true) {
        Some(StatusCode::NOT_MODIFIED)
    } else {
        None
    };
    let Some(status) = status else {
        return Ok(None);
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::ETAG,
        etag.parse().map_err(|_error| HubApiError::NotFound)?,
    );
    Ok(Some(response))
}

fn matches_etag(headers: &HeaderMap, name: header::HeaderName, etag: &str, weak: bool) -> bool {
    shardline_protocol::entity_tag_fields_match(
        headers.get_all(name).iter().map(|value| value.as_bytes()),
        etag.as_bytes(),
        weak,
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn repeated_validator_fields_preserve_combined_grammar() {
        for name in [header::IF_MATCH, header::IF_NONE_MATCH] {
            for values in [["\"hash\"", "broken"], ["*", "\"other\""]] {
                let mut headers = HeaderMap::new();
                for value in values {
                    headers.append(name.clone(), value.parse().unwrap());
                }
                let status = precondition_response(&headers, "\"hash\"")
                    .unwrap()
                    .map(|response| response.status());
                assert_eq!(
                    status,
                    (name == header::IF_MATCH).then_some(StatusCode::PRECONDITION_FAILED),
                    "repeated fields must retain the combined field grammar"
                );
            }
        }
    }

    #[test]
    fn opaque_list_member_preserves_following_validator() {
        let mut headers = HeaderMap::new();
        for name in [header::IF_MATCH, header::IF_NONE_MATCH] {
            headers.clear();
            headers.insert(
                name.clone(),
                axum::http::HeaderValue::from_bytes(b"\"\x80,opaque\", \"hash\"").unwrap(),
            );
            let response = precondition_response(&headers, "\"hash\"").unwrap();
            if name == header::IF_MATCH {
                assert!(response.is_none());
            } else {
                assert_eq!(response.unwrap().status(), StatusCode::NOT_MODIFIED);
            }
        }
    }

    #[test]
    fn strong_precondition_precedes_weak_cache_validation_and_range() {
        let mut headers = HeaderMap::new();
        headers.insert(header::IF_MATCH, "W/\"hash\"".parse().unwrap());
        headers.insert(header::IF_NONE_MATCH, "*".parse().unwrap());
        headers.insert(header::RANGE, "bytes=999999-".parse().unwrap());
        assert_eq!(
            precondition_response(&headers, "\"hash\"")
                .unwrap()
                .unwrap()
                .status(),
            StatusCode::PRECONDITION_FAILED
        );
        headers.insert(header::IF_MATCH, "*".parse().unwrap());
        headers.remove(header::IF_NONE_MATCH);
        headers.append(header::IF_NONE_MATCH, "\"other\"".parse().unwrap());
        headers.append(header::IF_NONE_MATCH, "W/\"hash\"".parse().unwrap());
        let response = precondition_response(&headers, "\"hash\"")
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(response.headers()[header::ETAG], "\"hash\"");
        assert!(!response.headers().contains_key(header::CONTENT_RANGE));
        assert!(!response.headers().contains_key(header::CONTENT_LENGTH));
    }
}
