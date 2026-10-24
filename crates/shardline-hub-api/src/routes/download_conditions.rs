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
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .any(|value| {
            value == "*" || value == etag || (weak && value.strip_prefix("W/") == Some(etag))
        })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

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
