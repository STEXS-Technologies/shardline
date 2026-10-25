//! Plain HTTP downloads of Hub files published through native Xet.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use sha2::{Digest, Sha256};
use shardline_protocol::{ByteRange, HttpRangeParseError, parse_http_byte_range};

use super::{
    AppState,
    operational::verify_hub_xet_range_stream,
    reconstruction_helpers::{
        byte_range_stream_response, full_byte_stream_response, single_range_header,
    },
    reconstruction_routes::XetRepository,
};
use crate::{
    ServerError, admission::weights, error::ObjectStoreError, xet_adapter::validate_hash_path,
};

pub(super) async fn download_hub_xet_file(
    State(state): State<Arc<AppState>>,
    Path(sha256): Path<String>,
    repo: XetRepository,
    method: Method,
    headers: HeaderMap,
) -> Result<Response, ServerError> {
    super::operational::reject_commit_token_for_unbound_object(repo.capability())?;
    // Full-file verification can make one analytical burst take several
    // seconds on a small host. Queue within a finite budget rather than
    // increasing the number of active verifications to handle the burst.
    let admit = download_admission(&state.admission, std::time::Duration::from_secs(30)).await?;
    validate_hash_path(&sha256)?;
    let scope = repo.capability().namespace().ok_or(ServerError::NotFound)?;
    let identity = state
        .backend
        .xet_file_identity(scope, &sha256)
        .await?
        .ok_or(ServerError::NotFound)?;
    let total = identity.total_bytes;
    // A zero-length response has no final frame to withhold. Reject a forged
    // empty-file identity before headers can declare the transfer complete.
    if total == 0 && identity.sha256 != hex::encode(Sha256::digest(b"")) {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    let etag = format!("\"{}\"", identity.sha256);
    let selection = select(&headers, &method, total, &etag)?;
    let mut response = match selection {
        Selection::Status(status) => {
            let mut response = status.into_response();
            if status == StatusCode::RANGE_NOT_SATISFIABLE {
                response.headers_mut().insert(
                    header::CONTENT_RANGE,
                    format!("bytes */{total}")
                        .parse()
                        .map_err(|_error| ServerError::InvalidRangeHeader)?,
                );
            }
            response
        }
        Selection::Full | Selection::Partial(_) if method == Method::HEAD => {
            let mut response = Response::new(Body::empty());
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, total.into());
            response
        }
        selection @ (Selection::Full | Selection::Partial(_)) => {
            let (source, length) = state
                .backend
                .read_file_stream_scoped(&identity.file_id, None, None, Some(scope))
                .await?;
            if length != total {
                return Err(ServerError::ObjectStore(
                    ObjectStoreError::StoredLengthMismatch,
                ));
            }
            let range = match selection {
                Selection::Partial(range) => Some(range),
                Selection::Full | Selection::Status(_) => None,
            };
            // All source bytes are verified even for a small range. The final
            // selected frame is withheld until the complete SHA-256 is checked.
            let verified = verify_hub_xet_range_stream(source, total, identity.sha256, range);
            let admitted = admitted_stream(verified, admit);
            if let Some(range) = range {
                byte_range_stream_response(
                    admitted,
                    state.transfer_limiter.clone(),
                    range,
                    total,
                    range.len().ok_or(ServerError::Overflow)?,
                )
            } else {
                full_byte_stream_response(admitted, state.transfer_limiter.clone(), total)
            }
        }
    };
    response.headers_mut().insert(
        header::ETAG,
        etag.parse()
            .map_err(|_error| ServerError::InvalidRangeHeader)?,
    );
    response.headers_mut().insert(
        header::ACCEPT_RANGES,
        "bytes"
            .parse()
            .map_err(|_error| ServerError::InvalidRangeHeader)?,
    );
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/octet-stream"
            .parse()
            .map_err(|_error| ServerError::InvalidRangeHeader)?,
    );
    Ok(response)
}

enum Selection {
    Full,
    Partial(ByteRange),
    Status(StatusCode),
}

// Analytical readers issue bursts of independent ranges. Wait before metadata
// or payload work, rather than turning a short burst into a retry storm. The
// active work limit is unchanged and an abandoned future leaves the queue.
async fn download_admission(
    admission: &crate::admission::WeightedAdmission,
    deadline: std::time::Duration,
) -> Result<tokio::sync::OwnedSemaphorePermit, ServerError> {
    tokio::time::timeout(deadline, admission.acquire(weights::RECONSTRUCTION))
        .await
        .map_err(|_elapsed| ServerError::WorkQueueSaturated)?
        .ok_or(ServerError::WorkQueueSaturated)
}

fn admitted_stream(
    source: crate::download_stream::ServerByteStream,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> crate::download_stream::ServerByteStream {
    Box::pin(stream::unfold(
        (source, permit),
        |(mut source, permit)| async move { source.next().await.map(|item| (item, (source, permit))) },
    ))
}

fn etag_matches(headers: &HeaderMap, name: header::HeaderName, etag: &str, weak: bool) -> bool {
    headers.get_all(name).iter().any(|value| {
        shardline_protocol::entity_tag_list_matches(value.as_bytes(), etag.as_bytes(), weak)
    })
}

fn select(
    headers: &HeaderMap,
    method: &Method,
    length: u64,
    etag: &str,
) -> Result<Selection, ServerError> {
    if headers.contains_key(header::IF_MATCH)
        && !etag_matches(headers, header::IF_MATCH, etag, false)
    {
        return Ok(Selection::Status(StatusCode::PRECONDITION_FAILED));
    }
    if etag_matches(headers, header::IF_NONE_MATCH, etag, true) {
        return Ok(Selection::Status(StatusCode::NOT_MODIFIED));
    }
    // RFC 9110 Range applies to GET; date-based If-Range cannot match because
    // this immutable identity endpoint does not advertise Last-Modified.
    if method != Method::GET {
        return Ok(Selection::Full);
    }
    if let Some(value) = single_range_header(headers, header::IF_RANGE)?
        && value.to_str().ok() != Some(etag)
    {
        return Ok(Selection::Full);
    }
    let Some(value) = single_range_header(headers, header::RANGE)? else {
        return Ok(Selection::Full);
    };
    let Ok(value) = value.to_str() else {
        return Ok(Selection::Full);
    };
    Ok(match parse_http_byte_range(value, length) {
        Ok(range) => Selection::Partial(range),
        Err(HttpRangeParseError::Unsatisfiable) => {
            Selection::Status(StatusCode::RANGE_NOT_SATISFIABLE)
        }
        // Unsupported or malformed ranges may be ignored; never synthesize a
        // multipart response or claim that the full body is a partial response.
        Err(_) => Selection::Full,
    })
}

#[cfg(test)]
mod cancellation_http;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn download_admission_waits_without_increasing_active_capacity() {
        let admission =
            crate::admission::WeightedAdmission::new(std::num::NonZeroUsize::new(16).unwrap());
        let held = admission.try_acquire(16).unwrap();
        let waiting = download_admission(&admission, std::time::Duration::from_secs(1));
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiting)
                .await
                .is_err()
        );
        assert_eq!(admission.available_permits(), 0);
        drop(held);
        let acquired = waiting.await.unwrap();
        assert_eq!(admission.available_permits(), 0);
        drop(acquired);
        assert_eq!(admission.available_permits(), 16);
    }

    #[tokio::test]
    async fn timed_out_download_admission_does_not_consume_later_capacity() {
        let admission =
            crate::admission::WeightedAdmission::new(std::num::NonZeroUsize::new(16).unwrap());
        let held = admission.try_acquire(16).unwrap();
        assert!(matches!(
            download_admission(&admission, std::time::Duration::from_millis(10)).await,
            Err(ServerError::WorkQueueSaturated)
        ));
        drop(held);
        assert!(admission.try_acquire(16).is_some());
    }
    use futures_util::TryStreamExt;
    use sha2::{Digest, Sha256};

    #[tokio::test]
    async fn range_cannot_complete_before_unselected_bytes_are_verified() {
        let bytes = b"first half, second half";
        let range = ByteRange::new(0, 3).unwrap();
        for digest in [hex::encode(Sha256::digest(bytes)), "00".repeat(32)] {
            let source = Box::pin(stream::iter([
                Ok(axum::body::Bytes::from_static(&bytes[..10])),
                Ok(axum::body::Bytes::from_static(&bytes[10..])),
            ]));
            let mut verified = verify_hub_xet_range_stream(
                source,
                bytes.len() as u64,
                digest.clone(),
                Some(range),
            );
            if digest.starts_with("00000000") {
                assert!(
                    verified.next().await.unwrap().is_err(),
                    "no selected frame may complete the range before verifying bytes outside it"
                );
            } else {
                assert_eq!(
                    verified.try_collect::<Vec<_>>().await.unwrap().concat(),
                    b"firs"
                );
            }
        }
    }

    #[tokio::test]
    async fn slow_or_cancelled_response_retains_then_releases_admission() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let response = admitted_stream(Box::pin(stream::pending()), permit);
        assert!(semaphore.clone().try_acquire_owned().is_err());
        drop(response);
        assert!(semaphore.try_acquire_owned().is_ok());
    }

    #[test]
    fn conditional_precedence_strong_and_weak_validators() {
        let etag = "\"identity\"";
        let mut headers = HeaderMap::new();
        headers.insert(header::RANGE, "bytes=0-3".parse().unwrap());
        headers.insert(header::IF_MATCH, "W/\"identity\"".parse().unwrap());
        assert!(matches!(
            select(&headers, &Method::GET, 10, etag).unwrap(),
            Selection::Status(StatusCode::PRECONDITION_FAILED)
        ));
        headers.insert(header::IF_MATCH, "*".parse().unwrap());
        headers.insert(
            header::IF_NONE_MATCH,
            "\"other\", W/\"identity\"".parse().unwrap(),
        );
        assert!(matches!(
            select(&headers, &Method::GET, 10, etag).unwrap(),
            Selection::Status(StatusCode::NOT_MODIFIED)
        ));
        headers.remove(header::IF_NONE_MATCH);
        headers.insert(header::IF_RANGE, "W/\"identity\"".parse().unwrap());
        assert!(matches!(
            select(&headers, &Method::GET, 10, etag).unwrap(),
            Selection::Full
        ));
        headers.insert(header::IF_RANGE, etag.parse().unwrap());
        assert!(matches!(
            select(&headers, &Method::GET, 10, etag).unwrap(),
            Selection::Partial(_)
        ));
        assert!(matches!(
            select(&headers, &Method::HEAD, 10, etag).unwrap(),
            Selection::Full
        ));
    }

    #[test]
    fn opaque_list_member_preserves_following_validator() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::IF_MATCH,
            axum::http::HeaderValue::from_bytes(b"\"\x80,opaque\", \"identity\"").unwrap(),
        );
        assert!(matches!(
            select(&headers, &Method::GET, 10, "\"identity\"").unwrap(),
            Selection::Full
        ));
        headers.remove(header::IF_MATCH);
        headers.insert(
            header::IF_NONE_MATCH,
            axum::http::HeaderValue::from_bytes(b"\"\x80,opaque\", W/\"identity\"").unwrap(),
        );
        assert!(matches!(
            select(&headers, &Method::GET, 10, "\"identity\"").unwrap(),
            Selection::Status(StatusCode::NOT_MODIFIED)
        ));
    }
}
