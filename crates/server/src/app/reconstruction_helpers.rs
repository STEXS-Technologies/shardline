use std::collections::BTreeSet;

use axum::{
    body::{Body, Bytes},
    http::{
        HeaderMap, StatusCode, Uri,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::{IntoResponse, Response},
};
use futures_util::{StreamExt, stream};
use shardline_protocol::{ByteRange, RepositoryScope, parse_http_byte_range};
use tokio::sync::OwnedSemaphorePermit;

use super::{AppState, MAX_BATCH_RECONSTRUCTION_FILE_IDS, MAX_BATCH_RECONSTRUCTION_QUERY_BYTES};
use crate::{
    ServerError,
    download_stream::ServerByteStream,
    error::ObjectStoreError,
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
    xet_adapter::{
        FileReconstructionResponse, FileReconstructionV2Response, reconstruction_v2_from_v1,
        validate_hash_path,
    },
};

struct TransferByteStreamState {
    byte_stream: ServerByteStream,
    transfer_limiter: TransferLimiter,
    remaining_bytes: u64,
    active_permit: Option<OwnedSemaphorePermit>,
}

pub(super) fn byte_range_stream_response(
    byte_stream: ServerByteStream,
    transfer_limiter: TransferLimiter,
    range: ByteRange,
    total_length: u64,
    transfer_length: u64,
) -> Response {
    (
        StatusCode::PARTIAL_CONTENT,
        [
            (CONTENT_TYPE, "application/octet-stream".to_owned()),
            (ACCEPT_RANGES, "bytes".to_owned()),
            (
                CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    range.start(),
                    range.end_inclusive(),
                    total_length
                ),
            ),
            (CONTENT_LENGTH, transfer_length.to_string()),
        ],
        metered_transfer_body(byte_stream, transfer_limiter, transfer_length),
    )
        .into_response()
}

#[must_use]
pub fn full_byte_stream_response(
    byte_stream: ServerByteStream,
    transfer_limiter: TransferLimiter,
    total_length: u64,
) -> Response {
    (
        StatusCode::OK,
        [
            (CONTENT_TYPE, "application/octet-stream".to_owned()),
            (CONTENT_LENGTH, total_length.to_string()),
            (ACCEPT_RANGES, "bytes".to_owned()),
        ],
        metered_transfer_body(byte_stream, transfer_limiter, total_length),
    )
        .into_response()
}

fn metered_transfer_body(
    byte_stream: ServerByteStream,
    transfer_limiter: TransferLimiter,
    transfer_length: u64,
) -> Body {
    let stream_state = TransferByteStreamState {
        byte_stream,
        transfer_limiter,
        remaining_bytes: transfer_length,
        active_permit: None,
    };
    let body_stream = stream::try_unfold(stream_state, |mut stream_state| async move {
        stream_state.active_permit.take();

        if stream_state.remaining_bytes == 0 {
            let next = stream_state.byte_stream.next().await;
            if next.is_some() {
                return Err(ServerError::ObjectStore(
                    ObjectStoreError::StoredLengthMismatch,
                ));
            }

            return Ok::<Option<(Bytes, TransferByteStreamState)>, ServerError>(None);
        }

        let Some(next) = stream_state.byte_stream.next().await else {
            return Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch,
            ));
        };
        let bytes = next?;
        let read = u64::try_from(bytes.len())?;
        if read == 0 || read > stream_state.remaining_bytes {
            return Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch,
            ));
        }
        let permit = stream_state.transfer_limiter.acquire_bytes(read).await?;

        stream_state.remaining_bytes = stream_state
            .remaining_bytes
            .checked_sub(read)
            .ok_or(ServerError::Overflow)?;
        stream_state.active_permit = Some(permit);

        Ok(Some((bytes, stream_state)))
    });
    Body::from_stream(body_stream)
}

pub(super) async fn load_reconstruction_response(
    state: &AppState,
    file_id: &str,
    content_hash: Option<&str>,
    requested_range: Option<ByteRange>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<FileReconstructionResponse, ServerError> {
    if let Some(requested_range) = requested_range {
        return state
            .backend
            .reconstruction(
                file_id,
                content_hash,
                Some(requested_range),
                repository_scope,
            )
            .await;
    }

    let Some(content_hash) = content_hash else {
        return state
            .backend
            .reconstruction(file_id, None, None, repository_scope)
            .await;
    };

    let cache_key =
        ReconstructionCacheService::version_key(file_id, content_hash, repository_scope);
    state
        .reconstruction_cache
        .get_or_load(&cache_key, || async {
            state
                .backend
                .reconstruction(file_id, Some(content_hash), None, repository_scope)
                .await
        })
        .await
}

pub(super) async fn load_reconstruction_v2_response(
    state: &AppState,
    file_id: &str,
    content_hash: Option<&str>,
    requested_range: Option<ByteRange>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<FileReconstructionV2Response, ServerError> {
    let response = load_reconstruction_response(
        state,
        file_id,
        content_hash,
        requested_range,
        repository_scope,
    )
    .await?;
    Ok(reconstruction_v2_from_v1(response))
}

pub(super) async fn parse_reconstruction_request_range(
    state: &AppState,
    headers: &HeaderMap,
    file_id: &str,
    content_hash: Option<&str>,
    repository_scope: Option<&RepositoryScope>,
) -> Result<Option<ByteRange>, ServerError> {
    let Some(header_value) = headers.get(RANGE) else {
        return Ok(None);
    };
    let header_value = header_value
        .to_str()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let total_bytes = state
        .backend
        .file_total_bytes(file_id, content_hash, repository_scope)
        .await?;
    let range = parse_http_byte_range(header_value, total_bytes).map_err(ServerError::from)?;
    Ok(Some(range))
}

pub(super) fn parse_required_xorb_transfer_range(
    headers: &HeaderMap,
    total_length: u64,
) -> Result<ByteRange, ServerError> {
    let header_value = headers.get(RANGE).ok_or(ServerError::InvalidRangeHeader)?;
    let header_value = header_value
        .to_str()
        .map_err(|_error| ServerError::InvalidRangeHeader)?;
    let range = parse_http_byte_range(header_value, total_length).map_err(ServerError::from)?;
    Ok(range)
}

pub(super) fn parse_batch_reconstruction_file_ids(uri: &Uri) -> Result<Vec<String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };

    parse_batch_reconstruction_query(query)
}

/// Parses a batch reconstruction query string into a list of file IDs.
///
/// # Errors
///
/// Returns [`ServerError::RequestQueryTooLarge`] if the query exceeds the maximum allowed byte count,
/// or [`ServerError::InvalidFileId`] / [`ServerError::InvalidContentHash`] if any file ID or
/// content hash is malformed.
pub fn parse_batch_reconstruction_query(query: &str) -> Result<Vec<String>, ServerError> {
    if query.len() > MAX_BATCH_RECONSTRUCTION_QUERY_BYTES {
        return Err(ServerError::RequestQueryTooLarge);
    }

    let mut file_ids = Vec::new();
    let mut seen = BTreeSet::new();
    for parameter in query.split('&') {
        let Some((key, value)) = parameter.split_once('=') else {
            continue;
        };
        if key != "file_id" {
            continue;
        }
        validate_hash_path(value)?;
        if !seen.insert(value) {
            continue;
        }
        let next_len = file_ids.len().checked_add(1).ok_or(ServerError::Overflow)?;
        if next_len > MAX_BATCH_RECONSTRUCTION_FILE_IDS {
            return Err(ServerError::TooManyBatchReconstructionFileIds);
        }
        file_ids.push(value.to_owned());
    }

    Ok(file_ids)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, Uri, header::RANGE};

    // --- parse_batch_reconstruction_query tests ---

    #[test]
    fn reconstruct_single_chunk_file() {
        // Single valid file ID in the query string
        let hash = "a".repeat(64);
        let query = format!("file_id={hash}");
        let result = parse_batch_reconstruction_query(&query).unwrap();
        assert_eq!(result, vec![hash]);
    }

    #[test]
    fn reconstruct_multi_chunk_file() {
        // Multiple valid file IDs
        let h1 = "a".repeat(64);
        let h2 = "b".repeat(64);
        let h3 = "c".repeat(64);
        let query = format!("file_id={h1}&file_id={h2}&file_id={h3}");
        let result = parse_batch_reconstruction_query(&query).unwrap();
        assert_eq!(result, vec![h1, h2, h3]);
    }

    #[test]
    fn reconstruct_empty_file() {
        // Empty query returns empty list
        let result = parse_batch_reconstruction_query("").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn reconstruct_with_compression() {
        // Non-file_id parameters are ignored
        let hash = "a".repeat(64);
        let query = format!("file_id={hash}&other_param=ignored&another=value");
        let result = parse_batch_reconstruction_query(&query).unwrap();
        assert_eq!(result, vec![hash]);
    }

    #[test]
    fn reconstruct_deduplicates_file_ids() {
        let hash = "a".repeat(64);
        let query = format!("file_id={hash}&file_id={hash}");
        let result = parse_batch_reconstruction_query(&query).unwrap();
        assert_eq!(result.len(), 1);
    }

    // --- parse_batch_reconstruction_file_ids tests ---

    #[test]
    fn file_ids_from_empty_uri() {
        let uri: Uri = "https://example.com/batch".parse().unwrap();
        let result = parse_batch_reconstruction_file_ids(&uri).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn file_ids_from_valid_uri() {
        let hash = "a".repeat(64);
        let uri: Uri = format!("https://example.com/batch?file_id={hash}")
            .parse()
            .unwrap();
        let result = parse_batch_reconstruction_file_ids(&uri).unwrap();
        assert_eq!(result, vec![hash]);
    }

    #[test]
    fn file_ids_from_uri_with_invalid_hash() {
        let uri: Uri = "https://example.com/batch?file_id=not-a-hash"
            .parse()
            .unwrap();
        let result = parse_batch_reconstruction_file_ids(&uri);
        assert!(result.is_err());
    }

    // --- parse_required_xorb_transfer_range tests ---

    #[test]
    fn xorb_range_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, "bytes=0-1023".parse().unwrap());
        let range = parse_required_xorb_transfer_range(&headers, 4096).unwrap();
        assert_eq!(range.start(), 0);
        assert_eq!(range.end_inclusive(), 1023);
    }

    #[test]
    fn xorb_range_missing_header() {
        let headers = HeaderMap::new();
        let result = parse_required_xorb_transfer_range(&headers, 4096);
        assert!(matches!(result, Err(ServerError::InvalidRangeHeader)));
    }

    #[test]
    fn xorb_range_invalid_syntax() {
        let mut headers = HeaderMap::new();
        headers.insert(RANGE, "not-a-range".parse().unwrap());
        let result = parse_required_xorb_transfer_range(&headers, 4096);
        assert!(matches!(result, Err(ServerError::InvalidRangeHeader)));
    }

    // ── metered_transfer_body edge cases ────────────────────────────────────

    /// Helper: creates a stream that yields exactly the given bytes.
    fn stream_from_bytes(bytes: &'static [u8]) -> ServerByteStream {
        use futures_util::stream;
        Box::pin(stream::iter(vec![Ok(Bytes::from_static(bytes))]))
    }

    /// Helper: creates an empty stream.
    fn empty_stream() -> ServerByteStream {
        use futures_util::stream;
        Box::pin(stream::empty())
    }

    /// Helper: creates a stream that yields multiple chunks.
    fn multi_chunk_stream() -> ServerByteStream {
        use futures_util::stream;
        let chunks: Vec<Result<Bytes, ServerError>> = vec![
            Ok(Bytes::from_static(b"hello ")),
            Ok(Bytes::from_static(b"world")),
        ];
        Box::pin(stream::iter(chunks))
    }

    /// Helper: drains a Body into a Vec<u8> or returns the error encountered.
    async fn drain_body(body: Body) -> Result<Vec<u8>, String> {
        let bytes = axum::body::to_bytes(body, usize::MAX).await;
        match bytes {
            Ok(b) => Ok(b.to_vec()),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metered_transfer_body_stream_ends_early_returns_length_mismatch() {
        let limiter = crate::TransferLimiter::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        // Stream has 5 bytes but we claim 10 → stream ends early → StoredLengthMismatch
        let body = metered_transfer_body(stream_from_bytes(b"hello"), limiter, 10);
        let result = drain_body(body).await;
        assert!(result.is_err(), "expected error for early stream end");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metered_transfer_body_stream_has_extra_data_returns_length_mismatch() {
        let limiter = crate::TransferLimiter::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        // Claim 5 bytes but stream has 11 → extra data after remaining_bytes hits 0
        let body = metered_transfer_body(multi_chunk_stream(), limiter, 5);
        let result = drain_body(body).await;
        assert!(result.is_err(), "expected error for extra stream data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metered_transfer_body_accepts_exact_stream() {
        let limiter = crate::TransferLimiter::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let body = metered_transfer_body(stream_from_bytes(b"exact-data"), limiter, 10);
        let result = drain_body(body).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"exact-data");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metered_transfer_body_empty_stream_returns_length_mismatch() {
        let limiter = crate::TransferLimiter::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        // Claim 5 bytes but stream is empty → stream ends early
        let body = metered_transfer_body(empty_stream(), limiter, 5);
        let result = drain_body(body).await;
        assert!(result.is_err(), "expected error for empty stream");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn metered_transfer_body_detects_too_large_chunk() {
        let limiter = crate::TransferLimiter::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        // Create a stream whose first chunk is larger than remaining_bytes
        use futures_util::stream;
        let oversized: Vec<Result<Bytes, ServerError>> =
            vec![Ok(Bytes::from_static(b"too-big-chunk-here"))];
        let body = metered_transfer_body(Box::pin(stream::iter(oversized)), limiter, 3);
        let result = drain_body(body).await;
        assert!(result.is_err(), "expected error for oversized chunk");
    }
}
