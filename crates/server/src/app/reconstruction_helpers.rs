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
use shardline_protocol::{ByteRange, RepositoryScope, ShardlineHash, parse_http_byte_range};
use tokio::sync::OwnedSemaphorePermit;

use super::{AppState, MAX_BATCH_RECONSTRUCTION_FILE_IDS, MAX_BATCH_RECONSTRUCTION_QUERY_BYTES};
use crate::{
    ServerError,
    download_stream::ServerByteStream,
    model::{FileReconstructionResponse, FileReconstructionV2Response},
    reconstruction::reconstruction_v2_from_v1,
    reconstruction_cache::ReconstructionCacheService,
    transfer_limiter::TransferLimiter,
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

pub(super) fn full_byte_stream_response(
    byte_stream: ServerByteStream,
    transfer_limiter: TransferLimiter,
    total_length: u64,
) -> Response {
    (
        StatusCode::OK,
        [
            (CONTENT_TYPE, "application/octet-stream".to_owned()),
            (CONTENT_LENGTH, total_length.to_string()),
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
                return Err(ServerError::StoredObjectLengthMismatch);
            }

            return Ok::<Option<(Bytes, TransferByteStreamState)>, ServerError>(None);
        }

        let Some(next) = stream_state.byte_stream.next().await else {
            return Err(ServerError::StoredObjectLengthMismatch);
        };
        let bytes = next?;
        let read = u64::try_from(bytes.len())?;
        if read == 0 || read > stream_state.remaining_bytes {
            return Err(ServerError::StoredObjectLengthMismatch);
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

pub(super) fn validate_xorb_prefix(prefix: &str) -> Result<(), ServerError> {
    if prefix != "default" {
        return Err(ServerError::InvalidXorbPrefix);
    }

    Ok(())
}

pub(super) fn validate_xet_hash_path(value: &str) -> Result<(), ServerError> {
    ShardlineHash::parse_api_hex(value)?;
    Ok(())
}

pub(super) fn validate_optional_content_hash(
    content_hash: Option<&str>,
) -> Result<(), ServerError> {
    if let Some(content_hash) = content_hash {
        validate_xet_hash_path(content_hash)?;
    }

    Ok(())
}

pub(super) fn parse_batch_reconstruction_file_ids(uri: &Uri) -> Result<Vec<String>, ServerError> {
    let Some(query) = uri.query() else {
        return Ok(Vec::new());
    };

    parse_batch_reconstruction_query(query)
}

pub(super) fn parse_batch_reconstruction_query(query: &str) -> Result<Vec<String>, ServerError> {
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
        ShardlineHash::parse_api_hex(value)?;
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
