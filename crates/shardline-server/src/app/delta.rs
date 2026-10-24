use std::{ops::Range, sync::Arc};

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, header::CACHE_CONTROL},
    response::IntoResponse,
};

use super::{
    AppState, reconstruction_helpers::hub_storage_scope, reconstruction_routes::XetRepository,
};
use crate::{ServerError, admission::weights, xet_adapter::validate_hash_path};

pub(super) async fn file_chunk_hashes(
    State(state): State<Arc<AppState>>,
    Path(file_id): Path<String>,
    repo: XetRepository,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ServerError> {
    let _admit = state
        .admission
        .try_acquire(weights::RECONSTRUCTION)
        .ok_or(ServerError::WorkQueueSaturated)?;
    let _parsing = state
        .pools
        .parsing
        .try_acquire()
        .ok_or(ServerError::WorkQueueSaturated)?;
    validate_hash_path(&file_id)?;
    let hub_scope = hub_storage_scope(&state, &file_id, repo.capability()).await?;
    let scope = hub_scope.as_ref().or_else(|| repo.capability().namespace());
    let size = state
        .backend
        .file_total_bytes(&file_id, None, scope)
        .await?;
    let mut values = headers.get_all("x-range-dirty").iter();
    let value = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or(ServerError::InvalidContentHash)?;
    if values.next().is_some() {
        return Err(ServerError::InvalidContentHash);
    }
    let ranges = parse_dirty(value, size)?;
    let response = state.backend.native_delta(&file_id, scope, ranges).await?;
    Ok(([(CACHE_CONTROL, "private, no-store")], Json(response)))
}

fn parse_dirty(header: &str, size: u64) -> Result<Vec<Range<u64>>, ServerError> {
    let invalid = || ServerError::InvalidContentHash;
    let input = header.strip_prefix("bytes=").ok_or_else(invalid)?;
    if input.len() > 16_384 {
        return Err(invalid());
    }
    let mut ranges = Vec::new();
    for item in input.split(',') {
        if ranges.len() >= 256 {
            return Err(invalid());
        }
        let (start, end) = item.trim().split_once('-').ok_or_else(invalid)?;
        let number = |value: &str| -> Result<u64, ServerError> {
            if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid());
            }
            value.parse().map_err(|_error| invalid())
        };
        let range = if start.is_empty() {
            let suffix = number(end)?;
            if suffix == 0 {
                return Err(invalid());
            }
            size.saturating_sub(suffix)..size
        } else {
            let start = number(start)?;
            let end = if end.is_empty() {
                size
            } else {
                let last = number(end)?;
                if last < start {
                    return Err(invalid());
                }
                last.saturating_add(1).min(size)
            };
            start..end
        };
        if range.start < range.end {
            ranges.push(range);
        }
    }
    if ranges.is_empty() {
        return Err(invalid());
    }
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use super::parse_dirty;

    #[test]
    fn dirty_ranges_follow_inclusive_http_syntax() {
        assert_eq!(
            parse_dirty("bytes=7-10, -5, 90-", 100).unwrap(),
            vec![7..11, 95..100, 90..100]
        );
        assert_eq!(
            parse_dirty("bytes=99-18446744073709551615", 100).unwrap(),
            std::slice::from_ref(&(99..100))
        );
        for value in [
            "",
            "bytes=",
            "bytes=2-1",
            "bytes=-0",
            "bytes=+1-2",
            "bytes=100-200",
            "bytes=1-2,",
        ] {
            assert!(parse_dirty(value, 100).is_err(), "{value}");
        }
    }
}
