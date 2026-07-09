use std::{collections::BTreeMap, sync::Arc};

use axum::{
    http::{HeaderMap, HeaderValue, header::CONTENT_TYPE},
    response::Response,
};
use shardline_protocol::{ByteRange, parse_http_byte_range};

use crate::ServerError;

use super::{AppState, byte_range_stream_response, full_byte_stream_response};

pub(crate) async fn direct_object_response(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    object_key: &shardline_storage::ObjectKey,
    content_type: &str,
    content_digest: Option<String>,
    protocol: &str,
) -> Result<Response, ServerError> {
    let total_length = state.backend.object_length(object_key).await?;
    let range = parse_optional_range(headers, total_length)?;
    let byte_stream = state
        .backend
        .read_object_stream(object_key, total_length, range)
        .await?;
    let mut response = if let Some(range) = range {
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
    crate::metrics::record_download(protocol, total_length, 0.0, true);
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

pub(crate) fn parse_query_map(uri: &axum::http::Uri) -> Result<BTreeMap<String, String>, ServerError> {
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

pub(crate) fn parse_query_values(uri: &axum::http::Uri, key: &str) -> Result<Vec<String>, ServerError> {
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
