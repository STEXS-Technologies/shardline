//! HTTP layer for the sdx CAS read path (M2a).
//!
//! Covers reconstruction requests (with optional `Range:` header) and ranged
//! xorb fetches through the shardline transfer endpoint
//! (`/transfer/xorb/{prefix}/{hash}`, namespace `default`), including 206
//! single-range and `multipart/byteranges` response handling
//! (`docs/SDX_PLAN.md` §7 item 6).
//!
//! Shardline's transfer handler (`crates/shardline-server/src/app/operational.rs`
//! `read_xorb_transfer`) serves **single-range** 206 responses with a
//! `Content-Range: bytes start-end/total` header (see `byte_range_stream_response`
//! in `reconstruction_helpers.rs`); it never emits multipart. The multipart
//! parser is provided for cross-frontend compatibility (M7) and is unit-tested
//! against the RFC 7233 format.

use reqwest::{Response, StatusCode, header};
use serde::Deserialize;
use shardline_xet_adapter::{FileReconstructionResponse, FileReconstructionV2Response};

use crate::error::TransferError;

/// Inclusive byte range (`start..=end`), the wire semantics used by the Xet
/// protocol for reconstruction and xorb byte ranges
/// (`docs/PROTOCOL_CONFORMANCE.md` "Range Semantics").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// First byte offset (inclusive).
    pub start: u64,
    /// Final byte offset (inclusive).
    pub end: u64,
}

impl ByteRange {
    /// Creates an inclusive byte range.
    #[must_use]
    pub const fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// Returns the number of bytes covered by this range.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.end.saturating_sub(self.start).saturating_add(1)
    }

    /// Returns `true` when the range covers no bytes (`start > end`).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.end < self.start
    }

    /// Returns the `Range: bytes=start-end` header value for this range.
    #[must_use]
    pub fn to_range_header(&self) -> String {
        format!("bytes={}-{}", self.start, self.end)
    }
}

/// A byte range of a serialized xorb returned by the transfer endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangedXorb {
    /// The fetched serialized chunk payload bytes.
    pub data: Vec<u8>,
    /// The byte range the server actually served (from `Content-Range`, or the
    /// full body for a plain 200).
    pub served_range: ByteRange,
}

/// HTTP client for the CAS read path.
#[derive(Debug, Clone)]
pub struct TransferClient {
    client: reqwest::Client,
}

impl TransferClient {
    /// Creates a transfer client using the supplied HTTP client.
    #[must_use]
    pub const fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Fetches a V1 reconstruction response for `file_id`.
    ///
    /// When `range` is present, a `Range: bytes=start-end` header (inclusive)
    /// is sent and the response terms cover only the chunks intersecting that
    /// range.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the response cannot
    /// be parsed.
    pub async fn reconstruction_v1(
        &self,
        base_url: &str,
        token: &str,
        file_id: &str,
        range: Option<ByteRange>,
    ) -> Result<FileReconstructionResponse, TransferError> {
        let response = self
            .get_reconstruction(base_url, token, file_id, range, "v1")
            .await?;
        let body = response.bytes().await?;
        serde_json::from_slice(&body).map_err(|error| transfer_error_from_json(&error))
    }

    /// Fetches a V2 reconstruction response for `file_id`.
    ///
    /// See [`TransferClient::reconstruction_v1`] for the range semantics.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the response cannot
    /// be parsed.
    pub async fn reconstruction_v2(
        &self,
        base_url: &str,
        token: &str,
        file_id: &str,
        range: Option<ByteRange>,
    ) -> Result<FileReconstructionV2Response, TransferError> {
        let response = self
            .get_reconstruction(base_url, token, file_id, range, "v2")
            .await?;
        let body = response.bytes().await?;
        serde_json::from_slice(&body).map_err(|error| transfer_error_from_json(&error))
    }

    /// Fetches `range` bytes of a serialized xorb from an absolute transfer
    /// URL (as advertised in the reconstruction `fetch_info`/`xorbs`).
    ///
    /// Accepts a single-range 206 (with `Content-Range`) or, defensively, a
    /// `multipart/byteranges` body. A plain 200 is treated as the full xorb
    /// starting at offset 0.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails, the status is not
    /// success, or the response is not a valid range body.
    pub async fn fetch_xorb_range(
        &self,
        url: &str,
        token: &str,
        range: ByteRange,
    ) -> Result<RangedXorb, TransferError> {
        let response = self
            .client
            .get(url)
            .bearer_auth(token)
            .header(header::RANGE, range.to_range_header())
            .send()
            .await?;
        let response = ensure_success(response).await?;

        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();

        if content_type.starts_with("multipart/byteranges") {
            let body = response.bytes().await?;
            let parts = parse_multipart_byteranges(&content_type, &body)?;
            let mut data = Vec::new();
            for part in parts {
                data.extend_from_slice(&part.data);
            }
            let length = u64::try_from(data.len()).unwrap_or(u64::MAX);
            return Ok(RangedXorb {
                data,
                served_range: ByteRange::new(0, length),
            });
        }

        let partial_range = if response.status() == StatusCode::PARTIAL_CONTENT {
            let value = response
                .headers()
                .get(header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    TransferError::InvalidResponse(
                        "206 response missing Content-Range header".to_owned(),
                    )
                })?;
            Some(parse_content_range(value)?)
        } else {
            None
        };
        let body = response.bytes().await?;
        let data = body.to_vec();
        let served_range = partial_range
            .unwrap_or_else(|| ByteRange::new(0, u64::try_from(data.len()).unwrap_or(u64::MAX)));
        Ok(RangedXorb { data, served_range })
    }

    async fn get_reconstruction(
        &self,
        base_url: &str,
        token: &str,
        file_id: &str,
        range: Option<ByteRange>,
        api_version: &str,
    ) -> Result<Response, TransferError> {
        let capacity = base_url
            .len()
            .saturating_add(api_version.len())
            .saturating_add(file_id.len())
            .saturating_add(32);
        let mut url = String::with_capacity(capacity);
        url.push_str(base_url.trim_end_matches('/'));
        url.push('/');
        url.push_str(api_version);
        url.push_str("/reconstructions/");
        url.push_str(file_id);
        let mut request = self
            .client
            .get(&url)
            .bearer_auth(token)
            .header(header::ACCEPT, "application/json");
        if let Some(range) = range {
            request = request.header(header::RANGE, range.to_range_header());
        }
        let response = request.send().await?;
        ensure_success(response).await
    }
}

async fn ensure_success(response: Response) -> Result<Response, TransferError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    let body = response.text().await.unwrap_or_default();
    let message = parse_error_message(&body).unwrap_or(body);
    Err(http_error(status, message))
}

fn parse_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<ErrorBody>(body)
        .ok()
        .map(|parsed| parsed.error)
}

const fn http_error(status: StatusCode, message: String) -> TransferError {
    match status.as_u16() {
        400 => TransferError::BadRequest(message),
        401 => TransferError::Unauthorized(message),
        403 => TransferError::Forbidden(message),
        404 => TransferError::NotFound(message),
        416 => TransferError::RangeNotSatisfiable(message),
        429 => TransferError::TooManyRequests(message),
        _ => TransferError::HttpStatus {
            status: status.as_u16(),
            message,
        },
    }
}

fn transfer_error_from_json(error: &serde_json::Error) -> TransferError {
    TransferError::InvalidResponse(error.to_string())
}

/// Parses a `Content-Range: bytes start-end/total` header (inclusive end).
fn parse_content_range(value: &str) -> Result<ByteRange, TransferError> {
    let spec = value.trim().strip_prefix("bytes ").ok_or_else(|| {
        TransferError::InvalidResponse(format!("invalid Content-Range header: {value}"))
    })?;
    let (range_part, _total) = spec.split_once('/').ok_or_else(|| {
        TransferError::InvalidResponse(format!("invalid Content-Range header: {value}"))
    })?;
    let (start, end) = range_part.split_once('-').ok_or_else(|| {
        TransferError::InvalidResponse(format!("invalid Content-Range header: {value}"))
    })?;
    let start = start.parse::<u64>().map_err(|error| {
        TransferError::InvalidResponse(format!("invalid Content-Range header {value}: {error}"))
    })?;
    let end = end.parse::<u64>().map_err(|error| {
        TransferError::InvalidResponse(format!("invalid Content-Range header {value}: {error}"))
    })?;
    Ok(ByteRange::new(start, end))
}

/// A single part of a `multipart/byteranges` response (RFC 7233 §4.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultipartPart {
    /// The part's inclusive byte range (from its `Content-Range`).
    pub range: ByteRange,
    /// The part's body bytes.
    pub data: Vec<u8>,
}

/// Parses a `multipart/byteranges` response body (RFC 7233 §4.1), returning
/// parts in order of increasing range start.
///
/// # Errors
///
/// Returns [`TransferError::MalformedMultipart`] when the boundary cannot be
/// found or a part is missing its header/data separator or `Content-Range`.
pub fn parse_multipart_byteranges(
    content_type: &str,
    body: &[u8],
) -> Result<Vec<MultipartPart>, TransferError> {
    let boundary = extract_boundary(content_type).ok_or_else(|| {
        TransferError::MalformedMultipart(format!("no boundary in Content-Type: {content_type}"))
    })?;
    let first_delim = format!("--{boundary}");
    let delimiter = format!("\r\n--{boundary}");
    let first = find_subsequence(body, first_delim.as_bytes()).ok_or_else(|| {
        TransferError::MalformedMultipart("no boundary found in multipart body".to_owned())
    })?;
    let first_end = first
        .checked_add(first_delim.len())
        .ok_or_else(|| TransferError::MalformedMultipart("boundary offset overflow".to_owned()))?;
    let mut remaining = body.get(first_end..).ok_or_else(|| {
        TransferError::MalformedMultipart("boundary offset out of bounds".to_owned())
    })?;
    let mut parts = Vec::new();
    loop {
        if !remaining.starts_with(b"\r\n") {
            break;
        }
        remaining = remaining
            .get(2..)
            .ok_or_else(|| TransferError::MalformedMultipart("truncated part".to_owned()))?;
        let next_boundary = find_subsequence(remaining, delimiter.as_bytes());
        let part_bytes = match next_boundary {
            Some(position) => remaining.get(..position).ok_or_else(|| {
                TransferError::MalformedMultipart("part boundary out of bounds".to_owned())
            })?,
            None => remaining,
        };
        let Some(header_end) = find_subsequence(part_bytes, b"\r\n\r\n") else {
            return Err(TransferError::MalformedMultipart(
                "multipart part missing header/data separator".to_owned(),
            ));
        };
        let headers = part_bytes.get(..header_end).ok_or_else(|| {
            TransferError::MalformedMultipart("part header out of bounds".to_owned())
        })?;
        let data_start = header_end.checked_add(4).ok_or_else(|| {
            TransferError::MalformedMultipart("part header offset overflow".to_owned())
        })?;
        let data = part_bytes.get(data_start..).ok_or_else(|| {
            TransferError::MalformedMultipart("part data out of bounds".to_owned())
        })?;
        let range = parse_part_content_range(headers)?;
        parts.push(MultipartPart {
            range,
            data: data.to_vec(),
        });
        match next_boundary {
            Some(position) => {
                let after = position.checked_add(delimiter.len()).ok_or_else(|| {
                    TransferError::MalformedMultipart("boundary offset overflow".to_owned())
                })?;
                remaining = remaining.get(after..).ok_or_else(|| {
                    TransferError::MalformedMultipart("boundary offset out of bounds".to_owned())
                })?;
            }
            None => break,
        }
    }
    parts.sort_by_key(|part| part.range.start);
    Ok(parts)
}

fn extract_boundary(content_type: &str) -> Option<String> {
    content_type
        .split(';')
        .map(str::trim)
        .find_map(|part| part.strip_prefix("boundary="))
        .map(|value| value.trim_matches('"').to_owned())
}

fn parse_part_content_range(headers: &[u8]) -> Result<ByteRange, TransferError> {
    let headers_text = std::str::from_utf8(headers).map_err(|error| {
        TransferError::MalformedMultipart(format!("invalid part headers: {error}"))
    })?;
    for line in headers_text.split("\r\n") {
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-range:") {
            return parse_content_range(value);
        }
    }
    Err(TransferError::MalformedMultipart(
        "multipart part missing Content-Range header".to_owned(),
    ))
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Server error envelope `{"error": "..."}`.
#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

#[cfg(test)]
mod tests {
    use super::parse_multipart_byteranges;

    #[test]
    fn parse_multipart_single_part() {
        let boundary = "abc123";
        let body = format!(
            "--{boundary}\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-99/1000\r\n\r\nHello World\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let parts = parse_multipart_byteranges(&content_type, body.as_bytes()).unwrap();
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].range, super::ByteRange::new(0, 99));
        assert_eq!(parts[0].data, b"Hello World");
    }

    #[test]
    fn parse_multipart_multiple_parts_sorted_by_start() {
        let boundary = "sep";
        let body = format!(
            "--{boundary}\r\nContent-Range: bytes 100-199/1000\r\n\r\nPart2Data\r\n--{boundary}\r\nContent-Range: bytes 0-49/1000\r\n\r\nPart1Data\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let parts = parse_multipart_byteranges(&content_type, body.as_bytes()).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].range, super::ByteRange::new(0, 49));
        assert_eq!(parts[0].data, b"Part1Data");
        assert_eq!(parts[1].range, super::ByteRange::new(100, 199));
        assert_eq!(parts[1].data, b"Part2Data");
    }

    #[test]
    fn parse_multipart_empty_body_missing_boundary() {
        let content_type = "multipart/byteranges; boundary=xyz";
        let result = parse_multipart_byteranges(content_type, b"");
        assert!(result.is_err());
    }

    #[test]
    fn parse_multipart_part_missing_header_separator() {
        let boundary = "xyz";
        let body = format!(
            "--{boundary}\r\nContent-Range: bytes 0-9/100\r\nMISSING_SEPARATOR\r\n--{boundary}--\r\n"
        );
        let content_type = format!("multipart/byteranges; boundary={boundary}");
        let result = parse_multipart_byteranges(&content_type, body.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn parse_multipart_quoted_boundary() {
        let boundary = "quoted-boundary";
        let body =
            format!("--{boundary}\r\nContent-Range: bytes 0-1/2\r\n\r\nab\r\n--{boundary}--\r\n");
        let content_type = format!("multipart/byteranges; boundary=\"{boundary}\"");
        let parts = parse_multipart_byteranges(&content_type, body.as_bytes()).unwrap();
        assert_eq!(parts[0].data, b"ab");
    }
}
