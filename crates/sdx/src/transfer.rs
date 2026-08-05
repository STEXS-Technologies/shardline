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
use shardline_xet_adapter::{
    FileReconstructionResponse, FileReconstructionV2Response, ShardUploadResponse,
    XorbUploadResponse,
};

use bytes::Bytes;

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
    /// Stable per-client id sent as `X-Xet-Session-Id` on every request
    /// (`docs/SDX_PLAN.md` §4.4.4).
    session_id: String,
}

impl TransferClient {
    /// Creates a transfer client using the supplied HTTP client.
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            session_id: generate_session_id(),
        }
    }

    /// Overrides the `X-Xet-Session-Id` sent on every request.
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = session_id.into();
        self
    }

    /// Returns the `X-Xet-Session-Id` this client sends on every request.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Applies the common per-request headers (correlation session id).
    fn with_session(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header(SESSION_ID_HEADER.clone(), &self.session_id)
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
        let request = self
            .with_session(self.client.get(url).bearer_auth(token))
            .header(header::RANGE, range.to_range_header());
        let response = request.send().await?;
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

    /// Fetches a raw `GET` path under `base_url` and returns the response body.
    ///
    /// HTTP 404 is reported as `Ok(None)` — the global dedup query
    /// (`/v1/chunks/default-merkledb/{hash}`) treats 404 as a cache miss, not
    /// an error. Every other non-success status is mapped to a typed
    /// [`TransferError`] (including 429, which is surfaced without retry; M4
    /// adds retry policies).
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the status is a
    /// non-404 error status.
    pub async fn get_optional_bytes(
        &self,
        base_url: &str,
        token: &str,
        path: &str,
    ) -> Result<Option<Bytes>, TransferError> {
        let url = format!("{}{}", base_url.trim_end_matches('/'), path);
        let response = self
            .with_session(self.client.get(&url).bearer_auth(token))
            .send()
            .await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let response = ensure_success(response).await?;
        let body = response.bytes().await?;
        Ok(Some(body))
    }

    /// Probes whether a serialized xorb already exists via
    /// `HEAD /v1/xorbs/default/{hash}`.
    ///
    /// HTTP 200 reports `Ok(true)`, HTTP 404 `Ok(false)`, and every other
    /// status is surfaced as a typed [`TransferError`]. Used by the upload
    /// path as the idempotency probe before re-uploading a xorb.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the status is not
    /// 200/404.
    pub async fn head_xorb(
        &self,
        base_url: &str,
        token: &str,
        hash: &str,
    ) -> Result<bool, TransferError> {
        let url = format!("{}/v1/xorbs/default/{hash}", base_url.trim_end_matches('/'));
        let response = self
            .with_session(self.client.head(&url).bearer_auth(token))
            .send()
            .await?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => {
                let retry_after = parse_retry_after(response.headers());
                let message = response.text().await.unwrap_or_default();
                Err(http_error(status, message, retry_after))
            }
        }
    }

    /// Uploads a serialized xorb via `POST /v1/xorbs/default/{hash}`.
    ///
    /// The body is streamed in [`XORB_UPLOAD_PROGRESS_BLOCK_SIZE`] (512 KiB)
    /// progress blocks with an explicit `Content-Length` (the body advertises
    /// its exact total size, so the HTTP layer frames it with a
    /// `Content-Length` header rather than chunked transfer-encoding). This
    /// lets the server pre-reject oversized bodies and report progress without
    /// buffering the request twice. Idempotent server-side: re-uploading an
    /// existing xorb returns `was_inserted = false`.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the server returns
    /// a non-success status.
    pub async fn upload_xorb(
        &self,
        base_url: &str,
        token: &str,
        hash: &str,
        serialized: Bytes,
    ) -> Result<XorbUploadResponse, TransferError> {
        let url = format!("{}/v1/xorbs/default/{hash}", base_url.trim_end_matches('/'));
        let length = u64::try_from(serialized.len()).unwrap_or(u64::MAX);
        let body = reqwest::Body::wrap(SizedStreamBody::new(
            xorb_progress_stream(serialized),
            length,
        ));
        let response = self
            .with_session(self.client.post(&url).bearer_auth(token))
            .body(body)
            .send()
            .await?;
        let response = ensure_success(response).await?;
        let response_bytes = response.bytes().await?;
        serde_json::from_slice(&response_bytes).map_err(|error| transfer_error_from_json(&error))
    }

    /// Uploads a serialized metadata shard via `POST /v1/shards`.
    ///
    /// The caller must have uploaded every xorb the shard references first
    /// (xorbs-before-shard); the server rejects shards referencing absent
    /// xorbs. The response reports whether the shard was newly registered.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request fails or the server returns
    /// a non-success status.
    pub async fn upload_shard(
        &self,
        base_url: &str,
        token: &str,
        body: Vec<u8>,
    ) -> Result<ShardUploadResponse, TransferError> {
        let url = format!("{}/v1/shards", base_url.trim_end_matches('/'));
        let response = self
            .with_session(self.client.post(&url).bearer_auth(token))
            .body(body)
            .send()
            .await?;
        let response = ensure_success(response).await?;
        let response_bytes = response.bytes().await?;
        serde_json::from_slice(&response_bytes).map_err(|error| transfer_error_from_json(&error))
    }

    /// Issues an arbitrary CAS/API request (with the `X-Xet-Session-Id` and
    /// bearer headers) and returns `(status, body)` for any status.
    ///
    /// This is the low-level primitive used by the path-namespace and revision
    /// metadata layers (M5b). Non-2xx statuses are mapped to typed
    /// [`TransferError`]s so the M4 [`RetryContext`] can classify them; the
    /// caller matches specific statuses (e.g. 409 → `RevisionExists`) from the
    /// error.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] for transport failures and every non-2xx
    /// status.
    pub(crate) async fn request_raw(
        &self,
        method: &reqwest::Method,
        url: &str,
        token: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<(StatusCode, Vec<u8>), TransferError> {
        let mut request = self
            .with_session(self.client.request(method.clone(), url).bearer_auth(token))
            .header(header::ACCEPT, "application/json");
        if let Some(body) = body {
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .json(body);
        }
        let response = request.send().await?;
        let status = response.status();
        let retry_after = parse_retry_after(response.headers());
        let body_bytes = response.bytes().await?.to_vec();
        if status.is_success() {
            return Ok((status, body_bytes));
        }
        let message = String::from_utf8_lossy(&body_bytes).into_owned();
        Err(http_error(status, message, retry_after))
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
            .with_session(self.client.get(&url).bearer_auth(token))
            .header(header::ACCEPT, "application/json");
        if let Some(range) = range {
            request = request.header(header::RANGE, range.to_range_header());
        }
        let response = request.send().await?;
        ensure_success(response).await
    }
}

/// `X-Xet-Session-Id` correlation header sent on every CAS request.
static SESSION_ID_HEADER: reqwest::header::HeaderName =
    reqwest::header::HeaderName::from_static("x-xet-session-id");

/// Generates a stable-enough per-client session id without pulling a UUID/rand
/// dependency: a process counter plus the wall-clock timestamp.
fn generate_session_id() -> String {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("sdx-{now:x}-{counter}")
}

/// Size of each progress block in a streamed xorb upload body.
pub const XORB_UPLOAD_PROGRESS_BLOCK_SIZE: usize = 512 * 1024;

/// Splits `serialized` into [`XORB_UPLOAD_PROGRESS_BLOCK_SIZE`] blocks for a
/// streamed request body (progress granularity; the total length is carried by
/// the explicit `Content-Length` header).
fn xorb_progress_stream(
    serialized: Bytes,
) -> impl futures_util::Stream<Item = Result<Bytes, std::convert::Infallible>> {
    futures_util::stream::unfold(serialized, |mut remaining| async move {
        if remaining.is_empty() {
            return None;
        }
        let take = XORB_UPLOAD_PROGRESS_BLOCK_SIZE.min(remaining.len());
        let head = remaining.split_to(take);
        Some((Ok(head), remaining))
    })
}

/// A streamed [`http_body::Body`] that advertises an exact total length, so the
/// HTTP/1.1 layer frames it with an explicit `Content-Length` header while data
/// is still delivered in streaming progress blocks.
struct SizedStreamBody<S> {
    inner: std::pin::Pin<Box<S>>,
    remaining: u64,
}

impl<S> SizedStreamBody<S> {
    fn new(stream: S, total: u64) -> Self {
        Self {
            inner: Box::pin(stream),
            remaining: total,
        }
    }
}

impl<S> http_body::Body for SizedStreamBody<S>
where
    S: futures_util::Stream<Item = Result<Bytes, std::convert::Infallible>> + Send + 'static,
{
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(data))) => {
                this.remaining = this
                    .remaining
                    .saturating_sub(u64::try_from(data.len()).unwrap_or(u64::MAX));
                std::task::Poll::Ready(Some(Ok(http_body::Frame::data(data))))
            }
            std::task::Poll::Ready(Some(Err(error))) => std::task::Poll::Ready(Some(Err(error))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        let mut hint = http_body::SizeHint::default();
        hint.set_exact(self.remaining);
        hint
    }

    fn is_end_stream(&self) -> bool {
        self.remaining == 0
    }
}

async fn ensure_success(response: Response) -> Result<Response, TransferError> {
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("unknown");
    tracing::debug!(request_id, status = %status, "CAS request completed");
    if status.is_success() {
        return Ok(response);
    }
    let retry_after = parse_retry_after(response.headers());
    let body = response.text().await.unwrap_or_default();
    let message = parse_error_message(&body).unwrap_or(body);
    Err(http_error(status, message, retry_after))
}

/// Parses a `Retry-After` delta-seconds value (shardline sends `Retry-After: 1`
/// on 503s). Non-numeric values (HTTP-date) are ignored.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn parse_error_message(body: &str) -> Option<String> {
    serde_json::from_str::<ErrorBody>(body)
        .ok()
        .map(|parsed| parsed.error)
}

const fn http_error(
    status: StatusCode,
    message: String,
    retry_after: Option<u64>,
) -> TransferError {
    match status.as_u16() {
        400 => TransferError::BadRequest(message),
        401 => TransferError::Unauthorized(message),
        403 => TransferError::Forbidden(message),
        404 => TransferError::NotFound(message),
        416 => TransferError::RangeNotSatisfiable(message),
        429 => TransferError::TooManyRequests {
            message,
            retry_after,
        },
        _ => TransferError::HttpStatus {
            status: status.as_u16(),
            message,
            retry_after,
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
    use super::{TransferClient, parse_multipart_byteranges};
    use bytes::Bytes;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

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

    /// Verifies at the wire level that `upload_xorb` sends an explicit
    /// `Content-Length` header (framing a streamed 512 KiB progress body) equal
    /// to the serialized xorb length. The shardline server relies on this to
    /// pre-reject oversized request bodies via the body size hint.
    #[tokio::test]
    async fn upload_xorb_sends_explicit_content_length_on_wire() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let n = sock.read(&mut buf).await.unwrap();
            let _ = sock
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 21\r\n\r\n{\"was_inserted\":true}",
                )
                .await;
            buf[..n].to_vec()
        });

        let client = TransferClient::new(reqwest::Client::new());
        let serialized = Bytes::from(vec![7u8; 194]);
        let base = format!("http://{addr}");
        let result = client
            .upload_xorb(&base, "token", &"ab".repeat(32), serialized)
            .await;
        let response = result.unwrap();
        assert!(response.was_inserted);

        let raw = server.await.unwrap();
        let text = String::from_utf8_lossy(&raw);
        assert!(
            text.to_lowercase().contains("content-length: 194"),
            "upload_xorb request missing explicit Content-Length:\n{text}"
        );
        // The full serialized xorb body follows the header block.
        assert!(
            raw.len() >= 194,
            "upload_xorb request body too short: {} bytes",
            raw.len()
        );
    }

    /// Verifies the `X-Xet-Session-Id` correlation header is sent on every
    /// request and is stable per client.
    #[tokio::test]
    async fn requests_send_x_xet_session_id_header() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
            .mount(&server)
            .await;

        let client = TransferClient::new(reqwest::Client::new()).with_session_id("my-session");
        let session_id = client.session_id().to_owned();
        assert_eq!(session_id, "my-session");
        let _ = client
            .get_optional_bytes(&server.uri(), "tok", "/probe")
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap_or_default();
        let value = requests[0]
            .headers
            .get("x-xet-session-id")
            .and_then(|value| value.to_str().ok());
        assert_eq!(value, Some("my-session"));
    }

    /// A default client generates a non-empty per-client session id.
    #[test]
    fn generated_session_id_is_non_empty() {
        let a = TransferClient::new(reqwest::Client::new());
        let b = TransferClient::new(reqwest::Client::new());
        assert!(!a.session_id().is_empty());
        assert!(!b.session_id().is_empty());
        // Extremely likely to differ, but the invariant is non-empty + stable.
        assert_eq!(a.session_id(), a.session_id());
    }
}
