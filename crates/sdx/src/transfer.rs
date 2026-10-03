//! HTTP client for the Xet CAS data plane: file-reconstruction requests and
//! ranged xorb (chunk-container) fetches.
//!
//! [`TransferClient`] issues reconstruction requests (with an optional
//! `Range:` header) and ranged xorb fetches through the shardline transfer
//! endpoint (`/transfer/xorb/{prefix}/{hash}`, namespace `default`), handling
//! 206 single-range and `multipart/byteranges` responses.
//!
//! Shardline's transfer handler (`crates/shardline-server/src/app/operational.rs`
//! `read_xorb_transfer`) serves **single-range** 206 responses with a
//! `Content-Range: bytes start-end/total` header (see `byte_range_stream_response`
//! in `reconstruction_helpers.rs`); it never emits multipart. The multipart
//! parser is provided for cross-frontend compatibility and is unit-tested
//! against the RFC 7233 format.

use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
    mpsc::{SyncSender, TrySendError},
};

use reqwest::{Response, StatusCode, header};
use serde::{Deserialize, de::DeserializeOwned};
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

/// HTTP client for the Xet CAS data plane: reconstruction requests and ranged
/// xorb fetches.
///
/// Cheap to clone and share across tasks; every request carries a stable
/// `X-Xet-Session-Id` correlation header.
///
/// # Examples
///
/// Fetch a byte range of a serialized xorb from a CAS endpoint:
///
/// ```no_run
/// # async fn example() -> Result<(), sdx::TransferError> {
/// use sdx::{ByteRange, TransferClient};
///
/// let client = TransferClient::new(reqwest::Client::new())
///     .with_session_id("my-session".to_owned());
///
/// // `url` is the absolute transfer URL from a reconstruction response and
/// // `token` is a read-scoped CAS token from `TokenService::read_token`.
/// let xorb = client
///     .fetch_xorb_range(
///         "https://cas.example.com/transfer/xorb/ab/0123…",
///         "read-token",
///         ByteRange::new(0, 4096),
///     )
///     .await?;
/// println!("fetched {} bytes", xorb.data.len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct TransferClient {
    client: reqwest::Client,
    /// Stable per-client id sent as `X-Xet-Session-Id` on every request
    /// (`docs/SDX_PLAN.md` §4.4.4).
    session_id: String,
    reconstruction_response_limit: usize,
}

impl TransferClient {
    /// Creates a transfer client using the supplied HTTP client.
    #[must_use]
    pub fn new(client: reqwest::Client) -> Self {
        Self {
            client,
            session_id: generate_session_id(),
            reconstruction_response_limit: 64 * 1024 * 1024,
        }
    }

    /// Sets the reconstruction JSON wire byte budget (default 64 MiB).
    ///
    /// This is a supported client envelope, not a universal protocol maximum:
    /// server term limits and advertised URL lengths are configurable. Increase
    /// it for larger deployments, using [`Self::reconstruction_envelope_bytes`]
    /// to derive a conservative compact-JSON budget. JSON extensions/whitespace
    /// require additional caller allowance. Each response is checked even when
    /// Content-Length is absent or misleading.
    #[must_use]
    pub const fn with_reconstruction_response_limit(mut self, limit: usize) -> Self {
        self.reconstruction_response_limit = limit;
        self
    }

    /// Derives a conservative compact-JSON byte bound for the server's v1/v2
    /// reconstruction DTOs, given maximum term count and full advertised URL length
    /// in UTF-8 bytes. At most one fetch entry/range is emitted per term, and
    /// at most one distinct 64-hex map key per term. Numeric fields are u64
    /// (20 digits); URL escaping costs at most 6 bytes per input byte.
    ///
    /// The bound is `75 + terms * (409 + 6 * url_bytes)`, including outer
    /// fields, map keys, punctuation, and v2's nested single-range arrays.
    /// It does not bound arbitrary extra fields or pretty-printed JSON.
    ///
    /// # Errors
    /// Returns [`TransferError`] if the envelope cannot fit the platform usize.
    pub fn reconstruction_envelope_bytes(
        terms: usize,
        max_url_bytes: usize,
    ) -> Result<usize, TransferError> {
        max_url_bytes
            .checked_mul(6)
            .and_then(|url| url.checked_add(409))
            .and_then(|entry| entry.checked_mul(terms))
            .and_then(|entries| entries.checked_add(75))
            .ok_or_else(|| {
                TransferError::InvalidResponse(
                    "reconstruction envelope exceeds addressable bytes".to_owned(),
                )
            })
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

    /// Fetches the reconstruction plan for `file_id` from the v1
    /// reconstruction endpoint.
    ///
    /// The plan lists the xorb byte ranges (`fetch_info`) and chunk terms that
    /// compose the file, which [`crate::reconstruction::reconstruct`] then
    /// fetches and assembles. When `range` is present, a
    /// `Range: bytes=start-end` header (inclusive) is sent and the response
    /// terms cover only the chunks intersecting that range.
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
        decode_json_response(response, self.reconstruction_response_limit)
            .await?
            .map_err(|error| transfer_error_from_json(&error))
    }

    /// Fetches the reconstruction plan for `file_id` from the v2
    /// reconstruction endpoint (`xorbs` fetch metadata).
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
        decode_json_response(response, self.reconstruction_response_limit)
            .await?
            .map_err(|error| transfer_error_from_json(&error))
    }

    /// Fetches a byte range of a serialized xorb from an absolute transfer
    /// URL.
    ///
    /// Xorbs are the serialized chunk containers that make up a file; the URL
    /// is the absolute transfer URL advertised in the reconstruction plan
    /// (`fetch_info`/`xorbs`). Accepts a single-range 206 (with
    /// `Content-Range`) or, defensively, a `multipart/byteranges` body. A
    /// plain 200 is treated as the full xorb starting at offset 0. Multipart
    /// ranges must be contiguous and non-overlapping so their coordinates can
    /// be represented by a single `served_range`; empty bodies are rejected.
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
        // HTTP 200 may contain the whole xorb even for a tiny requested
        // range. Upstream xorbs are at most 64 MiB; this ceiling allows a
        // complete serialized xorb plus compression/MIME overhead without an
        // unbounded fallback when the server ignores Range.
        self.fetch_xorb_range_inner(url, token, range, 128 * 1024 * 1024)
            .await
    }

    /// Streaming fetches bound both ordinary and multipart wire bodies, even
    /// when Content-Length is absent or misleading. Valid xorbs are at most
    /// 64 MiB; allow compression/header overhead plus a bounded MIME envelope.
    pub(crate) async fn fetch_xorb_range_bounded(
        &self,
        url: &str,
        token: &str,
        range: ByteRange,
    ) -> Result<RangedXorb, TransferError> {
        let limit = usize::try_from(range.len())
            .ok()
            .and_then(|length| length.checked_add(64 * 1024))
            .filter(|length| *length <= 128 * 1024 * 1024)
            .ok_or_else(|| {
                TransferError::InvalidResponse("xorb fetch exceeds wire body limit".to_owned())
            })?;
        self.fetch_xorb_range_inner(url, token, range, limit).await
    }

    async fn fetch_xorb_range_inner(
        &self,
        url: &str,
        token: &str,
        range: ByteRange,
        body_limit: usize,
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
            let body = read_response_bounded(response, body_limit).await?;
            let parts = parse_multipart_byteranges(&content_type, &body)?;
            let first = parts.first().ok_or_else(|| {
                TransferError::MalformedMultipart(
                    "multipart response contains no ranges".to_owned(),
                )
            })?;
            let start = first.range.start;
            let mut end = first.range.end;
            let mut data = Vec::new();
            for (index, part) in parts.into_iter().enumerate() {
                if part.range.is_empty()
                    || part.range.len() != u64::try_from(part.data.len()).unwrap_or(u64::MAX)
                {
                    return Err(TransferError::MalformedMultipart(
                        "multipart range length disagrees with its body".to_owned(),
                    ));
                }
                if index != 0 && end.checked_add(1) != Some(part.range.start) {
                    return Err(TransferError::MalformedMultipart(
                        "multipart ranges must be contiguous and non-overlapping".to_owned(),
                    ));
                }
                end = part.range.end;
                data.extend_from_slice(&part.data);
            }
            return Ok(RangedXorb {
                data,
                served_range: ByteRange::new(start, end),
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
        let body = read_response_bounded(response, body_limit).await?;
        let data = body;
        let length = u64::try_from(data.len()).unwrap_or(u64::MAX);
        let full_body_end = length.checked_sub(1).ok_or_else(|| {
            TransferError::InvalidResponse("xorb range response contains no bytes".to_owned())
        })?;
        let served_range = partial_range.unwrap_or_else(|| ByteRange::new(0, full_body_end));
        if served_range.len() != length {
            return Err(TransferError::InvalidResponse(
                "Content-Range length disagrees with response body".to_owned(),
            ));
        }
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
        self.get_optional_bytes_bounded(base_url, token, path, DEFAULT_SHARD_RESPONSE_LIMIT)
            .await
    }

    /// Fetches an optional raw body with a caller-defined wire byte budget.
    ///
    /// Use this for custom shard-ingest budgets or non-shard paths. The default
    /// variant permits 64 MiB, matching the default server ingest budget; this
    /// is a client envelope, not a maximum of every server configuration.
    ///
    /// # Errors
    /// Returns [`TransferError`] for transport/status failures or body overflow.
    pub async fn get_optional_bytes_bounded(
        &self,
        base_url: &str,
        token: &str,
        path: &str,
        response_limit: usize,
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
        let body = read_response_bounded(response, response_limit).await?;
        Ok(Some(Bytes::from(body)))
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
                let message = read_error_prefix(response).await;
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
        decode_json_response(response, ACK_RESPONSE_LIMIT)
            .await?
            .map_err(|error| transfer_error_from_json(&error))
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
        decode_json_response(response, ACK_RESPONSE_LIMIT)
            .await?
            .map_err(|error| transfer_error_from_json(&error))
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
    #[cfg(test)]
    pub(crate) async fn request_raw(
        &self,
        method: &reqwest::Method,
        url: &str,
        token: &str,
        body: Option<&serde_json::Value>,
        success_limit: usize,
    ) -> Result<(StatusCode, Vec<u8>), TransferError> {
        let response = self.request_response(method, url, token, body).await?;
        let status = response.status();
        let body_bytes = read_response_bounded(response, success_limit).await?;
        Ok((status, body_bytes))
    }

    /// JSON parsing errors remain separate from transport/status failures so
    /// metadata callers retain their operation labels and do not retry bad JSON.
    pub(crate) async fn request_json<T: DeserializeOwned + Send + 'static>(
        &self,
        method: &reqwest::Method,
        url: &str,
        token: &str,
        body: Option<&serde_json::Value>,
        success_limit: usize,
    ) -> Result<Result<T, JsonDecodeError>, TransferError> {
        let response = self.request_response(method, url, token, body).await?;
        decode_json_response(response, success_limit).await
    }

    async fn request_response(
        &self,
        method: &reqwest::Method,
        url: &str,
        token: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<Response, TransferError> {
        let mut request = self
            .with_session(self.client.request(method.clone(), url).bearer_auth(token))
            .header(header::ACCEPT, "application/json");
        if let Some(body) = body {
            request = request
                .header(header::CONTENT_TYPE, "application/json")
                .json(body);
        }
        ensure_success(request.send().await?).await
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

// Upload acknowledgements are fixed scalar DTOs (22 and 14 bytes at most).
// Allow a small compatibility envelope for JSON whitespace/additional fields.
const ACK_RESPONSE_LIMIT: usize = 64 * 1024;
// Default shard ingest budget. Generic callers needing a different budget can
// use the explicit bounded variant instead of trusting peer Content-Length.
const DEFAULT_SHARD_RESPONSE_LIMIT: usize = 64 * 1024 * 1024;
const ERROR_RESPONSE_LIMIT: usize = 8 * 1024;

/// Retains a bounded diagnostic prefix without replacing HTTP status/retry
/// classification when the error body is oversized or its stream fails.
async fn read_error_prefix(mut response: Response) -> String {
    let mut body = Vec::new();
    while let Ok(Some(chunk)) = response.chunk().await {
        let remaining = ERROR_RESPONSE_LIMIT.saturating_sub(body.len());
        body.extend_from_slice(chunk.get(..remaining).unwrap_or(&chunk));
        if body.len() == ERROR_RESPONSE_LIMIT {
            break;
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// Admission spans body collection, blocking decode and unclaimed-result
/// cleanup. Eight jobs are shared by all clients; transport buffers and DTOs
/// already returned to callers are outside this budget.
const JSON_DIAGNOSTIC_LIMIT: usize = 8 * 1024;

/// An owned diagnostic formatted on the admitted blocking worker. The original
/// serde error can contain an entire peer string; it never crosses the await.
#[derive(Debug)]
pub(crate) struct JsonDecodeError(String);

impl std::fmt::Display for JsonDecodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

struct JsonDiagnosticWriter {
    message: String,
    limit: usize,
}

impl std::fmt::Write for JsonDiagnosticWriter {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let remaining = self.limit.saturating_sub(self.message.len());
        if value.len() <= remaining {
            self.message.push_str(value);
            return Ok(());
        }
        let mut end = remaining;
        while !value.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        self.message.push_str(&value[..end]);
        Err(std::fmt::Error)
    }
}

impl JsonDecodeError {
    fn from_serde(error: serde_json::Error) -> Self {
        // Reserve enough for the marker and two maximum-width usize locations.
        let mut writer = JsonDiagnosticWriter {
            message: String::new(),
            limit: JSON_DIAGNOSTIC_LIMIT.saturating_sub(128),
        };
        if std::fmt::write(&mut writer, format_args!("{error}")).is_err() {
            writer.message.push_str(" [truncated; at line ");
            writer.message.push_str(&error.line().to_string());
            writer.message.push_str(" column ");
            writer.message.push_str(&error.column().to_string());
            writer.message.push(']');
        }
        // `error` (potentially huge) is dropped here on the blocking worker,
        // while JsonDecodeWork still owns its admission permit.
        drop(error);
        Self(writer.message)
    }
}

const JSON_DECODE_JOBS: usize = 8;
type JsonCleanupJob = Box<dyn FnOnce() + Send + 'static>;

struct JsonDecodePool {
    slots: Arc<tokio::sync::Semaphore>,
    cleanup: SyncSender<JsonCleanupJob>,
    faulted: Arc<AtomicBool>,
    #[cfg(test)]
    collections_started: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    jobs_submitted: std::sync::atomic::AtomicUsize,
}

impl JsonDecodePool {
    fn new(limit: usize) -> Result<Arc<Self>, String> {
        Self::with_worker(limit, |receiver, worker_fault| {
            let cleanup_thread = std::thread::Builder::new()
                .name("sdx-json-cleanup".to_owned())
                .spawn(move || {
                    // A failed destructor faults future admission but already
                    // admitted owners drain through this one fixed worker.
                    for job in receiver {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
                            worker_fault.store(true, Ordering::Release);
                        }
                    }
                })?;
            drop(cleanup_thread);
            Ok(())
        })
    }

    fn with_worker(
        limit: usize,
        start_worker: impl FnOnce(
            std::sync::mpsc::Receiver<JsonCleanupJob>,
            Arc<AtomicBool>,
        ) -> std::io::Result<()>,
    ) -> Result<Arc<Self>, String> {
        let (cleanup, receiver) = std::sync::mpsc::sync_channel::<JsonCleanupJob>(limit);
        let faulted = Arc::new(AtomicBool::new(false));
        start_worker(receiver, Arc::clone(&faulted))
            .map_err(|error| format!("JSON cleanup worker creation failed: {error}"))?;
        Ok(Arc::new(Self {
            slots: Arc::new(tokio::sync::Semaphore::new(limit)),
            cleanup,
            faulted,
            #[cfg(test)]
            collections_started: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            jobs_submitted: std::sync::atomic::AtomicUsize::new(0),
        }))
    }

    async fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, TransferError> {
        self.check_health()?;
        let permit = Arc::clone(&self.slots)
            .acquire_owned()
            .await
            .map_err(|error| {
                TransferError::InvalidResponse(format!("JSON admission failed: {error}"))
            })?;
        self.check_health()?;
        Ok(permit)
    }

    fn check_health(&self) -> Result<(), TransferError> {
        if self.faulted.load(Ordering::Acquire) {
            Err(TransferError::InvalidResponse(
                "JSON decoder cleanup worker faulted".to_owned(),
            ))
        } else {
            Ok(())
        }
    }
}

fn json_decode_pool() -> Result<Arc<JsonDecodePool>, TransferError> {
    static POOL: OnceLock<Result<Arc<JsonDecodePool>, String>> = OnceLock::new();
    POOL.get_or_init(|| JsonDecodePool::new(JSON_DECODE_JOBS))
        .as_ref()
        .map(Arc::clone)
        .map_err(|error| TransferError::InvalidResponse(error.clone()))
}

// Declaration order is intentional: Rust drops value/body before releasing
// admission, including when a destructor unwinds.
struct JsonCleanupValue<T> {
    _value: Result<T, JsonDecodeError>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct JsonResultOwner<T: Send + 'static> {
    value: Option<Result<T, JsonDecodeError>>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    pool: Arc<JsonDecodePool>,
}

impl<T: Send + 'static> JsonResultOwner<T> {
    fn take(mut self) -> Option<Result<T, JsonDecodeError>> {
        // No await between ownership transfer and returning to the caller.
        // The successful caller now owns DTO lifetime; canceled unclaimed
        // results use Drop's bounded cleanup path instead.
        let value = self.value.take();
        drop(self.permit.take());
        value
    }
}

impl<T: Send + 'static> Drop for JsonResultOwner<T> {
    fn drop(&mut self) {
        let (Some(value), Some(permit)) = (self.value.take(), self.permit.take()) else {
            return;
        };
        let cleanup_value = JsonCleanupValue {
            _value: value,
            _permit: permit,
        };
        let job: JsonCleanupJob = Box::new(move || drop(cleanup_value));
        // Every queued cleanup retains one of the eight permits, so a queue
        // with eight places cannot fill normally. A disconnected/full queue
        // is an internal fault: stop future admission, then synchronously
        // drop as the bounded last-resort exception (never spawn rescue jobs).
        if let Err(TrySendError::Full(job) | TrySendError::Disconnected(job)) =
            self.pool.cleanup.try_send(job)
        {
            self.pool.faulted.store(true, Ordering::Release);
            drop(job);
        }
    }
}

struct JsonDecodeWork {
    body: Vec<u8>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    pool: Arc<JsonDecodePool>,
}

async fn decode_json_response<T: DeserializeOwned + Send + 'static>(
    response: Response,
    limit: usize,
) -> Result<Result<T, JsonDecodeError>, TransferError> {
    decode_json_with_pool(response, limit, json_decode_pool()?).await
}

async fn decode_json_with_pool<T: DeserializeOwned + Send + 'static>(
    response: Response,
    limit: usize,
    pool: Arc<JsonDecodePool>,
) -> Result<Result<T, JsonDecodeError>, TransferError> {
    // Initialize the cleanup worker and acquire admission before collecting
    // any owned response bytes. Cancellation during collection drops permit.
    let permit = pool.admit().await?;
    #[cfg(test)]
    pool.collections_started.fetch_add(1, Ordering::Relaxed);
    let mut work = JsonDecodeWork {
        body: Vec::new(),
        permit: Some(permit),
        pool: Arc::clone(&pool),
    };
    read_response_into(response, limit, &mut work.body).await?;
    #[cfg(test)]
    pool.jobs_submitted.fetch_add(1, Ordering::Relaxed);
    let decoded = tokio::task::spawn_blocking(move || {
        let value = serde_json::from_slice(&work.body).map_err(JsonDecodeError::from_serde);
        let owner = JsonResultOwner {
            value: Some(value),
            permit: work.permit.take(),
            pool: Arc::clone(&work.pool),
        };
        drop(work);
        owner
    })
    .await
    .map_err(|error| {
        pool.faulted.store(true, Ordering::Release);
        TransferError::InvalidResponse(format!("JSON decode worker failed: {error}"))
    })?;
    decoded.take().ok_or_else(|| {
        TransferError::InvalidResponse("JSON result owner was already consumed".to_owned())
    })
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
    let body = read_error_prefix(response).await;
    let message = parse_error_message(&body).unwrap_or(body);
    Err(http_error(status, message, retry_after))
}

/// Parses one valid `Retry-After` field as delay-seconds or HTTP-date.
/// Oversized digit-only values saturate; retry policy still caps the wait.
/// Malformed or repeated fields fall back to the configured backoff.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<u64> {
    parse_retry_after_at(headers, std::time::SystemTime::now())
}

fn parse_retry_after_at(
    headers: &reqwest::header::HeaderMap,
    now: std::time::SystemTime,
) -> Option<u64> {
    let mut values = headers.get_all(reqwest::header::RETRY_AFTER).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let value = value.to_str().ok()?.trim_matches([' ', '\t']);
    if value.is_empty() {
        return None;
    }
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Some(value.bytes().fold(0u64, |seconds, byte| {
            seconds
                .saturating_mul(10)
                .saturating_add(u64::from(byte.saturating_sub(b'0')))
        }));
    }
    let deadline = httpdate::parse_http_date(value).ok()?;
    let remaining = deadline.duration_since(now).unwrap_or_default();
    Some(
        remaining
            .as_secs()
            .saturating_add(u64::from(remaining.subsec_nanos() != 0)),
    )
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

fn transfer_error_from_json(error: &JsonDecodeError) -> TransferError {
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
    if end < start {
        return Err(TransferError::InvalidResponse(format!(
            "inverted Content-Range header: {value}"
        )));
    }
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

async fn read_response_bounded(
    response: reqwest::Response,
    limit: usize,
) -> Result<Vec<u8>, TransferError> {
    let mut body = Vec::new();
    read_response_into(response, limit, &mut body).await?;
    Ok(body)
}

async fn read_response_into(
    mut response: reqwest::Response,
    limit: usize,
    body: &mut Vec<u8>,
) -> Result<(), TransferError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(TransferError::InvalidResponse(
            "response body exceeds byte limit".to_owned(),
        ));
    }
    // Never trust the peer's allocation hint. Check every streamed chunk before
    // extending our owned buffer, including chunked and compressed responses.
    while let Some(chunk) = response.chunk().await? {
        let required = body
            .len()
            .checked_add(chunk.len())
            .filter(|size| *size <= limit)
            .ok_or_else(|| {
                TransferError::InvalidResponse("response body exceeds byte limit".to_owned())
            })?;
        if required > body.capacity() {
            // Grow geometrically to avoid reallocating/copying on every HTTP
            // chunk, while keeping the allocated wire buffer within its cap.
            let target = required.max(body.capacity().saturating_mul(2)).min(limit);
            body.try_reserve_exact(target.saturating_sub(body.len()))
                .map_err(|error| {
                    TransferError::InvalidResponse(format!("response allocation failed: {error}"))
                })?;
        }
        body.extend_from_slice(&chunk);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TransferClient, parse_multipart_byteranges};
    use bytes::Bytes;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[derive(Default)]
    struct DecodeGate {
        open: std::sync::Mutex<bool>,
        changed: std::sync::Condvar,
        entered: tokio::sync::Notify,
    }

    impl DecodeGate {
        fn wait(&self) {
            let mut open = self
                .open
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.entered.notify_one();
            while !*open {
                open = self
                    .changed
                    .wait(open)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }

        fn release(&self) {
            *self
                .open
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
            self.changed.notify_all();
        }
    }

    struct ReleaseDecodeGate(std::sync::Arc<DecodeGate>);

    impl Drop for ReleaseDecodeGate {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    struct DecodeDropProbe {
        gate: std::sync::Arc<DecodeGate>,
        drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        fault: bool,
    }

    impl Drop for DecodeDropProbe {
        fn drop(&mut self) {
            self.gate.wait();
            self.drops
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if self.fault {
                // Inject an unwind without invoking a global panic hook.
                std::panic::resume_unwind(Box::new("injected JSON cleanup fault"));
            }
        }
    }

    async fn wait_decode_condition(condition: impl Fn() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn json_queued_decode_cancellation_retains_eight_slots_and_runtime_progress() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let pool = super::JsonDecodePool::new(8).unwrap();
            let gate = std::sync::Arc::new(DecodeGate::default());
            let release = ReleaseDecodeGate(std::sync::Arc::clone(&gate));
            let blocker_gate = std::sync::Arc::clone(&gate);
            let blocker = tokio::task::spawn_blocking(move || blocker_gate.wait());
            gate.entered.notified().await;
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
                .mount(&server)
                .await;
            let client = reqwest::Client::new();
            let mut requests = Vec::new();
            for _ in 0..10 {
                let response = client.get(server.uri()).send().await.unwrap();
                let decode_pool = std::sync::Arc::clone(&pool);
                requests.push(tokio::spawn(async move {
                    super::decode_json_with_pool::<serde_json::Value>(response, 64, decode_pool)
                        .await
                }));
            }
            wait_decode_condition(|| {
                pool.jobs_submitted
                    .load(std::sync::atomic::Ordering::Relaxed)
                    == 8
            })
            .await;
            assert_eq!(pool.slots.available_permits(), 0);
            for request in &requests {
                request.abort();
            }
            for request in requests {
                assert!(request.await.unwrap_err().is_cancelled());
            }
            // All eight queued jobs retain admission after their callers exit;
            // the two waiters never started body collection. A timer can run
            // while the only blocking worker is deliberately held.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            assert_eq!(pool.slots.available_permits(), 0);
            assert_eq!(
                pool.collections_started
                    .load(std::sync::atomic::Ordering::Relaxed),
                8
            );
            drop(release);
            blocker.await.unwrap();
            wait_decode_condition(|| pool.slots.available_permits() == 8).await;
            assert!(!pool.faulted.load(std::sync::atomic::Ordering::Acquire));
            let response = client.get(server.uri()).send().await.unwrap();
            assert_eq!(
                super::decode_json_with_pool::<serde_json::Value>(response, 64, pool)
                    .await
                    .unwrap()
                    .unwrap(),
                serde_json::json!({})
            );
        });
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_completed_result_cleanup_retains_slot_until_destructor_finishes() {
        let pool = super::JsonDecodePool::new(1).unwrap();
        let gate = std::sync::Arc::new(DecodeGate::default());
        let release = ReleaseDecodeGate(std::sync::Arc::clone(&gate));
        let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let owner = super::JsonResultOwner {
            value: Some(Ok(DecodeDropProbe {
                gate: std::sync::Arc::clone(&gate),
                drops: std::sync::Arc::clone(&drops),
                fault: false,
            })),
            permit: Some(pool.admit().await.unwrap()),
            pool: std::sync::Arc::clone(&pool),
        };
        let completed = tokio::task::spawn_blocking(move || owner);
        wait_decode_condition(|| completed.is_finished()).await;
        drop(completed);
        gate.entered.notified().await;
        // Dropping a completed JoinHandle runs only the bounded owner enqueue
        // on this executor; the deliberately blocked DTO drop is on cleanup.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        assert_eq!(pool.slots.available_permits(), 0);
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 0);
        let waiting_pool = std::sync::Arc::clone(&pool);
        let waiting = tokio::spawn(async move { waiting_pool.admit().await });
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(release);
        drop(waiting.await.unwrap().unwrap());
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(pool.slots.available_permits(), 1);

        let owner = super::JsonResultOwner {
            value: Some(Ok(serde_json::json!({"taken": true}))),
            permit: Some(pool.admit().await.unwrap()),
            pool: std::sync::Arc::clone(&pool),
        };
        assert_eq!(
            owner.take().unwrap().unwrap(),
            serde_json::json!({"taken": true})
        );
        assert_eq!(pool.slots.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_cleanup_fault_rejects_new_admission_and_drains_existing_owners() {
        let pool = super::JsonDecodePool::new(2).unwrap();
        let gate = std::sync::Arc::new(DecodeGate::default());
        let release = ReleaseDecodeGate(std::sync::Arc::clone(&gate));
        let drops = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for fault in [true, false] {
            drop(super::JsonResultOwner {
                value: Some(Ok(DecodeDropProbe {
                    gate: std::sync::Arc::clone(&gate),
                    drops: std::sync::Arc::clone(&drops),
                    fault,
                })),
                permit: Some(pool.admit().await.unwrap()),
                pool: std::sync::Arc::clone(&pool),
            });
        }
        gate.entered.notified().await;
        assert_eq!(pool.slots.available_permits(), 0);
        drop(release);
        wait_decode_condition(|| pool.slots.available_permits() == 2).await;
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert!(matches!(
            pool.admit().await,
            Err(super::TransferError::InvalidResponse(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn json_canceled_body_collection_releases_admission() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (release, released) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert_ne!(socket.read(&mut request).await.unwrap(), 0);
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\n{\r\n")
                .await
                .unwrap();
            let _ = released.await;
            let _ = socket.write_all(b"1\r\n}\r\n0\r\n\r\n").await;
        });
        let pool = super::JsonDecodePool::new(1).unwrap();
        let response = reqwest::Client::new()
            .get(format!("http://{addr}"))
            .send()
            .await
            .unwrap();
        let decode_pool = std::sync::Arc::clone(&pool);
        let task = tokio::spawn(async move {
            super::decode_json_with_pool::<serde_json::Value>(response, 64, decode_pool).await
        });
        wait_decode_condition(|| {
            pool.collections_started
                .load(std::sync::atomic::Ordering::Relaxed)
                == 1
        })
        .await;
        assert_eq!(pool.slots.available_permits(), 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.slots.available_permits(), 1);
        assert!(!pool.faulted.load(std::sync::atomic::Ordering::Acquire));
        let _ = release.send(());
        server.await.unwrap();
    }

    #[test]
    fn json_cleanup_thread_creation_failure_is_typed_and_has_no_admission() {
        let pool = super::JsonDecodePool::with_worker(8, |_, _| {
            Err(std::io::Error::other("injected thread creation failure"))
        })
        .map_err(super::TransferError::InvalidResponse);
        assert!(
            matches!(pool, Err(super::TransferError::InvalidResponse(message)) if message.contains("JSON cleanup worker creation failed"))
        );
        assert!(std::sync::Arc::ptr_eq(
            &super::json_decode_pool().unwrap(),
            &super::json_decode_pool().unwrap()
        ));
    }

    #[tokio::test]
    async fn json_decode_worker_fault_releases_slot_and_rejects_new_admission() {
        struct FaultingDecode;
        impl<'de> serde::Deserialize<'de> for FaultingDecode {
            fn deserialize<D: serde::Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
                std::panic::resume_unwind(Box::new("injected JSON decoder fault"));
            }
        }
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
            .mount(&server)
            .await;
        let response = reqwest::Client::new()
            .get(server.uri())
            .send()
            .await
            .unwrap();
        let pool = super::JsonDecodePool::new(1).unwrap();
        let result = super::decode_json_with_pool::<FaultingDecode>(
            response,
            64,
            std::sync::Arc::clone(&pool),
        )
        .await;
        assert!(
            matches!(result, Err(super::TransferError::InvalidResponse(message)) if message.contains("JSON decode worker failed"))
        );
        assert_eq!(pool.slots.available_permits(), 1);
        assert!(matches!(
            pool.admit().await,
            Err(super::TransferError::InvalidResponse(_))
        ));
    }

    #[test]
    fn json_diagnostic_preserves_short_errors_and_truncates_at_unicode_boundary() {
        let error = serde_json::from_str::<u64>("\"bad\"").unwrap_err();
        let original = error.to_string();
        assert_eq!(
            super::JsonDecodeError::from_serde(error).to_string(),
            original
        );
        let encoded = serde_json::to_string(&"é".repeat(20_000)).unwrap();
        let error = serde_json::from_str::<u64>(&encoded).unwrap_err();
        let line = error.line();
        let column = error.column();
        let diagnostic = super::JsonDecodeError::from_serde(error).to_string();
        assert!(diagnostic.len() <= super::JSON_DIAGNOSTIC_LIMIT);
        assert!(diagnostic.ends_with(&format!("[truncated; at line {line} column {column}]")));
        assert!(diagnostic.contains('é'));
        let mut writer = super::JsonDiagnosticWriter {
            message: String::new(),
            limit: 5,
        };
        assert!(std::fmt::Write::write_str(&mut writer, "ééé").is_err());
        assert_eq!(writer.message, "éé");
    }

    #[tokio::test]
    async fn reconstruction_wrong_type_diagnostic_is_bounded_and_typed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "offset_into_first_range": "x".repeat(1_000_000), "terms": [], "xorbs": {},
            })))
            .expect(1)
            .mount(&server)
            .await;
        let error = TransferClient::new(reqwest::Client::new())
            .reconstruction_v2(&server.uri(), "token", &"a".repeat(64), None)
            .await
            .unwrap_err();
        assert!(
            matches!(error, super::TransferError::InvalidResponse(message)
            if message.len() <= super::JSON_DIAGNOSTIC_LIMIT && message.contains("[truncated; at line 1 column "))
        );
    }

    #[test]
    fn reconstruction_envelope_covers_worst_numbers_escaping_and_nesting() {
        use shardline_xet_adapter::{
            ReconstructionChunkRange, ReconstructionFetchInfo, ReconstructionTerm,
            ReconstructionUrlRange,
        };
        use std::collections::BTreeMap;
        // Every URL byte takes six JSON bytes; every number takes 20 digits.
        let url = "\u{0001}".repeat(102);
        let terms = 65_536;
        let response = shardline_xet_adapter::FileReconstructionResponse {
            offset_into_first_range: u64::MAX,
            terms: (0..terms)
                .map(|n| ReconstructionTerm {
                    hash: format!("{n:064x}"),
                    unpacked_length: u64::MAX,
                    range: ReconstructionChunkRange {
                        start: u64::MAX,
                        end: u64::MAX,
                    },
                })
                .collect(),
            fetch_info: (0..terms)
                .map(|n| {
                    (
                        format!("{n:064x}"),
                        vec![ReconstructionFetchInfo {
                            range: ReconstructionChunkRange {
                                start: u64::MAX,
                                end: u64::MAX,
                            },
                            url: url.clone(),
                            url_range: ReconstructionUrlRange {
                                start: u64::MAX,
                                end: u64::MAX,
                            },
                        }],
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        };
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0 = self.0.checked_add(bytes.len()).unwrap();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let limit = TransferClient::reconstruction_envelope_bytes(terms, url.len()).unwrap();
        assert!(limit <= 64 * 1024 * 1024);
        let mut v1 = Counter(0);
        serde_json::to_writer(&mut v1, &response).unwrap();
        let response = shardline_xet_adapter::reconstruction_v2_from_v1(response);
        let mut v2 = Counter(0);
        serde_json::to_writer(&mut v2, &response).unwrap();
        assert!(v1.0 <= limit);
        assert!(v2.0 <= limit);
        assert!(v2.0 > 32 * 1024 * 1024);
        assert!(TransferClient::reconstruction_envelope_bytes(usize::MAX, 1).is_err());
        assert!(TransferClient::reconstruction_envelope_bytes(1, usize::MAX).is_err());
        assert_eq!(
            TransferClient::reconstruction_envelope_bytes(0, 0).unwrap(),
            75
        );
    }

    #[tokio::test]
    async fn optional_raw_and_acknowledgements_enforce_their_budgets() {
        let server = MockServer::start().await;
        Mock::given(path("/large"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'a'; 65_537]))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'a'; 65_537]))
            .mount(&server)
            .await;
        let client = TransferClient::new(reqwest::Client::new());
        assert!(matches!(
            client
                .get_optional_bytes_bounded(&server.uri(), "token", "/large", 16)
                .await,
            Err(crate::TransferError::InvalidResponse(_))
        ));
        assert!(matches!(
            client
                .request_raw(
                    &reqwest::Method::GET,
                    &format!("{}/large", server.uri()),
                    "token",
                    None,
                    16
                )
                .await,
            Err(crate::TransferError::InvalidResponse(_))
        ));
        assert!(matches!(
            client
                .upload_xorb(&server.uri(), "token", &"a".repeat(64), Bytes::new())
                .await,
            Err(crate::TransferError::InvalidResponse(_))
        ));
        assert!(matches!(
            client
                .upload_shard(&server.uri(), "token", Vec::new())
                .await,
            Err(crate::TransferError::InvalidResponse(_))
        ));
        // Caller can explicitly admit the same optional raw payload.
        let body = client
            .get_optional_bytes_bounded(&server.uri(), "token", "/large", 65_537)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body.len(), 65_537);
    }

    #[tokio::test]
    async fn acknowledgement_compatibility_envelope_accepts_extended_json() {
        let server = MockServer::start().await;
        Mock::given(path(
            "/v1/xorbs/default/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "was_inserted": true, "extension": "e".repeat(60_000),
        })))
        .mount(&server)
        .await;
        Mock::given(path("/v1/shards"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": 255, "extension": "e".repeat(60_000),
            })))
            .mount(&server)
            .await;
        let client = TransferClient::new(reqwest::Client::new());
        assert!(
            client
                .upload_xorb(&server.uri(), "token", &"a".repeat(64), Bytes::new())
                .await
                .unwrap()
                .was_inserted
        );
        assert_eq!(
            client
                .upload_shard(&server.uri(), "token", Vec::new())
                .await
                .unwrap()
                .result,
            255
        );
    }

    #[tokio::test]
    async fn metadata_chunked_overflow_stops_without_waiting_for_end_of_body() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            assert_ne!(socket.read(&mut request).await.unwrap(), 0);
            socket.write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n11\r\n12345678901234567\r\n").await.unwrap();
            // No terminating chunk: a reader collecting the entire response
            // would wait forever. Keep the server alive until client finishes.
            let mut probe = [0; 1];
            let _ = socket.read(&mut probe).await;
        });
        let client = TransferClient::new(reqwest::Client::new());
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.request_raw(
                &reqwest::Method::GET,
                &format!("http://{addr}"),
                "token",
                None,
                16,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(
            result,
            Err(crate::TransferError::InvalidResponse(_))
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_errors_preserve_http_status_retry_after_and_bounded_diagnostics() {
        let server = MockServer::start().await;
        Mock::given(path("/error"))
            .respond_with(
                ResponseTemplate::new(503)
                    .insert_header("Retry-After", "7")
                    .set_body_bytes(vec![b'e'; 100_000]),
            )
            .mount(&server)
            .await;
        let client = TransferClient::new(reqwest::Client::new());
        for result in [
            client
                .get_optional_bytes(&server.uri(), "token", "/error")
                .await
                .map(|_| ()),
            client
                .request_raw(
                    &reqwest::Method::GET,
                    &format!("{}/error", server.uri()),
                    "token",
                    None,
                    1,
                )
                .await
                .map(|_| ()),
        ] {
            match result {
                Err(crate::TransferError::HttpStatus {
                    status,
                    message,
                    retry_after,
                }) => {
                    assert_eq!(status, 503);
                    assert_eq!(retry_after, Some(7));
                    assert!(message.len() <= super::ERROR_RESPONSE_LIMIT);
                }
                other => assert!(
                    matches!(
                        other,
                        Err(crate::TransferError::HttpStatus { status: 503, .. })
                    ),
                    "unexpected error classification: {other:?}",
                ),
            }
        }
    }

    #[tokio::test]
    async fn bounded_xorb_http_body_rejects_declared_and_chunked_overflow() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for (declared, streaming) in [
            (None, true),
            (Some(1_000_000u64), true),
            (Some(129 * 1024 * 1024), false),
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 4096];
                let received = socket.read(&mut request).await.unwrap();
                assert_ne!(received, 0);
                let framing = declared.map_or_else(
                    || "Transfer-Encoding: chunked\r\n".to_owned(),
                    |length| format!("Content-Length: {length}\r\n"),
                );
                let headers = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-3/4\r\n{framing}Connection: close\r\n\r\n"
                );
                socket.write_all(headers.as_bytes()).await.unwrap();
                if declared.is_none() {
                    for _ in 0..10 {
                        if socket.write_all(b"2000\r\n").await.is_err() {
                            break;
                        }
                        if socket.write_all(&[7u8; 8192]).await.is_err() {
                            break;
                        }
                        if socket.write_all(b"\r\n").await.is_err() {
                            break;
                        }
                    }
                    let _ignored = socket.write_all(b"0\r\n\r\n").await;
                }
            });
            let transfer = TransferClient::new(reqwest::Client::new());
            let url = format!("http://{address}");
            let result = if streaming {
                transfer
                    .fetch_xorb_range_bounded(&url, "token", super::ByteRange::new(0, 3))
                    .await
            } else {
                transfer
                    .fetch_xorb_range(&url, "token", super::ByteRange::new(0, 3))
                    .await
            };
            let error = result.unwrap_err();
            assert!(
                matches!(error,crate::error::TransferError::InvalidResponse(ref reason) if reason.contains("byte limit"))
            );
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn fetch_xorb_range_preserves_full_body_inclusive_endpoints() {
        let server = MockServer::start().await;
        for body in [b"A".as_slice(), b"BBBB".as_slice()] {
            server.reset().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
                .mount(&server)
                .await;
            let ranged = TransferClient::new(reqwest::Client::new())
                .fetch_xorb_range(&server.uri(), "token", super::ByteRange::new(0, 3))
                .await
                .unwrap();
            assert_eq!(
                ranged.served_range,
                super::ByteRange::new(0, body.len() as u64 - 1)
            );
            assert_eq!(ranged.served_range.len(), ranged.data.len() as u64);
        }
    }

    #[tokio::test]
    async fn fetch_xorb_range_rejects_empty_and_inconsistent_responses() {
        let server = MockServer::start().await;
        let responses = [
            ResponseTemplate::new(200).set_body_bytes(Vec::<u8>::new()),
            ResponseTemplate::new(206)
                .insert_header("Content-Range", "bytes 10-13/20")
                .set_body_bytes(b"BB"),
            ResponseTemplate::new(206)
                .insert_header("Content-Range", "bytes 13-10/20")
                .set_body_bytes(b"BBBB"),
        ];
        for response in responses {
            server.reset().await;
            Mock::given(method("GET"))
                .respond_with(response)
                .mount(&server)
                .await;
            assert!(matches!(
                TransferClient::new(reqwest::Client::new())
                    .fetch_xorb_range(&server.uri(), "token", super::ByteRange::new(10, 13))
                    .await,
                Err(crate::TransferError::InvalidResponse(_))
            ));
        }
    }

    #[tokio::test]
    async fn fetch_xorb_range_preserves_contiguous_multipart_coordinates() {
        let server = MockServer::start().await;
        for body in [
            "--b\r\nContent-Range: bytes 10-13/20\r\n\r\nBBBB\r\n--b--\r\n",
            "--b\r\nContent-Range: bytes 12-13/20\r\n\r\nBB\r\n--b\r\nContent-Range: bytes 10-11/20\r\n\r\nBB\r\n--b--\r\n",
        ] {
            server.reset().await;
            Mock::given(method("GET"))
                .respond_with(
                    ResponseTemplate::new(206)
                        .set_body_raw(body, "multipart/byteranges; boundary=b"),
                )
                .mount(&server)
                .await;
            let ranged = TransferClient::new(reqwest::Client::new())
                .fetch_xorb_range(&server.uri(), "token", super::ByteRange::new(10, 13))
                .await
                .unwrap();
            assert_eq!(ranged.served_range, super::ByteRange::new(10, 13));
            assert_eq!(ranged.data, b"BBBB");
        }
    }

    #[tokio::test]
    async fn fetch_xorb_range_rejects_unrepresentable_multipart_responses() {
        let server = MockServer::start().await;
        for body in [
            "--b--\r\n",
            "--b\r\nContent-Range: bytes 10-13/20\r\n\r\nBB\r\n--b--\r\n",
            "--b\r\nContent-Range: bytes 10-11/20\r\n\r\nBB\r\n--b\r\nContent-Range: bytes 13-14/20\r\n\r\nBB\r\n--b--\r\n",
            "--b\r\nContent-Range: bytes 10-11/20\r\n\r\nBB\r\n--b\r\nContent-Range: bytes 11-12/20\r\n\r\nBB\r\n--b--\r\n",
        ] {
            server.reset().await;
            Mock::given(method("GET"))
                .respond_with(
                    ResponseTemplate::new(206)
                        .set_body_raw(body, "multipart/byteranges; boundary=b"),
                )
                .mount(&server)
                .await;
            assert!(matches!(
                TransferClient::new(reqwest::Client::new())
                    .fetch_xorb_range(&server.uri(), "token", super::ByteRange::new(10, 13))
                    .await,
                Err(crate::TransferError::MalformedMultipart(_))
            ));
        }
    }

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

#[cfg(test)]
mod retry_after_header_tests {
    use super::parse_retry_after_at;
    use reqwest::header::HeaderValue;
    use reqwest::header::{HeaderMap, RETRY_AFTER};
    use std::time::Duration;
    use std::time::SystemTime;
    fn now() -> SystemTime {
        httpdate::parse_http_date("Sat, 01 Jan 2000 00:00:00 GMT").unwrap()
    }
    fn parse(value: &str) -> Option<u64> {
        let mut headers = HeaderMap::new();
        headers.insert(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
        parse_retry_after_at(&headers, now())
    }
    #[test]
    fn digit_seconds_strict_and_saturating() {
        for (value, expected) in [
            ("0", 0),
            ("0001", 1),
            (" 1\t", 1),
            ("18446744073709551615", u64::MAX),
            ("18446744073709551616", u64::MAX),
        ] {
            assert_eq!(parse(value), Some(expected));
        }
        assert_eq!(parse(&"9".repeat(8192)), Some(u64::MAX));
    }
    #[test]
    fn invalid_seconds_and_dates_ignored() {
        for value in [
            "",
            "+1",
            "-1",
            "1.5",
            "1 2",
            "1,2",
            "later",
            "Sat, 99 Jan 2000 00:00:01 GMT",
        ] {
            assert_eq!(parse(value), None, "{value}");
        }
        let mut h = HeaderMap::new();
        h.insert(RETRY_AFTER, HeaderValue::from_bytes(b"\xff").unwrap());
        assert_eq!(parse_retry_after_at(&h, now()), None);
    }
    #[test]
    fn singleton_required_and_absence_falls_back() {
        assert_eq!(parse_retry_after_at(&HeaderMap::new(), now()), None);
        for values in [["0", "1"], ["1", "0"], ["0", "0"], ["bad", "1"]] {
            let mut h = HeaderMap::new();
            for value in values {
                h.append(RETRY_AFTER, HeaderValue::from_str(value).unwrap());
            }
            assert_eq!(parse_retry_after_at(&h, now()), None);
        }
        assert_eq!(parse("Sat, 01 Jan 2000 00:00:01 GMT, 1"), None);
    }
    #[test]
    fn dates_future_now_and_past() {
        assert_eq!(parse("Sat, 01 Jan 2000 00:00:01 GMT"), Some(1));
        assert_eq!(parse("Sat, 01 Jan 2000 00:00:00 GMT"), Some(0));
        assert_eq!(parse("Fri, 31 Dec 1999 23:59:59 GMT"), Some(0));
    }
    #[test]
    fn obsolete_dates_supported() {
        assert_eq!(parse("Saturday, 01-Jan-00 00:00:01 GMT"), Some(1));
        assert_eq!(parse("Sat Jan  1 00:00:01 2000"), Some(1));
    }
    #[test]
    fn positive_fractional_remaining_rounded_up() {
        let mut h = HeaderMap::new();
        h.insert(
            RETRY_AFTER,
            HeaderValue::from_static("Sat, 01 Jan 2000 00:00:01 GMT"),
        );
        assert_eq!(
            parse_retry_after_at(&h, now() + Duration::from_millis(500)),
            Some(1)
        );
        assert_eq!(
            parse_retry_after_at(&h, now() + Duration::from_nanos(999999999)),
            Some(1)
        );
        assert_eq!(
            parse_retry_after_at(&h, now() + Duration::from_secs(1)),
            Some(0)
        );
    }
}
