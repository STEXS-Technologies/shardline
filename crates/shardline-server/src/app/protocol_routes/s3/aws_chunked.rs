//! AWS chunked (`aws-chunked`) transfer-encoding decoder for the S3 frontend.
//!
//! Real S3 clients — mc, the AWS SDKs, pyarrow — stream request bodies with
//! AWS SigV4 chunked encoding (`x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
//! or `Content-Encoding: aws-chunked`). The wire format is:
//!
//! ```text
//! <hex-size>[;chunk-signature=<hex>]\r\n<data>\r\n  ...  0[;chunk-signature=...]\r\n\r\n
//! ```
//!
//! The decoder strips the framing so the CDC ingestor stores the **decoded**
//! payload (and its size), matching what the client sent. Chunk signatures are
//! NOT verified (documented deviation — the access key *is* the credential).

use std::io::{Error as IoError, ErrorKind};

use axum::{
    body::Bytes,
    http::{HeaderMap, header::CONTENT_ENCODING},
};
use futures_util::{Stream, stream};

use crate::{ServerError, overflow::checked_add, upload_ingest::RequestBodyReader};

/// The SigV4 streaming-payload marker header value.
const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// Maximum length of one chunk-size line (hex size + optional signature).
const MAX_AWS_CHUNK_LINE_BYTES: usize = 1024;

/// Returns whether the request body is AWS-chunked encoded.
///
/// Detection is via the SigV4 streaming marker (`x-amz-content-sha256`) or the
/// `Content-Encoding: aws-chunked` header (AWS SDKs use the marker; mc uses
/// the encoding header).
#[must_use]
pub fn is_aws_chunked(headers: &HeaderMap) -> bool {
    let marker = headers
        .get("x-amz-content-sha256")
        .and_then(|value| value.to_str().ok())
        == Some(STREAMING_AWS4_HMAC_SHA256_PAYLOAD);
    if marker {
        return true;
    }
    headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|encodings| {
            encodings
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("aws-chunked"))
        })
}

/// Returns the decoded-content length declared by the client, when present.
///
/// `x-amz-decoded-content-length` is only sent with the SigV4 streaming form;
/// `Content-Length` on a chunked body is the *framed* length and must not be
/// used for the decoded size.
#[must_use]
pub fn declared_decoded_content_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-amz-decoded-content-length")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

/// The decoder's parse state machine.
enum AwsChunkState {
    /// Reading the `<hex-size>[;chunk-signature=...]` line.
    ChunkSize,
    /// Reading `remaining` bytes of chunk data.
    Data { remaining: usize },
    /// Expecting the `\r\n` terminator after chunk data.
    Trailer,
    /// The zero-size terminator chunk was seen.
    Done,
}

/// Incrementally decodes an AWS-chunked payload from a raw byte stream.
struct AwsChunkedDecoder {
    reader: RequestBodyReader,
    pending: Vec<u8>,
    state: AwsChunkState,
    max_decoded_bytes: u64,
    decoded_bytes: u64,
}

impl AwsChunkedDecoder {
    /// Pulls the next raw chunk into the pending buffer; `false` when the
    /// source is exhausted.
    async fn fill(&mut self) -> Result<bool, ServerError> {
        match self.reader.next_bytes().await? {
            Some(bytes) => {
                self.pending.extend_from_slice(&bytes);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn truncated() -> ServerError {
        ServerError::Io(IoError::new(
            ErrorKind::InvalidData,
            "truncated aws-chunked payload",
        ))
    }

    fn malformed() -> ServerError {
        ServerError::Io(IoError::new(
            ErrorKind::InvalidData,
            "malformed aws-chunked payload",
        ))
    }

    /// Produces the next decoded chunk, or `None` when the payload is done.
    async fn next_decoded(&mut self) -> Result<Option<Bytes>, ServerError> {
        loop {
            match self.state {
                AwsChunkState::ChunkSize => {
                    // Find the terminating `\n`; its preceding byte must be `\r`.
                    // Re-scan after every fill: a large payload can arrive in a
                    // single raw chunk, so the line-length guard must only fire
                    // when the line genuinely has no terminator within the bound.
                    let newline = loop {
                        if let Some(position) = self.pending.iter().position(|&byte| byte == b'\n')
                        {
                            break position;
                        }
                        if self.pending.len() > MAX_AWS_CHUNK_LINE_BYTES {
                            return Err(Self::malformed());
                        }
                        if !self.fill().await? {
                            return Err(Self::truncated());
                        }
                    };
                    let cr_before = newline
                        .checked_sub(1)
                        .and_then(|index| self.pending.get(index));
                    if cr_before != Some(&b'\r') {
                        return Err(Self::malformed());
                    }
                    let line: Vec<u8> = self.pending.drain(..=newline).collect();
                    let line = String::from_utf8_lossy(&line);
                    let size_hex = line.split(';').next().unwrap_or("").trim();
                    let chunk_size =
                        usize::from_str_radix(size_hex, 16).map_err(|_error| Self::malformed())?;
                    if chunk_size == 0 {
                        self.state = AwsChunkState::Done;
                    } else {
                        self.state = AwsChunkState::Data {
                            remaining: chunk_size,
                        };
                    }
                }
                AwsChunkState::Data { remaining } => {
                    if remaining == 0 {
                        self.state = AwsChunkState::Trailer;
                        continue;
                    }
                    if self.pending.is_empty() {
                        if !self.fill().await? {
                            return Err(Self::truncated());
                        }
                        continue;
                    }
                    let take = remaining.min(self.pending.len());
                    let chunk: Vec<u8> = self.pending.drain(..take).collect();
                    self.decoded_bytes = checked_add(
                        self.decoded_bytes,
                        u64::try_from(take).map_err(ServerError::from)?,
                    )?;
                    if self.decoded_bytes > self.max_decoded_bytes {
                        return Err(ServerError::RequestBodyTooLarge);
                    }
                    self.state = AwsChunkState::Data {
                        remaining: remaining.saturating_sub(take),
                    };
                    return Ok(Some(Bytes::from(chunk)));
                }
                AwsChunkState::Trailer => {
                    while self.pending.len() < 2 {
                        if !self.fill().await? {
                            return Err(Self::truncated());
                        }
                    }
                    if self.pending.get(..2) != Some(&b"\r\n"[..]) {
                        return Err(Self::malformed());
                    }
                    self.pending.drain(..2);
                    self.state = AwsChunkState::ChunkSize;
                }
                AwsChunkState::Done => {
                    self.pending.clear();
                    return Ok(None);
                }
            }
        }
    }
}

/// Wraps a raw request body reader into an AWS-chunked-decoded reader.
///
/// The returned stream yields the decoded payload bytes (no framing); the
/// decoded byte count is enforced against `max_decoded_bytes`.
pub fn decode_aws_chunked(
    reader: RequestBodyReader,
    max_decoded_bytes: u64,
) -> impl Stream<Item = Result<Bytes, ServerError>> {
    stream::unfold(
        AwsChunkedDecoder {
            reader,
            pending: Vec::new(),
            state: AwsChunkState::ChunkSize,
            max_decoded_bytes,
            decoded_bytes: 0,
        },
        |mut decoder| async move {
            match decoder.next_decoded().await {
                Ok(Some(bytes)) => Some((Ok(bytes), decoder)),
                Ok(None) => None,
                Err(error) => Some((Err(error), decoder)),
            }
        },
    )
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]

    use axum::http::{HeaderMap, HeaderValue};
    use futures_util::StreamExt;

    use super::*;

    /// Frames `data` as an AWS chunked payload (single chunk + terminator).
    fn frame(data: &[u8]) -> Vec<u8> {
        let mut out = format!("{:x}\r\n", data.len()).into_bytes();
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n0;chunk-signature=deadbeef\r\n\r\n");
        out
    }

    async fn decode_all(bytes: Vec<u8>, max: u64) -> Result<Vec<u8>, ServerError> {
        let reader = RequestBodyReader::from_bytes(Bytes::from(bytes));
        let stream = decode_aws_chunked(reader, max);
        tokio::pin!(stream);
        let mut decoded = Vec::new();
        while let Some(chunk) = stream.next().await {
            decoded.extend_from_slice(&chunk?);
        }
        Ok(decoded)
    }

    #[tokio::test]
    async fn decodes_single_chunk_payload() {
        let data = b"hello wave-b\n";
        let decoded = decode_all(frame(data), 1024).await.unwrap();
        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn decodes_multiple_chunks_across_raw_reads() {
        // A payload split across two chunks (and the decoder is fed one raw
        // byte at a time to exercise the buffering).
        let mut framed = "3;chunk-signature=aa\r\nabc\r\n".to_owned().into_bytes();
        framed.extend_from_slice(&"2;chunk-signature=bb\r\nde\r\n".to_owned().into_bytes());
        framed.extend_from_slice(b"0;chunk-signature=cc\r\n\r\n");
        let reader = RequestBodyReader::from_stream(futures_util::stream::iter(
            framed
                .into_iter()
                .map(|byte| Ok::<_, ServerError>(Bytes::from(vec![byte]))),
        ));
        let stream = decode_aws_chunked(reader, 1024);
        tokio::pin!(stream);
        let mut decoded = Vec::new();
        while let Some(chunk) = stream.next().await {
            decoded.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(decoded, b"abcde");
    }

    #[tokio::test]
    async fn enforces_decoded_size_limit() {
        let error = decode_all(frame(b"0123456789"), 5).await.unwrap_err();
        assert!(matches!(error, ServerError::RequestBodyTooLarge));
    }

    #[tokio::test]
    async fn rejects_truncated_payload() {
        let error = decode_all(b"5\r\nabc".to_vec(), 1024).await.unwrap_err();
        assert!(format!("{error:?}").contains("aws-chunked"));
    }

    #[tokio::test]
    async fn rejects_malformed_chunk_size_line() {
        let error = decode_all(b"zz\r\nabc\r\n0\r\n\r\n".to_vec(), 1024)
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("aws-chunked"));
    }

    #[tokio::test]
    async fn rejects_empty_payload_without_terminator() {
        let error = decode_all(b"\r\n".to_vec(), 1024).await.unwrap_err();
        assert!(format!("{error:?}").contains("aws-chunked"));
    }

    #[test]
    fn detects_aws_chunked_via_sigv4_marker_and_encoding() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-content-sha256",
            HeaderValue::from_static("STREAMING-AWS4-HMAC-SHA256-PAYLOAD"),
        );
        assert!(is_aws_chunked(&headers));
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_ENCODING, HeaderValue::from_static("aws-chunked"));
        assert!(is_aws_chunked(&headers));
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_ENCODING,
            HeaderValue::from_static("gzip, aws-chunked"),
        );
        assert!(is_aws_chunked(&headers));
        let headers = HeaderMap::new();
        assert!(!is_aws_chunked(&headers));
    }

    #[tokio::test]
    async fn decodes_large_chunk_payload() {
        let data = vec![0xAB_u8; 8_388_608];
        let mut framed = format!("{:x}\r\n", data.len()).into_bytes();
        framed.extend_from_slice(&data);
        framed.extend_from_slice(b"\r\n0;chunk-signature=deadbeef\r\n\r\n");
        let decoded = decode_all(framed, 1 << 30).await.unwrap();
        assert_eq!(decoded.len(), data.len());
    }
}
