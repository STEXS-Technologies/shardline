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

/// Validates the required singleton decoded-content length for AWS-chunked bodies.
/// Missing, repeated, malformed, or overflowing values are rejected before ingestion.
///
/// AWS-chunked uploads require `x-amz-decoded-content-length`;
/// `Content-Length` on a chunked body is the *framed* length and must not be
/// used for the decoded size.
pub fn declared_decoded_content_length(
    headers: &HeaderMap,
) -> Result<u64, shardline_s3_adapter::S3Error> {
    let invalid =
        || shardline_s3_adapter::S3Error::invalid_argument("Invalid x-amz-decoded-content-length");
    let mut values = headers.get_all("x-amz-decoded-content-length").iter();
    let value = values.next().ok_or_else(invalid)?;
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = value.to_str().map_err(|_error| invalid())?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid());
    }
    value.parse().map_err(|_error| invalid())
}

/// The decoder's parse state machine.
enum AwsChunkState {
    /// Reading the `<hex-size>[;chunk-signature=...]` line.
    ChunkSize,
    /// Reading `remaining` bytes of chunk data.
    Data {
        remaining: usize,
    },
    /// Expecting the `\r\n` terminator after chunk data.
    Trailer,
    /// The zero-size terminator chunk was seen.
    Completion,
    Done,
}

/// Incrementally decodes an AWS-chunked payload from a raw byte stream.
struct AwsChunkedDecoder {
    reader: RequestBodyReader,
    pending: Vec<u8>,
    state: AwsChunkState,
    max_decoded_bytes: u64,
    decoded_bytes: u64,
    expected_decoded_bytes: Option<u64>,
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

    /// Reads a bounded CRLF-terminated line, allowing EOF only between lines.
    async fn line(&mut self) -> Result<Option<Vec<u8>>, ServerError> {
        loop {
            if let Some(end) = self.pending.windows(2).position(|bytes| bytes == b"\r\n") {
                if end.saturating_add(2) > MAX_AWS_CHUNK_LINE_BYTES {
                    return Err(Self::malformed());
                }
                let mut line: Vec<u8> = self.pending.drain(..end.saturating_add(2)).collect();
                line.truncate(end);
                return Ok(Some(line));
            }
            if self.pending.len() >= MAX_AWS_CHUNK_LINE_BYTES {
                return Err(Self::malformed());
            }
            if !self.fill().await? {
                return if self.pending.is_empty() {
                    Ok(None)
                } else {
                    Err(Self::truncated())
                };
            }
        }
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        let mut terminated = false;
        let mut trailers_seen = false;
        let mut trailers_closed = false;
        while let Some(line) = self.line().await? {
            if line.is_empty() {
                terminated = true;
                trailers_closed |= trailers_seen;
                continue;
            }
            if trailers_closed {
                return Err(Self::malformed());
            }
            let Some(colon) = line.iter().position(|byte| *byte == b':') else {
                return Err(Self::malformed());
            };
            let name = line.get(..colon).ok_or_else(Self::malformed)?;
            let name = std::str::from_utf8(name).map_err(|_error| Self::malformed())?;
            let checksum = name.to_ascii_lowercase();
            let recognized = checksum
                .strip_prefix("x-amz-checksum-")
                .is_some_and(|suffix| {
                    !suffix.is_empty()
                        && suffix
                            .bytes()
                            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                })
                || name.eq_ignore_ascii_case("x-amz-trailer-signature");
            let value = line
                .get(colon.saturating_add(1)..)
                .ok_or_else(Self::malformed)?;
            // AWS documents an optional LF after a checksum value before CRLF.
            let value = value.strip_suffix(b"\n").unwrap_or(value);
            if !recognized
                || value.is_empty()
                || !value
                    .iter()
                    .all(|byte| byte.is_ascii_graphic() || *byte == b' ' || *byte == b'\t')
            {
                return Err(Self::malformed());
            }
            trailers_seen = true;
            terminated = false;
        }
        if !terminated {
            return Err(Self::truncated());
        }
        if self
            .expected_decoded_bytes
            .is_some_and(|expected| expected != self.decoded_bytes)
        {
            return Err(Self::malformed());
        }
        Ok(())
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
                    let line = self.line().await?.ok_or_else(Self::truncated)?;
                    let size = line.split(|byte| *byte == b';').next().unwrap_or_default();
                    if size.is_empty() || !size.iter().all(u8::is_ascii_hexdigit) {
                        return Err(Self::malformed());
                    }
                    let size_hex = std::str::from_utf8(size).map_err(|_error| Self::malformed())?;
                    let chunk_size =
                        usize::from_str_radix(size_hex, 16).map_err(|_error| Self::malformed())?;
                    if chunk_size == 0 {
                        self.state = AwsChunkState::Completion;
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
                AwsChunkState::Completion => {
                    self.finish().await?;
                    self.state = AwsChunkState::Done;
                    return Ok(None);
                }
                AwsChunkState::Done => return Ok(None),
            }
        }
    }
}

/// Wraps a raw request body reader into an AWS-chunked-decoded reader.
///
/// The returned stream yields the decoded payload bytes (no framing); the
/// decoded byte count is enforced against `max_decoded_bytes`, and a declared
/// length must match before EOF is returned. Completion framing and the entire
/// source stream are consumed, including checksum trailers (not cryptographically verified).
pub fn decode_aws_chunked(
    reader: RequestBodyReader,
    max_decoded_bytes: u64,
    expected_decoded_bytes: Option<u64>,
) -> impl Stream<Item = Result<Bytes, ServerError>> {
    stream::unfold(
        AwsChunkedDecoder {
            reader,
            pending: Vec::new(),
            state: AwsChunkState::ChunkSize,
            max_decoded_bytes,
            decoded_bytes: 0,
            expected_decoded_bytes,
        },
        |mut decoder| async move {
            match decoder.next_decoded().await {
                Ok(Some(bytes)) => Some((Ok(bytes), decoder)),
                Ok(None) => None,
                Err(error) => {
                    decoder.state = AwsChunkState::Done;
                    Some((Err(error), decoder))
                }
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
        let stream = decode_aws_chunked(reader, max, None);
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
        let stream = decode_aws_chunked(reader, 1024, None);
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

    #[test]
    fn decoded_length_requires_one_unsigned_representable_value() {
        let mut headers = HeaderMap::new();
        assert!(declared_decoded_content_length(&headers).is_err());
        for invalid in ["", "-1", "+1", " 1", "1 ", "1,1", "18446744073709551616"] {
            headers.insert(
                "x-amz-decoded-content-length",
                HeaderValue::from_str(invalid).unwrap(),
            );
            assert!(
                declared_decoded_content_length(&headers).is_err(),
                "{invalid}"
            );
        }
        for valid in ["0", "3", "18446744073709551615"] {
            headers.insert(
                "x-amz-decoded-content-length",
                HeaderValue::from_str(valid).unwrap(),
            );
            assert_eq!(
                declared_decoded_content_length(&headers).unwrap(),
                valid.parse::<u64>().unwrap()
            );
        }
        headers.append(
            "x-amz-decoded-content-length",
            HeaderValue::from_static("3"),
        );
        assert!(declared_decoded_content_length(&headers).is_err());
    }

    async fn fragmented_result(wire: &[u8], expected: u64) -> Result<Vec<u8>, ServerError> {
        let reader = RequestBodyReader::from_stream(stream::iter(
            Bytes::copy_from_slice(wire)
                .into_iter()
                .map(|byte| Ok::<_, ServerError>(Bytes::from(vec![byte]))),
        ));
        let decoded = decode_aws_chunked(reader, 4096, Some(expected));
        tokio::pin!(decoded);
        let mut result = Vec::new();
        while let Some(chunk) = decoded.next().await {
            result.extend_from_slice(&chunk?);
        }
        Ok(result)
    }

    #[tokio::test]
    async fn completion_requires_framing_eof_and_exact_length() {
        for invalid in [
            b"3\r\nabc\r\n0\r\n".as_slice(),
            b"3\r\nabc\r\n0\r\n\r\ngarbage\r\n",
            b"3\r\nabc\r\n0\r\nx-unknown: value\r\n\r\n",
            b"3\r\nabc\r\n0\r\nx-amz-checksum-crc32: value\r\n",
        ] {
            assert!(fragmented_result(invalid, 3).await.is_err());
        }
        let wire = b"3\r\nabc\r\n0\r\n\r\n";
        assert!(fragmented_result(wire, 2).await.is_err());
        assert!(fragmented_result(wire, 4).await.is_err());
        assert_eq!(fragmented_result(wire, 3).await.unwrap(), b"abc");
        assert_eq!(fragmented_result(b"0\r\n\r\n", 0).await.unwrap(), b"");
    }

    #[tokio::test]
    async fn accepts_fragmented_aws_checksum_and_signature_trailers() {
        for completion in [
            "x-amz-checksum-crc32c:AAAA\r\n\r\n",
            "x-amz-checksum-crc32c:AAAA\n\r\nx-amz-trailer-signature:deadbeef\r\n\r\n",
            "\r\nx-amz-checksum-sha256:AAAA\r\n\r\n\r\n",
        ] {
            let wire =
                format!("3;chunk-signature=aa\r\nabc\r\n0;chunk-signature=bb\r\n{completion}");
            assert_eq!(fragmented_result(wire.as_bytes(), 3).await.unwrap(), b"abc");
        }
    }

    #[tokio::test]
    async fn size_line_bound_is_independent_of_source_frame_boundaries() {
        let wire = format!("0;chunk-signature={}\r\n\r\n", "a".repeat(2048));
        assert!(decode_all(wire.as_bytes().to_vec(), 4096).await.is_err());
        assert!(fragmented_result(wire.as_bytes(), 0).await.is_err());
        let valid = format!("0;{}\r\n\r\n", "a".repeat(MAX_AWS_CHUNK_LINE_BYTES - 4));
        assert_eq!(fragmented_result(valid.as_bytes(), 0).await.unwrap(), b"");
        let invalid = format!("0;{}\r\n\r\n", "a".repeat(MAX_AWS_CHUNK_LINE_BYTES - 3));
        assert!(fragmented_result(invalid.as_bytes(), 0).await.is_err());
    }

    #[tokio::test]
    async fn late_source_error_is_propagated_once_after_zero_chunk() {
        let reader = RequestBodyReader::from_stream(stream::iter(vec![
            Ok(Bytes::from_static(b"3\r\nabc\r\n0\r\n\r\n")),
            Err(ServerError::Io(IoError::new(
                ErrorKind::UnexpectedEof,
                "late framed body error",
            ))),
        ]));
        let decoded = decode_aws_chunked(reader, 4096, Some(3));
        tokio::pin!(decoded);
        assert_eq!(decoded.next().await.unwrap().unwrap(), b"abc".as_slice());
        assert!(decoded.next().await.unwrap().is_err());
        assert!(decoded.next().await.is_none());
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
