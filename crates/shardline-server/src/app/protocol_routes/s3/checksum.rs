//! Streaming validation for S3 checksum request headers.

use axum::http::HeaderMap;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
use futures_util::stream;
use sha1::Sha1;
use sha2::{Digest as _, Sha256};

use crate::{ServerError, upload_ingest::RequestBodyReader};

const CHECKSUM_HEADERS: [&str; 5] = [
    "x-amz-checksum-crc32",
    "x-amz-checksum-crc32c",
    "x-amz-checksum-crc64nvme",
    "x-amz-checksum-sha1",
    "x-amz-checksum-sha256",
];

enum DigestState {
    Crc32(CrcDigest),
    Crc32c(CrcDigest),
    Crc64Nvme(CrcDigest),
    Sha1(Sha1),
    Sha256(Sha256),
}

impl DigestState {
    fn new(header: &str) -> Self {
        match header {
            "x-amz-checksum-crc32" => Self::Crc32(CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc)),
            "x-amz-checksum-crc32c" => Self::Crc32c(CrcDigest::new(CrcAlgorithm::Crc32Iscsi)),
            "x-amz-checksum-crc64nvme" => Self::Crc64Nvme(CrcDigest::new(CrcAlgorithm::Crc64Nvme)),
            "x-amz-checksum-sha1" => Self::Sha1(Sha1::new()),
            _ => Self::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Crc32(digest) | Self::Crc32c(digest) | Self::Crc64Nvme(digest) => {
                digest.update(bytes);
            }
            Self::Sha1(digest) => digest.update(bytes),
            Self::Sha256(digest) => digest.update(bytes),
        }
    }

    fn encoded(&self) -> String {
        match self {
            Self::Crc32(digest) | Self::Crc32c(digest) => {
                let [_, _, _, _, a, b, c, d] = digest.finalize().to_be_bytes();
                STANDARD.encode([a, b, c, d])
            }
            Self::Crc64Nvme(digest) => STANDARD.encode(digest.finalize().to_be_bytes()),
            Self::Sha1(digest) => STANDARD.encode(digest.clone().finalize()),
            Self::Sha256(digest) => STANDARD.encode(digest.clone().finalize()),
        }
    }
}

pub(super) struct PayloadChecksum {
    header: &'static str,
    expected: String,
    digest: DigestState,
}

impl PayloadChecksum {
    pub(super) const fn header(&self) -> &'static str {
        self.header
    }

    pub(super) fn expected(&self) -> &str {
        &self.expected
    }

    fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    fn matches(&self) -> bool {
        self.digest.encoded() == self.expected
    }
}

/// Parses one standard S3 checksum header. Unsupported checksum negotiation is
/// rejected separately so a client cannot believe Shardline persisted a value
/// that it only validated for this request.
pub(super) fn parse(
    headers: &HeaderMap,
) -> Result<Option<PayloadChecksum>, shardline_s3_adapter::S3Error> {
    let mut supplied = None;
    for name in CHECKSUM_HEADERS {
        let mut values = headers.get_all(name).iter();
        let Some(value) = values.next() else {
            continue;
        };
        if values.next().is_some() || supplied.is_some() {
            return Err(shardline_s3_adapter::S3Error::invalid_argument(
                "Repeated or conflicting S3 checksum headers",
            ));
        }
        let encoded = value.to_str().map_err(|_invalid_header| {
            shardline_s3_adapter::S3Error::invalid_argument("Invalid S3 checksum header")
        })?;
        let decoded = STANDARD.decode(encoded).map_err(|_invalid_checksum| {
            shardline_s3_adapter::S3Error::invalid_argument("Invalid S3 checksum header")
        })?;
        let expected_len = match name {
            "x-amz-checksum-crc32" | "x-amz-checksum-crc32c" => 4,
            "x-amz-checksum-crc64nvme" => 8,
            "x-amz-checksum-sha1" => 20,
            _ => 32,
        };
        if decoded.len() != expected_len || STANDARD.encode(&decoded) != encoded {
            return Err(shardline_s3_adapter::S3Error::invalid_argument(
                "Invalid S3 checksum header",
            ));
        }
        supplied = Some(PayloadChecksum {
            header: name,
            expected: encoded.to_owned(),
            digest: DigestState::new(name),
        });
    }
    for name in headers.keys().map(axum::http::HeaderName::as_str) {
        if name.starts_with("x-amz-checksum-") && !CHECKSUM_HEADERS.contains(&name) {
            return Err(shardline_s3_adapter::S3Error::not_implemented());
        }
    }
    Ok(supplied)
}

pub(super) fn verify_body(body: RequestBodyReader, checksum: PayloadChecksum) -> RequestBodyReader {
    let body_stream = stream::try_unfold((body, checksum), |(mut body, mut checksum)| async move {
        match body.next_bytes().await? {
            Some(bytes) => {
                checksum.update(&bytes);
                Ok(Some((bytes, (body, checksum))))
            }
            None if checksum.matches() => Ok(None),
            None => Err(ServerError::RequestBodyChecksumMismatch),
        }
    });
    RequestBodyReader::from_stream(body_stream)
}

pub(super) fn reject_unpersisted_negotiation(
    headers: &HeaderMap,
) -> Result<(), shardline_s3_adapter::S3Error> {
    for name in ["x-amz-checksum-algorithm", "x-amz-checksum-type"] {
        if headers.contains_key(name) {
            return Err(shardline_s3_adapter::S3Error::not_implemented());
        }
    }
    Ok(())
}

pub(super) fn reject_unverified_headers(
    headers: &HeaderMap,
) -> Result<(), shardline_s3_adapter::S3Error> {
    if headers
        .keys()
        .any(|name| name.as_str().starts_with("x-amz-checksum-"))
    {
        return Err(shardline_s3_adapter::S3Error::not_implemented());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DigestState, PayloadChecksum, parse};
    use axum::http::{HeaderMap, HeaderValue};
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    #[test]
    fn checksum_algorithms_match_standard_vectors() {
        for (name, expected) in [
            ("x-amz-checksum-crc32", "y/Q5Jg=="),
            ("x-amz-checksum-crc32c", "4waSgw=="),
            ("x-amz-checksum-crc64nvme", "rosUhgp5mIg="),
            ("x-amz-checksum-sha1", "98O8HYCOBHMq32eZZczDTKeuNEE="),
            (
                "x-amz-checksum-sha256",
                "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=",
            ),
        ] {
            let mut digest = DigestState::new(name);
            digest.update(b"123456789");
            assert_eq!(digest.encoded(), expected, "{name}");
        }
    }

    #[test]
    fn checksum_headers_must_be_single_canonical_values() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-checksum-sha256", HeaderValue::from_static("AAAA"));
        assert!(parse(&headers).is_err());

        headers.clear();
        headers.insert(
            "x-amz-checksum-sha256",
            HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
        );
        assert!(parse(&headers).unwrap().is_some());

        headers.insert("x-amz-checksum-crc32", HeaderValue::from_static("AAAAAA=="));
        assert!(parse(&headers).is_err());
    }

    #[test]
    fn expected_checksums_are_not_mutated_while_streaming() {
        let expected = STANDARD.encode([1, 2, 3, 4]);
        let checksum = PayloadChecksum {
            header: "x-amz-checksum-crc32",
            expected: expected.clone(),
            digest: DigestState::new("x-amz-checksum-crc32"),
        };
        assert_eq!(checksum.expected(), expected);
    }
}
