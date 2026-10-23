//! Streaming validation for S3 checksum request headers.

use axum::http::HeaderMap;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
use futures_util::stream;
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use sha2::{Digest as _, Sha256};

use crate::{ServerError, upload_ingest::RequestBodyReader};

const MULTIPART_CONFIG_KEY: &str = "shardline-internal-s3-multipart-checksum-v1";
pub(super) const MULTIPART_MANIFEST_KEY: &str = "shardline-internal-s3-checksum-v1";

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct MultipartPartChecksum {
    pub(super) part_number: u32,
    pub(super) size_bytes: u64,
    pub(super) checksum_crc32: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub(super) struct MultipartChecksumManifest {
    pub(super) checksum_crc32: String,
    pub(super) parts: Vec<MultipartPartChecksum>,
}

pub(super) fn is_internal_metadata(name: &str) -> bool {
    name.eq_ignore_ascii_case(MULTIPART_CONFIG_KEY)
        || name.eq_ignore_ascii_case(MULTIPART_MANIFEST_KEY)
}

pub(super) fn negotiate_multipart(
    headers: &HeaderMap,
    user_metadata: &mut Vec<(String, String)>,
) -> Result<bool, shardline_s3_adapter::S3Error> {
    let algorithm = singleton_header(headers, "x-amz-checksum-algorithm")?;
    let checksum_type = singleton_header(headers, "x-amz-checksum-type")?;
    let Some(algorithm) = algorithm else {
        if checksum_type.is_some() {
            return Err(shardline_s3_adapter::S3Error::invalid_argument(
                "Checksum type requires a checksum algorithm",
            ));
        }
        return Ok(false);
    };
    if algorithm != "CRC32" {
        return Err(shardline_s3_adapter::S3Error::not_implemented());
    }
    if checksum_type.is_some_and(|value| value != "COMPOSITE") {
        return Err(shardline_s3_adapter::S3Error::not_implemented());
    }
    if headers.contains_key("x-amz-sdk-checksum-algorithm") {
        return Err(shardline_s3_adapter::S3Error::not_implemented());
    }
    user_metadata.retain(|(name, _)| !name.eq_ignore_ascii_case(MULTIPART_CONFIG_KEY));
    user_metadata.push((
        MULTIPART_CONFIG_KEY.to_owned(),
        "CRC32:COMPOSITE".to_owned(),
    ));
    Ok(true)
}

pub(super) fn has_multipart_negotiation(user_metadata: &[(String, String)]) -> bool {
    user_metadata.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case(MULTIPART_CONFIG_KEY) && value == "CRC32:COMPOSITE"
    })
}

pub(super) fn attach_multipart_manifest(
    user_metadata: &mut Vec<(String, String)>,
    parts: Vec<MultipartPartChecksum>,
) -> Result<MultipartChecksumManifest, shardline_s3_adapter::S3Error> {
    if parts.is_empty() || parts.len() > 10_000 {
        return Err(shardline_s3_adapter::S3Error::invalid_part());
    }
    let mut previous = 0;
    let mut checksum_bytes = Vec::with_capacity(parts.len().saturating_mul(4));
    for part in &parts {
        let decoded = STANDARD
            .decode(&part.checksum_crc32)
            .map_err(|_| shardline_s3_adapter::S3Error::invalid_part())?;
        if part.part_number == 0
            || part.part_number > 10_000
            || part.part_number <= previous
            || decoded.len() != 4
            || STANDARD.encode(&decoded) != part.checksum_crc32
        {
            return Err(shardline_s3_adapter::S3Error::invalid_part());
        }
        previous = part.part_number;
        checksum_bytes.extend_from_slice(&decoded);
    }
    let mut digest = DigestState::new("x-amz-checksum-crc32");
    digest.update(&checksum_bytes);
    let manifest = MultipartChecksumManifest {
        checksum_crc32: format!("{}-{}", digest.encoded(), parts.len()),
        parts,
    };
    let value = serde_json::to_string(&manifest)
        .map_err(|_error| shardline_s3_adapter::S3Error::internal())?;
    user_metadata.retain(|(name, _)| {
        !name.eq_ignore_ascii_case(MULTIPART_MANIFEST_KEY)
            && !name.eq_ignore_ascii_case(MULTIPART_CONFIG_KEY)
    });
    user_metadata.push((MULTIPART_MANIFEST_KEY.to_owned(), value));
    Ok(manifest)
}

pub(super) fn read_multipart_manifest(
    user_metadata: &[(String, String)],
    expected_count: u32,
    expected_size: u64,
) -> Result<Option<MultipartChecksumManifest>, shardline_s3_adapter::S3Error> {
    let mut found = None;
    for (name, value) in user_metadata {
        if !name.eq_ignore_ascii_case(MULTIPART_MANIFEST_KEY) {
            continue;
        }
        if found.is_some() {
            return Err(shardline_s3_adapter::S3Error::internal());
        }
        found = Some(value);
    }
    let manifest: Option<MultipartChecksumManifest> = found
        .map(|value| {
            serde_json::from_str(value).map_err(|_error| shardline_s3_adapter::S3Error::internal())
        })
        .transpose()?;
    if let Some(manifest) = &manifest {
        let mut total_size = 0_u64;
        let mut previous = 0_u32;
        let mut checksum_bytes = Vec::with_capacity(manifest.parts.len().saturating_mul(4));
        for part in &manifest.parts {
            let decoded = STANDARD
                .decode(&part.checksum_crc32)
                .map_err(|_| shardline_s3_adapter::S3Error::internal())?;
            if part.part_number == 0
                || part.part_number > 10_000
                || part.part_number <= previous
                || decoded.len() != 4
                || STANDARD.encode(&decoded) != part.checksum_crc32
            {
                return Err(shardline_s3_adapter::S3Error::internal());
            }
            previous = part.part_number;
            checksum_bytes.extend_from_slice(&decoded);
            total_size = total_size
                .checked_add(part.size_bytes)
                .ok_or_else(shardline_s3_adapter::S3Error::internal)?;
        }
        if manifest.parts.len() != usize::try_from(expected_count).unwrap_or(usize::MAX)
            || total_size != expected_size
        {
            return Err(shardline_s3_adapter::S3Error::internal());
        }
        let mut digest = DigestState::new("x-amz-checksum-crc32");
        digest.update(&checksum_bytes);
        if manifest.checksum_crc32 != format!("{}-{}", digest.encoded(), manifest.parts.len()) {
            return Err(shardline_s3_adapter::S3Error::internal());
        }
    }
    Ok(manifest)
}

fn singleton_header<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a str>, shardline_s3_adapter::S3Error> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(shardline_s3_adapter::S3Error::invalid_argument(
            "Repeated checksum negotiation header",
        ));
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| shardline_s3_adapter::S3Error::invalid_argument("Invalid checksum header"))
}

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

pub(super) fn verify_multipart_part_checksums(
    body: RequestBodyReader,
    part_sizes: Vec<u64>,
    expected: Vec<String>,
) -> Result<RequestBodyReader, shardline_s3_adapter::S3Error> {
    if part_sizes.is_empty() || part_sizes.len() != expected.len() {
        return Err(shardline_s3_adapter::S3Error::invalid_part());
    }
    let body_stream = stream::try_unfold(
        (
            body,
            part_sizes,
            expected,
            0_usize,
            0_u64,
            DigestState::new("x-amz-checksum-crc32"),
        ),
        |(mut body, part_sizes, expected, mut index, mut consumed, mut digest)| async move {
            loop {
                while index < part_sizes.len() && part_sizes[index] == 0 {
                    if digest.encoded() != expected[index] {
                        return Err(ServerError::RequestBodyChecksumMismatch);
                    }
                    index += 1;
                    consumed = 0;
                    digest = DigestState::new("x-amz-checksum-crc32");
                }
                match body.next_bytes().await? {
                    Some(bytes) => {
                        let mut offset = 0_usize;
                        while offset < bytes.len() {
                            if index >= part_sizes.len() {
                                return Err(ServerError::RequestBodyChecksumMismatch);
                            }
                            let remaining = part_sizes[index]
                                .checked_sub(consumed)
                                .ok_or(ServerError::RequestBodyChecksumMismatch)?;
                            let available = bytes.len().saturating_sub(offset);
                            let take = usize::try_from(remaining)
                                .unwrap_or(usize::MAX)
                                .min(available);
                            let end = offset.saturating_add(take);
                            digest.update(
                                bytes
                                    .get(offset..end)
                                    .ok_or(ServerError::RequestBodyChecksumMismatch)?,
                            );
                            offset = end;
                            consumed = consumed
                                .checked_add(u64::try_from(take)?)
                                .ok_or(ServerError::RequestBodyChecksumMismatch)?;
                            if consumed == part_sizes[index] {
                                if digest.encoded() != expected[index] {
                                    return Err(ServerError::RequestBodyChecksumMismatch);
                                }
                                index += 1;
                                consumed = 0;
                                digest = DigestState::new("x-amz-checksum-crc32");
                            }
                        }
                        return Ok(Some((
                            bytes,
                            (body, part_sizes, expected, index, consumed, digest),
                        )));
                    }
                    None => {
                        while index < part_sizes.len() && part_sizes[index] == 0 {
                            if digest.encoded() != expected[index] {
                                return Err(ServerError::RequestBodyChecksumMismatch);
                            }
                            index += 1;
                            consumed = 0;
                            digest = DigestState::new("x-amz-checksum-crc32");
                        }
                        if index != part_sizes.len() || consumed != 0 {
                            return Err(ServerError::RequestBodyChecksumMismatch);
                        }
                        return Ok(None);
                    }
                }
            }
        },
    );
    Ok(RequestBodyReader::from_stream(body_stream))
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
    use super::{
        DigestState, MultipartPartChecksum, PayloadChecksum, attach_multipart_manifest, parse,
        read_multipart_manifest,
    };
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

    #[test]
    fn multipart_manifest_round_trips_and_detects_tampering() {
        let mut metadata = Vec::new();
        let part_checksum = STANDARD.encode([1_u8, 2, 3, 4]);
        let manifest = attach_multipart_manifest(
            &mut metadata,
            vec![MultipartPartChecksum {
                part_number: 1,
                size_bytes: 17,
                checksum_crc32: part_checksum,
            }],
        )
        .unwrap();
        assert_eq!(
            read_multipart_manifest(&metadata, 1, 17).unwrap(),
            Some(manifest)
        );

        let mut tampered = metadata;
        let (_, value) = tampered.first_mut().unwrap();
        let mut manifest: super::MultipartChecksumManifest = serde_json::from_str(value).unwrap();
        manifest.checksum_crc32 = format!("{}-1", STANDARD.encode([0_u8, 0, 0, 0]));
        *value = serde_json::to_string(&manifest).unwrap();
        assert!(read_multipart_manifest(&tampered, 1, 17).is_err());
    }
}
