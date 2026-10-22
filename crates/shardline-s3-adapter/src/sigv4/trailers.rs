//! Bounded declared-checksum verification and optional AWS trailer authentication.
use super::{S3Error, denied};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Algorithm {
    Crc32,
    Crc32c,
    Crc64nvme,
    Sha1,
    Sha256,
}

impl Algorithm {
    const fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32c => "x-amz-checksum-crc32c",
            Self::Crc64nvme => "x-amz-checksum-crc64nvme",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
    }
}

pub(super) fn parse_names(value: &str) -> Result<Vec<Algorithm>, S3Error> {
    if value.len() > 256 {
        return Err(denied());
    }
    let mut result = Vec::new();
    for name in value.split(',') {
        let algorithm = match name.trim() {
            "x-amz-checksum-crc32" => Algorithm::Crc32,
            "x-amz-checksum-crc32c" => Algorithm::Crc32c,
            "x-amz-checksum-crc64nvme" => Algorithm::Crc64nvme,
            "x-amz-checksum-sha1" => Algorithm::Sha1,
            "x-amz-checksum-sha256" => Algorithm::Sha256,
            _ => return Err(denied()),
        };
        if result.contains(&algorithm) {
            return Err(denied());
        }
        result.push(algorithm);
    }
    result.sort_by_key(|algorithm| algorithm.name());
    Ok(result)
}

enum Checksum {
    Crc(crc_fast::Digest, bool),
    Sha1(sha1::Sha1),
    Sha256(Sha256),
}

impl Checksum {
    fn new(algorithm: Algorithm) -> Self {
        use crc_fast::CrcAlgorithm;
        match algorithm {
            Algorithm::Crc32 => Self::Crc(crc_fast::Digest::new(CrcAlgorithm::Crc32IsoHdlc), false),
            Algorithm::Crc32c => Self::Crc(crc_fast::Digest::new(CrcAlgorithm::Crc32Iscsi), false),
            Algorithm::Crc64nvme => Self::Crc(crc_fast::Digest::new(CrcAlgorithm::Crc64Nvme), true),
            Algorithm::Sha1 => Self::Sha1(sha1::Sha1::new()),
            Algorithm::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Crc(digest, _) => digest.update(bytes),
            Self::Sha1(digest) => digest.update(bytes),
            Self::Sha256(digest) => digest.update(bytes),
        }
    }

    fn encoded(&self) -> String {
        match self {
            Self::Crc(digest, wide) => {
                let bytes = digest.finalize().to_be_bytes();
                if *wide {
                    STANDARD.encode(bytes)
                } else {
                    let [_, _, _, _, a, b, c, d] = bytes;
                    STANDARD.encode([a, b, c, d])
                }
            }
            Self::Sha1(digest) => STANDARD.encode(digest.clone().finalize()),
            Self::Sha256(digest) => STANDARD.encode(digest.clone().finalize()),
        }
    }
}

#[cfg(test)]
mod tests;

pub(super) struct TrailerVerifier {
    checksums: Vec<(Algorithm, Checksum, Option<String>)>,
    signed: bool,
    leading_empty: bool,
    signature_required: bool,
}

impl TrailerVerifier {
    pub(super) fn new(algorithms: &[Algorithm], signature_required: bool) -> Self {
        Self {
            checksums: algorithms
                .iter()
                .map(|algorithm| (*algorithm, Checksum::new(*algorithm), None))
                .collect(),
            signed: false,
            leading_empty: false,
            signature_required,
        }
    }

    pub(super) fn update(&mut self, bytes: &[u8]) {
        for (_, checksum, _) in &mut self.checksums {
            checksum.update(bytes);
        }
    }

    pub(super) fn accept_line(
        &mut self,
        line: &[u8],
        key: &[u8],
        timestamp: &str,
        scope: &str,
        previous: &[u8; 32],
    ) -> Result<bool, S3Error> {
        if line.is_empty() {
            if self.signed
                || (!self.signature_required
                    && self.checksums.iter().all(|(_, _, value)| value.is_some()))
            {
                return Ok(true);
            }
            if !self.leading_empty
                && (self.checksums.iter().all(|(_, _, value)| value.is_none())
                    || (self.signature_required
                        && self.checksums.iter().all(|(_, _, value)| value.is_some())))
            {
                self.leading_empty = true;
                return Ok(false);
            }
            return Err(denied());
        }
        if self.signed {
            return Err(denied());
        }
        let line = std::str::from_utf8(line.strip_suffix(b"\n").unwrap_or(line))
            .map_err(|_invalid_utf8| denied())?;
        let (name, value) = line.split_once(':').ok_or_else(denied)?;
        let value = value.trim();
        if name == "x-amz-trailer-signature" {
            if !self.signature_required || value.len() != 64 {
                return Err(denied());
            }
            let mut canonical = String::new();
            for (algorithm, _, actual) in &self.checksums {
                canonical.push_str(algorithm.name());
                canonical.push(':');
                canonical.push_str(actual.as_deref().ok_or_else(denied)?);
                canonical.push('\n');
            }
            let to_sign = format!(
                "AWS4-HMAC-SHA256-TRAILER\n{timestamp}\n{scope}\n{}\n{}",
                hex::encode(previous),
                hex::encode(Sha256::digest(canonical.as_bytes()))
            );
            let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_invalid_key| denied())?;
            mac.update(to_sign.as_bytes());
            mac.verify_slice(&hex::decode(value).map_err(|_invalid_hex| denied())?)
                .map_err(|_invalid_signature| denied())?;
            self.signed = true;
            return Ok(false);
        }
        let (_, checksum, stored) = self
            .checksums
            .iter_mut()
            .find(|(algorithm, _, _)| algorithm.name() == name)
            .ok_or_else(denied)?;
        if stored.is_some() {
            return Err(denied());
        }
        if checksum.encoded() != value {
            return Err(S3Error {
                code: "BadDigest",
                message: "The supplied checksum did not match the received payload.".to_owned(),
                status: axum::http::StatusCode::BAD_REQUEST,
            });
        }
        *stored = Some(value.to_owned());
        Ok(false)
    }
}
