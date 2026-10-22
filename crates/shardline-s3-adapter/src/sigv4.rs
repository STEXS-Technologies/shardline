//! Cryptographic request verification, separate from the legacy token bridge.
//!
//! This verifies the request commitment only. A caller MUST verify a returned
//! payload digest against the received bytes before publishing any side effect.
//! Signed streaming requests require feeding every wire byte to the returned
//! chunk verifier and checking its finish result before publication.
use axum::http::{HeaderMap, Method, Uri};
use chrono::NaiveDateTime;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::S3Error;

mod streaming;
pub use streaming::SigV4ChunkVerifier;
use streaming::StreamingContext;
mod trailers;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// Signing material resolved by the server, never from untrusted request data.
pub struct SigV4Credential<'credential> {
    /// Public access-key identifier.
    pub access_key: &'credential str,
    /// Secret signing key; deliberately has no Debug implementation.
    pub secret_key: &'credential str,
    /// Optional session credential that must also be signed.
    pub session_token: Option<&'credential str>,
}

/// A verified request commitment, not proof that its body was received intact.
#[derive(Debug, Clone, PartialEq, Eq)]
#[must_use = "Check the received payload digest before publishing side effects"]
pub struct VerifiedSigV4 {
    /// Digest the caller must check before committing the request body.
    /// None denotes UNSIGNED-PAYLOAD or an authenticated streaming chain.
    pub payload_sha256: Option<[u8; 32]>,
    streaming: Option<StreamingContext>,
}

/// Verifies header or query authentication with a configured S3 region.
///
/// Header timestamps allow 15 minutes of clock skew. Presigned URLs expire
/// after at most seven days and allow 15 minutes of future clock skew.
/// Repeated credentials, mixed header/query authentication, malformed scope,
/// unsigned security-token headers and unsupported streaming modes fail closed.
///
/// # Errors
/// Returns AccessDenied for malformed, expired or invalid signatures.
pub fn verify_sigv4(
    method: &Method,
    uri: &Uri,
    headers: &HeaderMap,
    credential: &SigV4Credential<'_>,
    region: &str,
    now_unix_seconds: i64,
) -> Result<VerifiedSigV4, S3Error> {
    if credential.access_key.is_empty() || credential.secret_key.is_empty() || region.is_empty() {
        return Err(denied());
    }
    let query = query_pairs(uri)?;
    let presigned = query.iter().any(|(name, _)| name.starts_with("X-Amz-"));
    let authorization = singleton_header(headers, "authorization")?;
    let (scope, signed, signature, timestamp) = if presigned {
        if authorization.is_some() || unique(&query, "X-Amz-Algorithm")? != ALGORITHM {
            return Err(denied());
        }
        (
            unique(&query, "X-Amz-Credential")?,
            unique(&query, "X-Amz-SignedHeaders")?,
            unique(&query, "X-Amz-Signature")?,
            unique(&query, "X-Amz-Date")?,
        )
    } else {
        let authorization_fields = authorization
            .ok_or_else(denied)?
            .strip_prefix("AWS4-HMAC-SHA256 ")
            .ok_or_else(denied)?;
        let fields = authorization_fields
            .split(',')
            .map(|field| {
                let (name, value) = field.trim().split_once('=').ok_or_else(denied)?;
                if name.is_empty() || value.is_empty() {
                    return Err(denied());
                }
                Ok((name, value))
            })
            .collect::<Result<Vec<_>, S3Error>>()?;
        if fields.len() != 3
            || ["Credential", "SignedHeaders", "Signature"]
                .iter()
                .any(|expected| fields.iter().filter(|(name, _)| name == expected).count() != 1)
        {
            return Err(denied());
        }
        let field = |name| {
            let mut values = fields.iter().filter(|(key, _)| *key == name);
            let value = values.next().map(|(_, value)| *value).ok_or_else(denied)?;
            if values.next().is_some() {
                return Err(denied());
            }
            Ok(value)
        };
        (
            field("Credential")?,
            field("SignedHeaders")?,
            field("Signature")?,
            singleton_header(headers, "x-amz-date")?.ok_or_else(denied)?,
        )
    };
    let scope_parts = scope.split('/').collect::<Vec<_>>();
    let [access_key, date, signed_region, service, terminator] = scope_parts.as_slice() else {
        return Err(denied());
    };
    if *access_key != credential.access_key
        || *signed_region != region
        || *service != "s3"
        || *terminator != "aws4_request"
        || date.len() != 8
        || !timestamp.starts_with(date)
        || timestamp.len() != 16
    {
        return Err(denied());
    }
    let issued = NaiveDateTime::parse_from_str(timestamp, "%Y%m%dT%H%M%SZ")
        .map_err(|_invalid_time| denied())?
        .and_utc()
        .timestamp();
    if issued > now_unix_seconds.saturating_add(900) {
        return Err(denied());
    }
    let lifetime = if presigned {
        let expires = unique(&query, "X-Amz-Expires")?
            .parse::<i64>()
            .map_err(|_invalid_expiry| denied())?;
        if !(1..=604800).contains(&expires) {
            return Err(denied());
        }
        expires
    } else {
        900
    };
    if now_unix_seconds > issued.saturating_add(lifetime) {
        return Err(denied());
    }
    let names = signed.split(';').collect::<Vec<_>>();
    if !names.contains(&"host")
        || names
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left >= right))
        || names.iter().any(|name| {
            name.is_empty()
                || name.bytes().any(|byte| {
                    !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && byte != b'-'
                })
        })
        || (!presigned && !names.contains(&"x-amz-date"))
    {
        return Err(denied());
    }
    let token = if presigned {
        optional_unique(&query, "X-Amz-Security-Token")?
    } else {
        singleton_header(headers, "x-amz-security-token")?
    };
    let token_matches = match (token, credential.session_token) {
        (Some(actual), Some(expected)) => bool::from(actual.as_bytes().ct_eq(expected.as_bytes())),
        (None, None) => true,
        _ => false,
    };
    if !token_matches || (!presigned && token.is_some() && !names.contains(&"x-amz-security-token"))
    {
        return Err(denied());
    }
    for name in headers.keys() {
        if name.as_str().starts_with("x-amz-")
            && name != "x-amz-content-sha256"
            && !names.contains(&name.as_str())
        {
            return Err(denied());
        }
    }
    let payload = singleton_header(headers, "x-amz-content-sha256")?.unwrap_or(if presigned {
        "UNSIGNED-PAYLOAD"
    } else {
        ""
    });
    let unsigned_chunks = payload == "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    let trailer_mode = payload == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER" || unsigned_chunks;
    let streaming = payload == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" || trailer_mode;
    if streaming && (presigned || method != Method::PUT) {
        return Err(denied());
    }
    let payload_sha256 = if payload == "UNSIGNED-PAYLOAD" || streaming {
        None
    } else {
        Some(
            hex::decode(payload)
                .map_err(|_invalid_hash| denied())?
                .try_into()
                .map_err(|_invalid_length| denied())?,
        )
    };
    let mut canonical_headers = String::new();
    for name in names {
        let values = headers
            .get_all(name)
            .iter()
            .map(|value| {
                value
                    .to_str()
                    .map(|value| value.split_ascii_whitespace().collect::<Vec<_>>().join(" "))
                    .map_err(|_invalid_header| denied())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if values.is_empty() || (name == "host" && values.len() != 1) {
            return Err(denied());
        }
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(&values.join(","));
        canonical_headers.push('\n');
    }
    // S3 paths are not normalized: repeated slashes and dot segments identify
    // different keys. Uri already holds the encoded wire path.
    let path = if uri.path().is_empty() {
        "/"
    } else {
        uri.path()
    };
    let mut pairs = query
        .iter()
        .filter(|(name, _)| !presigned || name != "X-Amz-Signature")
        .map(|(name, value)| (encode(name), encode(value)))
        .collect::<Vec<_>>();
    pairs.sort();
    let canonical_query = pairs
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical =
        format!("{method}\n{path}\n{canonical_query}\n{canonical_headers}\n{signed}\n{payload}");
    let scope_suffix = scope
        .strip_prefix(access_key)
        .and_then(|scope| scope.strip_prefix('/'))
        .ok_or_else(denied)?;
    let to_sign = format!(
        "{ALGORITHM}\n{timestamp}\n{scope_suffix}\n{}",
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let prefixed_secret = Zeroizing::new(format!("AWS4{}", credential.secret_key));
    let date_key = mac(prefixed_secret.as_bytes(), date.as_bytes())?;
    let region_key = mac(&date_key, region.as_bytes())?;
    let service_key = mac(&region_key, b"s3")?;
    let signing_key = mac(&service_key, b"aws4_request")?;
    let signature = hex::decode(signature).map_err(|_invalid_signature| denied())?;
    let mut verifier =
        Hmac::<Sha256>::new_from_slice(&signing_key).map_err(|_invalid_key| denied())?;
    verifier.update(to_sign.as_bytes());
    verifier
        .verify_slice(&signature)
        .map_err(|_mismatch| denied())?;
    let streaming = if streaming {
        let encoding = singleton_header(headers, "content-encoding")?.ok_or_else(denied)?;
        if !encoding
            .split(',')
            .any(|encoding| encoding.trim().eq_ignore_ascii_case("aws-chunked"))
        {
            return Err(denied());
        }
        let length =
            singleton_header(headers, "x-amz-decoded-content-length")?.ok_or_else(denied)?;
        if length.is_empty() || !length.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(denied());
        }
        Some(StreamingContext {
            unsigned_chunks,
            trailers: if trailer_mode {
                trailers::parse_names(
                    singleton_header(headers, "x-amz-trailer")?.ok_or_else(denied)?,
                )?
            } else {
                if singleton_header(headers, "x-amz-trailer")?.is_some() {
                    return Err(denied());
                }
                Vec::new()
            },
            key: shardline_protocol::SecretBytes::new(signing_key.to_vec()),
            timestamp: timestamp.to_owned(),
            scope: scope_suffix.to_owned(),
            previous: signature
                .try_into()
                .map_err(|_invalid_signature| denied())?,
            length: length.parse().map_err(|_invalid_length| denied())?,
        })
    } else {
        None
    };
    Ok(VerifiedSigV4 {
        payload_sha256,
        streaming,
    })
}

impl VerifiedSigV4 {
    /// Creates a bounded verifier for the authenticated AWS chunk-signature
    /// chain. Feed every wire byte and require successful finish before commit.
    #[must_use]
    pub fn streaming_verifier(&self) -> Option<SigV4ChunkVerifier> {
        self.streaming.clone().map(SigV4ChunkVerifier::new)
    }
    /// Checks a completed body digest in constant time before publication.
    #[must_use]
    pub fn matches_payload(&self, actual_sha256: &[u8; 32]) -> bool {
        self.streaming.is_none()
            && self
                .payload_sha256
                .is_none_or(|expected| bool::from(expected.ct_eq(actual_sha256)))
    }
}

fn denied() -> S3Error {
    S3Error::access_denied()
}

fn mac(key: &[u8], value: &[u8]) -> Result<Zeroizing<Vec<u8>>, S3Error> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_invalid_key| denied())?;
    mac.update(value);
    Ok(Zeroizing::new(mac.finalize().into_bytes().to_vec()))
}

fn singleton_header<'headers>(
    headers: &'headers HeaderMap,
    name: &str,
) -> Result<Option<&'headers str>, S3Error> {
    let mut values = headers.get_all(name).iter();
    let value = values
        .next()
        .map(|value| value.to_str().map_err(|_invalid_header| denied()))
        .transpose()?;
    if values.next().is_some() {
        return Err(denied());
    }
    Ok(value)
}

fn unique<'pairs>(pairs: &'pairs [(String, String)], name: &str) -> Result<&'pairs str, S3Error> {
    optional_unique(pairs, name)?.ok_or_else(denied)
}

fn optional_unique<'pairs>(
    pairs: &'pairs [(String, String)],
    name: &str,
) -> Result<Option<&'pairs str>, S3Error> {
    let mut values = pairs.iter().filter(|(key, _)| key == name);
    let value = values.next().map(|(_, value)| value.as_str());
    if values.next().is_some() {
        return Err(denied());
    }
    Ok(value)
}

fn query_pairs(uri: &Uri) -> Result<Vec<(String, String)>, S3Error> {
    uri.query()
        .filter(|query| !query.is_empty())
        .unwrap_or("")
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let decode = |encoded: &str| {
                percent_encoding::percent_decode_str(encoded)
                    .decode_utf8()
                    .map(|decoded| decoded.into_owned())
                    .map_err(|_invalid_encoding| denied())
            };
            Ok((decode(key)?, decode(value)?))
        })
        .collect()
}

fn encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
                char::from(byte).to_string()
            } else {
                format!("%{byte:02X}")
            }
        })
        .collect()
}

#[cfg(test)]
mod tests;
