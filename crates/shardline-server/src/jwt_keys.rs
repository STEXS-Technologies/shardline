//! Shared external signing-key transport and JWT signature verification.
//!
//! Providers own discovery and cache freshness; claim mapping is shared with the
//! external JWT policy module. The
//! cryptographic and bounded-document checks have one implementation.

use crate::jwt_algorithm::JwtAlgorithm;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use shardline_server_core::AuthError;
use std::{str::FromStr, sync::OnceLock};

// Bound actual streamed bytes, including responses without Content-Length.
pub(super) const MAX_AUTH_JSON_BYTES: usize = 1024 * 1024;

/// Shared task owner. The refresh future never holds this owner, so the last
/// provider clone releases the task without an ownership cycle.
#[derive(Default)]
pub(super) struct RefreshTask {
    handle: OnceLock<tokio::task::JoinHandle<()>>,
}

impl std::ops::Deref for RefreshTask {
    type Target = OnceLock<tokio::task::JoinHandle<()>>;
    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Drop for RefreshTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.get() {
            handle.abort();
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct JwksResponse {
    pub(super) keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
pub(super) struct Jwk {
    pub(super) kid: String,
    #[serde(rename = "kty")]
    pub(super) key_type: String,
    pub(super) n: Option<String>,
    pub(super) e: Option<String>,
    #[serde(rename = "x")]
    pub(super) x_coord: Option<String>,
    #[serde(rename = "y")]
    pub(super) y_coord: Option<String>,
    #[serde(rename = "use")]
    pub(super) key_use: Option<String>,
    #[serde(rename = "key_ops")]
    pub(super) key_operations: Option<Vec<String>>,
    #[serde(rename = "alg")]
    pub(super) algorithm: Option<String>,
    #[serde(rename = "crv")]
    pub(super) curve: Option<String>,
}

pub(super) async fn read_auth_json_bounded<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, String> {
    if !response.status().is_success() {
        return Err(format!(
            "authentication endpoint returned {}",
            response.status()
        ));
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_AUTH_JSON_BYTES as u64)
    {
        return Err("authentication JSON exceeds the 1 MiB document limit".to_owned());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if chunk.len() > MAX_AUTH_JSON_BYTES.saturating_sub(body.len()) {
            return Err("authentication JSON exceeds the 1 MiB document limit".to_owned());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|error| error.to_string())
}

pub(super) fn jwk_allows_verification(jwk: &Jwk, algorithm: Algorithm) -> bool {
    if !is_algorithm_compatible(&jwk.key_type, algorithm)
        || jwk.key_use.as_deref().is_some_and(|usage| usage != "sig")
        || jwk.algorithm.as_deref().is_some_and(|declared| {
            !Algorithm::from_str(declared).is_ok_and(|declared| declared == algorithm)
        })
    {
        return false;
    }
    if let Some(operations) = &jwk.key_operations {
        // Verification permission is required. Only the related sign/verify
        // combination is valid here; duplicates and unrelated uses are rejected.
        if operations.len() > 2
            || operations
                .iter()
                .filter(|operation| operation.as_str() == "verify")
                .count()
                != 1
            || operations
                .iter()
                .any(|operation| !matches!(operation.as_str(), "sign" | "verify"))
        {
            return false;
        }
    }
    match (jwk.key_type.as_str(), algorithm) {
        ("EC", Algorithm::ES256) => jwk.curve.as_deref() == Some("P-256"),
        ("EC", Algorithm::ES384) => jwk.curve.as_deref() == Some("P-384"),
        _ => true,
    }
}

fn is_algorithm_compatible(key_type: &str, algorithm: Algorithm) -> bool {
    matches!(
        (key_type, algorithm),
        (
            "RSA",
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512
        ) | ("EC", Algorithm::ES256 | Algorithm::ES384)
            | (
                "RSA",
                Algorithm::PS256 | Algorithm::PS384 | Algorithm::PS512
            )
    )
}

fn build_decoding_key(jwk: &Jwk, algorithm: Algorithm) -> Result<DecodingKey, String> {
    match algorithm {
        Algorithm::RS256
        | Algorithm::RS384
        | Algorithm::RS512
        | Algorithm::PS256
        | Algorithm::PS384
        | Algorithm::PS512 => {
            let n = jwk.n.as_ref().ok_or("RSA key missing n parameter")?;
            let e = jwk.e.as_ref().ok_or("RSA key missing e parameter")?;
            DecodingKey::from_rsa_components(n, e).map_err(|e| format!("invalid RSA key: {e}"))
        }
        Algorithm::ES256 | Algorithm::ES384 => {
            let x = jwk.x_coord.as_ref().ok_or("EC key missing x parameter")?;
            let y = jwk.y_coord.as_ref().ok_or("EC key missing y parameter")?;
            DecodingKey::from_ec_components(x, y).map_err(|e| format!("invalid EC key: {e}"))
        }
        Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512 | Algorithm::EdDSA => {
            Err(format!("unsupported algorithm: {algorithm:?}"))
        }
    }
}

fn base64_decode_url(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.decode(input)
}

/// Verify key purpose, signature, issuer and optional audience before exposing claims.
pub(super) fn verify_jwt_payload(
    header_b64: &str,
    payload_b64: &str,
    signature_b64: &str,
    keys: &[Jwk],
    issuer: &str,
    audience: Option<&str>,
) -> Result<serde_json::Value, AuthError> {
    let header_json = base64_decode_url(header_b64)
        .map_err(|e| AuthError::ProviderError(format!("invalid JWT header: {e}")))?;
    let header: serde_json::Value = serde_json::from_slice(&header_json)
        .map_err(|e| AuthError::ProviderError(format!("invalid JWT header JSON: {e}")))?;

    let kid = header
        .get("kid")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AuthError::ProviderError("missing kid in JWT header".to_owned()))?;

    let alg_str = header
        .get("alg")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AuthError::ProviderError("missing alg in JWT header".to_owned()))?;

    let alg = JwtAlgorithm::from_str(alg_str)
        .map_err(|_e| AuthError::ProviderError(format!("unsupported algorithm: {alg_str}")))?;

    // Alg-confusion guard: never accept `none` (unauthenticated) or a
    // symmetric HMAC variant when verifying against asymmetric JWKS keys.
    if !alg.is_asymmetric() {
        if alg.is_none() {
            return Err(AuthError::InvalidToken);
        }
        return Err(AuthError::ProviderError(format!(
            "unsupported or insecure algorithm: {}",
            alg.as_str()
        )));
    }

    let algorithm = Algorithm::from_str(alg_str)
        .map_err(|_e| AuthError::ProviderError(format!("unsupported algorithm: {alg_str}")))?;
    let jwk = keys
        .iter()
        .find(|k| k.kid == kid && jwk_allows_verification(k, algorithm))
        .ok_or_else(|| AuthError::ProviderError(format!("no matching key for kid {kid}")))?;

    let decoding_key = build_decoding_key(jwk, algorithm)
        .map_err(|e| AuthError::ProviderError(format!("failed to build decoding key: {e}")))?;

    let mut validation = Validation::new(algorithm);
    validation.set_issuer(&[issuer]);
    // An issuer constraint alone permits an absent `iss` claim. Require
    // its presence so every accepted token identifies the pinned issuer.
    validation.required_spec_claims.insert("iss".to_owned());
    if let Some(audience) = audience {
        validation.set_audience(&[audience]);
        // Regression (F-82): jsonwebtoken's `validate_audience` treats a
        // token with a MISSING `aud` claim as valid when an audience IS
        // configured — the `(TryParse::NotPresent, Some(_))` pair falls
        // through every match arm and returns Ok. Requiring the claim's
        // presence restores the intended boundary: a token that omits
        // `aud` entirely must not authenticate when the operator pinned
        // an audience.
        validation.required_spec_claims.insert("aud".to_owned());
    } else {
        // jsonwebtoken's `validate_aud` defaults to `true` and, when no
        // audience is configured, rejects every token that carries an
        // `aud` claim (InvalidAudience). OIDC Core / RFC 9068 make `aud`
        // required in ID/access tokens, so the default (unset audience)
        // shape would reject every real IdP token. Explicitly disable aud
        // validation to make the documented permissive default true.
        validation.validate_aud = false;
    }

    let token = format!("{header_b64}.{payload_b64}.{signature_b64}");
    let token_data = decode::<serde_json::Value>(&token, &decoding_key, &validation)
        .map_err(|e| AuthError::ProviderError(format!("JWT verification failed: {e}")))?;

    Ok(token_data.claims)
}

/// Signing-key transport requires HTTPS, with loopback HTTP for local tooling.
pub(super) fn is_secure_url(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() == "https" {
        return true;
    }
    if parsed.scheme() != "http" {
        return false;
    }
    match parsed.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
pub(super) mod test_support;
#[cfg(test)]
mod tests;
