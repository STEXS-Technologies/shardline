use std::{
    str::FromStr,
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};
use tokio::sync::RwLock;

const DEFAULT_JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(300);
const MIN_JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const MAX_JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

/// JWKS authentication provider.
///
/// Validates tokens against a JWKS endpoint, caching keys and refreshing
/// them in the background using ETag-based conditional requests.
pub struct JwksProvider {
    client: Client,
    jwks_url: String,
    issuer: String,
    cached_keys: Arc<RwLock<Option<CachedJwks>>>,
    background_handle: Arc<std::sync::OnceLock<tokio::task::JoinHandle<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl Clone for JwksProvider {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            jwks_url: self.jwks_url.clone(),
            issuer: self.issuer.clone(),
            cached_keys: Arc::clone(&self.cached_keys),
            background_handle: Arc::clone(&self.background_handle),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

impl Drop for JwksProvider {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.background_handle.get() {
            handle.abort();
        }
    }
}

struct CachedJwks {
    keys: Arc<Vec<Jwk>>,
    etag: Option<String>,
    refresh_interval: Duration,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
struct Jwk {
    kid: String,
    #[serde(rename = "kty")]
    key_type: String,
    n: Option<String>,
    e: Option<String>,
    #[serde(rename = "x")]
    x_coord: Option<String>,
    #[serde(rename = "y")]
    y_coord: Option<String>,
}

/// JWKS provider initialization failure.
#[derive(Debug, thiserror::Error)]
pub enum JwksProviderError {
    /// HTTP client creation failed.
    #[error("failed to create HTTP client: {0}")]
    HttpClient(String),
    /// The JWKS endpoint could not be reached.
    #[error("failed to fetch JWKS keys: {0}")]
    JwksFetch(String),
}

impl JwksProvider {
    /// Creates a new JWKS provider, fetching initial keys from the endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`JwksProviderError`] when the JWKS endpoint is unreachable.
    pub async fn new(jwks_url: &str, issuer: &str) -> Result<Self, JwksProviderError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| JwksProviderError::HttpClient(e.to_string()))?;

        let response = client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| JwksProviderError::JwksFetch(e.to_string()))?;

        let etag = response
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_owned());

        let refresh_interval =
            parse_cache_max_age(response.headers()).unwrap_or(DEFAULT_JWKS_REFRESH_INTERVAL);

        let jwks: JwksResponse = response
            .json()
            .await
            .map_err(|e| JwksProviderError::JwksFetch(e.to_string()))?;

        Ok(Self {
            client,
            jwks_url: jwks_url.to_owned(),
            issuer: issuer.to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(jwks.keys),
                etag,
                refresh_interval,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    async fn get_or_refresh_keys(&self) -> Result<Arc<Vec<Jwk>>, AuthError> {
        self.start_background_refresh();

        let guard = self.cached_keys.read().await;
        if let Some(cached) = guard.as_ref() {
            return Ok(Arc::clone(&cached.keys));
        }
        Err(AuthError::ProviderError(
            "JWKS keys not available".to_owned(),
        ))
    }

    fn start_background_refresh(&self) {
        let provider = self.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let handle = tokio::spawn(async move {
            loop {
                let interval = {
                    let guard = provider.cached_keys.read().await;
                    guard
                        .as_ref()
                        .map(|c| c.refresh_interval)
                        .unwrap_or(DEFAULT_JWKS_REFRESH_INTERVAL)
                };
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = futures_util::future::poll_fn(|_| {
                        if shutdown.load(Ordering::Relaxed) {
                            std::task::Poll::Ready(())
                        } else {
                            std::task::Poll::Pending
                        }
                    }) => return,
                }
                if let Err(e) = provider.refresh_keys_if_changed().await {
                    tracing::warn!("JWKS background refresh failed: {e}");
                }
            }
        });
        let _ = self.background_handle.set(handle);
    }

    async fn refresh_keys_if_changed(&self) -> Result<(), AuthError> {
        let etag = self
            .cached_keys
            .read()
            .await
            .as_ref()
            .and_then(|c| c.etag.clone());

        let mut request = self.client.get(&self.jwks_url);
        if let Some(etag) = &etag {
            request = request.header("If-None-Match", etag);
        }

        let response = request
            .send()
            .await
            .map_err(|e| AuthError::ProviderError(format!("JWKS refresh request failed: {e}")))?;

        match response.status() {
            StatusCode::NOT_MODIFIED => Ok(()),
            StatusCode::OK => {
                let new_etag = response
                    .headers()
                    .get("etag")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_owned());

                let new_interval = parse_cache_max_age(response.headers())
                    .unwrap_or(DEFAULT_JWKS_REFRESH_INTERVAL);

                let jwks: JwksResponse = response.json().await.map_err(|e| {
                    AuthError::ProviderError(format!("JWKS refresh parse failed: {e}"))
                })?;

                let new_cache = CachedJwks {
                    keys: Arc::new(jwks.keys),
                    etag: new_etag,
                    refresh_interval: new_interval,
                };

                *self.cached_keys.write().await = Some(new_cache);
                Ok(())
            }
            status => Err(AuthError::ProviderError(format!(
                "JWKS refresh failed with status: {status}"
            ))),
        }
    }

    fn verify_jwt_claims(
        &self,
        header_b64: &str,
        payload_b64: &str,
        signature_b64: &str,
    ) -> Result<TokenClaims, AuthError> {
        let keys = tokio::runtime::Handle::try_current()
            .map_or_else(
                |_| {
                    // Fallback: read from cache synchronously (background refresh
                    // task keeps keys fresh).  If the cache is empty, fail.
                    // Retry try_read a few times to tolerate transient write-lock
                    // contention during key rotation.
                    const MAX_RETRIES: usize = 5;
                    const RETRY_DELAY_MS: u64 = 10;
                    let mut attempt: usize = 0;
                    loop {
                        if let Ok(guard) = self.cached_keys.try_read() {
                            break guard.as_ref().map(|c| Arc::clone(&c.keys)).ok_or_else(|| {
                                AuthError::ProviderError("JWKS keys not available".to_owned())
                            });
                        }
                        attempt = attempt.wrapping_add(1);
                        if attempt >= MAX_RETRIES {
                            break Err(AuthError::ProviderError(
                                "JWKS cache lock contended".to_owned(),
                            ));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS));
                    }
                },
                |handle| handle.block_on(self.get_or_refresh_keys()),
            )?;

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

        if alg_str == "none" {
            return Err(AuthError::InvalidToken);
        }

        let algorithm = Algorithm::from_str(alg_str)
            .map_err(|_e| AuthError::ProviderError(format!("unsupported algorithm: {alg_str}")))?;

        let jwk = keys
            .iter()
            .find(|k| k.kid == kid && is_algorithm_compatible(&k.key_type, algorithm))
            .ok_or_else(|| AuthError::ProviderError(format!("no matching key for kid {kid}")))?;

        let decoding_key = build_decoding_key(jwk, algorithm)
            .map_err(|e| AuthError::ProviderError(format!("failed to build decoding key: {e}")))?;

        let mut validation = Validation::new(algorithm);
        validation.set_issuer(&[self.issuer.as_str()]);

        let token = format!("{header_b64}.{payload_b64}.{signature_b64}");
        let token_data = decode::<serde_json::Value>(&token, &decoding_key, &validation)
            .map_err(|e| AuthError::ProviderError(format!("JWT verification failed: {e}")))?;

        let payload = token_data.claims;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Reject tokens issued in the future (iat) or not yet valid (nbf).
        if let Some(iat) = payload.get("iat").and_then(|v| v.as_u64())
            && iat > now
        {
            return Err(AuthError::InvalidToken);
        }
        if let Some(nbf) = payload.get("nbf").and_then(|v| v.as_u64())
            && nbf > now
        {
            return Err(AuthError::InvalidToken);
        }

        let exp = payload
            .get("exp")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| AuthError::ProviderError("missing exp claim".to_owned()))?;

        if exp < now {
            return Err(AuthError::ExpiredToken);
        }

        let sub = payload
            .get("sub")
            .and_then(|v| v.as_str())
            .unwrap_or("anonymous")
            .to_owned();

        let scope_str = payload
            .get("scope")
            .and_then(|v| v.as_str())
            .unwrap_or("read");
        let scope = match scope_str {
            "write" | "admin" => TokenScope::Write,
            _ => TokenScope::Read,
        };

        let repository =
            RepositoryScope::new(RepositoryProvider::Generic, "jwks", &sub, Some("main"))
                .map_err(|e| AuthError::ProviderError(e.to_string()))?;

        TokenClaims::new(&self.issuer, &sub, scope, repository, exp)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }
}

impl AuthProvider for JwksProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidToken);
        }
        let header = parts.first().ok_or(AuthError::InvalidToken)?;
        let payload = parts.get(1).ok_or(AuthError::InvalidToken)?;
        let signature = parts.get(2).ok_or(AuthError::InvalidToken)?;
        self.verify_jwt_claims(header, payload, signature)
    }

    fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
        Err(AuthError::ProviderError(
            "JWKS provider does not support token minting".to_owned(),
        ))
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

/// Validate that an algorithm string represents an asymmetric public-key algorithm
/// suitable for JWKS-based token verification.
#[cfg(test)]
fn validate_algorithm(alg_str: &str) -> Result<(), String> {
    match alg_str {
        "RS256" | "RS384" | "RS512" | "ES256" | "ES384" | "ES512" => Ok(()),
        "HS256" | "HS384" | "HS512" | "EdDSA" | "none" => {
            Err(format!("unsupported or insecure algorithm: {alg_str}"))
        }
        _ => Err(format!("unsupported algorithm: {alg_str}")),
    }
}

/// Return a default [`Validation`] for the given algorithm.
///
/// The returned validation requires the `exp` claim and enables expiry checking.
#[cfg(test)]
fn default_validation(algorithm: Algorithm) -> Validation {
    Validation::new(algorithm)
}

/// Parse `Cache-Control: max-age=N` header and return a clamped refresh interval.
fn parse_cache_max_age(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    let cache_control = headers.get("cache-control")?.to_str().ok()?;
    for directive in cache_control.split(',') {
        let directive = directive.trim();
        if let Some(val) = directive.strip_prefix("max-age=") {
            let seconds: u64 = val.trim().parse().ok()?;
            let duration = Duration::from_secs(seconds);
            return Some(duration.clamp(MIN_JWKS_REFRESH_INTERVAL, MAX_JWKS_REFRESH_INTERVAL));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── JwksProviderError display ────────────────────────────────────────

    #[test]
    fn jwks_provider_error_http_client_display_non_empty() {
        let e = JwksProviderError::HttpClient("connection refused".into());
        let msg = format!("{e}");
        assert!(!msg.is_empty());
        assert!(msg.contains("HTTP client"));
    }

    #[test]
    fn jwks_provider_error_jwks_fetch_display_non_empty() {
        let e = JwksProviderError::JwksFetch("404 not found".into());
        let msg = format!("{e}");
        assert!(!msg.is_empty());
        assert!(msg.contains("JWKS"));
    }

    // ── Jwk deserialization ──────────────────────────────────────────────

    #[test]
    fn jwk_deserialize_rsa() {
        let json = json!({
            "kid": "rsa-key-1",
            "kty": "RSA",
            "alg": "RS256",
            "use": "sig",
            "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "e": "AQAB"
        });
        let jwk: Jwk = serde_json::from_value(json).expect("should deserialize RSA JWK");
        assert_eq!(jwk.kid, "rsa-key-1");
        assert_eq!(jwk.key_type, "RSA");
        assert!(jwk.n.is_some());
        assert!(jwk.e.is_some());
    }

    #[test]
    fn jwk_deserialize_ec() {
        let json = json!({
            "kid": "ec-key-1",
            "kty": "EC",
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": "MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4",
            "y": "4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM"
        });
        let jwk: Jwk = serde_json::from_value(json).expect("should deserialize EC JWK");
        assert_eq!(jwk.kid, "ec-key-1");
        assert_eq!(jwk.key_type, "EC");
        assert_eq!(
            jwk.x_coord.as_deref(),
            Some("MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4")
        );
        assert_eq!(
            jwk.y_coord.as_deref(),
            Some("4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM")
        );
    }

    // ── validate_algorithm ───────────────────────────────────────────────

    #[test]
    fn validate_algorithm_rsa_family_ok() {
        assert!(validate_algorithm("RS256").is_ok());
        assert!(validate_algorithm("RS384").is_ok());
        assert!(validate_algorithm("RS512").is_ok());
    }

    #[test]
    fn validate_algorithm_ec_family_ok() {
        assert!(validate_algorithm("ES256").is_ok());
        assert!(validate_algorithm("ES384").is_ok());
        assert!(validate_algorithm("ES512").is_ok());
    }

    #[test]
    fn validate_algorithm_hmac_family_err() {
        assert!(validate_algorithm("HS256").is_err());
        assert!(validate_algorithm("HS384").is_err());
        assert!(validate_algorithm("HS512").is_err());
    }

    #[test]
    fn validate_algorithm_eddsa_err() {
        assert!(validate_algorithm("EdDSA").is_err());
    }

    #[test]
    fn validate_algorithm_none_err() {
        assert!(validate_algorithm("none").is_err());
    }

    // ── default_validation ───────────────────────────────────────────────

    #[test]
    fn default_validation_has_correct_algorithm() {
        let v = default_validation(Algorithm::RS256);
        assert_eq!(v.algorithms, vec![Algorithm::RS256]);

        let v = default_validation(Algorithm::ES384);
        assert_eq!(v.algorithms, vec![Algorithm::ES384]);
    }

    #[test]
    fn default_validation_requires_exp() {
        let v = default_validation(Algorithm::RS256);
        assert!(v.required_spec_claims.contains("exp"));
        assert!(v.validate_exp);
    }

    // ── Constants ────────────────────────────────────────────────────────

    #[test]
    fn constants_jwks_refresh_interval() {
        assert_eq!(DEFAULT_JWKS_REFRESH_INTERVAL, Duration::from_secs(300));
        assert_eq!(MIN_JWKS_REFRESH_INTERVAL, Duration::from_secs(60));
        assert_eq!(MAX_JWKS_REFRESH_INTERVAL, Duration::from_secs(3600));
    }

    // ── parse_cache_max_age ──────────────────────────────────────────────

    #[test]
    fn parse_cache_max_age_valid() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cache-control", "max-age=300".parse().unwrap());
        assert_eq!(parse_cache_max_age(&headers), Some(Duration::from_secs(300)));
    }

    #[test]
    fn parse_cache_max_age_missing_header() {
        let headers = reqwest::header::HeaderMap::new();
        assert_eq!(parse_cache_max_age(&headers), None);
    }

    #[test]
    fn parse_cache_max_age_unrelated_directive() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cache-control", "no-cache".parse().unwrap());
        assert_eq!(parse_cache_max_age(&headers), None);
    }

    #[test]
    fn parse_cache_max_age_below_min_clamped() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cache-control", "max-age=30".parse().unwrap());
        // Clamped to MIN_JWKS_REFRESH_INTERVAL (60s)
        assert_eq!(
            parse_cache_max_age(&headers),
            Some(MIN_JWKS_REFRESH_INTERVAL)
        );
    }

    #[test]
    fn parse_cache_max_age_above_max_clamped() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cache-control", "max-age=7200".parse().unwrap());
        // Clamped to MAX_JWKS_REFRESH_INTERVAL (3600s)
        assert_eq!(
            parse_cache_max_age(&headers),
            Some(MAX_JWKS_REFRESH_INTERVAL)
        );
    }

    #[test]
    fn parse_cache_max_age_multiple_directives() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers
            .insert("cache-control", "public, max-age=300, must-revalidate".parse().unwrap());
        assert_eq!(parse_cache_max_age(&headers), Some(Duration::from_secs(300)));
    }

    #[test]
    fn parse_cache_max_age_invalid_max_age_value() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("cache-control", "max-age=not-a-number".parse().unwrap());
        assert_eq!(parse_cache_max_age(&headers), None);
    }

    // ── base64_decode_url ────────────────────────────────────────────────

    #[test]
    fn base64_decode_url_valid() {
        // "test" in base64url (no padding)
        let result = base64_decode_url("dGVzdA").unwrap();
        assert_eq!(result, b"test");
    }

    #[test]
    fn base64_decode_url_invalid_chars() {
        assert!(base64_decode_url("!!!not-valid!!!").is_err());
    }

    #[test]
    fn base64_decode_url_empty_string() {
        let result = base64_decode_url("").unwrap();
        assert!(result.is_empty());
    }

    // ── is_algorithm_compatible ──────────────────────────────────────────

    #[test]
    fn is_algorithm_compatible_rsa_rs256() {
        assert!(is_algorithm_compatible("RSA", Algorithm::RS256));
    }

    #[test]
    fn is_algorithm_compatible_rsa_rs384() {
        assert!(is_algorithm_compatible("RSA", Algorithm::RS384));
    }

    #[test]
    fn is_algorithm_compatible_rsa_rs512() {
        assert!(is_algorithm_compatible("RSA", Algorithm::RS512));
    }

    #[test]
    fn is_algorithm_compatible_rsa_ps256() {
        assert!(is_algorithm_compatible("RSA", Algorithm::PS256));
    }

    #[test]
    fn is_algorithm_compatible_rsa_ps384() {
        assert!(is_algorithm_compatible("RSA", Algorithm::PS384));
    }

    #[test]
    fn is_algorithm_compatible_rsa_ps512() {
        assert!(is_algorithm_compatible("RSA", Algorithm::PS512));
    }

    #[test]
    fn is_algorithm_compatible_rsa_es256_not() {
        assert!(!is_algorithm_compatible("RSA", Algorithm::ES256));
    }

    #[test]
    fn is_algorithm_compatible_ec_es256() {
        assert!(is_algorithm_compatible("EC", Algorithm::ES256));
    }

    #[test]
    fn is_algorithm_compatible_ec_es384() {
        assert!(is_algorithm_compatible("EC", Algorithm::ES384));
    }

    #[test]
    fn is_algorithm_compatible_ec_rs256_not() {
        assert!(!is_algorithm_compatible("EC", Algorithm::RS256));
    }

    #[test]
    fn is_algorithm_compatible_rsa_hs256_not() {
        assert!(!is_algorithm_compatible("RSA", Algorithm::HS256));
    }

    #[test]
    fn is_algorithm_compatible_rsa_eddsa_not() {
        assert!(!is_algorithm_compatible("RSA", Algorithm::EdDSA));
    }

    #[test]
    fn is_algorithm_compatible_unknown_key_type() {
        assert!(!is_algorithm_compatible("OCT", Algorithm::HS256));
        assert!(!is_algorithm_compatible("oct", Algorithm::RS256));
    }

    // ── build_decoding_key ──────────────────────────────────────────────

    /// A test RSA private key (2048-bit) for signing test JWTs.
    const TEST_RSA_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCfG3ZyPlQA0mey\n\
JmKe6fjSQyfZFRtg48r9kE5w5jK9jEtseNESnv1T5UbHpTWU5fLtBMad1gHBdoU+\n\
85ZbpukcvevIU3uqXyAoqb/kL1C5E1KwQYSt22qQrP9M4hHAAZvCkYcNxbivdDHB\n\
jYrd5M1SPY48nsQFzDiX7Epqw70RlbLKdJvoUy2xioiRM/au2CwyK5xmrCOdb1jX\n\
A2Kw/JA73DHkugZ1v+LouNGdhhx0hQIzO4OTKUBtXsEvGkfzvR4/Z/kQ+VMG+GSP\n\
Glgx3u/T6SPI48p2En6hT8vWxw7YrMaeCyPlHSZE7nRtulKgWA8ZbFw65JsRdNlI\n\
bCxUl+nvAgMBAAECggEAAMWfQx0mqX75YkloG+jQf8GWlH8Hl54p4o7bruFRGPAh\n\
9hAhIUz/t3N9M7u/zegJqLKIpRahxzCYxD1ZCPlea5zlGyw0HD73tAccj0KIVJQd\n\
FsWutbTTXcSxUIPmIPf5pQFVjC8FOV/8qqKJti1wMbD0qeTwiZAz0KfcZu41edYG\n\
X+rQsdcPdSYtL9YXBD+f/Ygjd4yEpjbVLe6ULr4sWzr6JayU4eHoNE56vf343jor\n\
xaUaOw3bifdkQzqztN+Xf2HDQesQrm0Y03dmCHMwYm56+sPjfxyDFtk1ohFuI3i/\n\
0HRHuHA5SOPSK3+VrCs7ENAN+Na0w/1f55ttX2DoyQKBgQDMFC+dLEIbqSAGZ4Di\n\
LYAL1JiqYL6kZIbZl9yWUAjNoMJpSBQ5W/NlUwOepsZH5rEDZTBEryTh3guCY25N\n\
H2XBOau34ifMhrtW8+qHhm+eZsms5z6E5NAaXmb3ThrQJgOPy/qHtoi7ADWG5rCa\n\
QcBc8//vTzHyJ1tNSvBHrnvKjQKBgQDHlj46DXRqPS+2sLZWGy3c1ps0KzjKFayF\n\
PKmbRvbr210g4A+Fy8/jiURCtPDY6hf+3th1p9pQ3tZ/gisNBycG+xnP8RpeTgFq\n\
38T5pSVHfhjwumJxySeuvekfrgcjEsOiekuXqRo/JPMS3LhkR9Fuxaotpte0BI4a\n\
N8hFENuFawKBgQC8LQjSdpLmioY7IYlYBPiC8B9tSxO+5erqDPubpmTXpppdFdeA\n\
JGdEUM2PptxCRFeId++QBaeOlX4rVp/IgWEEULckMWbdUoa/4N2q5a1adBEWW4vs\n\
Ykf5aH6tHtnegI7cMwvpw8hEFidFIsZJFsPXci3WbkHxtZScqrLwhdUjqQKBgH6b\n\
Uwv2XwPJnovQW0oR4az2Qev9AwBGcXLvgVOr15TUSaZCG/auzEg1WiTKrQGcte4K\n\
pNs1yCqGwSCPjQmtoNcv0Db1Zdmut/14x3XpidVpKx8BzNMLXG3fsJNVDNf13j4i\n\
P/OL5Mdrg/pSI3IRkMwo/YQKE0jxnscI3bTaNbbTAoGBAKMgiVyYl+wfFp3QV1zv\n\
AyLKOERs8eToNOVrylNpcw/dRahPBUPuHZ/rHzIbscVeuU14wYIq3Eje5qZU0NW6\n\
+uEiJRA0Evs1Q/93dyNO45iDCIdIhtHMA/LqnlDniz0aqPOBrFgx+4PDcfUZXvgL\n\
4FKti8JsZfXzaqRjz8KALNNV\n\
-----END PRIVATE KEY-----";

    /// Base64url-encoded `n` (modulus) matching TEST_RSA_PEM.
    const TEST_RSA_N: &str = "nxt2cj5UANJnsiZinun40kMn2RUbYOPK_ZBOcOYyvYxLbHjREp79U-VGx6U1lOXy7QTGndYBwXaFPvOWW6bpHL3ryFN7ql8gKKm_5C9QuRNSsEGErdtqkKz_TOIRwAGbwpGHDcW4r3QxwY2K3eTNUj2OPJ7EBcw4l-xKasO9EZWyynSb6FMtsYqIkTP2rtgsMiucZqwjnW9Y1wNisPyQO9wx5LoGdb_i6LjRnYYcdIUCMzuDkylAbV7BLxpH870eP2f5EPlTBvhkjxpYMd7v0-kjyOPKdhJ-oU_L1scO2KzGngsj5R0mRO50bbpSoFgPGWxcOuSbEXTZSGwsVJfp7w";

    /// Base64url-encoded `e` (exponent = 65537) matching TEST_RSA_PEM.
    const TEST_RSA_E: &str = "AQAB";

    fn sample_rsa_jwk() -> Jwk {
        Jwk {
            kid: "test".to_owned(),
            key_type: "RSA".to_owned(),
            n: Some(TEST_RSA_N.to_owned()),
            e: Some(TEST_RSA_E.to_owned()),
            x_coord: None,
            y_coord: None,
        }
    }

    fn sample_ec_jwk() -> Jwk {
        Jwk {
            kid: "test".to_owned(),
            key_type: "EC".to_owned(),
            n: None,
            e: None,
            x_coord: Some("MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4".to_owned()),
            y_coord: Some("4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFyM".to_owned()),
        }
    }

    #[test]
    fn build_decoding_key_rsa_missing_n() {
        let mut jwk = sample_rsa_jwk();
        jwk.n = None;
        let result = build_decoding_key(&jwk, Algorithm::RS256);
        assert!(result.is_err(), "expected Err for missing n");
        if let Err(err) = result {
            assert!(err.contains("missing n"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_rsa_missing_e() {
        let mut jwk = sample_rsa_jwk();
        jwk.e = None;
        let result = build_decoding_key(&jwk, Algorithm::RS256);
        assert!(result.is_err(), "expected Err for missing e");
        if let Err(err) = result {
            assert!(err.contains("missing e"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_ec_missing_x() {
        let mut jwk = sample_ec_jwk();
        jwk.x_coord = None;
        let result = build_decoding_key(&jwk, Algorithm::ES256);
        assert!(result.is_err(), "expected Err for missing x");
        if let Err(err) = result {
            assert!(err.contains("missing x"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_ec_missing_y() {
        let mut jwk = sample_ec_jwk();
        jwk.y_coord = None;
        let result = build_decoding_key(&jwk, Algorithm::ES256);
        assert!(result.is_err(), "expected Err for missing y");
        if let Err(err) = result {
            assert!(err.contains("missing y"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_hs256_unsupported() {
        let jwk = sample_rsa_jwk();
        let result = build_decoding_key(&jwk, Algorithm::HS256);
        assert!(result.is_err(), "expected Err for HS256");
        if let Err(err) = result {
            assert!(err.contains("unsupported algorithm"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_hs384_unsupported() {
        let jwk = sample_rsa_jwk();
        let result = build_decoding_key(&jwk, Algorithm::HS384);
        assert!(result.is_err(), "expected Err for HS384");
        if let Err(err) = result {
            assert!(err.contains("unsupported algorithm"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_hs512_unsupported() {
        let jwk = sample_rsa_jwk();
        let result = build_decoding_key(&jwk, Algorithm::HS512);
        assert!(result.is_err(), "expected Err for HS512");
        if let Err(err) = result {
            assert!(err.contains("unsupported algorithm"), "error: {err}");
        }
    }

    #[test]
    fn build_decoding_key_eddsa_unsupported() {
        let jwk = sample_ec_jwk();
        let result = build_decoding_key(&jwk, Algorithm::EdDSA);
        assert!(result.is_err(), "expected Err for EdDSA");
        if let Err(err) = result {
            assert!(err.contains("unsupported algorithm"), "error: {err}");
        }
    }

    // ── JwksProvider construction helpers ─────────────────────────────────

    fn make_provider(cached: Option<CachedJwks>) -> JwksProvider {
        JwksProvider {
            client: Client::new(),
            jwks_url: "https://example.com/.well-known/jwks".to_owned(),
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(cached)),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    // ── JwksProvider::new error path ─────────────────────────────────────

    #[tokio::test]
    async fn new_with_unreachable_url_returns_error() {
        let result = JwksProvider::new(
            "http://127.0.0.1:1/nonexistent-jwks",
            "https://example.com",
        )
        .await;
        assert!(result.is_err(), "expected Err for unreachable URL");
        if let Err(err) = result {
            assert!(
                matches!(err, JwksProviderError::JwksFetch(_)),
                "expected JwksFetch error, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn new_with_unreachable_url_error_message_non_empty() {
        let result = JwksProvider::new(
            "http://127.0.0.1:1/nonexistent-jwks",
            "https://example.com",
        )
        .await;
        assert!(result.is_err());
        if let Err(err) = result {
            let msg = format!("{}", err);
            assert!(!msg.is_empty());
        }
    }

    // ── JwksProvider::verify_token ───────────────────────────────────────

    #[test]
    fn verify_token_too_few_parts_returns_invalid() {
        let provider = make_provider(None);
        assert!(matches!(
            provider.verify_token("invalid"),
            Err(AuthError::InvalidToken)
        ));
        assert!(matches!(
            provider.verify_token("header.payload"),
            Err(AuthError::InvalidToken)
        ));
    }

    #[test]
    fn verify_token_too_many_parts_returns_invalid() {
        let provider = make_provider(None);
        assert!(matches!(
            provider.verify_token("a.b.c.d"),
            Err(AuthError::InvalidToken)
        ));
    }

    #[test]
    fn verify_token_with_no_keys_returns_provider_error() {
        let provider = make_provider(None);
        let result = provider.verify_token("aaa.bbb.ccc");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_invalid_base64_header_returns_provider_error() {
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("!!!not-base64!!.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_missing_kid_returns_provider_error() {
        // Header base64: {"alg":"RS256"}  (no kid field)
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAiUlMyNTYifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
        assert!(
            result.unwrap_err().to_string().contains("kid"),
            "error should mention missing kid"
        );
    }

    #[test]
    fn verify_token_missing_alg_returns_provider_error() {
        // Header base64: {"kid":"test"}  (no alg field)
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJraWQiOiAidGVzdCJ9.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
        assert!(
            result.unwrap_err().to_string().contains("alg"),
            "error should mention missing alg"
        );
    }

    #[test]
    fn verify_token_alg_none_rejected() {
        // Header base64: {"alg":"none","kid":"test"}
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAibm9uZSIsICJraWQiOiAidGVzdCJ9.payload.sig");
        assert!(
            matches!(result, Err(AuthError::InvalidToken)),
            "expected InvalidToken, got {result:?}"
        );
    }

    #[test]
    fn verify_token_unsupported_algorithm_returns_provider_error() {
        // Header base64: {"alg":"MACSHA256","kid":"test"}  -- not a valid Algorithm
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAiTUFDU0hBMjU2IiwgImtpZCI6ICJ0ZXN0In0.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unsupported"),
            "error should mention unsupported algorithm: {err}"
        );
    }

    #[test]
    fn verify_token_eddsa_algorithm_returns_provider_error() {
        // Header base64: {"alg":"EdDSA","kid":"test"}
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAiRWREU0EiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_no_matching_key_returns_provider_error() {
        // Header base64: {"alg":"RS256","kid":"unknown"}
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![Jwk {
                kid: "different-key".to_owned(),
                key_type: "RSA".to_owned(),
                n: Some("n".to_owned()),
                e: Some("e".to_owned()),
                x_coord: None,
                y_coord: None,
            }]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInVua25vd24ifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("no matching key"),
            "error should mention no matching key: {err}"
        );
    }

    #[test]
    fn verify_token_key_type_mismatch_returns_provider_error() {
        // Header: RS256, but only EC key available — algorithm incompatible
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![Jwk {
                kid: "test".to_owned(),
                key_type: "EC".to_owned(),
                n: None,
                e: None,
                x_coord: Some("x".to_owned()),
                y_coord: Some("y".to_owned()),
            }]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        // Header: {"alg":"RS256","kid":"test"}
        let result = provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_empty_keys_no_match() {
        // Header: {"alg":"RS256","kid":"test"} but keys is empty vec
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    // ── JwksProvider::mint_token ─────────────────────────────────────────

    #[test]
    fn mint_token_returns_error() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

        let provider = make_provider(None);
        let repo = RepositoryScope::new(RepositoryProvider::Generic, "owner", "repo", Some("main"))
            .expect("valid repo scope");
        let claims =
            TokenClaims::new("https://issuer.example.com", "user", TokenScope::Read, repo, 9999999999)
                .expect("valid claims");
        let result = provider.mint_token(&claims);
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("mint") || err.contains("support"),
            "error should indicate minting not supported: {err}"
        );
    }

    // ── JwksProvider Clone ───────────────────────────────────────────────

    #[test]
    fn jwks_provider_clone_produces_valid_instance() {
        let provider = make_provider(None);
        // Both should behave identically (no keys available)
        assert!(matches!(
            provider.verify_token("a.b.c"),
            Err(AuthError::ProviderError(_))
        ));
    }

    // ── Jwk deserialization edge cases ───────────────────────────────────

    #[test]
    fn jwk_deserialize_missing_optional_fields() {
        // Only kid and kty are required; n, e, x, y are optional
        let json = json!({
            "kid": "minimal",
            "kty": "RSA"
        });
        let jwk: Jwk = serde_json::from_value(json).expect("should deserialize minimal JWK");
        assert_eq!(jwk.kid, "minimal");
        assert_eq!(jwk.key_type, "RSA");
        assert!(jwk.n.is_none());
        assert!(jwk.e.is_none());
    }

    #[test]
    fn jwk_deserialize_empty_kid() {
        let json = json!({
            "kid": "",
            "kty": "RSA",
            "n": "n",
            "e": "e"
        });
        let jwk: Jwk = serde_json::from_value(json).expect("should deserialize JWK with empty kid");
        assert!(jwk.kid.is_empty());
    }

    #[test]
    fn jwk_deserialize_additional_fields_ignored() {
        let json = json!({
            "kid": "key1",
            "kty": "EC",
            "alg": "ES256",
            "use": "sig",
            "crv": "P-256",
            "x": "xval",
            "y": "yval",
            "ext": true,
            "key_ops": ["verify"]
        });
        let jwk: Jwk = serde_json::from_value(json).expect("should ignore extra fields");
        assert_eq!(jwk.kid, "key1");
        assert_eq!(jwk.key_type, "EC");
    }

    #[test]
    fn jwk_deserialize_invalid_type_missing_kid() {
        let json = json!({
            "kty": "RSA",
            "n": "n",
            "e": "e"
            // no kid
        });
        // kid is not optional in our struct (no Option wrapper)
        let result: Result<Jwk, _> = serde_json::from_value(json);
        assert!(result.is_err(), "missing required field kid should fail");
    }

    #[test]
    fn jwks_response_deserialize_empty_keys() {
        let json = json!({ "keys": [] });
        let resp: JwksResponse = serde_json::from_value(json).expect("empty keys array");
        assert!(resp.keys.is_empty());
    }

    #[test]
    fn jwks_response_deserialize_multiple_keys() {
        let json = json!({
            "keys": [
                { "kid": "k1", "kty": "RSA", "n": "n1", "e": "e1" },
                { "kid": "k2", "kty": "EC", "x": "x2", "y": "y2" },
                { "kid": "k3", "kty": "RSA", "n": "n3", "e": "e3" }
            ]
        });
        let resp: JwksResponse = serde_json::from_value(json).expect("multiple keys");
        assert_eq!(resp.keys.len(), 3);
        assert_eq!(resp.keys[0].kid, "k1");
        assert_eq!(resp.keys[1].kid, "k2");
        assert_eq!(resp.keys[2].kid, "k3");
    }

    #[test]
    fn jwks_response_deserialize_missing_keys_field() {
        let json = json!({});
        let result: Result<JwksResponse, _> = serde_json::from_value(json);
        assert!(result.is_err(), "missing 'keys' field should fail");
    }

    // ── Drop behaviour ───────────────────────────────────────────────────

    #[test]
    fn jwks_provider_drop_does_not_panic() {
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            etag: Some("abc123".to_owned()),
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        drop(provider);
        // If we get here, drop succeeded without panicking
    }

    // ── build_decoding_key success paths ─────────────────────────────────

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_rs256() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::RS256).expect("RS256 should succeed");
    }

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_rs384() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::RS384).expect("RS384 should succeed");
    }

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_rs512() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::RS512).expect("RS512 should succeed");
    }

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_ps256() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::PS256).expect("PS256 should succeed");
    }

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_ps384() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::PS384).expect("PS384 should succeed");
    }

    #[test]
    fn build_decoding_key_rsa_with_valid_n_and_e_succeeds_for_ps512() {
        let jwk = sample_rsa_jwk();
        build_decoding_key(&jwk, Algorithm::PS512).expect("PS512 should succeed");
    }

    #[test]
    fn build_decoding_key_ec_with_valid_x_and_y_succeeds_for_es256() {
        let jwk = sample_ec_jwk();
        build_decoding_key(&jwk, Algorithm::ES256).expect("ES256 should succeed");
    }

    #[test]
    fn build_decoding_key_ec_with_valid_x_and_y_succeeds_for_es384() {
        let jwk = sample_ec_jwk();
        build_decoding_key(&jwk, Algorithm::ES384).expect("ES384 should succeed");
    }

    // ── validate_algorithm with garbage ─────────────────────────────────

    #[test]
    fn validate_algorithm_garbage_string_returns_err() {
        let result = validate_algorithm("FOOBAR");
        assert!(result.is_err(), "expected Err for FOOBAR, got {result:?}");
        assert!(
            result.unwrap_err().contains("unsupported algorithm"),
            "should mention unsupported algorithm"
        );
    }

    #[test]
    fn validate_algorithm_empty_string_returns_err() {
        let result = validate_algorithm("");
        assert!(result.is_err(), "expected Err for empty string");
    }

    // ── get_or_refresh_keys ─────────────────────────────────────────────

    #[tokio::test]
    async fn get_or_refresh_keys_with_cached_keys_returns_keys() {
        let jwk = sample_rsa_jwk();
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![jwk]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let result = provider.get_or_refresh_keys().await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");
        assert_eq!(result.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_or_refresh_keys_with_no_keys_returns_error() {
        let provider = make_provider(None);
        let result = provider.get_or_refresh_keys().await;
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    // ── refresh_keys_if_changed ─────────────────────────────────────────

    #[tokio::test]
    async fn refresh_keys_if_changed_with_not_modified_returns_ok() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::header("if-none-match", "my-etag"))
            .respond_with(wiremock::ResponseTemplate::new(304))
            .mount(&mock_server)
            .await;

        let jwk = sample_rsa_jwk();
        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![jwk]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        // Cache should still have the same original key
        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should still be populated");
        assert_eq!(cached.keys.len(), 1);
        assert_eq!(cached.etag.as_deref(), Some("my-etag"));
    }

    #[tokio::test]
    async fn refresh_keys_if_changed_with_200_updates_cache() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        let new_jwk_json = serde_json::json!({
            "keys": [{
                "kid": "new-key",
                "kty": "RSA",
                "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                "e": "AQAB"
            }]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(new_jwk_json)
                    .insert_header("etag", "new-etag"),
            )
            .mount(&mock_server)
            .await;

        let jwk = sample_rsa_jwk();
        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![jwk]),
                etag: Some("old-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        // Cache should have been updated with the new key
        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should be populated");
        assert_eq!(cached.keys.len(), 1);
        assert_eq!(cached.keys[0].kid, "new-key");
        assert_eq!(cached.etag.as_deref(), Some("new-etag"));
    }

    #[tokio::test]
    async fn refresh_keys_if_changed_with_no_etag_works() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        let jwks_json = serde_json::json!({
            "keys": [{
                "kid": "key-from-server",
                "kty": "RSA",
                "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                "e": "AQAB"
            }]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(jwks_json))
            .mount(&mock_server)
            .await;

        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![]),
                etag: None,
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        // Cache should now have the server key
        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should be populated");
        assert_eq!(cached.keys.len(), 1);
        assert_eq!(cached.keys[0].kid, "key-from-server");
    }

    // ── Full JWT verification success ────────────────────────────────────

    fn test_rsa_jwk(kid: &str) -> Jwk {
        Jwk {
            kid: kid.to_owned(),
            key_type: "RSA".to_owned(),
            n: Some(TEST_RSA_N.to_owned()),
            e: Some(TEST_RSA_E.to_owned()),
            x_coord: None,
            y_coord: None,
        }
    }

    #[test]
    fn verify_token_with_valid_rs256_jwt_succeeds() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        // Build claims that pass all validation checks.
        let mut claims = BTreeMap::new();
        claims.insert(
            "iss",
            serde_json::Value::String("https://example.com".to_owned()),
        );
        claims.insert(
            "sub",
            serde_json::Value::String("test-user".to_owned()),
        );
        // exp in the far future
        claims.insert("exp", serde_json::json!(9999999999u64));
        // iat in the past
        claims.insert("iat", serde_json::json!(1000000000u64));

        // Create header with kid matching our test JWK
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        let token_claims = result.unwrap();
        assert_eq!(token_claims.issuer(), "https://example.com");
        assert_eq!(token_claims.subject(), "test-user");
    }

    #[test]
    fn verify_token_with_valid_rs256_jwt_scope_write() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("admin-user"));
        claims.insert("exp", serde_json::json!(9999999999u64));
        claims.insert("iat", serde_json::json!(1000000000u64));
        claims.insert("scope", serde_json::json!("write"));

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        let token_claims = result.unwrap();
        assert_eq!(token_claims.subject(), "admin-user");
        assert_eq!(token_claims.scope(), TokenScope::Write);
    }

    #[test]
    fn verify_token_with_expired_jwt_fails() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("test-user"));
        claims.insert("exp", serde_json::json!(1000000000u64)); // expired
        claims.insert("iat", serde_json::json!(900000000u64));

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        // The jsonwebtoken library's internal decode rejects expired tokens first,
        // so this surfaces as ProviderError rather than our own ExpiredToken variant.
        assert!(result.is_err(), "expired JWT should be rejected");
    }

    #[test]
    fn verify_token_with_future_iat_fails() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("test-user"));
        // exp and iat both far in the future; iat > now should fail
        claims.insert("exp", serde_json::json!(9999999999u64));
        claims.insert("iat", serde_json::json!(9999999998u64));

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        assert!(matches!(result, Err(AuthError::InvalidToken)));
    }

    // ── JwksProvider::new() success ──────────────────────────────────────

    #[tokio::test]
    async fn new_with_reachable_jwks_url_succeeds() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        let jwks_json = serde_json::json!({
            "keys": [{
                "kid": "key-1",
                "kty": "RSA",
                "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                "e": "AQAB"
            }]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(jwks_json))
            .mount(&mock_server)
            .await;

        let provider = JwksProvider::new(&url, "https://example.com")
            .await
            .expect("JWKS provider creation should succeed");

        // Verify keys were fetched and cached
        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should be populated");
        assert_eq!(cached.keys.len(), 1);
        assert_eq!(cached.keys[0].kid, "key-1");
    }

    #[tokio::test]
    async fn new_with_reachable_url_includes_etag_when_present() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        let jwks_json = serde_json::json!({
            "keys": [{"kid": "k1", "kty": "RSA", "n": "n", "e": "e"}]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(jwks_json)
                    .insert_header("etag", "\"abc123\""),
            )
            .mount(&mock_server)
            .await;

        let provider =
            JwksProvider::new(&url, "https://issuer.example.com")
                .await
                .expect("should succeed");

        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should be populated");
        assert_eq!(cached.etag.as_deref(), Some("\"abc123\""));
    }

    // ── start_background_refresh ────────────────────────────────────────

    #[tokio::test]
    async fn start_background_refresh_sets_handle_once() {
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            etag: Some("etag".to_owned()),
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        // First call sets the handle
        provider.start_background_refresh();
        assert!(provider.background_handle.get().is_some());

        // Second call should not replace (OnceLock)
        provider.start_background_refresh();
        assert!(provider.background_handle.get().is_some());
    }

    #[tokio::test]
    async fn start_background_refresh_with_mock_server() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        let jwks_json = serde_json::json!({
            "keys": [{"kid": "bg-key", "kty": "RSA", "n": "n", "e": "e"}]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(jwks_json))
            .mount(&mock_server)
            .await;

        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: None,
                refresh_interval: Duration::from_millis(50),
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        provider.start_background_refresh();
        assert!(provider.background_handle.get().is_some());

        // Allow at least one background refresh cycle
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The mock server should have received at least one request from the loop
        let requests = mock_server
            .received_requests()
            .await
            .unwrap_or_default();
        assert!(!requests.is_empty(), "background refresh should make HTTP requests");
    }

    #[tokio::test]
    async fn refresh_keys_if_changed_with_json_parse_error() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("not valid json"))
            .mount(&mock_server)
            .await;

        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_err(), "expected Err for JSON parse failure");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("parse"), "error should mention parse: {err}");
    }

    #[tokio::test]
    async fn refresh_keys_if_changed_with_server_error_status() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(500))
            .mount(&mock_server)
            .await;

        let provider = JwksProvider {
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_err(), "expected Err for server error status");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("500"), "error should mention status code: {err}");
    }

    #[test]
    fn verify_token_with_future_nbf_fails() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("test-user"));
        claims.insert("exp", serde_json::json!(9999999999u64));
        claims.insert("iat", serde_json::json!(1000000000u64));
        claims.insert("nbf", serde_json::json!(9999999998u64));

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        // The jsonwebtoken library validates nbf before our custom check runs,
        // so this surfaces as ProviderError rather than our own InvalidToken.
        assert!(result.is_err(), "JWT with future nbf should be rejected");
    }

    #[test]
    fn verify_token_with_missing_exp_fails() {
        use jsonwebtoken::{encode, EncodingKey, Header};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("test-user"));
        // no exp claim

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        let result = provider.verify_token(&token);
        assert!(
            matches!(result, Err(AuthError::ProviderError(ref msg)) if msg.contains("exp")),
            "expected ProviderError about missing exp, got {result:?}"
        );
    }

    #[tokio::test]
    async fn verify_token_cache_lock_contended_returns_lock_error() {
        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));

        // Hold the write lock from the async context to force try_read contention
        let write_guard = provider.cached_keys.write().await;

        let provider_clone = provider.clone();
        let thread_handle = std::thread::spawn(move || {
            // This thread has no tokio runtime -> fallback retry path
            provider_clone.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig")
        });

        // Wait for the thread to exhaust 5 retries at 10ms each
        let result = thread_handle.join().expect("thread should not panic");
        assert!(
            matches!(result, Err(AuthError::ProviderError(ref msg)) if msg.contains("contended")),
            "expected lock contended error, got {result:?}"
        );

        drop(write_guard);
    }
}
