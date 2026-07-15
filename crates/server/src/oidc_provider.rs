use std::{
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use reqwest::Client;
use serde::Deserialize;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(3600);
const JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(1800);

/// OpenID Connect authentication provider.
///
/// Validates tokens against an OIDC issuer by fetching JWKS keys from the
/// issuer's discovery endpoint.  Keys are refreshed in a background task to
/// prevent auth outages when the cache TTL expires.
pub struct OidcProvider {
    client: Client,
    issuer: String,
    audience: Option<String>,
    cached_keys: Arc<Mutex<Option<CachedJwks>>>,
    jwks_url: String,
    _background_handle: Arc<std::sync::OnceLock<tokio::task::JoinHandle<()>>>,
    shutdown: Arc<AtomicBool>,
}

impl Clone for OidcProvider {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            cached_keys: Arc::clone(&self.cached_keys),
            jwks_url: self.jwks_url.clone(),
            _background_handle: Arc::clone(&self._background_handle),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

struct CachedJwks {
    keys: Arc<Vec<Jwk>>,
    fetched_at: Instant,
}

impl Clone for CachedJwks {
    fn clone(&self) -> Self {
        Self {
            keys: Arc::clone(&self.keys),
            fetched_at: self.fetched_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    jwks_uri: String,
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

/// OIDC provider initialization failure.
#[derive(Debug, thiserror::Error)]
pub enum OidcProviderError {
    /// HTTP client creation failed.
    #[error("failed to create HTTP client: {0}")]
    HttpClient(String),
    /// OIDC discovery endpoint could not be reached.
    #[error("failed to fetch OIDC discovery document: {0}")]
    DiscoveryFetch(String),
    /// The JWKS endpoint could not be reached.
    #[error("failed to fetch JWKS keys: {0}")]
    JwksFetch(String),
}

impl OidcProvider {
    /// Creates a new OIDC provider by fetching the issuer's discovery document
    /// and starting a background task to periodically refresh JWKS keys.
    ///
    /// # Errors
    ///
    /// Returns [`OidcProviderError`] when the discovery endpoint is unreachable
    /// or the response is malformed.
    pub async fn new(issuer: &str, audience: Option<String>) -> Result<Self, OidcProviderError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| OidcProviderError::HttpClient(e.to_string()))?;

        let discovery_url = format!("{issuer}/.well-known/openid-configuration");
        let discovery: OidcDiscovery = client
            .get(&discovery_url)
            .send()
            .await
            .map_err(|e| OidcProviderError::DiscoveryFetch(e.to_string()))?
            .json()
            .await
            .map_err(|e| OidcProviderError::DiscoveryFetch(e.to_string()))?;

        let jwks_url = discovery.jwks_uri;
        let jwks: JwksResponse = client
            .get(&jwks_url)
            .send()
            .await
            .map_err(|e| OidcProviderError::JwksFetch(e.to_string()))?
            .json()
            .await
            .map_err(|e| OidcProviderError::JwksFetch(e.to_string()))?;

        let cached_keys = Arc::new(Mutex::new(Some(CachedJwks {
            keys: Arc::new(jwks.keys),
            fetched_at: Instant::now(),
        })));

        let provider = Self {
            client: client.clone(),
            issuer: issuer.to_owned(),
            audience,
            cached_keys,
            jwks_url,
            _background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        // Start background refresh to prevent the cache from expiring.
        provider.start_background_refresh();

        Ok(provider)
    }

    fn start_background_refresh(&self) {
        let provider = self.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(JWKS_REFRESH_INTERVAL).await;
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                match provider.client.get(&provider.jwks_url).send().await {
                    Ok(response) => match response.json::<JwksResponse>().await {
                        Ok(jwks) => {
                            if let Ok(mut guard) = provider.cached_keys.lock() {
                                *guard = Some(CachedJwks {
                                    keys: Arc::new(jwks.keys),
                                    fetched_at: Instant::now(),
                                });
                            }
                        }
                        Err(e) => {
                            tracing::warn!("OIDC JWKS refresh: failed to parse response: {e}")
                        }
                    },
                    Err(e) => tracing::warn!("OIDC JWKS refresh: HTTP error: {e}"),
                }
            }
        });
        let _ = self._background_handle.set(handle);
    }

    fn get_cached_keys(&self) -> Option<Arc<Vec<Jwk>>> {
        let guard = self.cached_keys.lock().ok()?;
        let cached = guard.as_ref()?;
        // The background refresh task keeps keys fresh, but we always check
        // the TTL as a safety net.  If the TTL expires (e.g. background task
        // failed repeatedly), we return None and auth will fail immediately
        // rather than accepting potentially stale keys.
        if cached.fetched_at.elapsed() < JWKS_CACHE_TTL {
            return Some(Arc::clone(&cached.keys));
        }
        None
    }

    fn verify_jwt_claims(
        &self,
        header_b64: &str,
        payload_b64: &str,
        signature_b64: &str,
    ) -> Result<TokenClaims, AuthError> {
        let keys = self.get_cached_keys().ok_or_else(|| {
            AuthError::ProviderError("JWKS keys not available or expired".to_owned())
        })?;

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
        if let Some(ref audience) = self.audience {
            validation.set_audience(&[audience.as_str()]);
        }

        let token = format!("{header_b64}.{payload_b64}.{signature_b64}");
        let token_data = decode::<serde_json::Value>(&token, &decoding_key, &validation)
            .map_err(|e| AuthError::ProviderError(format!("JWT verification failed: {e}")))?;

        let payload = token_data.claims;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

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
            RepositoryScope::new(RepositoryProvider::Generic, "oidc", &sub, Some("main"))
                .map_err(|e| AuthError::ProviderError(e.to_string()))?;

        TokenClaims::new(&self.issuer, &sub, scope, repository, exp)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }
}

impl Drop for OidcProvider {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self._background_handle.get() {
            handle.abort();
        }
    }
}

impl AuthProvider for OidcProvider {
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
            "OIDC provider does not support token minting".to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── OidcProviderError display ────────────────────────────────────────

    #[test]
    fn oidc_provider_error_http_client_display_non_empty() {
        let e = OidcProviderError::HttpClient("connection refused".into());
        let msg = format!("{e}");
        assert!(!msg.is_empty());
        assert!(msg.contains("HTTP client"));
    }

    #[test]
    fn oidc_provider_error_discovery_fetch_display_non_empty() {
        let e = OidcProviderError::DiscoveryFetch("timeout".into());
        let msg = format!("{e}");
        assert!(!msg.is_empty());
        assert!(msg.contains("discovery"));
    }

    #[test]
    fn oidc_provider_error_jwks_fetch_display_non_empty() {
        let e = OidcProviderError::JwksFetch("404 not found".into());
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
        assert!(jwk.n.is_some(), "RSA key should have n field");
        assert!(jwk.e.is_some(), "RSA key should have e field");
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

    // ── JwksResponse deserialization ─────────────────────────────────────

    #[test]
    fn jwks_response_deserialize_with_keys() {
        let json = json!({
            "keys": [
                {
                    "kid": "k1",
                    "kty": "RSA",
                    "alg": "RS256",
                    "n": "m",
                    "e": "e"
                },
                {
                    "kid": "k2",
                    "kty": "EC",
                    "alg": "ES256",
                    "crv": "P-256",
                    "x": "x",
                    "y": "y"
                }
            ]
        });
        let resp: JwksResponse =
            serde_json::from_value(json).expect("should deserialize JWKS response");
        assert_eq!(resp.keys.len(), 2);
        assert_eq!(resp.keys[0].kid, "k1");
        assert_eq!(resp.keys[1].kid, "k2");
    }

    #[test]
    fn jwks_response_deserialize_empty() {
        let json = json!({ "keys": [] });
        let resp: JwksResponse =
            serde_json::from_value(json).expect("should deserialize empty JWKS");
        assert!(resp.keys.is_empty());
    }

    // ── OidcDiscovery deserialization ────────────────────────────────────

    #[test]
    fn oidc_discovery_deserialize_valid() {
        let json = json!({
            "jwks_uri": "https://example.com/.well-known/jwks",
            "issuer": "https://example.com",
            "authorization_endpoint": "https://example.com/auth"
        });
        let disco: OidcDiscovery =
            serde_json::from_value(json).expect("should deserialize discovery doc");
        assert_eq!(disco.jwks_uri, "https://example.com/.well-known/jwks");
    }

    #[test]
    fn oidc_discovery_deserialize_missing_jwks_uri() {
        let json = json!({
            "issuer": "https://example.com"
            // no jwks_uri
        });
        let result: Result<OidcDiscovery, _> = serde_json::from_value(json);
        assert!(result.is_err(), "missing jwks_uri should fail");
    }

    #[test]
    fn oidc_discovery_deserialize_additional_fields_ignored() {
        let json = json!({
            "jwks_uri": "https://example.com/jwks",
            "issuer": "https://example.com",
            "authorization_endpoint": "https://example.com/auth",
            "token_endpoint": "https://example.com/token",
            "userinfo_endpoint": "https://example.com/userinfo",
            "response_types_supported": ["code"]
        });
        let disco: OidcDiscovery =
            serde_json::from_value(json).expect("should ignore extra fields");
        assert_eq!(disco.jwks_uri, "https://example.com/jwks");
    }

    // ── JwksResponse deserialization edge cases ──────────────────────────

    #[test]
    fn jwks_response_deserialize_missing_keys_field() {
        let json = json!({});
        let result: Result<JwksResponse, _> = serde_json::from_value(json);
        assert!(result.is_err(), "missing 'keys' field should fail");
    }

    #[test]
    fn jwks_response_deserialize_extra_fields() {
        let json = json!({
            "keys": [],
            "extra": "field"
        });
        let resp: JwksResponse =
            serde_json::from_value(json).expect("extra fields should be ignored");
        assert!(resp.keys.is_empty());
    }

    // ── is_algorithm_compatible (oidc_provider version) ──────────────────

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

    // ── build_decoding_key (oidc_provider version) ───────────────────────

    fn sample_rsa_jwk() -> Jwk {
        Jwk {
            kid: "test".to_owned(),
            key_type: "RSA".to_owned(),
            n: Some("0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4Qy5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw".to_owned()),
            e: Some("AQAB".to_owned()),
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

    // ── base64_decode_url (oidc_provider version) ────────────────────────

    #[test]
    fn base64_decode_url_valid() {
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

    // ── OidcProvider construction helpers ────────────────────────────────

    fn make_provider(issuer: &str, audience: Option<String>, cached: Option<CachedJwks>) -> OidcProvider {
        OidcProvider {
            client: Client::new(),
            issuer: issuer.to_owned(),
            audience,
            cached_keys: Arc::new(Mutex::new(cached)),
            jwks_url: format!("{issuer}/.well-known/jwks"),
            _background_handle: Arc::new(std::sync::OnceLock::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    fn make_provider_no_audience(cached: Option<CachedJwks>) -> OidcProvider {
        make_provider("https://example.com", None, cached)
    }

    // ── OidcProvider::new error path ─────────────────────────────────────

    #[tokio::test]
    async fn new_with_unreachable_issuer_returns_error() {
        let result = OidcProvider::new("http://127.0.0.1:1", None).await;
        assert!(result.is_err(), "expected Err for unreachable issuer");
        if let Err(err) = result {
            assert!(
                matches!(err, OidcProviderError::DiscoveryFetch(_) | OidcProviderError::HttpClient(_)),
                "expected DiscoveryFetch or HttpClient, got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn new_with_unreachable_issuer_error_message_non_empty() {
        let result = OidcProvider::new("http://127.0.0.1:1", None).await;
        assert!(result.is_err());
        if let Err(err) = result {
            let msg = format!("{}", err);
            assert!(!msg.is_empty());
        }
    }

    // ── get_cached_keys ──────────────────────────────────────────────────

    #[test]
    fn get_cached_keys_returns_none_when_empty() {
        let provider = make_provider_no_audience(None);
        assert!(provider.get_cached_keys().is_none());
    }

    #[test]
    fn get_cached_keys_returns_keys_when_fresh() {
        let keys = Arc::new(vec![sample_rsa_jwk()]);
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::clone(&keys),
            fetched_at: Instant::now(),
        }));
        let result = provider.get_cached_keys();
        assert!(result.is_some(), "fresh cache should return keys");
        // Verify reference equality via Arc
        assert!(Arc::ptr_eq(&result.unwrap(), &keys));
    }

    #[test]
    fn get_cached_keys_returns_none_when_expired() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            fetched_at: Instant::now()
                .checked_sub(Duration::from_secs(7200))
                .unwrap_or_else(Instant::now),
        }));
        assert!(provider.get_cached_keys().is_none(), "expired cache should return None");
    }

    #[test]
    fn get_cached_keys_returns_none_just_before_expiry_boundary() {
        // fetched_at should still be valid if elapsed < JWKS_CACHE_TTL
        let just_inside = Instant::now()
            .checked_sub(JWKS_CACHE_TTL - Duration::from_secs(1))
            .unwrap_or_else(Instant::now);
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            fetched_at: just_inside,
        }));
        assert!(provider.get_cached_keys().is_some(), "cache just inside TTL should return keys");
    }

    // ── OidcProvider::verify_token ───────────────────────────────────────

    #[test]
    fn verify_token_too_few_parts_returns_invalid() {
        let provider = make_provider_no_audience(None);
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
        let provider = make_provider_no_audience(None);
        assert!(matches!(
            provider.verify_token("a.b.c.d"),
            Err(AuthError::InvalidToken)
        ));
    }

    #[test]
    fn verify_token_with_no_keys_returns_provider_error() {
        let provider = make_provider_no_audience(None);
        let result = provider.verify_token("aaa.bbb.ccc");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_with_expired_cache_returns_provider_error() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            fetched_at: Instant::now()
                .checked_sub(Duration::from_secs(7200))
                .unwrap_or_else(Instant::now),
        }));
        let result = provider.verify_token("aaa.bbb.ccc");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError for expired cache, got {result:?}"
        );
    }

    #[test]
    fn verify_token_invalid_base64_header_returns_provider_error() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
        }));
        let result = provider.verify_token("!!!not-base64!!.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_missing_kid_returns_provider_error() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
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
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
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
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
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
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
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
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![]),
            fetched_at: Instant::now(),
        }));
        let result = provider.verify_token("eyJhbGciOiAiRWREU0EiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    #[test]
    fn verify_token_no_matching_key_returns_provider_error() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![Jwk {
                kid: "different-key".to_owned(),
                key_type: "RSA".to_owned(),
                n: Some("n".to_owned()),
                e: Some("e".to_owned()),
                x_coord: None,
                y_coord: None,
            }]),
            fetched_at: Instant::now(),
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
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![Jwk {
                kid: "test".to_owned(),
                key_type: "EC".to_owned(),
                n: None,
                e: None,
                x_coord: Some("x".to_owned()),
                y_coord: Some("y".to_owned()),
            }]),
            fetched_at: Instant::now(),
        }));
        let result = provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    // ── OidcProvider verify_token with audience ───────────────────────────

    #[test]
    fn verify_token_with_audience_and_no_keys_returns_provider_error() {
        // Provider configured with audience, but no keys cached
        let provider = make_provider("https://issuer.example.com", Some("my-audience".to_owned()), None);
        let result = provider.verify_token("aaa.bbb.ccc");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    // ── OidcProvider::mint_token ─────────────────────────────────────────

    #[test]
    fn mint_token_returns_error() {
        use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};

        let provider = make_provider_no_audience(None);
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

    // ── OidcProvider Clone ───────────────────────────────────────────────

    #[test]
    fn oidc_provider_clone_produces_valid_instance() {
        let provider = make_provider_no_audience(None);
        assert!(matches!(
            provider.verify_token("a.b.c"),
            Err(AuthError::ProviderError(_))
        ));
    }

    // ── Drop behaviour ───────────────────────────────────────────────────

    #[test]
    fn oidc_provider_drop_does_not_panic() {
        let provider = make_provider_no_audience(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            fetched_at: Instant::now(),
        }));
        drop(provider);
        // If we get here, drop succeeded without panicking
    }

    // ── Jwk deserialization edge cases ───────────────────────────────────

    #[test]
    fn jwk_deserialize_missing_optional_fields() {
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
        });
        let result: Result<Jwk, _> = serde_json::from_value(json);
        assert!(result.is_err(), "missing required field kid should fail");
    }

    // ── Error display contains correct text (additional) ─────────────────

    #[test]
    fn oidc_provider_error_display_http_client() {
        let e = OidcProviderError::HttpClient("ssl error".into());
        assert_eq!(format!("{e}"), "failed to create HTTP client: ssl error");
    }

    #[test]
    fn oidc_provider_error_display_discovery() {
        let e = OidcProviderError::DiscoveryFetch("timeout".into());
        assert_eq!(
            format!("{e}"),
            "failed to fetch OIDC discovery document: timeout"
        );
    }

    #[test]
    fn oidc_provider_error_display_jwks() {
        let e = OidcProviderError::JwksFetch("500".into());
        assert_eq!(format!("{e}"), "failed to fetch JWKS keys: 500");
    }
}
