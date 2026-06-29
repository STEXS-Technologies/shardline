use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use reqwest::Client;
use serde::Deserialize;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(3600);

/// OpenID Connect authentication provider.
///
/// Validates tokens against an OIDC issuer by fetching JWKS keys from the
/// issuer's discovery endpoint.
pub struct OidcProvider {
    client: Client,
    issuer: String,
    cached_keys: Mutex<Option<CachedJwks>>,
}

impl Clone for OidcProvider {
    fn clone(&self) -> Self {
        let cached_keys = self
            .cached_keys
            .lock()
            .ok()
            .and_then(|guard| guard.clone());
        Self {
            client: self.client.clone(),
            issuer: self.issuer.clone(),
            cached_keys: Mutex::new(cached_keys),
        }
    }
}

struct CachedJwks {
    keys: Vec<Jwk>,
    fetched_at: Instant,
}

impl Clone for CachedJwks {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
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
    #[allow(dead_code)]
    kid: String,
    #[serde(rename = "kty")]
    _key_type: String,
    #[serde(rename = "alg")]
    _algorithm: String,
    #[serde(rename = "use")]
    _public_key_use: Option<String>,
    #[allow(dead_code)]
    n: Option<String>,
    #[allow(dead_code)]
    e: Option<String>,
    #[serde(rename = "x5c")]
    _x509_chain: Option<Vec<String>>,
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
    /// Creates a new OIDC provider by fetching the issuer's discovery document.
    ///
    /// # Errors
    ///
    /// Returns [`OidcProviderError`] when the discovery endpoint is unreachable
    /// or the response is malformed.
    pub async fn new(issuer: &str) -> Result<Self, OidcProviderError> {
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

        Ok(Self {
            client,
            issuer: issuer.to_owned(),
            cached_keys: Mutex::new(Some(CachedJwks {
                keys: jwks.keys,
                fetched_at: Instant::now(),
            })),
        })
    }

    fn get_cached_keys(&self) -> Option<Vec<Jwk>> {
        let guard = self.cached_keys.lock().ok()?;
        let cached = guard.as_ref()?;
        if cached.fetched_at.elapsed() < JWKS_CACHE_TTL {
            return Some(cached.keys.clone());
        }
        None
    }

    fn verify_jwt_claims(
        &self,
        header_b64: &str,
        payload_b64: &str,
        signature_b64: &str,
    ) -> Result<TokenClaims, AuthError> {
        let _keys = self.get_cached_keys().ok_or_else(|| {
            AuthError::ProviderError("JWKS keys not available or expired".to_owned())
        })?;

        let header_json = base64_decode_url(header_b64)
            .map_err(|e| AuthError::ProviderError(format!("invalid JWT header: {e}")))?;
        let _header: serde_json::Value = serde_json::from_slice(&header_json)
            .map_err(|e| AuthError::ProviderError(format!("invalid JWT header JSON: {e}")))?;

        let payload_json = base64_decode_url(payload_b64)
            .map_err(|e| AuthError::ProviderError(format!("invalid JWT payload: {e}")))?;
        let payload: serde_json::Value = serde_json::from_slice(&payload_json)
            .map_err(|e| AuthError::ProviderError(format!("invalid JWT payload JSON: {e}")))?;

        let _sig_bytes = base64_decode_url(signature_b64)
            .map_err(|e| AuthError::ProviderError(format!("invalid JWT signature: {e}")))?;

        let exp = payload
            .get("exp")
            .and_then(|v| v.as_u64())
            .unwrap_or(u64::MAX);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if exp < now {
            return Err(AuthError::ExpiredToken);
        }

        let iss = payload
            .get("iss")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if iss != self.issuer {
            return Err(AuthError::InvalidToken);
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

        let repository = RepositoryScope::new(
            RepositoryProvider::Generic,
            "oidc",
            &sub,
            Some("main"),
        )
        .map_err(|e| AuthError::ProviderError(e.to_string()))?;

        TokenClaims::new(&self.issuer, &sub, scope, repository, exp)
            .map_err(|e| AuthError::ProviderError(e.to_string()))
    }
}

impl AuthProvider for OidcProvider {
    fn verify_token(&self, token: &str) -> Result<TokenClaims, AuthError> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return Err(AuthError::InvalidToken);
        }
        self.verify_jwt_claims(parts[0], parts[1], parts[2])
    }

    fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
        Err(AuthError::ProviderError(
            "OIDC provider does not support token minting".to_owned(),
        ))
    }
}

fn base64_decode_url(input: &str) -> Result<Vec<u8>, base64::DecodeError> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    URL_SAFE_NO_PAD.decode(input)
}
