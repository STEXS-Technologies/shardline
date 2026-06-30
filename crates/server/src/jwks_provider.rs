use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use reqwest::Client;
use serde::Deserialize;
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthError, AuthProvider};

const JWKS_CACHE_TTL: Duration = Duration::from_secs(300);

/// JWKS authentication provider.
///
/// Validates tokens against a static JWKS endpoint, caching keys with a TTL.
pub struct JwksProvider {
    client: Client,
    jwks_url: String,
    issuer: String,
    cached_keys: Mutex<Option<CachedJwks>>,
}

impl Clone for JwksProvider {
    fn clone(&self) -> Self {
        let cached_keys = self
            .cached_keys
            .lock()
            .ok()
            .and_then(|guard| guard.clone());
        Self {
            client: self.client.clone(),
            jwks_url: self.jwks_url.clone(),
            issuer: self.issuer.clone(),
            cached_keys: Mutex::new(cached_keys),
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
struct JwksResponse {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct Jwk {
    kid: String,
    #[serde(rename = "kty")]
    key_type: String,
    #[serde(rename = "alg")]
    algorithm: String,
    #[serde(rename = "use")]
    public_key_use: Option<String>,
    n: Option<String>,
    e: Option<String>,
    #[serde(rename = "x")]
    x_coord: Option<String>,
    #[serde(rename = "y")]
    y_coord: Option<String>,
    #[serde(rename = "crv")]
    curve: Option<String>,
    #[serde(rename = "x5c")]
    x509_chain: Option<Vec<String>>,
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

        let jwks: JwksResponse = client
            .get(jwks_url)
            .send()
            .await
            .map_err(|e| JwksProviderError::JwksFetch(e.to_string()))?
            .json()
            .await
            .map_err(|e| JwksProviderError::JwksFetch(e.to_string()))?;

        Ok(Self {
            client,
            jwks_url: jwks_url.to_owned(),
            issuer: issuer.to_owned(),
            cached_keys: Mutex::new(Some(CachedJwks {
                keys: Arc::new(jwks.keys),
                fetched_at: Instant::now(),
            })),
        })
    }

    async fn get_or_refresh_keys(&self) -> Result<Arc<Vec<Jwk>>, AuthError> {
        {
            let guard = self
                .cached_keys
                .lock()
                .map_err(|e| AuthError::ProviderError(e.to_string()))?;
            if let Some(cached) = guard.as_ref() {
                if cached.fetched_at.elapsed() < JWKS_CACHE_TTL {
                    return Ok(Arc::clone(&cached.keys));
                }
            }
        }

        let jwks: JwksResponse = self
            .client
            .get(&self.jwks_url)
            .send()
            .await
            .map_err(|e| AuthError::ProviderError(format!("JWKS fetch failed: {e}")))?
            .json()
            .await
            .map_err(|e| AuthError::ProviderError(format!("JWKS parse failed: {e}")))?;

        let keys = Arc::new(jwks.keys);
        let mut guard = self
            .cached_keys
            .lock()
            .map_err(|e| AuthError::ProviderError(e.to_string()))?;
        *guard = Some(CachedJwks {
            keys: Arc::clone(&keys),
            fetched_at: Instant::now(),
        });
        Ok(keys)
    }

    fn verify_jwt_claims(
        &self,
        header_b64: &str,
        payload_b64: &str,
        signature_b64: &str,
    ) -> Result<TokenClaims, AuthError> {
        let keys = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.get_or_refresh_keys())
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

        let token = format!("{header_b64}.{payload_b64}.{signature_b64}");
        let token_data = decode::<serde_json::Value>(&token, &decoding_key, &validation)
            .map_err(|e| AuthError::ProviderError(format!("JWT verification failed: {e}")))?;

        let payload = token_data.claims;

        let exp = payload
            .get("exp")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| AuthError::ProviderError("missing exp claim".to_owned()))?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
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

        let repository = RepositoryScope::new(
            RepositoryProvider::Generic,
            "jwks",
            &sub,
            Some("main"),
        )
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
        #[allow(clippy::indexing_slicing)]
        self.verify_jwt_claims(parts[0], parts[1], parts[2])
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
        ("RSA", Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512)
            | ("EC", Algorithm::ES256 | Algorithm::ES384)
            | ("RSA", Algorithm::PS256 | Algorithm::PS384 | Algorithm::PS512)
    )
}

fn build_decoding_key(jwk: &Jwk, algorithm: Algorithm) -> Result<DecodingKey, String> {
    match algorithm {
        Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512
        | Algorithm::PS256 | Algorithm::PS384 | Algorithm::PS512 => {
            let n = jwk
                .n
                .as_ref()
                .ok_or("RSA key missing n parameter")?;
            let e = jwk
                .e
                .as_ref()
                .ok_or("RSA key missing e parameter")?;
            DecodingKey::from_rsa_components(n, e).map_err(|e| format!("invalid RSA key: {e}"))
        }
        Algorithm::ES256 | Algorithm::ES384 => {
            let x = jwk
                .x_coord
                .as_ref()
                .ok_or("EC key missing x parameter")?;
            let y = jwk
                .y_coord
                .as_ref()
                .ok_or("EC key missing y parameter")?;
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
