use std::{
    str::FromStr,
    sync::{Arc, OnceLock},
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
    background_handle: Arc<OnceLock<tokio::task::JoinHandle<()>>>,
}

impl Clone for JwksProvider {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            jwks_url: self.jwks_url.clone(),
            issuer: self.issuer.clone(),
            cached_keys: Arc::clone(&self.cached_keys),
            background_handle: Arc::clone(&self.background_handle),
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
            background_handle: Arc::new(OnceLock::new()),
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
        let handle = tokio::spawn(async move {
            loop {
                let interval = {
                    let guard = provider.cached_keys.read().await;
                    guard
                        .as_ref()
                        .map(|c| c.refresh_interval)
                        .unwrap_or(DEFAULT_JWKS_REFRESH_INTERVAL)
                };
                tokio::time::sleep(interval).await;
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
