use crate::jwt_keys::{
    Jwk, JwksResponse, RefreshTask, is_secure_url as is_secure_jwks_url, read_auth_json_bounded,
    verify_jwt_payload,
};
#[cfg(test)]
use jsonwebtoken::Algorithm;
use std::{
    sync::Arc,
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::Duration,
};

use reqwest::{Client, StatusCode};
use shardline_protocol::TokenClaims;
#[cfg(test)]
use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenScope};
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
    pub(crate) claim_mapping: crate::external_jwt_claims::ExternalJwtClaims,
    client: Client,
    jwks_url: String,
    issuer: String,
    audience: Option<String>,
    cached_keys: Arc<RwLock<Option<CachedJwks>>>,
    background_handle: Arc<RefreshTask>,
    shutdown: Arc<AtomicBool>,
}

impl Clone for JwksProvider {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            jwks_url: self.jwks_url.clone(),
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            claim_mapping: self.claim_mapping.clone(),
            cached_keys: Arc::clone(&self.cached_keys),
            background_handle: Arc::clone(&self.background_handle),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

struct CachedJwks {
    keys: Arc<Vec<Jwk>>,
    etag: Option<String>,
    refresh_interval: Duration,
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
    /// The JWKS URL uses an insecure scheme (non-https, non-loopback).
    #[error("jwks url must use https (loopback http allowed for development), got {0}")]
    InsecureUrl(String),
}

impl JwksProvider {
    /// Creates a new JWKS provider, fetching initial keys from the endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`JwksProviderError`] when the JWKS endpoint is unreachable.
    pub async fn new(jwks_url: &str, issuer: &str) -> Result<Self, JwksProviderError> {
        // Regression (F-95): JWKS key transport must be encrypted (RFC 8414
        // §2). Enforce the scheme gate here too, mirroring the config-load
        // gate in `load_server_config_from_env`, so a programmatic caller can
        // never construct a provider that fetches signing keys over plain-http
        // (loopback http is tolerated for local development / test tooling).
        if !is_secure_jwks_url(jwks_url) {
            return Err(JwksProviderError::InsecureUrl(jwks_url.to_owned()));
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            // Keep the configured signing-key origin pinned on initial and refresh fetches.
            .redirect(reqwest::redirect::Policy::none())
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

        let jwks: JwksResponse = read_auth_json_bounded(response)
            .await
            .map_err(JwksProviderError::JwksFetch)?;

        let provider = Self {
            audience: None,
            client,
            jwks_url: jwks_url.to_owned(),
            issuer: issuer.to_owned(),
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(jwks.keys),
                etag,
                refresh_interval,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        provider.start_background_refresh();
        Ok(provider)
    }

    /// Pins the intended recipient of externally signed tokens.
    ///
    /// # Errors
    /// Returns an error for an empty audience. Server startup requires this setting.
    pub fn with_audience(mut self, audience: String) -> Result<Self, AuthError> {
        if audience.trim().is_empty() {
            return Err(AuthError::InvalidToken);
        }
        self.audience = Some(audience);
        Ok(self)
    }

    fn start_background_refresh(&self) {
        let client = self.client.clone();
        let jwks_url = self.jwks_url.clone();
        let cached_keys = Arc::clone(&self.cached_keys);
        let shutdown = Arc::clone(&self.shutdown);
        let handle = tokio::spawn(async move {
            loop {
                let interval = {
                    let guard = cached_keys.read().await;
                    guard
                        .as_ref()
                        .map(|c| c.refresh_interval)
                        .unwrap_or(DEFAULT_JWKS_REFRESH_INTERVAL)
                };
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = futures_util::future::poll_fn(|_| {
                        if shutdown.load(Ordering::Acquire) {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }) => return,
                }
                if let Err(e) =
                    Self::refresh_cached_keys_if_changed(&client, &jwks_url, &cached_keys).await
                {
                    tracing::warn!("JWKS background refresh failed: {e}");
                }
            }
        });
        if let Err(handle) = self.background_handle.set(handle) {
            // Concurrent or repeated starts must not detach an extra refresh task.
            handle.abort();
        }
    }

    #[cfg(test)]
    async fn refresh_keys_if_changed(&self) -> Result<(), AuthError> {
        Self::refresh_cached_keys_if_changed(&self.client, &self.jwks_url, &self.cached_keys).await
    }

    async fn refresh_cached_keys_if_changed(
        client: &Client,
        jwks_url: &str,
        cached_keys: &RwLock<Option<CachedJwks>>,
    ) -> Result<(), AuthError> {
        let etag = cached_keys
            .read()
            .await
            .as_ref()
            .and_then(|c| c.etag.clone());

        let mut request = client.get(jwks_url);
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

                let jwks: JwksResponse = read_auth_json_bounded(response).await.map_err(|e| {
                    AuthError::ProviderError(format!("JWKS refresh parse failed: {e}"))
                })?;

                let new_cache = CachedJwks {
                    keys: Arc::new(jwks.keys),
                    etag: new_etag,
                    refresh_interval: new_interval,
                };

                *cached_keys.write().await = Some(new_cache);
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
        let keys = {
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
                // Use yield_now + brief park instead of thread::sleep to avoid
                // blocking a tokio worker thread for the full 10ms. The yield
                // lets other tasks run, and the park provides a brief backoff.
                std::thread::yield_now();
                std::thread::park_timeout(Duration::from_millis(RETRY_DELAY_MS));
            }
        }?;

        let payload = verify_jwt_payload(
            header_b64,
            payload_b64,
            signature_b64,
            &keys,
            &self.issuer,
            self.audience.as_deref(),
        )?;

        self.claim_mapping.map(&payload, &self.issuer, "jwks")
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
    use crate::jwt_keys::MAX_AUTH_JSON_BYTES;
    use crate::jwt_keys::test_support::{
        TEST_RSA_E, TEST_RSA_N, TEST_RSA_PEM, sample_rsa_jwk, test_rsa_jwk,
    };
    use serde_json::json;

    #[tokio::test]
    async fn jwks_constructor_rejects_error_status_even_with_keys_json() {
        let server = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(503).set_body_json(serde_json::json!({"keys": []})),
            )
            .mount(&server)
            .await;
        let result = JwksProvider::new(&server.uri(), "issuer").await;
        assert!(
            matches!(result, Err(JwksProviderError::JwksFetch(message)) if message.contains("503"))
        );
    }

    #[tokio::test]
    async fn jwks_constructor_rejects_redirect_without_contacting_target() {
        let target = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({"keys": []})),
            )
            .expect(0)
            .mount(&target)
            .await;
        let origin = wiremock::MockServer::start().await;
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(
                wiremock::ResponseTemplate::new(302).insert_header("Location", target.uri()),
            )
            .mount(&origin)
            .await;
        let result = JwksProvider::new(&origin.uri(), "issuer").await;
        assert!(
            matches!(result, Err(JwksProviderError::JwksFetch(message)) if message.contains("302"))
        );
        assert!(
            target
                .received_requests()
                .await
                .expect("target requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn jwks_constructor_rejects_oversized_document() {
        let server = wiremock::MockServer::start().await;
        let mut body = br#"{"keys":[]}"#.to_vec();
        body.resize(MAX_AUTH_JSON_BYTES + 1, b' ');
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;
        let result = JwksProvider::new(&server.uri(), "issuer").await;
        assert!(
            matches!(result, Err(JwksProviderError::JwksFetch(message)) if message.contains("document limit"))
        );
    }

    #[tokio::test]
    async fn jwks_refresh_failure_preserves_previous_key_cache() {
        let server = wiremock::MockServer::start().await;
        let mut provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![sample_rsa_jwk()]),
            etag: Some("initial".to_owned()),
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        provider.jwks_url = server.uri();
        let initial_kid = provider
            .cached_keys
            .read()
            .await
            .as_ref()
            .expect("initial cache")
            .keys[0]
            .kid
            .clone();
        for status in [503, 200] {
            server.reset().await;
            let mut body = br#"{"keys":[]}"#.to_vec();
            if status == 200 {
                body.resize(MAX_AUTH_JSON_BYTES + 1, b' ');
            }
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .respond_with(
                    wiremock::ResponseTemplate::new(status)
                        .set_body_bytes(body)
                        .insert_header("etag", "replacement"),
                )
                .mount(&server)
                .await;
            assert!(provider.refresh_keys_if_changed().await.is_err());
            let guard = provider.cached_keys.read().await;
            let cached = guard.as_ref().expect("previous cache retained");
            assert_eq!(cached.keys[0].kid, initial_kid);
            assert_eq!(cached.etag.as_deref(), Some("initial"));
        }
    }

    #[test]
    fn jwt_verification_respects_rsa_jwk_metadata_and_selects_signing_key() {
        crate::jwt_keys::test_support::assert_rsa_jwk_metadata_policy(|keys| {
            let provider = make_provider(Some(CachedJwks {
                keys: Arc::new(keys),
                etag: None,
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }));
            move |token: &str| provider.verify_token(token).is_ok()
        });
    }

    #[test]
    fn jwt_verification_requires_matching_ec_curve_metadata() {
        crate::jwt_keys::test_support::assert_ec_jwk_curve_policy(|keys| {
            let provider = make_provider(Some(CachedJwks {
                keys: Arc::new(keys),
                etag: None,
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }));
            move |token: &str| provider.verify_token(token).is_ok()
        });
    }

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

    #[test]
    fn jwks_provider_error_insecure_url_display_non_empty() {
        let e = JwksProviderError::InsecureUrl("http://keys.example.com/jwks".into());
        let msg = format!("{e}");
        assert!(!msg.is_empty());
        assert!(msg.contains("https"), "message: {msg}");
        assert!(msg.contains("keys.example.com"), "message: {msg}");
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
    fn parse_cache_max_age_policy() {
        for (value, seconds) in [
            (None, None),
            (Some("max-age=300"), Some(300)),
            (Some("no-cache"), None),
            (Some("max-age=30"), Some(60)),
            (Some("max-age=7200"), Some(3600)),
            (Some("public, max-age=300, must-revalidate"), Some(300)),
            (Some("max-age=not-a-number"), None),
        ] {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Some(value) = value {
                headers.insert("cache-control", value.parse().unwrap());
            }
            assert_eq!(
                parse_cache_max_age(&headers),
                seconds.map(Duration::from_secs),
                "Cache-Control {value:?}"
            );
        }
    }

    // ── JwksProvider construction helpers ─────────────────────────────────

    fn make_provider(cached: Option<CachedJwks>) -> JwksProvider {
        JwksProvider {
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: "https://example.com/.well-known/jwks".to_owned(),
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(cached)),
            background_handle: Arc::new(RefreshTask::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    // ── JwksProvider::new error path ─────────────────────────────────────

    #[tokio::test]
    async fn new_with_insecure_http_url_returns_insecure_url_error() {
        // Regression (F-95): a non-https, non-loopback JWKS URL must be
        // rejected by the provider itself, before any HTTP client is built or
        // request is made.
        let result = JwksProvider::new(
            "http://keys.example.com/.well-known/jwks",
            "https://example.com",
        )
        .await;
        assert!(
            result.is_err(),
            "a plain-http non-loopback JWKS URL must be rejected before any fetch"
        );
        if let Err(err) = result {
            assert!(
                matches!(err, JwksProviderError::InsecureUrl(_)),
                "expected InsecureUrl error, got {err:?}"
            );
            let msg = err.to_string();
            assert!(msg.contains("https"), "message: {msg}");
        }
    }

    #[tokio::test]
    async fn new_with_unreachable_url_returns_error() {
        let result =
            JwksProvider::new("http://127.0.0.1:1/nonexistent-jwks", "https://example.com").await;
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
        let result =
            JwksProvider::new("http://127.0.0.1:1/nonexistent-jwks", "https://example.com").await;
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
        let result =
            provider.verify_token("eyJhbGciOiAiTUFDU0hBMjU2IiwgImtpZCI6ICJ0ZXN0In0.payload.sig");
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
        let result =
            provider.verify_token("eyJhbGciOiAiRWREU0EiLCAia2lkIjogInRlc3QifQ.payload.sig");
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
                key_use: None,
                key_operations: None,
                algorithm: None,
                curve: None,
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
        let result =
            provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInVua25vd24ifQ.payload.sig");
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
                key_use: None,
                key_operations: None,
                algorithm: None,
                curve: Some("P-256".to_owned()),
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
        let result =
            provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig");
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
        let result =
            provider.verify_token("eyJhbGciOiAiUlMyNTYiLCAia2lkIjogInRlc3QifQ.payload.sig");
        assert!(
            matches!(result, Err(AuthError::ProviderError(_))),
            "expected ProviderError, got {result:?}"
        );
    }

    // ── JwksProvider::mint_token ─────────────────────────────────────────

    #[test]
    fn mint_token_returns_error() {
        let provider = make_provider(None);
        let repo = RepositoryScope::new(RepositoryProvider::Generic, "owner", "repo", Some("main"))
            .expect("valid repo scope");
        let claims = TokenClaims::new(
            "https://issuer.example.com",
            "user",
            TokenScope::Read,
            repo,
            9999999999,
        )
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
    fn jwks_provider_without_cached_keys_rejects_token() {
        let provider = make_provider(None);
        // No keys are available.
        assert!(matches!(
            provider.verify_token("a.b.c"),
            Err(AuthError::ProviderError(_))
        ));
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![jwk]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![jwk]),
                etag: Some("old-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![]),
                etag: None,
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
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

    #[tokio::test]
    async fn jwks_rotation_rejects_tokens_for_removed_kids_and_accepts_new_kids() {
        use jsonwebtoken::{EncodingKey, Header, encode};
        use std::collections::BTreeMap;

        let mock_server = wiremock::MockServer::start().await;
        let rotated_jwks = json!({
            "keys": [{
                "kid": "rotated-key",
                "kty": "RSA",
                "n": TEST_RSA_N,
                "e": TEST_RSA_E,
            }]
        });
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::header(
                "if-none-match",
                "old-jwks-version",
            ))
            .respond_with(
                wiremock::ResponseTemplate::new(200)
                    .set_body_json(rotated_jwks)
                    .insert_header("etag", "rotated-jwks-version"),
            )
            .mount(&mock_server)
            .await;

        let sign = |kid: &str| {
            let mut claims = BTreeMap::new();
            claims.insert("iss", json!("https://example.com"));
            claims.insert("sub", json!("rotating-user"));
            claims.insert("exp", json!(9_999_999_999_u64));
            claims.insert("iat", json!(1_000_000_000_u64));
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some(kid.to_owned());
            let key = EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes())
                .expect("test RSA key should be valid");
            encode(&header, &claims, &key).expect("test JWT should be signed")
        };

        let old_token = sign("old-key");
        let rotated_token = sign("rotated-key");
        let provider = JwksProvider {
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: mock_server.uri(),
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![test_rsa_jwk("old-key")]),
                etag: Some("old-jwks-version".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let before_rotation = {
            let provider = provider.clone();
            let old_token = old_token.clone();
            std::thread::spawn(move || provider.verify_token(&old_token))
                .join()
                .expect("verification thread should not panic")
        };
        assert!(
            before_rotation.is_ok(),
            "old JWKS key should authenticate before rotation"
        );

        provider
            .refresh_keys_if_changed()
            .await
            .expect("ETag refresh should apply the rotated JWKS");

        let stale_after_rotation = {
            let provider = provider.clone();
            std::thread::spawn(move || provider.verify_token(&old_token))
                .join()
                .expect("verification thread should not panic")
        };
        assert!(
            stale_after_rotation.is_err(),
            "a token whose kid was removed during JWKS rotation must be rejected"
        );

        let current_after_rotation = {
            let provider = provider.clone();
            std::thread::spawn(move || provider.verify_token(&rotated_token))
                .join()
                .expect("verification thread should not panic")
        };
        assert!(
            current_after_rotation.is_ok(),
            "a token signed for the rotated JWKS kid must authenticate"
        );
    }

    #[test]
    fn verify_token_requires_matching_issuer_and_string_subject() {
        use jsonwebtoken::{EncodingKey, Header, encode};

        let provider = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("test-key-1")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key-1".to_owned());
        let key = EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let valid = serde_json::json!({
            "iss": "https://example.com",
            "sub": "test-user",
            "scope": "write",
            "exp": 9999999999u64,
        });

        // All cases have a valid signature and expiry. Rejection must come
        // from the identity claims rather than signature verification.
        for (claim, invalid_value) in [
            ("iss", None),
            ("iss", Some(serde_json::json!("https://wrong.example.com"))),
            ("iss", Some(serde_json::json!(42))),
            ("sub", None),
            ("sub", Some(serde_json::Value::Null)),
            ("sub", Some(serde_json::json!(42))),
            ("sub", Some(serde_json::json!(["test-user"]))),
            ("sub", Some(serde_json::json!(""))),
            ("sub", Some(serde_json::json!("  "))),
        ] {
            let mut claims = valid.clone();
            let object = claims.as_object_mut().expect("claims object");
            if let Some(value) = invalid_value {
                object.insert(claim.to_owned(), value);
            } else {
                object.remove(claim);
            }
            let token = encode(&header, &claims, &key).expect("signed test token");
            assert!(
                provider.verify_token(&token).is_err(),
                "must reject invalid {claim}: {claims}"
            );
        }

        for subject in ["test-user", "anonymous", "subject with spaces", "δοκιμή"] {
            let mut claims = valid.clone();
            claims["sub"] = serde_json::json!(subject);
            let token = encode(&header, &claims, &key).expect("signed test token");
            let verified = provider.verify_token(&token).expect("valid identity");
            assert_eq!(verified.subject(), subject);
            assert_eq!(verified.repository().name(), subject);
            assert_eq!(verified.scope(), TokenScope::Write);
        }
    }

    #[test]
    fn verify_token_with_valid_rs256_jwt_succeeds() {
        use jsonwebtoken::{EncodingKey, Header, encode};
        use std::collections::BTreeMap;

        // Build claims that pass all validation checks.
        let mut claims = BTreeMap::new();
        claims.insert(
            "iss",
            serde_json::Value::String("https://example.com".to_owned()),
        );
        claims.insert("sub", serde_json::Value::String("test-user".to_owned()));
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
        use jsonwebtoken::{EncodingKey, Header, encode};
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
    fn verify_token_with_aud_claim_succeeds() {
        use jsonwebtoken::{EncodingKey, Header, encode};
        use std::collections::BTreeMap;

        // Regression (F-81): JwksProvider has no audience configuration, and
        // jsonwebtoken's `validate_aud` defaults to true with no audience set,
        // which rejects any token that carries an `aud` claim
        // (InvalidAudience). OIDC Core / RFC 9068 require `aud` in real IdP
        // tokens, so every aud-bearing token was rejected. The provider must
        // disable aud validation so the permissive default is true.
        let mut claims = BTreeMap::new();
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("aud-user"));
        claims.insert("aud", serde_json::json!("some-oidc-client"));
        claims.insert("exp", serde_json::json!(9999999999u64));
        claims.insert("iat", serde_json::json!(1000000000u64));

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
            result.is_ok(),
            "an aud-bearing token must verify when no audience is configured, got {result:?}"
        );
        let token_claims = result.unwrap();
        assert_eq!(token_claims.subject(), "aud-user");
    }

    #[test]
    fn verify_token_with_expired_jwt_fails() {
        use jsonwebtoken::{EncodingKey, Header, encode};
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
        use jsonwebtoken::{EncodingKey, Header, encode};
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

        let provider = JwksProvider::new(&url, "https://issuer.example.com")
            .await
            .expect("should succeed");

        let guard = provider.cached_keys.read().await;
        let cached = guard.as_ref().expect("cache should be populated");
        assert_eq!(cached.etag.as_deref(), Some("\"abc123\""));
    }

    #[tokio::test]
    async fn dropping_provider_clone_preserves_refresh_until_last_owner() {
        let provider = make_provider(None);
        let weak_cache = Arc::downgrade(&provider.cached_keys);
        let weak_owner = Arc::downgrade(&provider.background_handle);
        provider.start_background_refresh();
        tokio::task::yield_now().await;
        assert!(
            !provider
                .background_handle
                .get()
                .expect("refresh handle")
                .is_finished()
        );

        let survivor = provider.clone();
        drop(provider);
        tokio::task::yield_now().await;
        assert!(!survivor.shutdown.load(Ordering::Acquire));
        assert!(
            !survivor
                .background_handle
                .get()
                .expect("refresh handle")
                .is_finished()
        );

        // A repeated start must not leave an unowned task retaining the cache.
        survivor.start_background_refresh();
        tokio::task::yield_now().await;
        drop(survivor);
        assert!(
            weak_owner.upgrade().is_none(),
            "last owner must release task guard"
        );
        for _ in 0..10 {
            tokio::task::yield_now().await;
            if weak_cache.upgrade().is_none() {
                return;
            }
        }
        assert!(
            weak_cache.upgrade().is_none(),
            "refresh task retained its cache after final provider was dropped"
        );
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: None,
                refresh_interval: Duration::from_millis(50),
            }))),
            background_handle: Arc::new(RefreshTask::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        provider.start_background_refresh();
        assert!(provider.background_handle.get().is_some());

        // Allow at least one background refresh cycle
        tokio::time::sleep(Duration::from_millis(150)).await;

        // The mock server should have received at least one request from the loop
        let requests = mock_server.received_requests().await.unwrap_or_default();
        assert!(
            !requests.is_empty(),
            "background refresh should make HTTP requests"
        );
    }

    // ── E2E token verification with mock JWKS provider ────────────────────

    #[tokio::test]
    async fn test_auth_with_jwks_provider_e2e() {
        let mock_server = wiremock::MockServer::start().await;
        let url = mock_server.uri();

        // Serve JWKS endpoint with the real RSA public key matching TEST_RSA_PEM
        let jwks_json = serde_json::json!({
            "keys": [{
                "kid": "e2e-key",
                "kty": "RSA",
                "n": TEST_RSA_N,
                "e": TEST_RSA_E,
            }]
        });

        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(jwks_json))
            .mount(&mock_server)
            .await;

        // Create provider via the real constructor (fetches keys from mock)
        // Fetch keys from the mock server into a CachedJwks structure.
        let client = Client::new();
        let resp = client.get(&url).send().await.expect("fetch jwks");
        let jwks: JwksResponse = resp.json().await.expect("parse jwks");
        let jwks = CachedJwks {
            keys: Arc::new(jwks.keys),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        };

        // Build provider with pre-populated cache (avoids block_on at verify time).
        let provider = make_provider(Some(jwks));

        // Sign a valid JWT with the matching RSA key
        use jsonwebtoken::{EncodingKey, Header, encode};
        use std::collections::BTreeMap;

        let mut claims = BTreeMap::new();
        // Issuer must match the provider's issuer (https://example.com from make_provider)
        claims.insert("iss", serde_json::json!("https://example.com"));
        claims.insert("sub", serde_json::json!("jwks-e2e-user"));
        claims.insert("aud", serde_json::json!("shardline"));
        claims.insert("exp", serde_json::json!(9999999999u64));
        claims.insert("iat", serde_json::json!(1000000000u64));

        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("e2e-key".to_owned());

        let encoding_key =
            EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("valid RSA PEM");
        let token = encode(&header, &claims, &encoding_key).expect("should sign token");

        // Verify the token through the provider.
        // verify_token reads from background-refreshed cache; call from non-async context to exercise sync path.
        let result = std::thread::spawn(move || provider.verify_token(&token))
            .join()
            .expect("thread should not panic");
        assert!(result.is_ok(), "expected Ok, got {result:?}");

        let token_claims = result.unwrap();
        assert_eq!(token_claims.issuer(), "https://example.com");
        assert_eq!(token_claims.subject(), "jwks-e2e-user");

        // Exercise startup wiring and the real LFS exchange with a verifier
        // that cannot sign: the internal key must own issuance and CAS reads.
        use axum::{
            body::Body,
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let root = tempfile::tempdir().unwrap();
        let internal_key = b"internal-jwks-exchange-key-0123456789";
        let config = crate::ServerConfig::new(
            "127.0.0.1:0".parse().unwrap(),
            "http://localhost".to_owned(),
            root.path().to_owned(),
            std::num::NonZeroUsize::new(65536).unwrap(),
        )
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_auth_provider(crate::AuthProviderKind::Jwks)
        .with_auth_jwks_url(url)
        .with_auth_jwks_audience("shardline".to_owned())
        .with_auth_jwks_issuer("https://example.com".to_owned())
        .with_token_signing_key(internal_key.to_vec())
        .unwrap();
        let app = crate::app::router(config).await.unwrap();
        let external = encode(&header, &claims, &encoding_key).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/lfs/objects/batch")
                    .header("authorization", format!("Bearer {external}"))
                    .header("content-type", "application/vnd.git-lfs+json")
                    .body(Body::from(
                        serde_json::json!({
                            "operation": "download", "transfers": ["xet"], "objects": []
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let batch: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(batch["transfer"], "xet");

        let verifier = make_provider(Some(CachedJwks {
            keys: Arc::new(vec![test_rsa_jwk("e2e-key")]),
            etag: None,
            refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
        }));
        let auth = crate::auth::ServerAuth::from_external_jwt_provider(
            Box::new(verifier),
            Some(internal_key),
        )
        .unwrap();
        let hub = shardline_hub_api::auth::HubAuth::from_arc(auth.provider_arc());
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {external}").parse().unwrap(),
        );
        let verified = hub.authorize(&headers, TokenScope::Read).unwrap();
        let internal = hub.provider().mint_token(verified.claims()).unwrap();
        headers.insert(
            "authorization",
            format!("Bearer {internal}").parse().unwrap(),
        );
        assert_eq!(
            auth.authorize_xet(&headers, TokenScope::Read)
                .unwrap()
                .claims(),
            &token_claims
        );
        assert!(auth.authorize_xet(&headers, TokenScope::Write).is_err());
        assert_eq!(
            hub.authorize(&headers, TokenScope::Read).unwrap().claims(),
            &token_claims
        );
        let read_only = TokenClaims::new(
            shardline_protocol::HUB_XET_READ_TOKEN_ISSUER,
            token_claims.subject(),
            TokenScope::Read,
            token_claims.repository().clone(),
            token_claims.expires_at_unix_seconds(),
        )
        .unwrap();
        let restricted = hub.provider().mint_token(&read_only).unwrap();
        headers.insert(
            "authorization",
            format!("Bearer {restricted}").parse().unwrap(),
        );
        assert!(auth.authorize_xet(&headers, TokenScope::Read).is_ok());
        assert!(auth.authorize_xet(&headers, TokenScope::Write).is_err());
        assert!(auth.authorize(&headers, TokenScope::Read).is_err());
        assert!(hub.authorize(&headers, TokenScope::Read).is_err());
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
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
            audience: None,
            claim_mapping: crate::external_jwt_claims::ExternalJwtClaims::default(),
            client: Client::new(),
            jwks_url: url,
            issuer: "https://example.com".to_owned(),
            cached_keys: Arc::new(RwLock::new(Some(CachedJwks {
                keys: Arc::new(vec![sample_rsa_jwk()]),
                etag: Some("my-etag".to_owned()),
                refresh_interval: DEFAULT_JWKS_REFRESH_INTERVAL,
            }))),
            background_handle: Arc::new(RefreshTask::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        let result = provider.refresh_keys_if_changed().await;
        assert!(result.is_err(), "expected Err for server error status");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("500"),
            "error should mention status code: {err}"
        );
    }

    #[test]
    fn verify_token_with_future_nbf_fails() {
        use jsonwebtoken::{EncodingKey, Header, encode};
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
        use jsonwebtoken::{EncodingKey, Header, encode};
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
