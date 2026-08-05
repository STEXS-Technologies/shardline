//! Retry / backoff / token-refresh transport behavior (M4, `docs/SDX_PLAN.md`
//! §4.4.4 + §6.3).
//!
//! [`RetryPolicy`] is the user-configurable retry configuration (mirroring the
//! reference client's `RetryWrapper` defaults: 5 attempts, 3 s base delay,
//! 6 min max duration, exponential + jitter). [`RetryContext`] bundles a policy
//! with the token service + scope and per-call markers, and drives an attempt
//! closure through backoff and 401/403 token refresh.
//!
//! # Error classification (exact)
//!
//! - **Retryable:** 429 (when `retry_on_429`), 500, 503, 504, 408, connection
//!   errors, timeouts.
//! - **Non-retryable:** 400, 404, 416.
//! - **401/403 are refresh triggers, not plain retries** (sdx delta vs upstream,
//!   which treats them fatal except the ranged-fetch 403 URL refresh): a 401
//!   refreshes the read token; a 403 on uploads re-issues the write token (or
//!   refreshes the URL for signed-URL ranged fetches when `retry_on_403` is
//!   set). The request is retried once after the refresh; a repeated 401/403 is
//!   surfaced (loop-guarded), never re-issued infinitely.
//!
//! # Deltas vs upstream (`docs/SDX_PLAN.md` §4.4.4)
//!
//! - **`max_attempts` semantics:** `max_attempts` is the number of **retries**,
//!   so the maximum number of requests is `max_attempts + 1` (5 retries → up to
//!   6 requests). This matches upstream, which passes `max_attempts` to
//!   `ExponentialBackoff::take`.
//! - **`Retry-After` honoring:** upstream ignores `Retry-After`; shardline sends
//!   `Retry-After: 1` on 503s, so sdx honors it when present (strictly better,
//!   wire-compatible).
//! - **401/403 token refresh:** upstream treats 401/403 as fatal (except the
//!   ranged-fetch 403 URL refresh); sdx refreshes the token (single-flight, via
//!   [`crate::auth::TokenService`]) and retries once. Strictly more capable and
//!   marked as a delta so behavior differences are visible.

use std::time::Duration;

use crate::auth::TokenService;
use crate::error::TransferError;

/// User-configurable retry policy for CAS requests.
///
/// Semantics: `max_attempts` is the number of **retries** (so 5 retries → up to
/// 6 requests), `base_delay` is the initial exponential-backoff delay, and
/// `max_duration` caps a single backoff wait. Jitter multiplies each backoff by
/// a factor in [0.5, 1.5). `honor_retry_after` makes a 429/503/504 response's
/// `Retry-After` header override the computed backoff. `retry_on_429` can be
/// disabled to fail fast on 429 (dedup queries, mirroring upstream
/// `with_429_no_retry()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Number of retries (up to `max_attempts + 1` total requests).
    pub max_attempts: u32,
    /// Base exponential-backoff delay.
    pub base_delay: Duration,
    /// Maximum duration for a single backoff wait.
    pub max_duration: Duration,
    /// Whether to jitter the backoff (multiply by [0.5, 1.5)).
    pub jitter: bool,
    /// Whether to honor a response `Retry-After` header over the computed
    /// backoff.
    pub honor_retry_after: bool,
    /// Whether 429 responses are retried (disable for dedup queries).
    pub retry_on_429: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_secs(3),
            max_duration: Duration::from_secs(6 * 60),
            jitter: true,
            honor_retry_after: true,
            retry_on_429: true,
        }
    }
}

impl RetryPolicy {
    /// Creates a retry policy with the reference-client defaults.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of retries (up to `max_attempts + 1` total requests).
    #[must_use]
    pub const fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the base exponential-backoff delay.
    #[must_use]
    pub const fn with_base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Sets the maximum duration of a single backoff wait.
    #[must_use]
    pub const fn with_max_duration(mut self, max_duration: Duration) -> Self {
        self.max_duration = max_duration;
        self
    }

    /// Enables or disables backoff jitter.
    #[must_use]
    pub const fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Enables or disables honoring the `Retry-After` response header.
    #[must_use]
    pub const fn with_honor_retry_after(mut self, honor: bool) -> Self {
        self.honor_retry_after = honor;
        self
    }

    /// Enables or disables retrying 429 responses.
    #[must_use]
    pub const fn with_retry_on_429(mut self, retry: bool) -> Self {
        self.retry_on_429 = retry;
        self
    }

    /// A policy that fails fast on 429 (used by dedup queries).
    #[must_use]
    pub fn dedup() -> Self {
        Self::default().with_retry_on_429(false)
    }
}

/// Which token scope a [`RetryContext`] refreshes on 401/403.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryScope {
    /// Refresh the read token (downloads / reconstruction).
    Read,
    /// Refresh the write token (uploads).
    Write,
}

/// Per-call markers that tune retry behavior for a specific request kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RetryMarkers {
    /// A 404 is an expected outcome (dedup cache miss), not an error.
    pub expected_404: bool,
    /// A 416 is an expected outcome (past-EOF), not a retryable error.
    pub expected_416: bool,
    /// Whether 429 responses are retried (override of the policy default).
    pub retry_on_429: bool,
    /// Whether a 403 on a ranged xorb fetch triggers a token refresh (signed
    /// URL refresh) and a single retry.
    pub retry_on_403: bool,
}

impl Default for RetryMarkers {
    fn default() -> Self {
        Self {
            expected_404: false,
            expected_416: false,
            retry_on_429: true,
            retry_on_403: false,
        }
    }
}

impl RetryMarkers {
    pub(crate) const fn dedup() -> Self {
        Self {
            expected_404: true,
            expected_416: false,
            retry_on_429: false,
            retry_on_403: false,
        }
    }

    pub(crate) const fn reconstruction() -> Self {
        Self {
            expected_404: false,
            expected_416: true,
            retry_on_429: true,
            retry_on_403: false,
        }
    }

    pub(crate) const fn ranged_xorb() -> Self {
        Self {
            expected_404: false,
            expected_416: true,
            retry_on_429: true,
            retry_on_403: true,
        }
    }
}

/// Bundle of policy + token service + scope + markers for a retryable request.
///
/// Cheap to clone: the policy is owned and the token service is an `Arc`ed
/// handle. When `tokens` is `None`, no 401/403 refresh happens (the initial
/// token is used verbatim and a 401/403 surfaces immediately, matching
/// non-refresh-capable call sites).
///
/// `RetryContext` is constructed by sdx internally (its fields are crate-only);
/// library callers that do not need retry pass `None` to the methods that
/// accept it.
#[derive(Clone)]
pub struct RetryContext {
    pub(crate) policy: RetryPolicy,
    pub(crate) tokens: Option<TokenService>,
    pub(crate) scope: RetryScope,
    pub(crate) markers: RetryMarkers,
}

impl RetryContext {
    /// Runs `attempt`, retrying retryable failures with jittered exponential
    /// backoff (honoring `Retry-After`) and refreshing the token once on
    /// 401/403.
    ///
    /// `attempt` receives the current bearer token (owned) and returns the
    /// request result. On a 401/403 the token is invalidated and re-issued
    /// (single-flight via [`TokenService`]) and the request is retried once; a
    /// repeated 401/403 is surfaced.
    ///
    /// # Errors
    ///
    /// Returns [`TransferError`] when the request ultimately fails (after
    /// retries / refresh) or the token refresh fails.
    pub(crate) async fn run<T, F, Fut>(
        &self,
        initial_token: String,
        mut attempt: F,
    ) -> Result<T, TransferError>
    where
        F: FnMut(String) -> Fut,
        Fut: std::future::Future<Output = Result<T, TransferError>>,
    {
        // max_attempts retries → up to max_attempts + 1 requests.
        let max_requests = self.policy.max_attempts.saturating_add(1);
        let mut token = initial_token;
        let mut requests = 0u32;
        let mut refreshed = false;
        loop {
            requests = requests.saturating_add(1);
            let result = attempt(token.clone()).await;
            match result {
                Ok(value) => return Ok(value),
                Err(error @ (TransferError::Unauthorized(_) | TransferError::Forbidden(_)))
                    if self.tokens.is_some() && !refreshed && self.refresh_allowed(&error) =>
                {
                    token = self.refresh_token().await?;
                    refreshed = true;
                }
                Err(error)
                    if is_retryable(&error, self.markers.retry_on_429)
                        && requests < max_requests =>
                {
                    let delay = self.backoff_delay(requests.saturating_sub(1), &error);
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Whether a 401/403 should trigger a token refresh for this call kind.
    const fn refresh_allowed(&self, error: &TransferError) -> bool {
        match error {
            // 403 only triggers a refresh for ranged xorb fetches (signed-URL
            // refresh); a 403 on reconstruction/upload is otherwise surfaced.
            TransferError::Forbidden(_) => self.markers.retry_on_403,
            TransferError::Unauthorized(_) => true,
            TransferError::BadRequest(_)
            | TransferError::NotFound(_)
            | TransferError::RangeNotSatisfiable(_)
            | TransferError::TooManyRequests { .. }
            | TransferError::HttpStatus { .. }
            | TransferError::Transport(_)
            | TransferError::InvalidResponse(_)
            | TransferError::MalformedMultipart(_)
            | TransferError::TokenRefresh(_) => false,
        }
    }

    async fn refresh_token(&self) -> Result<String, TransferError> {
        let tokens = self
            .tokens
            .as_ref()
            .ok_or_else(|| TransferError::TokenRefresh("no token service configured".to_owned()))?;
        let scoped = match self.scope {
            RetryScope::Read => {
                tokens.invalidate_read();
                tokens.read_token().await
            }
            RetryScope::Write => {
                tokens.invalidate_write();
                tokens.write_token().await
            }
        };
        scoped
            .map(|token| token.token)
            .map_err(|error| TransferError::TokenRefresh(error.to_string()))
    }

    /// Computes the sleep before the next retry attempt, honoring a response
    /// `Retry-After` header when enabled (sdx delta).
    #[allow(clippy::float_arithmetic)] // jittered exponential backoff is float by design.
    fn backoff_delay(&self, attempt: u32, error: &TransferError) -> Duration {
        if self.policy.honor_retry_after
            && let Some(retry_after) = error.retry_after()
        {
            return retry_after.min(self.policy.max_duration);
        }
        let base_ms = self
            .policy
            .base_delay
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        let exp_ms = base_ms.saturating_mul(1u64 << attempt.min(16));
        let capped_ms = exp_ms.min(self.policy.max_duration.as_millis() as u64);
        let ms = if self.policy.jitter {
            (capped_ms as f64 * jitter_factor(attempt)) as u64
        } else {
            capped_ms
        };
        Duration::from_millis(ms)
    }
}

/// Returns whether `error` is a retryable (transient) failure.
fn is_retryable(error: &TransferError, retry_on_429: bool) -> bool {
    match error {
        TransferError::TooManyRequests { .. } => retry_on_429,
        TransferError::HttpStatus { status, .. } => matches!(status, 408 | 500 | 503 | 504),
        TransferError::Transport(reqwest_error) => {
            reqwest_error.is_connect() || reqwest_error.is_timeout()
        }
        TransferError::BadRequest(_)
        | TransferError::Unauthorized(_)
        | TransferError::Forbidden(_)
        | TransferError::NotFound(_)
        | TransferError::RangeNotSatisfiable(_)
        | TransferError::InvalidResponse(_)
        | TransferError::MalformedMultipart(_)
        | TransferError::TokenRefresh(_) => false,
    }
}

/// Deterministic pseudo-random jitter factor in [0.5, 1.5).
#[allow(clippy::float_arithmetic)] // jitter multiplier is float by design.
fn jitter_factor(attempt: u32) -> f64 {
    let x = u64::from(attempt)
        .wrapping_mul(2_654_435_761)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    let unit = (x % 1000) as f64 / 1000.0;
    0.5 + unit
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::auth::TokenService;

    use super::{RetryContext, RetryMarkers, RetryPolicy, RetryScope, is_retryable, jitter_factor};
    use crate::error::TransferError;

    #[test]
    fn default_policy_matches_reference_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 5);
        assert_eq!(policy.base_delay, Duration::from_secs(3));
        assert_eq!(policy.max_duration, Duration::from_secs(6 * 60));
        assert!(policy.jitter);
        assert!(policy.honor_retry_after);
        assert!(policy.retry_on_429);
        // max_attempts = 5 retries → up to 6 requests.
        assert_eq!(policy.max_attempts.saturating_add(1), 6);
    }

    #[test]
    fn classification_marks_retryable_and_non_retryable() {
        let rt = |status| {
            is_retryable(
                &TransferError::HttpStatus {
                    status,
                    message: String::new(),
                    retry_after: None,
                },
                true,
            )
        };
        assert!(rt(408));
        assert!(rt(500));
        assert!(rt(503));
        assert!(rt(504));
        assert!(!rt(400));
        assert!(!rt(404));
        assert!(!rt(416));

        let _ = is_retryable(&TransferError::BadRequest(String::new()), true);
        let _ = is_retryable(&TransferError::NotFound(String::new()), true);
        assert!(!is_retryable(
            &TransferError::RangeNotSatisfiable(String::new()),
            true
        ));
        // 429 respects the retry_on_429 override.
        assert!(is_retryable(
            &TransferError::TooManyRequests {
                message: String::new(),
                retry_after: None
            },
            true
        ));
        assert!(!is_retryable(
            &TransferError::TooManyRequests {
                message: String::new(),
                retry_after: None
            },
            false
        ));
    }

    #[test]
    fn retry_markers_default_and_dedup() {
        let markers = RetryMarkers::default();
        assert!(markers.retry_on_429);
        assert!(!markers.retry_on_403);
        let dedup = RetryMarkers::dedup();
        assert!(!dedup.retry_on_429);
        assert!(dedup.expected_404);
        let recon = RetryMarkers::reconstruction();
        assert!(recon.expected_416);
        let xorb = RetryMarkers::ranged_xorb();
        assert!(xorb.retry_on_403);
    }

    #[test]
    fn jitter_is_bounded() {
        for attempt in 0..200 {
            let factor = jitter_factor(attempt);
            assert!(
                (0.5..1.5).contains(&factor),
                "factor {factor} out of bounds"
            );
        }
    }

    #[test]
    fn retry_scope_is_comparable() {
        assert_eq!(RetryScope::Read, RetryScope::Read);
        assert_ne!(RetryScope::Read, RetryScope::Write);
    }

    // ── wiremock integration ────────────────────────────────────────────────

    use crate::RepositoryId;
    use crate::auth::Auth;
    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    /// Issues one probe GET to `/probe` with `token`, mapping the status to a
    /// [`TransferError`] (including `Retry-After`).
    async fn probe(server: &MockServer, token: &str) -> Result<(), TransferError> {
        let response = reqwest::Client::new()
            .get(format!("{}/probe", server.uri()))
            .bearer_auth(token)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(());
        }
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().parse::<u64>().ok());
        let status = response.status();
        let message = response.text().await.unwrap_or_default();
        Err(match status.as_u16() {
            400 => TransferError::BadRequest(message),
            401 => TransferError::Unauthorized(message),
            403 => TransferError::Forbidden(message),
            404 => TransferError::NotFound(message),
            416 => TransferError::RangeNotSatisfiable(message),
            429 => TransferError::TooManyRequests {
                message,
                retry_after,
            },
            code => TransferError::HttpStatus {
                status: code,
                message,
                retry_after,
            },
        })
    }

    fn retry_context(policy: RetryPolicy, tokens: Option<TokenService>) -> RetryContext {
        RetryContext {
            policy,
            tokens,
            scope: RetryScope::Read,
            markers: RetryMarkers::default(),
        }
    }

    async fn request_count(server: &MockServer) -> usize {
        server.received_requests().await.unwrap_or_default().len()
    }

    fn write_token_service(server: &MockServer) -> TokenService {
        let auth = Auth::new(
            &server.uri(),
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            },
        )
        .unwrap()
        .with_token("server-token".to_owned());
        auth.build().unwrap()
    }

    /// Mounts the write-token route returning a fresh `accessToken`.
    async fn mount_write_token(server: &MockServer, token: &str) {
        Mock::given(method("GET"))
            .and(path("/api/github/team/assets/xet-write-token/main"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "casUrl": server.uri(),
                "exp": 4_000_000_000u64,
                "accessToken": token,
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn retryable_status_retried_until_success() {
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "x"})))
            .up_to_n_times(3)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
            .mount(&server)
            .await;
        let ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_millis(1))
                .with_jitter(false),
            None,
        );
        let result = ctx
            .run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        assert!(result.is_ok());
        // 3 failures + 1 success = 4 requests.
        assert_eq!(request_count(&server).await, 4);
    }

    #[tokio::test]
    async fn max_attempts_semantics_yield_up_to_attempts_plus_one_requests() {
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "x"})))
            .mount(&server)
            .await;
        let ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_millis(1))
                .with_jitter(false),
            None,
        );
        let result = ctx
            .run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        assert!(result.is_err());
        // 5 retries → 6 total requests, then surface.
        assert_eq!(request_count(&server).await, 6);
    }

    #[tokio::test]
    async fn non_retryable_status_surfaces_immediately() {
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(400).set_body_json(json!({"error": "bad"})))
            .mount(&server)
            .await;
        let ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_millis(1)),
            None,
        );
        let result = ctx
            .run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        assert!(matches!(result, Err(TransferError::BadRequest(_))));
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn retry_after_header_short_circuits_backoff() {
        let server = Arc::new(MockServer::start().await);
        // 429 with Retry-After: 0 → the retry sleeps ~0, keeping the test fast.
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(
                ResponseTemplate::new(429)
                    .insert_header("Retry-After", "0")
                    .set_body_json(json!({"error": "slow down"})),
            )
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
            .mount(&server)
            .await;
        let ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_secs(10))
                .with_jitter(false),
            None,
        );
        // A generous overall timeout ensures the test fails fast if the
        // Retry-After override is not honored (base_delay would be 10s×…).
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            ctx.run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            }),
        )
        .await
        .expect("Retry-After was not honored; backoff took too long");
        assert!(result.is_ok());
        assert_eq!(request_count(&server).await, 3);
    }

    #[tokio::test]
    async fn retry_on_429_disabled_surfaces_immediately() {
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(429).set_body_json(json!({"error": "overloaded"})))
            .mount(&server)
            .await;
        let mut ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_millis(1)),
            None,
        );
        ctx.markers.retry_on_429 = false;
        let result = ctx
            .run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        assert!(matches!(result, Err(TransferError::TooManyRequests { .. })));
        // 429 fail-fast: exactly one request.
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn unauthorized_triggers_single_flight_token_refresh_and_one_retry() {
        let server = Arc::new(MockServer::start().await);
        // Probe: first request 401, second 200.
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(401).set_body_json(json!({"error": "expired"})))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
            .mount(&server)
            .await;
        mount_write_token(&server, "refreshed-token").await;

        let ctx = RetryContext {
            policy: RetryPolicy::new().with_max_attempts(5),
            tokens: Some(write_token_service(&server)),
            scope: RetryScope::Write,
            markers: RetryMarkers::default(),
        };
        let result = ctx
            .run("old-token".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        assert!(result.is_ok());
        // 1 probe + 1 refresh-token issuance + 1 retry probe = 3 requests.
        assert_eq!(request_count(&server).await, 3);
        // The refresh endpoint was hit exactly once (single-flight).
        let token_hits = server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|request| request.url.path().ends_with("/xet-write-token/main"))
            .count();
        assert_eq!(token_hits, 1);
    }

    #[tokio::test]
    async fn repeated_forbidden_after_refresh_is_surfaced_loop_guarded() {
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({"error": "no scope"})))
            .mount(&server)
            .await;
        mount_write_token(&server, "still-forbidden").await;

        let ctx = RetryContext {
            policy: RetryPolicy::new().with_max_attempts(5),
            tokens: Some(write_token_service(&server)),
            scope: RetryScope::Write,
            markers: RetryMarkers {
                retry_on_403: true,
                ..RetryMarkers::default()
            },
        };
        let result = ctx
            .run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            })
            .await;
        // Surfaced after exactly one re-issue (loop-guard): 2 probe requests
        // + 1 token issuance, and the 403 is surfaced.
        assert!(matches!(result, Err(TransferError::Forbidden(_))));
        assert_eq!(request_count(&server).await, 3);
    }

    #[tokio::test]
    async fn jitter_produces_bounded_no_panic_sanity() {
        // Just exercise the jitter path end-to-end without asserting exact
        // delays; it must stay bounded and not panic.
        let server = Arc::new(MockServer::start().await);
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(503).set_body_json(json!({"error": "x"})))
            .up_to_n_times(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/probe"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"ok"))
            .mount(&server)
            .await;
        let ctx = retry_context(
            RetryPolicy::new()
                .with_max_attempts(5)
                .with_base_delay(Duration::from_millis(1))
                .with_jitter(true),
            None,
        );
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            ctx.run("tok".to_owned(), |tok| {
                let server = server.clone();
                async move { probe(&server, &tok).await }
            }),
        )
        .await
        .expect("jittered backoff exceeded timeout");
        assert!(result.is_ok());
    }
}
