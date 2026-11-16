//! Retry / backoff / token-refresh transport behavior (M4, `docs/SDX_PLAN.md`
//! §4.4.4 + §6.3).
//!
//! [`RetryPolicy`] is the user-configurable retry configuration (mirroring the
//! reference client's `RetryWrapper` defaults: 5 attempts, 3 s base delay,
//! 6 min max duration, exponential + jitter). [`RetryContext`] bundles a policy
//! with the token service, scope, and refresh behavior, and drives an attempt
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
//!   refreshes the URL for signed-URL ranged fetches when `refresh_on_forbidden` is
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
/// a factor in [0.5, 1.5), rounded down to nanoseconds and capped at
/// `max_duration` after jitter. For computed backoff, zero base or maximum
/// duration gives an immediate retry. Honored `Retry-After` overrides the base
/// delay. `honor_retry_after` makes a 429/503/504 response's
/// `Retry-After` delay-seconds or HTTP-date override the computed backoff. `retry_on_429` can be
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

/// Policy, token service, scope, and refresh behavior for a retryable request.
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
    /// Whether a 403 triggers token refresh and one retry for this request kind.
    pub(crate) refresh_on_forbidden: bool,
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
                    if !refreshed
                        && (self.refresh_on_forbidden
                            || matches!(&error, TransferError::Unauthorized(_))) =>
                {
                    let Some(tokens) = &self.tokens else {
                        return Err(error);
                    };
                    token = self.refresh_token(tokens).await?;
                    refreshed = true;
                }
                Err(error)
                    if is_retryable(&error, self.policy.retry_on_429)
                        && requests < max_requests =>
                {
                    let delay = self.backoff_delay(requests.saturating_sub(1), &error);
                    tokio::time::sleep(delay).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn refresh_token(&self, tokens: &TokenService) -> Result<String, TransferError> {
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
    fn backoff_delay(&self, attempt: u32, error: &TransferError) -> Duration {
        if self.policy.honor_retry_after
            && let Some(retry_after) = error.retry_after()
        {
            return retry_after.min(self.policy.max_duration);
        }
        let exponential = self
            .policy
            .base_delay
            .saturating_mul(1u32 << attempt.min(16));
        let capped = exponential.min(self.policy.max_duration);
        if !self.policy.jitter {
            return capped;
        }
        // Duration has at most u64::MAX seconds. Nanoseconds times a factor
        // below 1500 fit in u128; saturation also makes that bound explicit.
        // Integer thousandths preserve the deterministic jitter distribution
        // without quantizing submillisecond durations or losing float precision.
        let jittered_nanos = capped
            .as_nanos()
            .saturating_mul(u128::from(jitter_per_mille(attempt)))
            / 1000;
        let nanos = jittered_nanos.min(self.policy.max_duration.as_nanos());
        let seconds = u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX);
        let subsecond = u32::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999);
        Duration::new(seconds, subsecond)
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

/// Deterministic jitter in integer thousandths, in [500, 1500).
fn jitter_per_mille(attempt: u32) -> u32 {
    let x = u64::from(attempt)
        .wrapping_mul(2_654_435_761)
        .wrapping_add(0x9E37_79B9_7F4A_7C15);
    u32::try_from(x % 1000).unwrap_or(0).saturating_add(500)
}

#[cfg(test)]
#[allow(clippy::float_arithmetic)] // Human-readable distribution checks only.
fn jitter_factor(attempt: u32) -> f64 {
    f64::from(jitter_per_mille(attempt)) / 1000.0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::auth::TokenService;

    use super::{RetryContext, RetryPolicy, RetryScope, is_retryable, jitter_factor};
    use crate::error::TransferError;

    fn transient_error() -> TransferError {
        TransferError::HttpStatus {
            status: 503,
            message: String::new(),
            retry_after: None,
        }
    }

    #[test]
    fn backoff_preserves_submillisecond_duration_and_growth_limit() {
        for base in [Duration::from_micros(500), Duration::from_micros(1500)] {
            let ctx = retry_context(
                RetryPolicy::new()
                    .with_base_delay(base)
                    .with_max_duration(Duration::MAX)
                    .with_jitter(false),
                None,
            );
            assert_eq!(ctx.backoff_delay(0, &transient_error()), base);
            assert_eq!(
                ctx.backoff_delay(1, &transient_error()),
                base.saturating_mul(2)
            );
            assert_eq!(
                ctx.backoff_delay(16, &transient_error()),
                ctx.backoff_delay(u32::MAX, &transient_error())
            );
        }
    }

    #[test]
    fn final_jittered_backoff_never_exceeds_configured_cap() {
        let maximum = Duration::from_millis(200);
        let ctx = retry_context(
            RetryPolicy::new()
                .with_base_delay(Duration::from_secs(10))
                .with_max_duration(maximum),
            None,
        );
        assert_eq!(ctx.backoff_delay(3, &transient_error()), maximum);
        assert_eq!(
            ctx.backoff_delay(0, &transient_error()),
            Duration::from_millis(197)
        );
        for attempt in 0..200 {
            assert!(ctx.backoff_delay(attempt, &transient_error()) <= maximum);
        }
    }

    #[test]
    fn huge_duration_caps_do_not_wrap_or_panic() {
        for maximum in [
            Duration::MAX,
            Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1)),
        ] {
            let ctx = retry_context(
                RetryPolicy::new()
                    .with_base_delay(Duration::from_millis(100))
                    .with_max_duration(maximum)
                    .with_jitter(false),
                None,
            );
            assert_eq!(
                ctx.backoff_delay(0, &transient_error()),
                Duration::from_millis(100)
            );
            let ctx = retry_context(
                RetryPolicy::new()
                    .with_base_delay(Duration::MAX)
                    .with_max_duration(maximum),
                None,
            );
            for attempt in 0..20 {
                assert!(ctx.backoff_delay(attempt, &transient_error()) <= maximum);
            }
        }
    }

    #[test]
    fn nanosecond_rounding_zero_and_retry_after_controls() {
        let error = transient_error();
        let ctx = retry_context(
            RetryPolicy::new()
                .with_base_delay(Duration::from_nanos(1))
                .with_max_duration(Duration::from_secs(1)),
            None,
        );
        assert_eq!(ctx.backoff_delay(0, &error), Duration::ZERO);
        assert_eq!(ctx.backoff_delay(3, &error), Duration::from_nanos(10));
        for policy in [
            RetryPolicy::new().with_base_delay(Duration::ZERO),
            RetryPolicy::new().with_max_duration(Duration::ZERO),
        ] {
            let ctx = retry_context(policy, None);
            assert_eq!(ctx.backoff_delay(0, &error), Duration::ZERO);
        }
        let capped = Duration::from_micros(500);
        let ctx = retry_context(RetryPolicy::new().with_max_duration(capped), None);
        for seconds in [0, 1, u64::MAX] {
            let error = TransferError::HttpStatus {
                status: 503,
                message: String::new(),
                retry_after: Some(seconds),
            };
            assert_eq!(
                ctx.backoff_delay(3, &error),
                Duration::from_secs(seconds).min(capped)
            );
        }
        let ctx = retry_context(
            RetryPolicy::new()
                .with_base_delay(Duration::from_millis(7))
                .with_jitter(false)
                .with_honor_retry_after(false),
            None,
        );
        let error = TransferError::HttpStatus {
            status: 503,
            message: String::new(),
            retry_after: Some(100),
        };
        assert_eq!(ctx.backoff_delay(0, &error), Duration::from_millis(7));
    }

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

        assert!(!is_retryable(
            &TransferError::BadRequest(String::new()),
            true
        ));
        assert!(!is_retryable(&TransferError::NotFound(String::new()), true));
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

    /// Uses the real bounded transport, status mapping, and Retry-After parser.
    async fn probe(server: &MockServer, token: &str) -> Result<(), TransferError> {
        crate::transfer::TransferClient::new(reqwest::Client::new())
            .request_raw(
                &reqwest::Method::GET,
                &format!("{}/probe", server.uri()),
                token,
                None,
                1024,
            )
            .await
            .map(|_response| ())
    }

    fn retry_context(policy: RetryPolicy, tokens: Option<TokenService>) -> RetryContext {
        RetryContext {
            policy,
            tokens,
            scope: RetryScope::Read,
            refresh_on_forbidden: false,
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
        for jitter in [false, true] {
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
                    .with_jitter(jitter),
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
            .expect("retry backoff exceeded timeout");
            assert!(result.is_ok());
            // 3 failures + 1 success = 4 requests.
            assert_eq!(request_count(&server).await, 4);
        }
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
    async fn retry_policy_controls_429_request_count() {
        for (policy, expected_requests) in [
            (RetryPolicy::new().with_retry_on_429(false), 1),
            (RetryPolicy::dedup(), 1),
            (RetryPolicy::new(), 3),
        ] {
            let server = Arc::new(MockServer::start().await);
            Mock::given(method("GET"))
                .and(path("/probe"))
                .respond_with(
                    ResponseTemplate::new(429).set_body_json(json!({"error": "overloaded"})),
                )
                .mount(&server)
                .await;
            let ctx = retry_context(
                policy.with_max_attempts(2).with_base_delay(Duration::ZERO),
                None,
            );
            let result = ctx
                .run("tok".to_owned(), |tok| {
                    let server = server.clone();
                    async move { probe(&server, &tok).await }
                })
                .await;
            assert!(matches!(result, Err(TransferError::TooManyRequests { .. })));
            assert_eq!(request_count(&server).await, expected_requests);
        }
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
            refresh_on_forbidden: false,
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
    async fn forbidden_refresh_requires_tokens_and_opt_in_and_is_loop_guarded() {
        for (has_tokens, refresh_on_forbidden, expected_requests) in [
            (false, false, 1),
            (false, true, 1),
            (true, false, 1),
            (true, true, 3), // Two probes and one token issuance; never refresh twice.
        ] {
            let server = Arc::new(MockServer::start().await);
            Mock::given(method("GET"))
                .and(path("/probe"))
                .respond_with(
                    ResponseTemplate::new(403).set_body_json(json!({"error": "no scope"})),
                )
                .mount(&server)
                .await;
            mount_write_token(&server, "still-forbidden").await;

            let ctx = RetryContext {
                policy: RetryPolicy::new().with_max_attempts(5),
                tokens: has_tokens.then(|| write_token_service(&server)),
                scope: RetryScope::Write,
                refresh_on_forbidden,
            };
            let result = ctx
                .run("tok".to_owned(), |tok| {
                    let server = server.clone();
                    async move { probe(&server, &tok).await }
                })
                .await;
            assert!(matches!(result, Err(TransferError::Forbidden(_))));
            assert_eq!(request_count(&server).await, expected_requests);
        }
    }
}
