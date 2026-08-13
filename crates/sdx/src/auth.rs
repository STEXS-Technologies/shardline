//! Authentication for Xet repositories: issue, cache, and transparently
//! refresh repo+revision-scoped read/write CAS tokens.
//!
//! Tokens are issued against the shardline token-issuance wire contract (the
//! "Step 1 — Token Issuance" section of `docs/XET_NATIVE_CLI.md`, design §5.2
//! of `docs/SDX_PLAN.md`):
//!
//! - `GET /api/{provider}/{owner}/{repo}/xet-read-token/{rev}` and
//!   `GET /api/{provider}/{owner}/{repo}/xet-write-token/{rev}`, using the
//!   route constants exported by `shardline-xet-adapter`.
//! - Authentication is either `Authorization: Bearer <server-token>` or
//!   `X-Shardline-Provider-Key: <api-key>`.
//! - The response is `{"casUrl": <string>, "exp": <unix-seconds>,
//!   "accessToken": <opaque-bearer>}` — the field names/types match the
//!   server's `XetCasTokenResponse` (`crates/shardline-server/src/model.rs`).
//!
//! Tokens are opaque to the client, repo+revision-scoped, and split read/write
//! (write ⊃ read). The [`TokenService`] caches `accessToken` + `exp` per scope
//! and transparently re-issues when fewer than
//! [`REFRESH_BUFFER_SECONDS`] remain before `exp`. Refresh is **single-flight**
//! (concurrent callers await the same in-flight issuance) and **loop-guarded**:
//! a token the server issues with less than the buffer remaining is surfaced as
//! [`AuthError::ShortLivedToken`] instead of being cached, which would
//! immediately trigger another refresh.

use std::{
    fmt,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::FutureExt;
use futures_util::future::Shared;
use reqwest::{RequestBuilder, Response, StatusCode, Url};
use serde::Deserialize;
use shardline_xet_adapter::{XET_READ_TOKEN_ROUTE, XET_WRITE_TOKEN_ROUTE};
use thiserror::Error;

use crate::config::{
    Credential, CredentialResolutionError, read_token_file, resolve_credential_from_env,
};

/// Refresh a cached token when fewer than this many seconds remain before `exp`.
/// The reference client uses the same 30-second buffer.
pub const REFRESH_BUFFER_SECONDS: u64 = 30;

/// Header carrying the provider bootstrap API key on token-issuance requests.
pub const PROVIDER_KEY_HEADER_NAME: &str = "x-shardline-provider-key";

/// Repository identity scoping token issuance.
///
/// Each segment is interpolated into the token route path, so all values are
/// the path-segment forms the server expects (e.g. `github`, `team`,
/// `assets`, `main`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryId {
    /// Provider name path segment.
    pub provider: String,
    /// Repository owner path segment.
    pub owner: String,
    /// Repository name path segment.
    pub repo: String,
    /// Revision path segment.
    pub revision: String,
}

/// HTTP client configuration for token-issuance requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpConfig {
    connect_timeout: Duration,
    request_timeout: Duration,
}

impl HttpConfig {
    /// Defaults: 60-second connect timeout, 60-second total request timeout.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            connect_timeout: Duration::from_secs(60),
            request_timeout: Duration::from_secs(60),
        }
    }

    /// Sets the connect timeout.
    #[must_use]
    pub const fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the total request timeout.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Returns the connect timeout.
    #[must_use]
    pub const fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the total request timeout.
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for the [`TokenService`]: endpoint, repository identity,
/// credential sources, optional subject, and HTTP timeouts.
///
/// Credential sources are resolved in priority order:
///
/// 1. explicit static token ([`Auth::with_token`])
/// 2. explicit API key ([`Auth::with_api_key`])
/// 3. token file ([`Auth::with_token_file`])
/// 4. environment (`SHARDLINE_TOKEN` / `SHARDLINE_API_KEY` /
///    `SHARDLINE_TOKEN_FILE`, resolved by [`crate::config`])
///
/// Config-file `[auth]` section parsing is CLI scope and is intentionally
/// not handled here.
#[derive(Clone)]
pub struct Auth {
    base_url: Url,
    repository: RepositoryId,
    token: Option<String>,
    api_key: Option<String>,
    token_file: Option<PathBuf>,
    subject: Option<String>,
    http: HttpConfig,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("base_url", &self.base_url)
            .field("repository", &self.repository)
            .field("token", &"<redacted>")
            .field("api_key", &"<redacted>")
            .field("token_file", &self.token_file)
            .field("subject", &self.subject)
            .field("http", &self.http)
            .finish()
    }
}

impl Auth {
    /// Creates an [`Auth`] for `repository` at `base_url`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InvalidBaseUrl`] when `base_url` is not a
    /// parseable HTTP(S) URL that can serve as a base for the token routes.
    pub fn new(base_url: &str, repository: RepositoryId) -> Result<Self, AuthError> {
        let base_url = Url::parse(base_url).map_err(|source| AuthError::InvalidBaseUrl {
            url: base_url.to_owned(),
            detail: source.to_string(),
        })?;
        if base_url.cannot_be_a_base() {
            return Err(AuthError::InvalidBaseUrl {
                url: base_url.to_string(),
                detail: "URL cannot be used as a base URL".to_owned(),
            });
        }
        Ok(Self {
            base_url,
            repository,
            token: None,
            api_key: None,
            token_file: None,
            subject: None,
            http: HttpConfig::default(),
        })
    }

    /// Returns the API base URL used for token issuance.
    #[must_use]
    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    /// Returns the repository identity this auth is scoped to.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryId {
        &self.repository
    }

    /// Sets an explicit opaque bearer token (highest credential priority).
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_token(mut self, token: String) -> Self {
        self.token = Some(token);
        self
    }

    /// Sets an explicit provider bootstrap API key.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_api_key(mut self, api_key: String) -> Self {
        self.api_key = Some(api_key);
        self
    }

    /// Sets an explicit token file path containing an opaque bearer token.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_token_file(mut self, path: PathBuf) -> Self {
        self.token_file = Some(path);
        self
    }

    /// Sets the optional `?subject=` query parameter sent with every issuance.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }

    /// Sets the HTTP client configuration (timeouts).
    #[must_use]
    pub const fn with_http(mut self, http: HttpConfig) -> Self {
        self.http = http;
        self
    }

    /// Builds the [`TokenService`] for this configuration.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Transport`] when the HTTP client cannot be built.
    pub fn build(self) -> Result<TokenService, AuthError> {
        let client = reqwest::Client::builder()
            .connect_timeout(self.http.connect_timeout)
            .timeout(self.http.request_timeout)
            .build()
            .map_err(|source| AuthError::Transport {
                message: source.to_string(),
                source: Arc::new(source),
            })?;
        Ok(TokenService::with_clock(
            client,
            self,
            Arc::new(unix_now_seconds),
        ))
    }

    fn token_url(&self, scope: Scope) -> Url {
        let mut url = self.base_url.clone();
        let base_path = url.path().trim_end_matches('/');
        let path = format!("{base_path}{}", self.token_path(scope));
        // `set_path` replaces the path wholesale; the base path prefix is
        // preserved by appending the route to it (mirrors
        // `shardline_xet_adapter::build_xorb_transfer_url`).
        url.set_path(&path);
        if let Some(subject) = &self.subject {
            url.query_pairs_mut().append_pair("subject", subject);
        }
        url
    }

    fn token_path(&self, scope: Scope) -> String {
        let route = match scope {
            Scope::Read => XET_READ_TOKEN_ROUTE,
            Scope::Write => XET_WRITE_TOKEN_ROUTE,
        };
        route
            .replace("{provider}", &self.repository.provider)
            .replace("{owner}", &self.repository.owner)
            .replace("{repo}", &self.repository.repo)
            .replace("{rev}", &self.repository.revision)
    }

    fn resolve_credential(
        &self,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Credential, AuthError> {
        if let Some(token) = &self.token {
            return Ok(Credential::Bearer(token.clone()));
        }
        if let Some(api_key) = &self.api_key {
            return Ok(Credential::ProviderKey(api_key.clone()));
        }
        if let Some(path) = &self.token_file {
            let contents = read_token_file(path).map_err(|source| AuthError::TokenFile {
                path: path.clone(),
                source: Arc::new(source),
            })?;
            return Ok(Credential::Bearer(contents));
        }
        let env_credential = resolve_credential_from_env(env)?;
        env_credential.ok_or(AuthError::MissingCredential)
    }
}

/// A repo+revision-scoped CAS token issued for one scope (read or write).
///
/// `token` is the opaque bearer string to present as
/// `Authorization: Bearer <token>` on CAS requests; `cas_url` is the base URL
/// for the CAS data plane advertised by the server.
#[derive(Clone, PartialEq, Eq)]
pub struct ScopedToken {
    /// Opaque bearer token for CAS requests.
    pub token: String,
    /// Token expiration timestamp as Unix seconds.
    pub exp: u64,
    /// CAS base URL advertised by the server (`casUrl`).
    pub cas_url: String,
}

impl fmt::Debug for ScopedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopedToken")
            .field("token", &"<redacted>")
            .field("exp", &self.exp)
            .field("cas_url", &self.cas_url)
            .finish()
    }
}

/// Errors surfaced by the token service.
///
/// Clone so errors can be shared between callers of a single-flight refresh.
#[derive(Debug, Clone, Error)]
pub enum AuthError {
    /// No credential resolved from any configured source.
    #[error(
        "no credential available: set a token, api key, or token file (or SHARDLINE_TOKEN / \
         SHARDLINE_API_KEY / SHARDLINE_TOKEN_FILE)"
    )]
    MissingCredential,
    /// The server rejected the credential with HTTP 401 Unauthorized.
    #[error("token issuance rejected (401 unauthorized): {message}")]
    Unauthorized {
        /// Error message from the server response body, when present.
        message: String,
    },
    /// The credential lacks the required scope (HTTP 403 Forbidden).
    #[error("token issuance denied (403 forbidden): {message}")]
    Forbidden {
        /// Error message from the server response body, when present.
        message: String,
    },
    /// Any other non-success HTTP status (e.g. 5xx).
    #[error("token issuance failed with HTTP {status}: {message}")]
    HttpStatus {
        /// The HTTP status code.
        status: u16,
        /// Error message from the server response body, when present.
        message: String,
    },
    /// The server issued a token that expires within the refresh buffer.
    #[error("token issued with expiration {exp} within the {buffer}s refresh buffer")]
    ShortLivedToken {
        /// The expiration timestamp the server issued.
        exp: u64,
        /// The configured refresh buffer in seconds.
        buffer: u64,
    },
    /// Transport-level failure (connect, DNS, timeout, TLS, ...).
    #[error("token issuance transport error: {message}")]
    Transport {
        /// Human-readable description of the transport failure.
        message: String,
        /// Underlying reqwest error.
        #[source]
        source: Arc<reqwest::Error>,
    },
    /// The response body could not be parsed as a token response.
    #[error("failed to parse token issuance response: {message}")]
    Parse {
        /// Human-readable description of the parse failure.
        message: String,
        /// Underlying serialization error.
        #[source]
        source: Arc<serde_json::Error>,
    },
    /// The configured token file could not be read.
    #[error("failed to read token file {path:?}: {source}")]
    TokenFile {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: Arc<std::io::Error>,
    },
    /// The configured base URL is not usable for token issuance.
    #[error("invalid token issuance base URL {url:?}: {detail}")]
    InvalidBaseUrl {
        /// The offending URL.
        url: String,
        /// Why it was rejected.
        detail: String,
    },
}

impl From<reqwest::Error> for AuthError {
    fn from(source: reqwest::Error) -> Self {
        Self::Transport {
            message: source.to_string(),
            source: Arc::new(source),
        }
    }
}

impl From<serde_json::Error> for AuthError {
    fn from(source: serde_json::Error) -> Self {
        Self::Parse {
            message: source.to_string(),
            source: Arc::new(source),
        }
    }
}

impl From<CredentialResolutionError> for AuthError {
    fn from(error: CredentialResolutionError) -> Self {
        match error {
            CredentialResolutionError::TokenFile { path, source } => AuthError::TokenFile {
                path,
                source: Arc::new(source),
            },
        }
    }
}

/// Type of an in-flight token issuance.
type RefreshFuture = Pin<Box<dyn Future<Output = Result<ScopedToken, AuthError>> + Send>>;

/// Environment variable lookup used for credential fallback.
type EnvLookup = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

/// Scope of a CAS token: read (downloads) or write (uploads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scope {
    /// `xet-read-token` scope.
    Read,
    /// `xet-write-token` scope.
    Write,
}

/// Per-scope cache plus the in-flight refresh future for single-flight.
#[derive(Default)]
struct CacheState {
    read: Option<ScopedToken>,
    write: Option<ScopedToken>,
    read_inflight: Option<Shared<RefreshFuture>>,
    write_inflight: Option<Shared<RefreshFuture>>,
}

/// Token-issuance client with per-scope caching and transparent refresh.
///
/// Resolves the repo+revision-scoped read/write CAS token used for downloads
/// and uploads. Cheap to clone and share across tasks: the shared state lives
/// behind an [`Arc`], and at most one issuance request is in flight per scope
/// (concurrent callers share the same in-flight refresh).
///
/// # Examples
///
/// ```no_run
/// # async fn example() -> Result<(), sdx::AuthError> {
/// use sdx::{Auth, RepositoryId};
///
/// let tokens = Auth::new(
///     "http://127.0.0.1:8080",
///     RepositoryId {
///         provider: "github".to_owned(),
///         owner: "team".to_owned(),
///         repo: "assets".to_owned(),
///         revision: "main".to_owned(),
///     },
/// )?
/// .with_api_key("bootstrap".to_owned())
/// .build()?;
///
/// // Resolve a read-scoped token for downloads (`write_token` for uploads);
/// // repeated calls hit the cache and only re-issue near expiration.
/// let read = tokens.read_token().await?;
/// println!("CAS base: {}", read.cas_url);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct TokenService {
    inner: Arc<TokenServiceInner>,
}

struct TokenServiceInner {
    client: reqwest::Client,
    auth: Auth,
    env: EnvLookup,
    clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<CacheState>,
}

impl TokenService {
    fn with_clock(
        client: reqwest::Client,
        auth: Auth,
        clock: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self::with_deps(client, auth, clock, Arc::new(process_env))
    }

    fn with_deps(
        client: reqwest::Client,
        auth: Auth,
        clock: Arc<dyn Fn() -> u64 + Send + Sync>,
        env: EnvLookup,
    ) -> Self {
        Self {
            inner: Arc::new(TokenServiceInner {
                client,
                auth,
                env,
                clock,
                state: Mutex::new(CacheState::default()),
            }),
        }
    }

    /// Returns a read-scoped CAS token, issuing or refreshing as needed.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when no credential is configured, issuance fails,
    /// the response cannot be parsed, or the server issues a token already
    /// inside the refresh buffer.
    pub async fn read_token(&self) -> Result<ScopedToken, AuthError> {
        self.token(Scope::Read).await
    }

    /// Returns a write-scoped CAS token, issuing or refreshing as needed.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] when no credential is configured, issuance fails,
    /// the response cannot be parsed, or the server issues a token already
    /// inside the refresh buffer.
    pub async fn write_token(&self) -> Result<ScopedToken, AuthError> {
        self.token(Scope::Write).await
    }

    /// Returns the CAS base URL from the most recently issued token, if any.
    #[must_use]
    pub fn cas_url(&self) -> Option<String> {
        let state = self.lock_state();
        state
            .read
            .as_ref()
            .or(state.write.as_ref())
            .map(|token| token.cas_url.clone())
    }

    /// Core single-flight, loop-guarded token resolution for `scope`.
    async fn token(&self, scope: Scope) -> Result<ScopedToken, AuthError> {
        let now = self.now();
        let shared = {
            let mut state = self.lock_state();
            if let Some(cached) = cached(scope, &state)
                && cached.exp.saturating_sub(now) >= REFRESH_BUFFER_SECONDS
            {
                return Ok(cached);
            }
            inflight(scope, &state).unwrap_or_else(|| {
                let future = self.start_refresh(scope);
                let started = future.shared();
                *inflight_mut(scope, &mut state) = Some(started.clone());
                started
            })
        };
        let result = shared.await;
        let mut state = self.lock_state();
        *inflight_mut(scope, &mut state) = None;
        let token = result?;
        let remaining = token.exp.saturating_sub(self.now());
        if remaining < REFRESH_BUFFER_SECONDS {
            // Loop guard: the server issued a token already inside the refresh
            // buffer (or already expired). Surfacing an error instead of
            // caching it prevents an immediate re-issue loop on the next call.
            return Err(AuthError::ShortLivedToken {
                exp: token.exp,
                buffer: REFRESH_BUFFER_SECONDS,
            });
        }
        *cached_mut(scope, &mut state) = Some(token.clone());
        Ok(token)
    }

    fn start_refresh(&self, scope: Scope) -> RefreshFuture {
        let this = Arc::clone(&self.inner);
        Box::pin(async move { this.issue(scope).await })
    }

    /// Drops the cached read token so the next [`read_token`](Self::read_token)
    /// re-issues one. Used by the M4 retry layer to force a fresh token after a
    /// 401/403 (the server rejected the cached token even though it is within
    /// its expiration window).
    pub(crate) fn invalidate_read(&self) {
        self.invalidate(Scope::Read);
    }

    /// Drops the cached write token so the next [`write_token`](Self::write_token)
    /// re-issues one. Used by the M4 retry layer on upload 403s.
    pub(crate) fn invalidate_write(&self) {
        self.invalidate(Scope::Write);
    }

    fn invalidate(&self, scope: Scope) {
        let mut state = self.lock_state();
        *cached_mut(scope, &mut state) = None;
    }

    fn now(&self) -> u64 {
        (self.inner.clock)()
    }

    fn lock_state(&self) -> MutexGuard<'_, CacheState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl TokenServiceInner {
    async fn issue(&self, scope: Scope) -> Result<ScopedToken, AuthError> {
        let credential = self.auth.resolve_credential(self.env.as_ref())?;
        let url = self.auth.token_url(scope);
        let request = self.client.get(url);
        let request = apply_credential(request, credential);
        let response = request.send().await?;
        let status = response.status();
        if !status.is_success() {
            let message = error_message(response).await;
            return Err(http_error(status, message));
        }
        let bytes = response.bytes().await?;
        let parsed: XetCasTokenResponse = serde_json::from_slice(&bytes)?;
        Ok(ScopedToken {
            token: parsed.access_token,
            exp: parsed.exp,
            cas_url: parsed.cas_url,
        })
    }
}

fn cached(scope: Scope, state: &CacheState) -> Option<ScopedToken> {
    match scope {
        Scope::Read => state.read.clone(),
        Scope::Write => state.write.clone(),
    }
}

const fn cached_mut(scope: Scope, state: &mut CacheState) -> &mut Option<ScopedToken> {
    match scope {
        Scope::Read => &mut state.read,
        Scope::Write => &mut state.write,
    }
}

fn inflight(scope: Scope, state: &CacheState) -> Option<Shared<RefreshFuture>> {
    match scope {
        Scope::Read => state.read_inflight.clone(),
        Scope::Write => state.write_inflight.clone(),
    }
}

fn inflight_mut(scope: Scope, state: &mut CacheState) -> &mut Option<Shared<RefreshFuture>> {
    match scope {
        Scope::Read => &mut state.read_inflight,
        Scope::Write => &mut state.write_inflight,
    }
}

fn apply_credential(request: RequestBuilder, credential: Credential) -> RequestBuilder {
    match credential {
        Credential::Bearer(token) => request.bearer_auth(token),
        Credential::ProviderKey(key) => request.header(PROVIDER_KEY_HEADER_NAME, key),
    }
}

const fn http_error(status: StatusCode, message: String) -> AuthError {
    let code = status.as_u16();
    match code {
        401 => AuthError::Unauthorized { message },
        403 => AuthError::Forbidden { message },
        _ => AuthError::HttpStatus {
            status: code,
            message,
        },
    }
}

async fn error_message(response: Response) -> String {
    let body = response.text().await.unwrap_or_default();
    serde_json::from_str::<ErrorBody>(&body)
        .map(|parsed| parsed.error)
        .unwrap_or(body)
}

/// Server error envelope `{"error": "..."}`.
#[derive(Debug, Deserialize)]
struct ErrorBody {
    error: String,
}

/// Wire response type matching the server's `XetCasTokenResponse`.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct XetCasTokenResponse {
    cas_url: String,
    exp: u64,
    access_token: String,
}

fn process_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    };

    use serde_json::json;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{header, method, path, query_param},
    };

    use super::{Auth, AuthError, EnvLookup, REFRESH_BUFFER_SECONDS, RepositoryId, TokenService};
    use crate::config::{SHARDLINE_API_KEY_ENV, SHARDLINE_TOKEN_ENV, SHARDLINE_TOKEN_FILE_ENV};

    /// Fixed "now" for clock-controlled tests (far from any real unix time).
    const NOW: u64 = 1_000_000_000;

    fn repository() -> RepositoryId {
        RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: "main".to_owned(),
        }
    }

    fn read_path() -> String {
        "/api/github/team/assets/xet-read-token/main".to_owned()
    }

    fn write_path() -> String {
        "/api/github/team/assets/xet-write-token/main".to_owned()
    }

    fn token_response(exp: u64, access_token: &str, cas_url: &str) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(json!({
            "casUrl": cas_url,
            "exp": exp,
            "accessToken": access_token,
        }))
    }

    fn service(auth: Auth, now: &Arc<AtomicU64>) -> TokenService {
        let clock: Arc<dyn Fn() -> u64 + Send + Sync> = {
            let now = Arc::clone(now);
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        TokenService::with_clock(reqwest::Client::new(), auth, clock)
    }

    fn service_with_env(auth: Auth, env: HashMap<String, String>) -> TokenService {
        let now = Arc::new(AtomicU64::new(NOW));
        let clock: Arc<dyn Fn() -> u64 + Send + Sync> = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        let env: EnvLookup = Arc::new(move |name| env.get(name).cloned());
        TokenService::with_deps(reqwest::Client::new(), auth, clock, env)
    }

    async fn request_count(server: &MockServer) -> usize {
        server.received_requests().await.unwrap_or_default().len()
    }

    fn bearer_auth(token: &str) -> String {
        format!("Bearer {token}")
    }

    #[tokio::test]
    async fn read_token_uses_read_endpoint_with_interpolated_path() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let token = service.read_token().await.unwrap();
        assert_eq!(token.token, "read-token");
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn write_token_uses_write_endpoint_with_interpolated_path() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(write_path()))
            .respond_with(token_response(NOW + 3600, "write-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let token = service.write_token().await.unwrap();
        assert_eq!(token.token, "write-token");
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn read_and_write_scopes_issue_distinct_tokens() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(write_path()))
            .respond_with(token_response(NOW + 3600, "write-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let read = service.read_token().await.unwrap();
        let write = service.write_token().await.unwrap();
        assert_eq!(read.token, "read-token");
        assert_eq!(write.token, "write-token");
        assert_ne!(read.token, write.token);
        assert_eq!(request_count(&server).await, 2);
    }

    #[tokio::test]
    async fn bearer_token_sent_via_authorization_header() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("authorization", &bearer_auth("server-token")))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let token = service.read_token().await.unwrap();
        assert_eq!(token.token, "read-token");
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn api_key_sent_via_provider_key_header() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("x-shardline-provider-key", "bootstrap-key"))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_api_key("bootstrap-key".to_owned());
        let service = service(auth, &now);

        let token = service.read_token().await.unwrap();
        assert_eq!(token.token, "read-token");
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn parses_exact_camel_case_response_fields() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        // Literal wire body matching the server's XetCasTokenResponse.
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                br#"{"casUrl":"http://cas.internal:8080/","exp":1700000000,"accessToken":"opaque-bearer"}"#,
                "application/json",
            ))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let token = service.read_token().await.unwrap();
        assert_eq!(token.token, "opaque-bearer");
        assert_eq!(token.exp, 1_700_000_000);
        assert_eq!(token.cas_url, "http://cas.internal:8080/");
    }

    #[tokio::test]
    async fn cas_url_surfaced_from_response() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let token = service.read_token().await.unwrap();
        assert_eq!(token.cas_url, server.uri());
        assert_eq!(service.cas_url(), Some(server.uri()));
    }

    #[tokio::test]
    async fn caching_repeated_calls_issue_once() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "cached-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let first = service.read_token().await.unwrap();
        let second = service.read_token().await.unwrap();
        let third = service.read_token().await.unwrap();
        assert_eq!(first.token, "cached-token");
        assert_eq!(second.token, "cached-token");
        assert_eq!(third.token, "cached-token");
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn refresh_token_within_buffer_reissues() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        // First issuance: a long-lived token.
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "token-1", &server.uri()))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // After the clock enters the 30s buffer, the second issuance gets a
        // fresh long-lived token. Same priority => insertion order decides.
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 7200, "token-2", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let first = service.read_token().await.unwrap();
        assert_eq!(first.token, "token-1");
        assert_eq!(request_count(&server).await, 1);

        // Advance the clock so the cached token is inside the 30s buffer.
        now.store(NOW + 3600 - REFRESH_BUFFER_SECONDS + 1, Ordering::Relaxed);
        let second = service.read_token().await.unwrap();
        assert_eq!(second.token, "token-2");
        assert_eq!(request_count(&server).await, 2);
    }

    #[tokio::test]
    async fn single_flight_concurrent_reads_one_request() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(200))
                    .set_body_json(json!({
                        "casUrl": server.uri(),
                        "exp": NOW + 3600,
                        "accessToken": "shared-token",
                    })),
            )
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let service = service.clone();
                tokio::spawn(async move { service.read_token().await })
            })
            .collect();
        let results = futures_util::future::join_all(tasks).await;
        for result in results {
            let token = result.unwrap().unwrap();
            assert_eq!(token.token, "shared-token");
        }
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn refresh_loop_guard_surfaces_error_and_stays_bounded() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 5, "too-short", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let first = service.read_token().await;
        assert!(matches!(first, Err(AuthError::ShortLivedToken { .. })));
        let second = service.read_token().await;
        assert!(matches!(second, Err(AuthError::ShortLivedToken { .. })));
        // Each call issues at most one request; no refresh loop is entered.
        assert_eq!(request_count(&server).await, 2);
    }

    #[tokio::test]
    async fn unauthorized_status_maps_to_typed_error() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(
                ResponseTemplate::new(401).set_body_json(json!({"error": "missing provider key"})),
            )
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let err = service.read_token().await.unwrap_err();
        assert!(matches!(err, AuthError::Unauthorized { .. }));
        assert!(err.to_string().contains("missing provider key"));
    }

    #[tokio::test]
    async fn forbidden_status_maps_to_typed_error() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(
                ResponseTemplate::new(403).set_body_json(json!({"error": "insufficient scope"})),
            )
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let err = service.read_token().await.unwrap_err();
        assert!(matches!(err, AuthError::Forbidden { .. }));
        assert!(err.to_string().contains("insufficient scope"));
    }

    #[tokio::test]
    async fn server_error_status_maps_to_typed_error() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error": "internal"})))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let err = service.read_token().await.unwrap_err();
        assert!(matches!(err, AuthError::HttpStatus { status: 500, .. }));
        assert!(err.to_string().contains("internal"));
    }

    #[tokio::test]
    async fn non_json_response_body_is_parse_error() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw(b"<html>not a token response</html>", "text/html"),
            )
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        let err = service.read_token().await.unwrap_err();
        assert!(matches!(err, AuthError::Parse { .. }));
    }

    #[tokio::test]
    async fn subject_query_param_transmitted_when_configured() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(query_param("subject", "alice"))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned())
            .with_subject("alice".to_owned());
        let service = service(auth, &now);

        service.read_token().await.unwrap();
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            requests[0].url.path(),
            "/api/github/team/assets/xet-read-token/main"
        );
        assert_eq!(requests[0].url.query(), Some("subject=alice"));
    }

    #[tokio::test]
    async fn no_subject_query_when_not_configured() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        Mock::given(method("GET"))
            .and(path(read_path()))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("server-token".to_owned());
        let service = service(auth, &now);

        service.read_token().await.unwrap();
        let requests = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            requests[0].url.path(),
            "/api/github/team/assets/xet-read-token/main"
        );
        assert_eq!(requests[0].url.query(), None);
    }

    #[tokio::test]
    async fn explicit_token_beats_env_credentials() {
        let server = MockServer::start().await;
        let mut env = HashMap::new();
        env.insert(SHARDLINE_TOKEN_ENV.to_owned(), "env-token".to_owned());
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("authorization", &bearer_auth("explicit-token")))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token("explicit-token".to_owned());
        let service = service_with_env(auth, env);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn explicit_api_key_beats_env_token() {
        let server = MockServer::start().await;
        let mut env = HashMap::new();
        env.insert(SHARDLINE_TOKEN_ENV.to_owned(), "env-token".to_owned());
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("x-shardline-provider-key", "explicit-key"))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_api_key("explicit-key".to_owned());
        let service = service_with_env(auth, env);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn missing_credential_errors_before_any_request() {
        let server = MockServer::start().await;
        let auth = Auth::new(&server.uri(), repository()).unwrap();
        let service = service_with_env(auth, HashMap::new());

        let err = service.read_token().await.unwrap_err();
        assert!(matches!(err, AuthError::MissingCredential));
        assert_eq!(request_count(&server).await, 0);
    }

    #[tokio::test]
    async fn token_file_credential_reads_and_sends_bearer() {
        let server = MockServer::start().await;
        let now = Arc::new(AtomicU64::new(NOW));
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "  file-token\n").unwrap();
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("authorization", &bearer_auth("file-token")))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository())
            .unwrap()
            .with_token_file(token_file);
        let service = service(auth, &now);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn env_token_used_when_no_explicit_credential() {
        let server = MockServer::start().await;
        let mut env = HashMap::new();
        env.insert(SHARDLINE_TOKEN_ENV.to_owned(), "env-token".to_owned());
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("authorization", &bearer_auth("env-token")))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository()).unwrap();
        let service = service_with_env(auth, env);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn env_api_key_used_when_no_explicit_credential() {
        let server = MockServer::start().await;
        let mut env = HashMap::new();
        env.insert(SHARDLINE_API_KEY_ENV.to_owned(), "env-key".to_owned());
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("x-shardline-provider-key", "env-key"))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository()).unwrap();
        let service = service_with_env(auth, env);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[tokio::test]
    async fn env_token_file_credential_reads_and_sends_bearer() {
        let server = MockServer::start().await;
        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("token");
        std::fs::write(&token_file, "  env-file-token\n").unwrap();
        let mut env = HashMap::new();
        env.insert(
            SHARDLINE_TOKEN_FILE_ENV.to_owned(),
            token_file.to_string_lossy().into_owned(),
        );
        Mock::given(method("GET"))
            .and(path(read_path()))
            .and(header("authorization", &bearer_auth("env-file-token")))
            .respond_with(token_response(NOW + 3600, "read-token", &server.uri()))
            .mount(&server)
            .await;
        let auth = Auth::new(&server.uri(), repository()).unwrap();
        let service = service_with_env(auth, env);

        service.read_token().await.unwrap();
        assert_eq!(request_count(&server).await, 1);
    }

    #[test]
    fn invalid_base_url_rejected_at_construction() {
        let error = Auth::new("not a url", repository()).unwrap_err();
        assert!(matches!(error, AuthError::InvalidBaseUrl { .. }));
    }

    #[test]
    fn non_base_url_rejected_at_construction() {
        let error = Auth::new("mailto:user@example.com", repository()).unwrap_err();
        assert!(matches!(error, AuthError::InvalidBaseUrl { .. }));
    }
}
