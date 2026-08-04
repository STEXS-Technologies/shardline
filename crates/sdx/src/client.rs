//! [`XetClient`] builder and handle for the sdx CAS read path (M2a / M2b1).
//!
//! The client maps a `xet://` endpoint URL (`docs/XET_NATIVE_CLI.md` "URL
//! Scheme") onto an API base URL and repository identity, holds the
//! [`TokenService`](crate::auth::TokenService) (M1), and exposes
//! [`DownloadSession`]s.
//!
//! ```text
//! xet://<host>[:<port>]/<provider>/<owner>/<repo>/<revision>
//! ```
//!
//! Path addressing (`xet://…/<revision>/<path>`) arrives in M5.

use std::{path::PathBuf, sync::Arc};

use url::Url;

use crate::{
    auth::{Auth, HttpConfig, RepositoryId},
    cache::{ChunkCache, DEFAULT_CHUNK_CACHE_BUDGET_BYTES},
    error::{SdxError, TransferError},
    group::XetStreamGroup,
    session::{DownloadSession, DownloadSessionInner},
    stream::{
        BufferSemaphore, DEFAULT_DOWNLOAD_BUFFER_LIMIT, DEFAULT_DOWNLOAD_BUFFER_SIZE,
        DEFAULT_DOWNLOAD_CONCURRENCY, DownloadStream, StreamLimits, UnorderedDownloadStream,
    },
    transfer::TransferClient,
};

/// A configured Xet client handle.
///
/// Clone is cheap: the handle shares the HTTP client, token service, endpoint
/// state, and the byte-denominated download buffer semaphore.
#[derive(Clone)]
pub struct XetClient {
    inner: Arc<DownloadSessionInner>,
}

impl XetClient {
    /// Creates a download session over the client's repository.
    #[must_use]
    pub fn download_session(&self) -> DownloadSession {
        DownloadSession {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Returns a pull-based streaming download of `file_id` (full file).
    ///
    /// See [`DownloadSession::download_stream`] for details.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed or token issuance fails.
    pub async fn download_stream(
        &self,
        file_id: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<DownloadStream, SdxError> {
        self.download_session()
            .download_stream(file_id, range)
            .await
    }

    /// Returns a pull-based streaming download of `file_id` that yields
    /// `(file_offset, Bytes)` pairs in completion order.
    ///
    /// See [`DownloadSession::download_unordered_stream`] for details.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed or token issuance fails.
    pub async fn download_unordered_stream(
        &self,
        file_id: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> Result<UnorderedDownloadStream, SdxError> {
        self.download_session()
            .download_unordered_stream(file_id, range)
            .await
    }

    /// Downloads `file_id` into the `std::io::Write` sink `writer`, returning
    /// the number of bytes written.
    ///
    /// See [`DownloadSession::download_to_writer`] for details.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// a fetch fails, or the writer fails.
    pub async fn download_to_writer<W: std::io::Write + Send + 'static>(
        &self,
        file_id: &str,
        writer: W,
    ) -> Result<u64, SdxError> {
        self.download_session()
            .download_to_writer(file_id, writer)
            .await
    }

    /// Downloads the whole file into a single [`bytes::Bytes`].
    ///
    /// See [`DownloadSession::download_bytes`] for details.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when `file_id` is malformed, token issuance fails,
    /// or a fetch fails.
    pub async fn download_bytes(&self, file_id: &str) -> Result<bytes::Bytes, SdxError> {
        self.download_session().download_bytes(file_id).await
    }

    /// Returns the CAS base URL from the most recently issued read/write
    /// token, if any (used to construct CAS transfer URLs when the response
    /// does not carry absolute URLs).
    #[must_use]
    pub fn cas_url(&self) -> Option<String> {
        self.inner.tokens.cas_url()
    }

    /// Creates a stream group over the client's repository.
    ///
    /// The group manages concurrent pull-based streams with abort-all and
    /// per-stream status (see [`XetStreamGroup`], `docs/SDX_PLAN.md` §4.4.3).
    #[must_use]
    pub fn new_download_stream_group(&self) -> XetStreamGroup {
        XetStreamGroup::new(self.inner.clone())
    }

    /// Returns the number of ranged xorb transfer requests issued by this
    /// client so far (cache misses only).
    #[must_use]
    pub fn xorb_fetch_count(&self) -> u64 {
        self.inner.xorb_fetch_count()
    }

    /// Returns the client-configured on-disk chunk cache, if any.
    #[must_use]
    pub fn chunk_cache(&self) -> Option<Arc<ChunkCache>> {
        self.inner.chunk_cache.clone()
    }
}

impl Default for XetClientBuilder {
    fn default() -> Self {
        Self {
            endpoint: None,
            auth: None,
            http: None,
            buffer_capacity: None,
            download_concurrency: DEFAULT_DOWNLOAD_CONCURRENCY,
            limits: StreamLimits::default(),
            chunk_cache_dir: None,
            chunk_cache: None,
            chunk_cache_budget: None,
        }
    }
}

/// Builder for [`XetClient`].
///
/// ```no_run
/// # async fn example() -> Result<(), sdx::SdxError> {
/// use sdx::{Auth, RepositoryId, XetClientBuilder};
///
/// let auth = Auth::new(
///     "http://127.0.0.1:8080",
///     RepositoryId {
///         provider: "github".to_owned(),
///         owner: "team".to_owned(),
///         repo: "assets".to_owned(),
///         revision: "main".to_owned(),
///     },
/// )?
/// .with_api_key("bootstrap".to_owned())
/// .with_subject("user".to_owned());
///
/// let client = XetClientBuilder::new()
///     .endpoint("xet://127.0.0.1:8080/github/team/assets/main")
///     .auth(auth)
///     .build()?;
/// let session = client.download_session();
/// # Ok(())
/// # }
/// ```
pub struct XetClientBuilder {
    endpoint: Option<String>,
    auth: Option<Auth>,
    http: Option<HttpConfig>,
    buffer_capacity: Option<u64>,
    download_concurrency: usize,
    limits: StreamLimits,
    chunk_cache_dir: Option<PathBuf>,
    chunk_cache: Option<Arc<ChunkCache>>,
    chunk_cache_budget: Option<u64>,
}

impl XetClientBuilder {
    /// Creates an empty builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `xet://` endpoint URL for the target repository.
    #[must_use]
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Sets the authentication configuration (M1 [`Auth`]).
    #[must_use]
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Sets the HTTP client timeouts used for CAS transfers.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn http(mut self, http: HttpConfig) -> Self {
        self.http = Some(http);
        self
    }

    /// Sets a fixed byte-denominated download buffer capacity (the memory
    /// bound) used by every streaming download from this client.
    ///
    /// With no cap set, the buffer is shared across active downloads and scaled
    /// per active download (`base 2 GiB + n × 512 MiB`, hard limit 8 GiB).
    /// Memory-constrained clients (e.g. the CLI `cat` path) should set a modest
    /// cap (64–256 MiB) per `docs/SDX_PLAN.md` §4.4.4.
    #[must_use]
    pub const fn with_buffer_semaphore(mut self, capacity_bytes: u64) -> Self {
        self.buffer_capacity = Some(capacity_bytes);
        self
    }

    /// Sets the fixed CAS connection-permit count (how many ranged xorb
    /// fetches run concurrently per client). Defaults to 4; the adaptive
    /// controller arrives in M4.
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_download_concurrency(mut self, concurrency: usize) -> Self {
        self.download_concurrency = concurrency.max(1);
        self
    }

    /// Sets the streaming prefetch/buffer limits (term-metadata prefetch block
    /// sizes, prefetch lead, estimator tuning).
    #[must_use]
    pub const fn with_stream_limits(mut self, limits: StreamLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Enables the on-disk chunk cache rooted at `cache_dir` with the default
    /// budget ([`DEFAULT_CHUNK_CACHE_BUDGET_BYTES`], 2 GiB).
    ///
    /// Every ranged xorb fetch is checked against the cache first and stored
    /// back on success (see [`ChunkCache`] and `docs/SDX_PLAN.md` §4.4.1
    /// step 3). Combine with [`with_chunk_cache_budget`](Self::with_chunk_cache_budget)
    /// to tune the budget.
    #[must_use]
    pub fn with_chunk_cache_dir(mut self, cache_dir: impl Into<PathBuf>) -> Self {
        self.chunk_cache_dir = Some(cache_dir.into());
        self
    }

    /// Uses a prebuilt [`ChunkCache`] (full control over budget and location).
    #[must_use]
    pub fn with_chunk_cache(mut self, cache: ChunkCache) -> Self {
        self.chunk_cache = Some(Arc::new(cache));
        self
    }

    /// Sets the budget (bytes) used by the cache directory configured with
    /// [`with_chunk_cache_dir`](Self::with_chunk_cache_dir). Ignored when a
    /// prebuilt cache is supplied.
    #[must_use]
    pub const fn with_chunk_cache_budget(mut self, budget_bytes: u64) -> Self {
        self.chunk_cache_budget = Some(budget_bytes);
        self
    }

    /// Builds the client.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the endpoint URL cannot be mapped to an API
    /// base and repository identity, no [`Auth`] is configured, the token
    /// service cannot be built, the HTTP client cannot be created, or the
    /// configured chunk-cache directory cannot be initialized. (The streaming
    /// blocking runtime is resolved lazily on first stream use.)
    pub fn build(self) -> Result<XetClient, SdxError> {
        let endpoint = self.endpoint.ok_or_else(|| {
            SdxError::InvalidEndpoint("no endpoint configured; use `.endpoint(...)`".to_owned())
        })?;
        let (api_base, repository) = parse_endpoint(&endpoint)?;
        let auth = self.auth.ok_or_else(|| {
            SdxError::InvalidEndpoint("no auth configured; use `.auth(...)`".to_owned())
        })?;
        let auth_repository = auth.repository();
        if &repository != auth_repository {
            return Err(SdxError::InvalidEndpoint(format!(
                "endpoint repository {repository:?} does not match the auth repository {auth_repository:?}"
            )));
        }
        let tokens = auth.build()?;
        let http = self.http.unwrap_or_default();
        let http_client = reqwest::Client::builder()
            .connect_timeout(http.connect_timeout())
            .timeout(http.request_timeout())
            .build()
            .map_err(TransferError::from)?;
        let (buffer_semaphore, limits) = match self.buffer_capacity {
            Some(capacity) => (
                Arc::new(BufferSemaphore::new(capacity, capacity, capacity)),
                self.limits,
            ),
            None => (
                Arc::new(BufferSemaphore::new(
                    DEFAULT_DOWNLOAD_BUFFER_SIZE,
                    DEFAULT_DOWNLOAD_BUFFER_SIZE,
                    DEFAULT_DOWNLOAD_BUFFER_LIMIT,
                )),
                self.limits,
            ),
        };
        let download_permits = Arc::new(tokio::sync::Semaphore::new(
            self.download_concurrency.max(1),
        ));
        let chunk_cache = match self.chunk_cache {
            Some(cache) => Some(cache),
            None => {
                let budget = self
                    .chunk_cache_budget
                    .unwrap_or(DEFAULT_CHUNK_CACHE_BUDGET_BYTES);
                match self.chunk_cache_dir {
                    Some(dir) => Some(Arc::new(ChunkCache::new(dir, budget)?)),
                    None => None,
                }
            }
        };
        Ok(XetClient {
            inner: Arc::new(DownloadSessionInner {
                transfer: TransferClient::new(http_client),
                tokens,
                api_base,
                buffer_semaphore,
                active_downloads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                download_permits,
                limits,
                chunk_cache,
                xorb_fetch_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            }),
        })
    }
}

/// Splits a `xet://host[:port]/provider/owner/repo/revision` URL into an HTTP
/// API base URL and a [`RepositoryId`].
fn parse_endpoint(endpoint: &str) -> Result<(String, RepositoryId), SdxError> {
    let url = Url::parse(endpoint).map_err(|error| {
        SdxError::InvalidEndpoint(format!("cannot parse {endpoint:?}: {error}"))
    })?;
    if url.scheme() != "xet" {
        return Err(SdxError::InvalidEndpoint(format!(
            "unsupported scheme {:?} in {endpoint:?}; expected xet://",
            url.scheme()
        )));
    }
    let host = url
        .host_str()
        .ok_or_else(|| SdxError::InvalidEndpoint(format!("missing host in {endpoint:?}")))?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(SdxError::InvalidEndpoint(format!(
            "query or fragment in {endpoint:?} is not supported"
        )));
    }

    let mut api_base = String::new();
    api_base.push_str("http://");
    api_base.push_str(host);
    if let Some(port) = url.port() {
        api_base.push(':');
        api_base.push_str(&port.to_string());
    }

    let segments: Vec<&str> = url
        .path_segments()
        .map(|segments| segments.collect())
        .unwrap_or_default();
    if segments.len() != 4 {
        return Err(SdxError::InvalidEndpoint(format!(
            "expected exactly provider/owner/repo/revision in {endpoint:?}, got {} segment(s)",
            segments.len()
        )));
    }
    let mut parts = segments.into_iter();
    let provider = parts
        .next()
        .ok_or_else(|| SdxError::InvalidEndpoint("missing provider".to_owned()))?
        .to_owned();
    let owner = parts
        .next()
        .ok_or_else(|| SdxError::InvalidEndpoint("missing owner".to_owned()))?
        .to_owned();
    let repo = parts
        .next()
        .ok_or_else(|| SdxError::InvalidEndpoint("missing repo".to_owned()))?
        .to_owned();
    let revision = parts
        .next()
        .ok_or_else(|| SdxError::InvalidEndpoint("missing revision".to_owned()))?
        .to_owned();

    Ok((
        api_base,
        RepositoryId {
            provider,
            owner,
            repo,
            revision,
        },
    ))
}

#[cfg(test)]
mod tests {
    use crate::{RepositoryId, error::SdxError};

    use super::parse_endpoint;

    #[test]
    fn parse_endpoint_maps_host_port_and_identity() {
        let (base, repository) =
            parse_endpoint("xet://127.0.0.1:8080/github/team/assets/main").unwrap();
        assert_eq!(base, "http://127.0.0.1:8080");
        assert_eq!(
            repository,
            RepositoryId {
                provider: "github".to_owned(),
                owner: "team".to_owned(),
                repo: "assets".to_owned(),
                revision: "main".to_owned(),
            }
        );
    }

    #[test]
    fn parse_endpoint_defaults_to_port_80() {
        let (base, _) = parse_endpoint("xet://example.com/github/team/assets/main").unwrap();
        assert_eq!(base, "http://example.com");
    }

    #[test]
    fn parse_endpoint_rejects_wrong_scheme() {
        let error = parse_endpoint("http://example.com/github/team/assets/main").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
        assert!(error.to_string().contains("xet://"));
    }

    #[test]
    fn parse_endpoint_rejects_missing_host() {
        let error = parse_endpoint("xet:///github/team/assets/main").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
    }

    #[test]
    fn parse_endpoint_rejects_wrong_segment_count() {
        let error = parse_endpoint("xet://host/github/team/assets").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
        let error = parse_endpoint("xet://host/github/team/assets/main/extra").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
    }

    #[test]
    fn parse_endpoint_rejects_query_and_fragment() {
        let error = parse_endpoint("xet://host/github/team/assets/main?x=1").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
        let error = parse_endpoint("xet://host/github/team/assets/main#frag").unwrap_err();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
    }
}
