//! Build and use a Xet client: download files from (and upload files to) a
//! Xet repository, addressed by 64-hex content id.
//!
//! A [`XetClient`] maps a `xet://` endpoint URL onto an API base URL and a
//! repository identity, holds the [`TokenService`](crate::auth::TokenService)
//! that issues repo-scoped CAS tokens, and exposes downloads (and uploads)
//! over [`DownloadSession`]s. The endpoint form is:
//!
//! ```text
//! xet://<host>[:<port>]/<provider>/<owner>/<repo>/<revision>
//! ```
//!
//! (Milestone labels like `M2a`/`M2b1` in the code below refer to the internal
//! `docs/SDX_PLAN.md` design history; path addressing arrives in M5.)

use std::{path::PathBuf, sync::Arc};

use url::Url;

use crate::{
    auth::{Auth, HttpConfig, RepositoryId},
    cache::{ChunkCache, DEFAULT_CHUNK_CACHE_BUDGET_BYTES},
    dedup::DedupClient,
    error::{SdxError, TransferError},
    group::XetStreamGroup,
    retry::RetryPolicy,
    session::{DownloadSession, DownloadSessionInner},
    stream::{
        BufferSemaphore, DEFAULT_DOWNLOAD_BUFFER_LIMIT, DEFAULT_DOWNLOAD_BUFFER_SIZE,
        DEFAULT_DOWNLOAD_CONCURRENCY, DownloadStream, StreamLimits, UnorderedDownloadStream,
    },
    transfer::TransferClient,
};

/// A configured Xet client handle for one repository.
///
/// Built with [`XetClientBuilder`], then used to download files by 64-hex
/// content id (streaming with bounded memory) or upload new ones. Clone is
/// cheap: the handle shares the HTTP client, token service, endpoint state,
/// and the byte-denominated download buffer semaphore.
///
/// # Examples
///
/// Build a client and download a file:
///
/// ```no_run
/// # async fn example() -> Result<(), sdx::SdxError> {
/// use sdx::{Auth, RepositoryId, XetClientBuilder};
///
/// let client = XetClientBuilder::new()
///     .endpoint("xet://127.0.0.1:8080/github/team/assets/main")
///     .auth(
///         Auth::new(
///             "http://127.0.0.1:8080",
///             RepositoryId {
///                 provider: "github".to_owned(),
///                 owner: "team".to_owned(),
///                 repo: "assets".to_owned(),
///                 revision: "main".to_owned(),
///             },
///         )?
///         .with_api_key("bootstrap".to_owned()),
///     )
///     .build()?;
///
/// // `file_id` is the 64-hex content hash of the file to fetch.
/// let file_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
///
/// // In-memory convenience path (modest files):
/// let bytes = client.download_bytes(file_id).await?;
///
/// // Or stream chunk-by-chunk with bounded memory:
/// let mut stream = client.download_stream(file_id, None).await?;
/// while let Some(chunk) = stream.next().await? {
///     println!("chunk of {} bytes", chunk.len());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct XetClient {
    inner: Arc<DownloadSessionInner>,
}

impl XetClient {
    /// Crate-internal access to the shared session state (metadata modules).
    pub(crate) const fn download_inner(&self) -> &Arc<DownloadSessionInner> {
        &self.inner
    }

    /// Creates a download session over the client's repository.
    #[must_use]
    pub fn download_session(&self) -> DownloadSession {
        DownloadSession {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Downloads `file_id` as a streaming, bounded-memory byte reader (the
    /// full file).
    ///
    /// The returned [`DownloadStream`] yields `Bytes` chunks in file order; the
    /// file is never buffered whole. See [`DownloadSession::download_stream`]
    /// for details.
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

    /// Downloads `file_id` as a streaming, bounded-memory byte reader that
    /// yields `(file_offset, Bytes)` pairs in completion order (first chunk to
    /// finish first).
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

    /// Creates a stream group for running concurrent downloads under one
    /// handle, with abort-all and per-stream status (see [`XetStreamGroup`]).
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

    /// Returns a [`DedupClient`] for querying the global dedup store, sharing
    /// this client's CAS transport.
    ///
    /// Resolve a write-scoped token and pass its `token` and the CAS base URL
    /// to [`DedupClient::query_for_global_dedup_shard`].
    #[must_use]
    pub fn dedup_client(&self) -> DedupClient {
        DedupClient::new(self.inner.transfer.clone())
    }

    /// Creates an [`crate::upload::UploadSession`] for writing files into this
    /// client's repository.
    ///
    /// A session is reusable across multiple files and must be finalized with
    /// [`crate::upload::UploadSession::finalize`] (which uploads the session shard).
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the session's dedicated shard-upload client
    /// cannot be built.
    pub fn upload_session(&self) -> Result<crate::upload::UploadSession, SdxError> {
        crate::upload::UploadSession::new(&self.inner)
    }

    /// Uploads the local file at `path` and registers it under the remote path
    /// `remote` in the client's revision, returning the content-derived
    /// `file_id`. After this returns, `resolve_path(remote)` resolves to the
    /// uploaded file.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the file cannot be read or any upload or
    /// registration step fails.
    pub async fn upload_file(
        &self,
        path: impl AsRef<std::path::Path>,
        remote: &str,
    ) -> Result<crate::upload::UploadFileInfo, SdxError> {
        let session = self.upload_session()?;
        let info = session.upload_file(path, remote).await?;
        session.finalize().await?;
        Ok(info)
    }

    /// Uploads an in-memory payload as a content-addressed file and registers
    /// it under the remote path `remote`.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when any upload or registration step fails.
    pub async fn upload_bytes(
        &self,
        remote: &str,
        bytes: impl Into<bytes::Bytes>,
    ) -> Result<crate::upload::UploadFileInfo, SdxError> {
        let session = self.upload_session()?;
        let info = session.upload_bytes(remote, bytes).await?;
        session.finalize().await?;
        Ok(info)
    }

    /// Uploads a `std::io::Read` stream as a content-addressed file and
    /// registers it under the remote path `remote`.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the reader fails or any upload or
    /// registration step fails.
    pub async fn upload_stream<R: std::io::Read + Send + 'static>(
        &self,
        remote: &str,
        reader: R,
    ) -> Result<crate::upload::UploadFileInfo, SdxError> {
        let session = self.upload_session()?;
        let info = session.upload_stream(remote, reader).await?;
        session.finalize().await?;
        Ok(info)
    }

    /// Creates an upload group for running multiple concurrent uploads under
    /// one handle.
    ///
    /// The group owns an [`crate::upload::UploadSession`]; handles returned by
    /// [`crate::group::XetUploadCommit::upload_stream`] fan into the same pipeline and the
    /// group's `commit()` finalizes it.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the group's upload session cannot be built.
    pub fn new_upload_group(&self) -> Result<crate::group::XetUploadCommit, SdxError> {
        crate::group::XetUploadCommit::new(&self.inner)
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
            upload_concurrency: crate::upload::DEFAULT_UPLOAD_CONCURRENCY,
            limits: StreamLimits::default(),
            chunk_cache_dir: None,
            chunk_cache: None,
            chunk_cache_budget: None,
            upload_chunk_size: crate::chunker::DEFAULT_TARGET_CHUNK_SIZE,
            retry_policy: RetryPolicy::default(),
            session_id: None,
        }
    }
}

/// Builder for [`XetClient`]: configure the `xet://` endpoint, authentication,
/// memory/concurrency limits, chunk cache, and retry policy, then build.
///
/// # Examples
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
/// let _client = XetClientBuilder::new()
///     .endpoint("xet://127.0.0.1:8080/github/team/assets/main")
///     .auth(auth)
///     // Bound streaming memory to 256 MiB and allow 8 concurrent fetches.
///     .with_buffer_semaphore(256 * 1024 * 1024)
///     .with_download_concurrency(8)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct XetClientBuilder {
    endpoint: Option<String>,
    auth: Option<Auth>,
    http: Option<HttpConfig>,
    buffer_capacity: Option<u64>,
    download_concurrency: usize,
    upload_concurrency: usize,
    limits: StreamLimits,
    chunk_cache_dir: Option<PathBuf>,
    chunk_cache: Option<Arc<ChunkCache>>,
    chunk_cache_budget: Option<u64>,
    upload_chunk_size: usize,
    retry_policy: RetryPolicy,
    session_id: Option<String>,
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

    /// Configures the client from a full `xet://` URL (identity + optional
    /// path). The repository identity comes from the URL; the path is ignored
    /// for building (path addressing is the M5 tree API's job). The endpoint
    /// is set to the URL's 4-segment identity form.
    #[must_use]
    pub fn from_url(mut self, url: &crate::url::XetUrl) -> Self {
        self.endpoint = Some(url.endpoint_url());
        self
    }

    /// Sets the authentication configuration ([`Auth`]): how the client
    /// obtains repo-scoped CAS tokens.
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

    /// Sets how many ranged chunk fetches may run concurrently per client
    /// (default 4). Higher values can improve throughput on high-latency links
    /// at the cost of more in-flight requests.
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

    /// Sets the content-defined chunking (CDC) target chunk size used by
    /// uploads.
    ///
    /// Must be a power of two and greater than 64; the default is 64 KiB
    /// (mirroring the server's chunker).
    #[must_use]
    pub const fn with_upload_chunk_size(mut self, chunk_size: usize) -> Self {
        self.upload_chunk_size = chunk_size;
        self
    }

    /// Sets the fixed upload connection-permit count (how many xorb/shard
    /// uploads run concurrently per session). Defaults to 2 (M4 §6.4).
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_upload_concurrency(mut self, concurrency: usize) -> Self {
        self.upload_concurrency = concurrency.max(1);
        self
    }

    /// Sets the retry policy applied to all CAS requests (attempts, backoff,
    /// and 401/403 token refresh).
    #[must_use]
    pub const fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Sets a stable `X-Xet-Session-Id` sent on every request (defaults to a
    /// generated per-client id).
    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
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
        // M4 timeouts (§4.4.4): connect 60 s, read 300 s (resets per packet),
        // idle 60 s. No fixed total request timeout so long downloads are not
        // capped at a wall-clock limit.
        let http_client = reqwest::Client::builder()
            .connect_timeout(http.connect_timeout())
            .read_timeout(std::time::Duration::from_secs(300))
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .build()
            .map_err(TransferError::from)?;
        let session_id = self.session_id.clone().unwrap_or_default();
        let mut transfer = TransferClient::new(http_client);
        if !session_id.is_empty() {
            transfer = transfer.with_session_id(session_id);
        }
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
                transfer,
                tokens,
                api_base,
                buffer_semaphore,
                active_downloads: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                download_permits,
                limits,
                chunk_cache,
                xorb_fetch_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
                upload_chunk_size: self.upload_chunk_size,
                upload_concurrency: self.upload_concurrency,
                retry_policy: self.retry_policy,
                repository,
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
    use crate::{Auth, RepositoryId, XetClientBuilder, error::SdxError};

    use super::parse_endpoint;

    fn repository() -> RepositoryId {
        RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: "main".to_owned(),
        }
    }

    /// `from_url` uses the URL's 4-segment identity; building succeeds when the
    /// auth repository matches it.
    #[test]
    fn builder_from_url_builds_with_matching_auth_scope() {
        let url =
            crate::url::XetUrl::parse("xet://example.com/github/team/assets/main/dir/file.txt")
                .unwrap();
        let auth = Auth::new("http://example.com", repository())
            .unwrap()
            .with_api_key("bootstrap".to_owned());
        let client = XetClientBuilder::new()
            .from_url(&url)
            .auth(auth)
            .build()
            .unwrap();
        let _ = client;
    }

    /// `from_url` with a repository that mismatches the auth repository still
    /// fails the build's repo cross-check.
    #[test]
    fn builder_from_url_mismatched_auth_scope_errors() {
        let url = crate::url::XetUrl::parse("xet://example.com/github/other/assets/main").unwrap();
        let auth = Auth::new("http://example.com", repository())
            .unwrap()
            .with_api_key("bootstrap".to_owned());
        let error = XetClientBuilder::new()
            .from_url(&url)
            .auth(auth)
            .build()
            .err()
            .unwrap();
        assert!(matches!(error, SdxError::InvalidEndpoint(_)));
    }

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
