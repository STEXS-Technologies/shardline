//! [`XetClient`] builder and handle for the sdx CAS read path (M2a).
//!
//! The client maps a `xet://` endpoint URL (`docs/XET_NATIVE_CLI.md` "URL
//! Scheme") onto an API base URL and repository identity, holds the
//! [`TokenService`] (M1), and exposes [`DownloadSession`]s.
//!
//! ```text
//! xet://<host>[:<port>]/<provider>/<owner>/<repo>/<revision>
//! ```
//!
//! Path addressing (`xet://…/<revision>/<path>`) arrives in M5.

use std::sync::Arc;

use url::Url;

use crate::{
    auth::{Auth, HttpConfig, RepositoryId},
    error::{SdxError, TransferError},
    session::{DownloadSession, DownloadSessionInner},
    transfer::TransferClient,
};

/// A configured Xet client handle.
///
/// Clone is cheap: the handle shares the HTTP client, token service, and
/// endpoint state.
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

    /// Returns the CAS base URL from the most recently issued read/write
    /// token, if any (used to construct CAS transfer URLs when the response
    /// does not carry absolute URLs).
    #[must_use]
    pub fn cas_url(&self) -> Option<String> {
        self.inner.tokens.cas_url()
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
#[derive(Default)]
pub struct XetClientBuilder {
    endpoint: Option<String>,
    auth: Option<Auth>,
    http: Option<HttpConfig>,
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

    /// Builds the client.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError`] when the endpoint URL cannot be mapped to an API
    /// base and repository identity, no [`Auth`] is configured, the token
    /// service cannot be built, or the HTTP client cannot be created.
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
        Ok(XetClient {
            inner: Arc::new(DownloadSessionInner {
                transfer: TransferClient::new(http_client),
                tokens,
                api_base,
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
