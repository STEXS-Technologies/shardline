//! Full `xet://` URL parsing for the CLI lane (M6a).
//!
//! [`XetUrl`] parses `xet://<host>[:<port>]/<provider>/<owner>/<repo>/<revision>/<path...>`
//! — the 4-segment repository identity plus an optional multi-segment path
//! (which may be empty or end with `/` to denote a directory).
//!
//! The internal builder parser (`crate::client::parse_endpoint`) remains the
//! strict 4-segment parser used by [`XetClientBuilder`](crate::XetClientBuilder);
//! [`XetUrl`] is the CLI-facing full parser, with [`XetUrl::endpoint_url`]
//! producing the 4-segment form the builder requires.

use url::Url;

use crate::{auth::RepositoryId, error::SdxError};

/// A parsed `xet://` URL: repository identity plus an optional path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XetUrl {
    /// HTTP API base URL (`http://host[:port]`) for control-plane requests.
    pub api_base: String,
    /// Provider path segment.
    pub provider: String,
    /// Repository owner path segment.
    pub owner: String,
    /// Repository name path segment.
    pub repo: String,
    /// Revision path segment.
    pub revision: String,
    /// Path after the revision (empty when the URL has no path), preserving a
    /// trailing slash that denotes a directory.
    pub path: String,
    /// The original input URL string.
    pub raw: String,
}

impl XetUrl {
    /// Parses a `xet://` URL.
    ///
    /// The scheme must be `xet`, a host is required, and there must be at least
    /// four path segments (`provider/owner/repo/revision`, each non-empty).
    /// Everything after the revision becomes [`XetUrl::path`]. Query strings
    /// and fragments are rejected.
    ///
    /// # Errors
    ///
    /// Returns [`SdxError::InvalidEndpoint`] for malformed URLs.
    pub fn parse(input: &str) -> Result<XetUrl, SdxError> {
        let url = Url::parse(input).map_err(|error| {
            SdxError::InvalidEndpoint(format!("cannot parse {input:?}: {error}"))
        })?;
        if url.scheme() != "xet" {
            return Err(SdxError::InvalidEndpoint(format!(
                "unsupported scheme {:?} in {input:?}; expected xet://",
                url.scheme()
            )));
        }
        let host = url
            .host_str()
            .ok_or_else(|| SdxError::InvalidEndpoint(format!("missing host in {input:?}")))?;
        if url.query().is_some() || url.fragment().is_some() {
            return Err(SdxError::InvalidEndpoint(format!(
                "query or fragment in {input:?} is not supported"
            )));
        }

        let mut api_base = String::with_capacity(host.len().saturating_add(16));
        api_base.push_str("http://");
        api_base.push_str(host);
        if let Some(port) = url.port() {
            api_base.push(':');
            api_base.push_str(&port.to_string());
        }

        let raw_segments: Vec<&str> = url
            .path_segments()
            .map(|segments| segments.collect())
            .unwrap_or_default();
        if raw_segments.len() < 4 {
            return Err(SdxError::InvalidEndpoint(format!(
                "expected provider/owner/repo/revision (plus optional path) in {input:?}, got {} segment(s)",
                raw_segments.len()
            )));
        }
        let (identity, path_segments) = raw_segments.split_at(4);
        let identity: Vec<String> = identity
            .iter()
            .map(|segment| decode_segment(segment, input))
            .collect::<Result<_, _>>()?;
        if identity.iter().any(|segment| segment.is_empty()) {
            return Err(SdxError::InvalidEndpoint(format!(
                "empty segment in repository identity in {input:?}"
            )));
        }
        let (provider, owner, repo, revision) = match identity.as_slice() {
            [provider, owner, repo, revision] => (
                provider.clone(),
                owner.clone(),
                repo.clone(),
                revision.clone(),
            ),
            _ => {
                return Err(SdxError::InvalidEndpoint(format!(
                    "invalid repository identity in {input:?}"
                )));
            }
        };
        // Path segments may include a trailing empty segment for a trailing
        // slash; drop trailing empties, decode the rest, and re-append a single
        // trailing slash to denote a directory.
        let mut path_segments: Vec<String> = path_segments
            .iter()
            .map(|segment| decode_segment(segment, input))
            .collect::<Result<_, _>>()?;
        while path_segments
            .last()
            .is_some_and(|segment| segment.is_empty())
        {
            path_segments.pop();
        }
        let trailing_slash = url.path().ends_with('/');
        let mut path = path_segments.join("/");
        if trailing_slash && !path.is_empty() {
            path.push('/');
        }

        Ok(XetUrl {
            api_base,
            provider,
            owner,
            repo,
            revision,
            path,
            raw: input.to_owned(),
        })
    }

    /// Returns the repository identity for client building.
    #[must_use]
    pub fn repository_id(&self) -> RepositoryId {
        RepositoryId {
            provider: self.provider.clone(),
            owner: self.owner.clone(),
            repo: self.repo.clone(),
            revision: self.revision.clone(),
        }
    }

    /// Returns the 4-segment `xet://` endpoint URL (identity only, no path)
    /// that [`XetClientBuilder::endpoint`](crate::XetClientBuilder::endpoint)
    /// accepts.
    #[must_use]
    pub fn endpoint_url(&self) -> String {
        format!(
            "xet://{}/{}/{}/{}/{}",
            self.authority(),
            self.provider,
            self.owner,
            self.repo,
            self.revision
        )
    }

    /// Returns a copy of this URL with `path` substituted as the new path
    /// (used by `cp`/`sync` target derivation). The path is normalized by the
    /// M5 tree API / server; leading and trailing slashes are stripped except a
    /// single trailing slash that denotes a directory.
    #[must_use]
    pub fn with_path(&self, path: &str) -> XetUrl {
        let path = path.trim_matches('/');
        let trailing_dir = path.is_empty() && self.path.ends_with('/');
        let mut url = self.clone();
        url.path = if trailing_dir {
            String::new()
        } else {
            path.to_owned()
        };
        url.raw = url.display();
        url
    }

    /// Returns the canonical `xet://` form of this URL.
    #[must_use]
    pub fn display(&self) -> String {
        let mut out = format!(
            "xet://{}/{}/{}/{}/{}",
            self.authority(),
            self.provider,
            self.owner,
            self.repo,
            self.revision
        );
        if !self.path.is_empty() {
            out.push('/');
            out.push_str(&self.path);
        }
        out
    }

    /// The `host[:port]` authority, derived from [`XetUrl::api_base`].
    fn authority(&self) -> &str {
        self.api_base
            .strip_prefix("http://")
            .unwrap_or(&self.api_base)
    }
}

/// Percent-decodes a single URL path segment.
fn decode_segment(segment: &str, input: &str) -> Result<String, SdxError> {
    percent_encoding::percent_decode_str(segment)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_error| {
            SdxError::InvalidEndpoint(format!("invalid percent-encoding in {input:?}"))
        })
}

impl std::fmt::Display for XetUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

#[cfg(test)]
mod tests {
    use super::XetUrl;
    use crate::error::SdxError;

    fn repo() -> crate::auth::RepositoryId {
        crate::auth::RepositoryId {
            provider: "github".to_owned(),
            owner: "team".to_owned(),
            repo: "assets".to_owned(),
            revision: "main".to_owned(),
        }
    }

    #[test]
    fn parses_full_url_with_path() {
        let url =
            XetUrl::parse("xet://example.com/github/team/assets/main/dir/sub/file.txt").unwrap();
        assert_eq!(url.api_base, "http://example.com");
        assert_eq!(url.provider, "github");
        assert_eq!(url.owner, "team");
        assert_eq!(url.repo, "assets");
        assert_eq!(url.revision, "main");
        assert_eq!(url.path, "dir/sub/file.txt");
        assert_eq!(url.repository_id(), repo());
    }

    #[test]
    fn parses_url_without_path() {
        let url = XetUrl::parse("xet://example.com/github/team/assets/main").unwrap();
        assert_eq!(url.path, "");
        assert_eq!(url.display(), "xet://example.com/github/team/assets/main");
    }

    #[test]
    fn parses_url_with_trailing_slash_directory() {
        let url = XetUrl::parse("xet://example.com/github/team/assets/main/dir/").unwrap();
        assert_eq!(url.path, "dir/");
        // Root directory (trailing slash, no path segments) yields an empty path.
        let root = XetUrl::parse("xet://example.com/github/team/assets/main/").unwrap();
        assert_eq!(root.path, "");
    }

    #[test]
    fn parses_percent_encoded_segments() {
        let url = XetUrl::parse("xet://example.com/github/team/assets/main/a%20b/c.txt").unwrap();
        assert_eq!(url.path, "a b/c.txt");
    }

    #[test]
    fn parses_port_and_ipv4() {
        let url = XetUrl::parse("xet://127.0.0.1:8080/github/team/assets/main").unwrap();
        assert_eq!(url.api_base, "http://127.0.0.1:8080");
        assert_eq!(
            url.endpoint_url(),
            "xet://127.0.0.1:8080/github/team/assets/main"
        );
    }

    #[test]
    fn rejects_missing_host() {
        let err = XetUrl::parse("xet:///github/team/assets/main").unwrap_err();
        assert!(matches!(err, SdxError::InvalidEndpoint(_)));
    }

    #[test]
    fn rejects_wrong_scheme() {
        let err = XetUrl::parse("http://example.com/github/team/assets/main").unwrap_err();
        assert!(matches!(err, SdxError::InvalidEndpoint(_)));
    }

    #[test]
    fn rejects_query_and_fragment() {
        assert!(XetUrl::parse("xet://h/g/o/r/r?x=1").is_err());
        assert!(XetUrl::parse("xet://h/g/o/r/r#frag").is_err());
    }

    #[test]
    fn rejects_too_few_segments() {
        for input in ["xet://h/g", "xet://h/g/o", "xet://h/g/o/r"] {
            let err = XetUrl::parse(input).unwrap_err();
            assert!(
                matches!(err, SdxError::InvalidEndpoint(_)),
                "expected error for {input}"
            );
        }
    }

    #[test]
    fn rejects_empty_identity_segment() {
        // A double slash in the identity yields an empty segment.
        let err = XetUrl::parse("xet://h/g//o/r").unwrap_err();
        assert!(matches!(err, SdxError::InvalidEndpoint(_)));
    }

    #[test]
    fn accepts_more_than_four_segments_as_path() {
        let url = XetUrl::parse("xet://h/g/o/r/v/a/b/c").unwrap();
        assert_eq!(url.path, "a/b/c");
    }

    #[test]
    fn repository_id_round_trips() {
        let url = XetUrl::parse("xet://example.com/github/team/assets/main/dir/file.txt").unwrap();
        assert_eq!(url.repository_id(), repo());
    }

    #[test]
    fn with_path_substitutes_and_preserves_identity() {
        let url = XetUrl::parse("xet://example.com/github/team/assets/main/old/path").unwrap();
        let target = url.with_path("new/target.txt");
        assert_eq!(target.path, "new/target.txt");
        assert_eq!(target.repository_id(), repo());
        assert_eq!(
            target.display(),
            "xet://example.com/github/team/assets/main/new/target.txt"
        );
        // The original is unchanged.
        assert_eq!(url.path, "old/path");
    }

    #[test]
    fn display_round_trips() {
        let original = "xet://127.0.0.1:8080/github/team/assets/main/dir/file.txt";
        let url = XetUrl::parse(original).unwrap();
        assert_eq!(url.display(), original);
        assert_eq!(url.raw, original);
    }

    #[test]
    fn endpoint_url_drops_path() {
        let url = XetUrl::parse("xet://h/github/team/assets/main/dir").unwrap();
        assert_eq!(url.endpoint_url(), "xet://h/github/team/assets/main");
    }
}
