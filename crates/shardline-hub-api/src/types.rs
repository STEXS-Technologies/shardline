//! Shared value types for the Hub API.
//!
//! These are small, internal-only newtypes that make stringly-typed
//! comparisons (Git Smart HTTP service names, search sort fields, webhook
//! URL schemes) explicit and safe to parse once at the boundary.

use std::str::FromStr;

/// The Git Smart HTTP service advertised via `GET /info/refs`.
///
/// Only the two well-known services are valid; anything else is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitSmartHttpService {
    /// `git-upload-pack` — used for clone/fetch.
    UploadPack,
    /// `git-receive-pack` — used for push.
    ReceivePack,
}

impl GitSmartHttpService {
    /// Returns the wire-format service name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UploadPack => "git-upload-pack",
            Self::ReceivePack => "git-receive-pack",
        }
    }
}

impl FromStr for GitSmartHttpService {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "git-upload-pack" => Ok(Self::UploadPack),
            "git-receive-pack" => Ok(Self::ReceivePack),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for GitSmartHttpService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Sort field for repo search results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HubSortField {
    /// Sort by last-modified time.
    LastModified,
    /// Sort by number of likes.
    Likes,
    /// Sort by number of downloads.
    Downloads,
}

impl HubSortField {
    /// Returns the wire-format field name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LastModified => "lastModified",
            Self::Likes => "likes",
            Self::Downloads => "downloads",
        }
    }
}

impl FromStr for HubSortField {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lastModified" => Ok(Self::LastModified),
            "likes" => Ok(Self::Likes),
            "downloads" => Ok(Self::Downloads),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for HubSortField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Sort direction for repo search results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

impl SortDirection {
    /// Returns the wire-format direction name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Asc => "asc",
            Self::Desc => "desc",
        }
    }
}

impl FromStr for SortDirection {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            _ => Err(()),
        }
    }
}

impl std::fmt::Display for SortDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Webhook URL scheme (case-insensitive, per RFC 3986).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookScheme {
    /// `http`
    Http,
    /// `https`
    Https,
}

impl WebhookScheme {
    /// Returns the lowercase scheme name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
}

impl FromStr for WebhookScheme {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("http") {
            Ok(Self::Http)
        } else if s.eq_ignore_ascii_case("https") {
            Ok(Self::Https)
        } else {
            Err(())
        }
    }
}

impl std::fmt::Display for WebhookScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_smart_http_service_accepts_valid() {
        assert_eq!(
            "git-upload-pack".parse::<GitSmartHttpService>(),
            Ok(GitSmartHttpService::UploadPack)
        );
        assert_eq!(
            "git-receive-pack".parse::<GitSmartHttpService>(),
            Ok(GitSmartHttpService::ReceivePack)
        );
    }

    #[test]
    fn git_smart_http_service_rejects_invalid() {
        assert!("".parse::<GitSmartHttpService>().is_err());
        assert!("git-upload".parse::<GitSmartHttpService>().is_err());
        assert!("Git-Upload-Pack".parse::<GitSmartHttpService>().is_err());
        assert!("git-http-backend".parse::<GitSmartHttpService>().is_err());
    }

    #[test]
    fn git_smart_http_service_as_str_round_trips() {
        for name in ["git-upload-pack", "git-receive-pack"] {
            assert_eq!(name.parse::<GitSmartHttpService>().unwrap().as_str(), name);
        }
    }

    #[test]
    fn hub_sort_field_accepts_valid() {
        assert_eq!(
            "lastModified".parse::<HubSortField>(),
            Ok(HubSortField::LastModified)
        );
        assert_eq!("likes".parse::<HubSortField>(), Ok(HubSortField::Likes));
        assert_eq!(
            "downloads".parse::<HubSortField>(),
            Ok(HubSortField::Downloads)
        );
    }

    #[test]
    fn hub_sort_field_rejects_invalid() {
        assert!("".parse::<HubSortField>().is_err());
        assert!("stars".parse::<HubSortField>().is_err());
        assert!("LastModified".parse::<HubSortField>().is_err());
    }

    #[test]
    fn sort_direction_accepts_valid() {
        assert_eq!("asc".parse::<SortDirection>(), Ok(SortDirection::Asc));
        assert_eq!("desc".parse::<SortDirection>(), Ok(SortDirection::Desc));
    }

    #[test]
    fn sort_direction_rejects_invalid() {
        assert!("".parse::<SortDirection>().is_err());
        assert!("Asc".parse::<SortDirection>().is_err());
        assert!("sideways".parse::<SortDirection>().is_err());
    }

    #[test]
    fn webhook_scheme_is_case_insensitive() {
        assert_eq!("http".parse::<WebhookScheme>(), Ok(WebhookScheme::Http));
        assert_eq!("HTTPS".parse::<WebhookScheme>(), Ok(WebhookScheme::Https));
        assert_eq!("HtTp".parse::<WebhookScheme>(), Ok(WebhookScheme::Http));
        assert_eq!("https".parse::<WebhookScheme>(), Ok(WebhookScheme::Https));
    }

    #[test]
    fn webhook_scheme_rejects_invalid() {
        assert!("".parse::<WebhookScheme>().is_err());
        assert!("ftp".parse::<WebhookScheme>().is_err());
        assert!("HTTP/2".parse::<WebhookScheme>().is_err());
    }
}
