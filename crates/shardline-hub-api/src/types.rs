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
    fn git_service_names_preserve_parsing_and_display_contracts() {
        for (wire, service) in [
            ("git-upload-pack", GitSmartHttpService::UploadPack),
            ("git-receive-pack", GitSmartHttpService::ReceivePack),
        ] {
            assert_eq!(wire.parse::<GitSmartHttpService>(), Ok(service));
            assert_eq!(service.as_str(), wire);
            assert_eq!(service.to_string(), wire);
        }
        for wire in ["", "git-upload", "Git-Upload-Pack", "git-http-backend"] {
            assert_eq!(wire.parse::<GitSmartHttpService>(), Err(()), "{wire:?}");
        }
    }

    #[test]
    fn sort_fields_preserve_parsing_and_display_contracts() {
        for (wire, field) in [
            ("lastModified", HubSortField::LastModified),
            ("likes", HubSortField::Likes),
            ("downloads", HubSortField::Downloads),
        ] {
            assert_eq!(wire.parse::<HubSortField>(), Ok(field));
            assert_eq!(field.as_str(), wire);
            assert_eq!(field.to_string(), wire);
        }
        for wire in ["", "stars", "LastModified"] {
            assert_eq!(wire.parse::<HubSortField>(), Err(()), "{wire:?}");
        }
    }

    #[test]
    fn sort_directions_preserve_parsing_and_display_contracts() {
        for (wire, direction) in [("asc", SortDirection::Asc), ("desc", SortDirection::Desc)] {
            assert_eq!(wire.parse::<SortDirection>(), Ok(direction));
            assert_eq!(direction.as_str(), wire);
            assert_eq!(direction.to_string(), wire);
        }
        for wire in ["", "Asc", "sideways"] {
            assert_eq!(wire.parse::<SortDirection>(), Err(()), "{wire:?}");
        }
    }

    #[test]
    fn webhook_schemes_accept_mixed_case_and_display_lowercase() {
        for (wire, scheme, canonical) in [
            ("http", WebhookScheme::Http, "http"),
            ("HTTPS", WebhookScheme::Https, "https"),
            ("HtTp", WebhookScheme::Http, "http"),
            ("https", WebhookScheme::Https, "https"),
        ] {
            assert_eq!(wire.parse::<WebhookScheme>(), Ok(scheme));
            assert_eq!(scheme.as_str(), canonical);
            assert_eq!(scheme.to_string(), canonical);
        }
        for wire in ["", "ftp", "HTTP/2"] {
            assert_eq!(wire.parse::<WebhookScheme>(), Err(()), "{wire:?}");
        }
    }
}
