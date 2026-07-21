use thiserror::Error;

/// Runtime protocol frontend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerFrontend {
    /// Validated Xet-compatible CAS frontend.
    Xet,
    /// Git LFS batch and object-transfer frontend.
    Lfs,
    /// Bazel-compatible HTTP remote-cache frontend.
    BazelHttp,
    /// OCI Distribution frontend.
    Oci,
    /// HuggingFace Hub API compatibility frontend.
    Hub,
}

impl ServerFrontend {
    /// Parses a frontend token.
    ///
    /// # Errors
    ///
    /// Returns [`ServerFrontendParseError`] when the token is not a supported
    /// frontend.
    pub fn parse(value: &str) -> Result<Self, ServerFrontendParseError> {
        match value {
            "xet" => Ok(Self::Xet),
            "lfs" => Ok(Self::Lfs),
            "bazel-http" => Ok(Self::BazelHttp),
            "oci" => Ok(Self::Oci),
            "hub" => Ok(Self::Hub),
            _ => Err(ServerFrontendParseError),
        }
    }

    /// Returns the canonical frontend token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Xet => "xet",
            Self::Lfs => "lfs",
            Self::BazelHttp => "bazel-http",
            Self::Oci => "oci",
            Self::Hub => "hub",
        }
    }
}

/// Invalid server frontend token.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("invalid server frontend")]
pub struct ServerFrontendParseError;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_xet() {
        let variant = ServerFrontend::Xet;
        assert_eq!(ServerFrontend::parse(variant.as_str()), Ok(variant));
    }

    #[test]
    fn roundtrip_lfs() {
        let variant = ServerFrontend::Lfs;
        assert_eq!(ServerFrontend::parse(variant.as_str()), Ok(variant));
    }

    #[test]
    fn roundtrip_bazel_http() {
        let variant = ServerFrontend::BazelHttp;
        assert_eq!(ServerFrontend::parse(variant.as_str()), Ok(variant));
    }

    #[test]
    fn roundtrip_oci() {
        let variant = ServerFrontend::Oci;
        assert_eq!(ServerFrontend::parse(variant.as_str()), Ok(variant));
    }

    #[test]
    fn roundtrip_hub() {
        let variant = ServerFrontend::Hub;
        assert_eq!(ServerFrontend::parse(variant.as_str()), Ok(variant));
    }

    #[test]
    fn parse_xet() {
        assert_eq!(ServerFrontend::parse("xet"), Ok(ServerFrontend::Xet));
    }

    #[test]
    fn parse_lfs() {
        assert_eq!(ServerFrontend::parse("lfs"), Ok(ServerFrontend::Lfs));
    }

    #[test]
    fn parse_oci() {
        assert_eq!(ServerFrontend::parse("oci"), Ok(ServerFrontend::Oci));
    }

    #[test]
    fn parse_hub() {
        assert_eq!(ServerFrontend::parse("hub"), Ok(ServerFrontend::Hub));
    }

    #[test]
    fn parse_bazel_http() {
        assert_eq!(
            ServerFrontend::parse("bazel-http"),
            Ok(ServerFrontend::BazelHttp)
        );
    }

    #[test]
    fn parse_empty_error() {
        assert!(ServerFrontend::parse("").is_err());
    }

    #[test]
    fn parse_invalid_error() {
        assert!(ServerFrontend::parse("invalid").is_err());
    }

    #[test]
    fn as_str_returns_correct_values() {
        assert_eq!(ServerFrontend::Xet.as_str(), "xet");
        assert_eq!(ServerFrontend::Lfs.as_str(), "lfs");
        assert_eq!(ServerFrontend::BazelHttp.as_str(), "bazel-http");
        assert_eq!(ServerFrontend::Oci.as_str(), "oci");
        assert_eq!(ServerFrontend::Hub.as_str(), "hub");
    }

    #[test]
    fn parse_error_display() {
        let err = ServerFrontendParseError;
        assert_eq!(err.to_string(), "invalid server frontend");
    }

    #[test]
    fn parse_error_debug_non_empty() {
        let err = ServerFrontendParseError;
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());
    }

    #[test]
    fn partial_eq_same_variant() {
        assert_eq!(ServerFrontend::Xet, ServerFrontend::Xet);
    }

    #[test]
    fn partial_eq_different_variant() {
        assert_ne!(ServerFrontend::Xet, ServerFrontend::Lfs);
    }
}
