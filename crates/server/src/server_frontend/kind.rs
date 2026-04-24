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
        }
    }
}

/// Invalid server frontend token.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("invalid server frontend")]
pub struct ServerFrontendParseError;
