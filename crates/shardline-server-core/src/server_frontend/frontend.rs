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
    /// S3-compatible object-storage frontend.
    S3,
}

impl ServerFrontend {
    /// Every frontend variant, in registration order.
    ///
    /// This is the single source of truth for "all frontends" used by the
    /// frontend-coexistence tests: the router is built with every variant so
    /// that ANY future frontend whose routes collide with another frontend's
    /// (e.g. the S3 `/{bucket}/{*key}` wildcard versus a Hub root-level
    /// parameter route) fails the suite immediately with the conflicting
    /// pair named. **Add every new variant here.**
    pub const ALL: [Self; 6] = [
        Self::Xet,
        Self::Lfs,
        Self::BazelHttp,
        Self::Oci,
        Self::Hub,
        Self::S3,
    ];
}

impl ServerFrontend {
    /// Parses a frontend token.
    ///
    /// # Examples
    ///
    /// ```
    /// use shardline_server_core::server_frontend::ServerFrontend;
    ///
    /// let frontend = ServerFrontend::parse("lfs")?;
    /// assert_eq!(frontend.as_str(), "lfs");
    /// assert!(ServerFrontend::parse("unknown").is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
            "s3" => Ok(Self::S3),
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
            Self::S3 => "s3",
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
    fn registered_frontends_preserve_their_wire_tokens() {
        let cases = [
            (ServerFrontend::Xet, "xet"),
            (ServerFrontend::Lfs, "lfs"),
            (ServerFrontend::BazelHttp, "bazel-http"),
            (ServerFrontend::Oci, "oci"),
            (ServerFrontend::Hub, "hub"),
            (ServerFrontend::S3, "s3"),
        ];
        assert_eq!(ServerFrontend::ALL, cases.map(|(frontend, _)| frontend));
        for (frontend, token) in cases {
            assert_eq!(frontend.as_str(), token);
            assert_eq!(ServerFrontend::parse(token), Ok(frontend), "{token}");
        }
    }

    #[test]
    fn rejects_unknown_case_changed_and_padded_tokens() {
        for value in ["", "invalid", "Xet", "LFS", "s3 ", " hub", "bazel_http"] {
            assert_eq!(
                ServerFrontend::parse(value),
                Err(ServerFrontendParseError),
                "{value:?}"
            );
        }
        assert_eq!(
            ServerFrontendParseError.to_string(),
            "invalid server frontend"
        );
    }
}
