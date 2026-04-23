use thiserror::Error;

/// Runtime protocol frontend selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServerFrontend {
    /// Xet-compatible CAS frontend.
    Xet,
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
            _ => Err(ServerFrontendParseError),
        }
    }

    /// Returns the canonical frontend token.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Xet => "xet",
        }
    }
}

/// Invalid server frontend token.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("invalid server frontend")]
pub struct ServerFrontendParseError;
