use clap::error::ErrorKind;
use thiserror::Error;

/// CLI parse failure.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{message}")]
pub struct CliParseError {
    kind: ErrorKind,
    message: String,
}

impl CliParseError {
    /// Returns the underlying clap error kind.
    #[must_use]
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns whether this parse failure represents help or version output.
    #[must_use]
    pub const fn is_help(&self) -> bool {
        matches!(
            self.kind,
            ErrorKind::DisplayHelp
                | ErrorKind::DisplayVersion
                | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        )
    }

    pub(crate) fn validation(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl From<clap::Error> for CliParseError {
    fn from(error: clap::Error) -> Self {
        Self {
            kind: error.kind(),
            message: format!("{error}"),
        }
    }
}
