//! Error type for the `sdx` file-management CLI lane.

use std::io;

use sdx::{AuthError, SdxError};
use thiserror::Error;

/// A failure in the `sdx` file-management command surface.
#[derive(Debug, Error)]
pub enum XetError {
    /// A plain user-facing message.
    #[error("{0}")]
    Message(String),
    /// A client-library failure.
    #[error(transparent)]
    Sdx(#[from] SdxError),
    /// An authentication/token-issuance failure.
    #[error(transparent)]
    Auth(#[from] AuthError),
    /// Remote-to-remote `cp` is not supported.
    #[error("remote-to-remote copy is not supported")]
    RemoteToRemote,
    /// Local-to-local `cp` is not supported.
    #[error("local-to-local copy is not supported; provide a xet:// remote")]
    LocalToLocal,
    /// An I/O failure.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// `--recursive` is required to transfer a directory.
    #[error("destination is a directory; add --recursive to transfer it")]
    DirectoryRequiresRecursive,
}

impl XetError {
    /// Builds a message error.
    pub(crate) const fn message(message: String) -> Self {
        Self::Message(message)
    }
}
