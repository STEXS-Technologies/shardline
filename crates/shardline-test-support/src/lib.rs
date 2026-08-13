//! Test support utilities shared by Shardline workspace crates.
//!
//! This crate only contains helpers that are useful across crate boundaries. It
//! is intentionally small so production crates can keep their dev-dependency
//! setup simple without depending on server internals.
//!
//! # Quick start
//!
//! [`TempStorage`] bundles a temporary directory with a default chunk size,
//! removing the repetitive `tempfile::tempdir()` + `NonZeroUsize::new(...)` +
//! assert boilerplate:
//!
//! ```
//! use shardline_test_support::TempStorage;
//!
//! let storage = TempStorage::new();
//! assert!(storage.path().is_dir());
//! assert_eq!(storage.chunk_size.get(), 128);
//! ```
//!
//! [`InvariantError`] turns a plain message into an error type that converts
//! into [`std::io::Error`], which is handy for test fixtures that must surface
//! invariant violations through IO error paths:
//!
//! ```
//! use shardline_test_support::InvariantError;
//!
//! let error = InvariantError::new("expected generated manifest to be stable");
//! assert_eq!(
//!     error.to_string(),
//!     "expected generated manifest to be stable"
//! );
//!
//! let io_error: std::io::Error = error.into();
//! assert_eq!(io_error.kind(), std::io::ErrorKind::InvalidData);
//! ```

#[cfg(feature = "docker")]
mod docker;

use std::{
    fmt::Display,
    io::{Error as IoError, ErrorKind},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[cfg(feature = "docker")]
pub use docker::{DockerLocalStack, DockerLocalStackBuilder, S3RawConfig};

/// Error type for test-only invariant failures.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct InvariantError {
    message: String,
}

impl InvariantError {
    /// Creates an invariant error with a displayable message.
    #[must_use]
    pub fn new(message: impl Display) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

impl From<InvariantError> for IoError {
    fn from(value: InvariantError) -> Self {
        Self::new(ErrorKind::InvalidData, value)
    }
}

/// Shared test fixture providing a temporary directory and default chunk size.
///
/// Eliminates the repetitive `tempfile::tempdir()` + `NonZeroUsize::new(128)` + assert
/// boilerplate found across ~105 test functions.
///
/// # Examples
///
/// ```
/// use shardline_test_support::TempStorage;
///
/// let storage = TempStorage::new();
/// assert!(storage.path().is_dir());
/// assert_eq!(storage.chunk_size.get(), 128);
/// ```
pub struct TempStorage {
    /// The temporary directory (kept alive for the test duration).
    pub temp: tempfile::TempDir,
    /// A default chunk size suitable for most tests.
    pub chunk_size: NonZeroUsize,
}

impl TempStorage {
    /// Creates a new temporary storage fixture.
    ///
    /// # Panics
    ///
    /// Panics if the temporary directory or chunk size cannot be created (should
    /// never happen in practice).
    #[must_use]
    pub fn new() -> Self {
        #[allow(clippy::expect_used)]
        let temp = tempfile::tempdir().expect("failed to create temporary directory");
        #[allow(clippy::expect_used)]
        let chunk_size =
            NonZeroUsize::new(128).expect("NonZeroUsize::new(128) should always succeed");
        Self { temp, chunk_size }
    }
    /// Returns the temporary directory path.
    #[must_use]
    pub fn path(&self) -> &Path {
        self.temp.path()
    }

    /// Returns the temporary directory path as an owned `PathBuf`.
    #[must_use]
    pub fn path_buf(&self) -> PathBuf {
        self.temp.path().to_path_buf()
    }
}

impl Default for TempStorage {
    fn default() -> Self {
        Self::new()
    }
}
