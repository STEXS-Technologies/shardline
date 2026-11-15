use std::{
    fmt::Display,
    io::{Error as IoError, ErrorKind},
    num::NonZeroUsize,
    path::{Path, PathBuf},
};

use thiserror::Error;

#[cfg(feature = "docker")]
pub use crate::docker::{DockerLocalStack, DockerLocalStackBuilder, S3RawConfig};
pub use crate::s3_fault_proxy::{S3FaultProxy, S3ProxyFault};

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

/// Deterministic xorshift payload shared by chaos fixtures.
///
/// Preserves fixture seeds without adding an RNG dependency.
#[must_use]
pub fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state & 0xff) as u8);
    }
    out
}

/// Counts files recursively, ignoring unreadable or missing fixture directories.
#[must_use]
pub fn count_files_recursive(dir: &Path) -> usize {
    let mut count: usize = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count = count.saturating_add(count_files_recursive(&path));
            } else {
                count = count.saturating_add(1);
            }
        }
    }
    count
}

/// Counts files beneath a local fixture storage root’s `chunks` directory.
#[must_use]
pub fn count_chunk_files(root: &Path) -> usize {
    count_files_recursive(&root.join("chunks"))
}
