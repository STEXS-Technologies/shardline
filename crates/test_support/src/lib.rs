//! Test support utilities shared by Shardline workspace crates.
//!
//! This crate only contains helpers that are useful across crate boundaries. It
//! is intentionally small so production crates can keep their dev-dependency
//! setup simple without depending on server internals.
//!
//! # Example
//!
//! ```
//! use shardline_test_support::InvariantError;
//!
//! let error = InvariantError::new("expected generated manifest to be stable");
//! assert_eq!(
//!     error.to_string(),
//!     "expected generated manifest to be stable"
//! );
//! ```

use std::{
    fmt::Display,
    io::{Error as IoError, ErrorKind},
};

use thiserror::Error;

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
