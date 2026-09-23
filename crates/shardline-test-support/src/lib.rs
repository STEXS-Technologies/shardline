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
mod fixtures;
#[cfg(feature = "docker")]
mod s3_fault_proxy;

pub use fixtures::{InvariantError, TempStorage};
