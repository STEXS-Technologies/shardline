#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string,
        clippy::panic
    )
)]

//! Pure validation functions, utility types, and constants for the Shardline
//! ecosystem.  This crate has no dependencies beyond `thiserror` and `std`.
//!
//! # Quick start
//!
//! Everything here is pure and dependency-free, so it is safe to use from any
//! storage or protocol layer:
//!
//! ```
//! use shardline_validation::{checked_add, validate_content_hash, validate_identifier};
//!
//! // Content hashes must be exactly 64 lowercase hex characters.
//! let hash = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//! assert!(validate_content_hash(hash).is_ok());
//! assert!(validate_content_hash(&"A".repeat(64)).is_err());
//!
//! // Identifiers must be safe single path components.
//! assert!(validate_identifier("models.bin").is_ok());
//! assert!(validate_identifier("../models.bin").is_err());
//!
//! // Checked arithmetic returns errors instead of wrapping on overflow.
//! assert_eq!(checked_add(2, 3)?, 5);
//! assert!(checked_add(u64::MAX, 1).is_err());
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Constants such as [`DEFAULT_LOCAL_GC_RETENTION_SECONDS`] and
//! [`DEFAULT_SHARD_METADATA_LIMITS`] are the single source of truth for
//! protocol bounds shared across the workspace.

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod ops;
mod types;
mod validation;

// ---------------------------------------------------------------------------
// Re-exports
// ---------------------------------------------------------------------------

pub use ops::{
    MAX_LOCAL_RECORD_METADATA_BYTES, RebuildOverflowError, checked_add, checked_increment,
    unix_now_seconds_checked,
};

pub use types::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS, DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_SHARD_METADATA_LIMITS, InvalidLifecycleMetadataError,
    InvalidReconstructionResponseError, InvalidSerializedShardError, ShardMetadataLimits,
};

pub use validation::{
    ValidateContentHashError, ValidateIdentifierError, validate_content_hash,
    validate_content_hash_with, validate_identifier,
};
