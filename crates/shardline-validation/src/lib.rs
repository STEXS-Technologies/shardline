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
