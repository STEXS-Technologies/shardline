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

//! Shared core types for the Shardline server ecosystem.
//!
//! This crate contains pure data structures and constants that are shared
//! between the server crate and potential future crate extractions.

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

pub mod auth;
pub mod object_store;
pub mod ops;
pub mod protocol_support;
pub mod server_frontend;
pub mod types;
pub mod validation;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Re-exports from submodules
// ---------------------------------------------------------------------------

// Auth — selective to avoid ambiguity
pub use auth::{AuthContext, AuthError, AuthProvider};

// Object store
pub use object_store::{
    OpsRecordKind, OpsRecordStore, ServerObjectStore, ServerObjectStoreError, read_full_object,
};

// Validation
pub use validation::{
    ValidateContentHashError, ValidateIdentifierError, chunk_hash,
    chunk_hash_from_chunk_object_key_if_present, chunk_object_key, content_hash,
    validate_content_hash, validate_content_hash_with, validate_identifier,
};

// Types (shared data structures, error enums, constants)
pub use types::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS, DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_SHARD_METADATA_LIMITS, InvalidLifecycleMetadataError,
    InvalidReconstructionResponseError, InvalidSerializedShardError, ShardMetadataLimits,
};

// Ops (misc utility functions, error, constants)
pub use ops::{
    MAX_LOCAL_RECORD_METADATA_BYTES, ParseStoredFileRecordError, RebuildOverflowError, checked_add,
    checked_increment, parse_stored_file_record_bytes, provider_directory,
    unix_now_seconds_checked,
};
