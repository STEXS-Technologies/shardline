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

/// Backward-compatible re-exports from the `shardline-auth` crate.
pub mod auth {
    pub use shardline_auth::*;
}

pub mod object_store;
pub mod ops;
pub mod protocol_support;
pub mod server_frontend;
pub mod validation;

#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Re-exports from submodules
// ---------------------------------------------------------------------------

// Object store
pub use object_store::{
    OpsRecordKind, OpsRecordStore, ServerObjectStore, ServerObjectStoreError, read_full_object,
};

// Validation (leftover functions that depend on server_core types)
pub use validation::{
    chunk_hash, chunk_hash_from_chunk_object_key_if_present, chunk_object_key, content_hash,
};

// Ops (leftover functions that depend on server_core or external crate types)
pub use ops::{ParseStoredFileRecordError, parse_stored_file_record_bytes, provider_directory};

// ---------------------------------------------------------------------------
// Re-exports from new crates (full backward compatibility)
// ---------------------------------------------------------------------------

pub use shardline_auth::*;
pub use shardline_validation::*;
