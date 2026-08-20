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
//!
//! # Quick start
//!
//! Store a content-addressed object under a validated key. This example uses
//! a `blackhole` store, which discards all writes and performs no I/O:
//!
//! ```
//! use shardline_protocol::ShardlineHash;
//! use shardline_server_core::ServerObjectStore;
//! use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
//!
//! let store = ServerObjectStore::blackhole();
//! let key = ObjectKey::parse("chunks/aa/bb/example.xorb")?;
//!
//! let hash = ShardlineHash::from_bytes([7; 32]);
//! let body = ObjectBody::from_slice(b"serialized chunk bytes");
//! let integrity = ObjectIntegrity::new(hash, body.as_slice().len() as u64);
//!
//! assert_eq!(
//!     ObjectStore::put_if_absent(&store, &key, body, &integrity)?,
//!     PutOutcome::Inserted
//! );
//! assert!(!ObjectStore::contains(&store, &key)?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! For filesystem-backed storage, use [`ServerObjectStore::local`] (see its
//! documentation for a `no_run` example).

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

/// Authentication and authorization types used by the Shardline server.
///
/// Re-exported from the `shardline-auth` crate for backward compatibility.
pub mod auth {
    pub use shardline_auth::*;
}

pub mod auth_capability;

pub mod at_rest;
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

// Auth capabilities
pub use auth_capability::{AuthorizedRepository, scope_allows};

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

// Explicit re-exports from shardline-auth (backward compatibility)
pub use shardline_auth::{
    AuthContext, AuthError, AuthProvider, Ed25519AuthProvider, LocalHmacProvider,
    PassthroughProvider, VerifiedAuthContext,
};
// Explicit re-exports from shardline-validation (backward compatibility)
pub use shardline_validation::{
    DEFAULT_LOCAL_GC_RETENTION_SECONDS, DEFAULT_MAX_SHARD_FILES,
    DEFAULT_MAX_SHARD_RECONSTRUCTION_TERMS, DEFAULT_MAX_SHARD_XORB_CHUNKS, DEFAULT_MAX_SHARD_XORBS,
    DEFAULT_SHARD_METADATA_LIMITS, InvalidLifecycleMetadataError,
    InvalidReconstructionResponseError, InvalidSerializedShardError,
    MAX_LOCAL_RECORD_METADATA_BYTES, RebuildOverflowError, ShardMetadataLimits,
    ValidateContentHashError, ValidateIdentifierError, checked_add, checked_increment,
    unix_now_seconds_checked, validate_content_hash, validate_content_hash_with,
    validate_identifier,
};
