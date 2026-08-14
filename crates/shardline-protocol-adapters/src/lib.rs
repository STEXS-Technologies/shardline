#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use,
        clippy::format_push_string
    )
)]

//! Shared protocol adapters for Git LFS, Bazel HTTP cache, and related
//! object-key mapping.
//!
//! This crate owns the small, self-contained functions that map protocol
//! identifiers to validated [`shardline_storage::ObjectKey`] values. It
//! avoids pulling in heavy server dependencies such as `axum` or `sqlx`.
//!
//! # Quick start
//!
//! Everything here is pure. The most common task is mapping an LFS object ID
//! to a storage key:
//!
//! ```
//! use shardline_protocol_adapters::{LfsOperation, TransferAdapter, lfs_object_key};
//! use shardline_server_core::AuthorizedRepository;
//!
//! let oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
//! let key = lfs_object_key(oid, &AuthorizedRepository::anonymous_full_access())?;
//! assert!(key.as_str().starts_with("protocols/lfs/global/objects/"));
//! assert!(key.as_str().ends_with(oid));
//!
//! // Malformed OIDs are rejected instead of being stored under a bad key.
//! assert!(
//!     lfs_object_key("not-a-valid-sha256", &AuthorizedRepository::anonymous_full_access())
//!         .is_err()
//! );
//!
//! // Batch operations and transfer adapters round-trip through their wire names.
//! assert_eq!("download".parse(), Ok(LfsOperation::Download));
//! assert_eq!(TransferAdapter::Xet.as_str(), "xet");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! See also [`bazel_cache_object_key`] for the Bazel HTTP cache mapping and
//! [`scope_namespace`] for repository-scoped key prefixes.

mod bazel;
mod lfs;

pub use bazel::{BazelCacheKind, bazel_cache_object_key};
pub use lfs::{
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectRequest,
    LfsObjectResponse, LfsOperation, LfsValidationError, TransferAdapter, cas_headers,
    lfs_object_key,
};

use shardline_storage::ObjectKey;

/// Protocol adapter error type.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProtocolError {
    /// A content hash was malformed (not 64 lowercase hex characters).
    #[error("content hash must be 64 hexadecimal characters")]
    InvalidContentHash,
}

impl From<shardline_storage::ObjectKeyError> for ProtocolError {
    fn from(_error: shardline_storage::ObjectKeyError) -> Self {
        Self::InvalidContentHash
    }
}

/// Validates that `value` is exactly 64 lowercase hexadecimal characters.
///
/// # Examples
///
/// ```
/// use shardline_protocol_adapters::validate_content_hash;
///
/// let oid = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
/// assert!(validate_content_hash(oid).is_ok());
/// assert!(validate_content_hash(&"A".repeat(64)).is_err(), "uppercase is rejected");
/// ```
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when the input is malformed.
pub fn validate_content_hash(value: &str) -> Result<(), ProtocolError> {
    shardline_server_core::validate_content_hash_with(value, || ProtocolError::InvalidContentHash)
}

/// Maps a raw string to a validated [`ObjectKey`].
///
/// # Examples
///
/// ```
/// use shardline_protocol_adapters::object_key;
///
/// let key = object_key("protocols/xet/global/xorbs/abcd")?;
/// assert!(key.as_str().ends_with("abcd"));
/// assert!(object_key("").is_err());
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when the key is empty, too
/// large, or contains unsafe path components.
pub fn object_key(value: &str) -> Result<ObjectKey, ProtocolError> {
    ObjectKey::parse(value).map_err(Into::into)
}

/// Produces a deterministic namespace prefix for a repository scope.
///
/// When the scope is `None` the global namespace `"global"` is returned.
/// Otherwise the provider, owner, name, and optional revision are hashed
/// with SHA-256 and hex-encoded.
///
/// # Examples
///
/// ```
/// use shardline_protocol::RepositoryProvider;
/// use shardline_protocol_adapters::scope_namespace;
///
/// assert_eq!(scope_namespace(None), "global");
///
/// let scope = shardline_protocol::RepositoryScope::new(
///     RepositoryProvider::GitHub,
///     "acme",
///     "assets",
///     None,
/// )?;
/// let namespace = scope_namespace(Some(&scope));
/// assert_eq!(namespace.len(), 64, "scoped namespaces are sha256 hex digests");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[must_use]
pub fn scope_namespace(repository_scope: Option<&shardline_protocol::RepositoryScope>) -> String {
    use sha2::{Digest, Sha256};

    repository_scope.map_or_else(
        || "global".to_owned(),
        |scope| {
            let mut hasher = Sha256::new();
            hasher.update(scope.provider().as_str().as_bytes());
            hasher.update([0]);
            hasher.update(scope.owner().as_bytes());
            hasher.update([0]);
            hasher.update(scope.name().as_bytes());
            hasher.update([0]);
            if let Some(revision) = scope.revision() {
                hasher.update(revision.as_bytes());
            }
            hex::encode(hasher.finalize())
        },
    )
}

#[cfg(test)]
mod tests;
