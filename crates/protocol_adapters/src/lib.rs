#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
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

mod bazel;
mod lfs;

pub use bazel::{BazelCacheKind, bazel_cache_object_key};
pub use lfs::{
    LFS_CONTENT_TYPE, LfsBatchRequest, LfsBatchResponse, LfsObjectError, LfsObjectRequest,
    LfsObjectResponse, lfs_object_key,
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
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when the input is malformed.
pub fn validate_content_hash(value: &str) -> Result<(), ProtocolError> {
    shardline_server_core::validate_content_hash_with(value, || ProtocolError::InvalidContentHash)
}

/// Maps a raw string to a validated [`ObjectKey`].
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
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    use super::*;

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
    }

    #[test]
    fn scope_namespace_none_returns_global() {
        assert_eq!(scope_namespace(None), "global");
    }

    #[test]
    fn scope_namespace_returns_64_char_hex() {
        let scope = test_scope();
        let ns = scope_namespace(Some(&scope));
        assert_eq!(ns.len(), 64);
        assert!(ns.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn scope_namespace_deterministic() {
        let scope = test_scope();
        let ns1 = scope_namespace(Some(&scope));
        let ns2 = scope_namespace(Some(&scope));
        assert_eq!(ns1, ns2);
    }

    #[test]
    fn scope_namespace_differs_with_revision() {
        let scope_no_rev = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None)
            .unwrap();
        let scope_rev = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "acme",
            "repo",
            Some("abc123"),
        )
        .unwrap();
        assert_ne!(
            scope_namespace(Some(&scope_no_rev)),
            scope_namespace(Some(&scope_rev))
        );
    }

    #[test]
    fn bazel_cache_object_key_valid() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert!(key.as_str().contains("protocols/bazel/global/cas/"));
    }

    #[test]
    fn bazel_cache_object_key_ac_kind() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        assert!(key.as_str().contains("protocols/bazel/global/ac/"));
    }

    #[test]
    fn bazel_cache_object_key_invalid_hash() {
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, "short", None).is_err());
    }

    #[test]
    fn bazel_cache_object_key_with_scope() {
        let hash = "a".repeat(64);
        let scope = test_scope();
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope)).unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("protocols/bazel/"));
    }

    #[test]
    fn lfs_object_key_valid() {
        let hash = "a".repeat(64);
        let key = lfs_object_key(&hash, None).unwrap();
        assert!(key.as_str().contains("protocols/lfs/global/objects/"));
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn lfs_object_key_invalid_hash() {
        assert!(lfs_object_key("not-a-hash", None).is_err());
    }

    #[test]
    fn lfs_object_key_with_scope() {
        let hash = "a".repeat(64);
        let scope = test_scope();
        let key = lfs_object_key(&hash, Some(&scope)).unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("protocols/lfs/"));
    }
}
