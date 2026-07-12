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
        let scope_no_rev =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap();
        let scope_rev =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", Some("abc123"))
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

    // --- ProtocolError::Display ---

    #[test]
    fn protocol_error_display() {
        let err = ProtocolError::InvalidContentHash;
        assert_eq!(
            err.to_string(),
            "content hash must be 64 hexadecimal characters"
        );
    }

    // --- ProtocolError::From<ObjectKeyError> ---

    #[test]
    fn protocol_error_from_object_key_error() {
        let oke = shardline_storage::ObjectKeyError::Empty;
        let pe: ProtocolError = oke.into();
        assert_eq!(
            pe.to_string(),
            "content hash must be 64 hexadecimal characters"
        );
    }

    // --- object_key() ---

    #[test]
    fn object_key_valid() {
        let key = object_key("valid/path").unwrap();
        assert_eq!(key.as_str(), "valid/path");
    }

    #[test]
    fn object_key_invalid_empty() {
        assert!(object_key("").is_err());
    }

    #[test]
    fn object_key_invalid_unsafe_path() {
        assert!(object_key("../unsafe").is_err());
    }

    // --- validate_content_hash() ---

    #[test]
    fn validate_content_hash_valid() {
        let hash = "a".repeat(64);
        assert!(validate_content_hash(&hash).is_ok());
    }

    #[test]
    fn validate_content_hash_too_short() {
        assert!(validate_content_hash("short").is_err());
    }

    #[test]
    fn validate_content_hash_uppercase() {
        let hash = "A".repeat(64);
        assert!(validate_content_hash(&hash).is_err());
    }

    #[test]
    fn validate_content_hash_empty() {
        assert!(validate_content_hash("").is_err());
    }

    #[test]
    fn validate_content_hash_non_hex_chars() {
        let hash = format!("{}g{}", "a".repeat(32), "a".repeat(31));
        assert!(validate_content_hash(&hash).is_err());
    }

    // --- LFS_CONTENT_TYPE ---

    #[test]
    fn lfs_content_type_is_correct() {
        assert_eq!(LFS_CONTENT_TYPE, "application/vnd.git-lfs+json");
    }

    // --- Cross-protocol namespace isolation ---

    #[test]
    fn cross_protocol_namespace_isolation() {
        let hash = "a".repeat(64);
        let lfs_key = lfs_object_key(&hash, None).unwrap();
        let bazel_key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();

        assert!(lfs_key.as_str().contains("protocols/lfs/"));
        assert!(bazel_key.as_str().contains("protocols/bazel/"));
        assert_ne!(lfs_key, bazel_key);

        assert!(!lfs_key.as_str().contains("protocols/bazel/"));
        assert!(!bazel_key.as_str().contains("protocols/lfs/"));

        assert!(!lfs_key.as_str().contains("/cas/"));
        assert!(!lfs_key.as_str().contains("/ac/"));

        assert!(!bazel_key.as_str().contains("/objects/"));
    }

    // --- Deterministic key construction ---

    #[test]
    fn deterministic_bazel_cache_object_key() {
        let hash = "a".repeat(64);
        let key1 = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        let key2 = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn deterministic_lfs_object_key() {
        let hash = "a".repeat(64);
        let key1 = lfs_object_key(&hash, None).unwrap();
        let key2 = lfs_object_key(&hash, None).unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn deterministic_object_key() {
        let key1 = object_key("some/path").unwrap();
        let key2 = object_key("some/path").unwrap();
        assert_eq!(key1, key2);
    }

    #[test]
    fn deterministic_with_scope() {
        let hash = "a".repeat(64);
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap();
        let key1 = lfs_object_key(&hash, Some(&scope)).unwrap();
        let key2 = lfs_object_key(&hash, Some(&scope)).unwrap();
        assert_eq!(key1, key2);
    }

    // --- Key format stability (regression prevention) ---

    #[test]
    fn key_format_stability_lfs() {
        let hash = "a".repeat(64);
        let key = lfs_object_key(&hash, None).unwrap();
        let expected_suffix = format!("protocols/lfs/global/objects/{hash}");
        assert!(key.as_str().ends_with(&expected_suffix));
    }

    #[test]
    fn key_format_stability_bazel_cas() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        let expected_suffix = format!("protocols/bazel/global/cas/{hash}");
        assert!(key.as_str().ends_with(&expected_suffix));
    }

    #[test]
    fn key_format_stability_bazel_ac() {
        let hash = "a".repeat(64);
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, None).unwrap();
        let expected_suffix = format!("protocols/bazel/global/ac/{hash}");
        assert!(key.as_str().ends_with(&expected_suffix));
    }

    // --- Scope isolation ---

    #[test]
    fn scope_isolation_lfs_different_scopes() {
        let hash = "a".repeat(64);
        let scope_a =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-a", None).unwrap();
        let scope_b =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-b", None).unwrap();
        let key_a = lfs_object_key(&hash, Some(&scope_a)).unwrap();
        let key_b = lfs_object_key(&hash, Some(&scope_b)).unwrap();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn scope_isolation_bazel_different_scopes() {
        let hash = "a".repeat(64);
        let scope_a =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-a", None).unwrap();
        let scope_b =
            RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-b", None).unwrap();
        let key_a = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope_a)).unwrap();
        let key_b = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope_b)).unwrap();
        assert_ne!(key_a, key_b);
    }

    #[test]
    fn scope_isolation_lfs_with_vs_without_scope() {
        let hash = "a".repeat(64);
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap();
        let key_with = lfs_object_key(&hash, Some(&scope)).unwrap();
        let key_without = lfs_object_key(&hash, None).unwrap();
        assert_ne!(key_with, key_without);
    }

    #[test]
    fn scope_isolation_bazel_with_vs_without_scope() {
        let hash = "a".repeat(64);
        let scope = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap();
        let key_with = bazel_cache_object_key(BazelCacheKind::Cas, &hash, Some(&scope)).unwrap();
        let key_without = bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).unwrap();
        assert_ne!(key_with, key_without);
    }

    // --- Cross-protocol hash format consistency ---

    #[test]
    fn cross_protocol_hash_consistency_valid() {
        let hash = "a".repeat(64);
        assert!(lfs_object_key(&hash, None).is_ok());
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, &hash, None).is_ok());
    }

    #[test]
    fn cross_protocol_hash_consistency_empty_rejected() {
        assert!(lfs_object_key("", None).is_err());
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, "", None).is_err());
    }

    #[test]
    fn cross_protocol_hash_consistency_short_rejected() {
        assert!(lfs_object_key("short", None).is_err());
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, "short", None).is_err());
    }

    #[test]
    fn cross_protocol_hash_consistency_uppercase_rejected() {
        let uppercase = "A".repeat(64);
        assert!(lfs_object_key(&uppercase, None).is_err());
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, &uppercase, None).is_err());
    }

    #[test]
    fn cross_protocol_hash_consistency_non_hex_rejected() {
        let non_hex = format!("{}g{}", "a".repeat(32), "a".repeat(31));
        assert!(lfs_object_key(&non_hex, None).is_err());
        assert!(bazel_cache_object_key(BazelCacheKind::Cas, &non_hex, None).is_err());
    }

    #[test]
    fn cross_protocol_validate_content_hash_used_consistently() {
        let hash = "a".repeat(64);
        assert!(validate_content_hash(&hash).is_ok());

        let uppercase = "A".repeat(64);
        assert!(validate_content_hash("").is_err());
        assert!(validate_content_hash("short").is_err());
        assert!(validate_content_hash(&uppercase).is_err());
    }
}
