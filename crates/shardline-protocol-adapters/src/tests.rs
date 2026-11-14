use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_server_core::{AuthProvider, AuthorizedRepository, LocalHmacProvider};

use super::*;

pub(super) fn test_scope() -> RepositoryScope {
    RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
}

/// Builds a capability carrying the given repository scope (or a permissive
/// anonymous capability when `None`), mirroring how the auth layer mints
/// capabilities: the claims are verified through a real provider so the
/// type-level seal is satisfied.
pub(super) fn test_capability(scope: Option<RepositoryScope>) -> AuthorizedRepository {
    scope.map_or_else(AuthorizedRepository::anonymous_full_access, |repo| {
        let claims = TokenClaims::new("local", "test", TokenScope::Write, repo, u64::MAX).unwrap();
        let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let token = provider.mint_token(&claims).unwrap();
        let ctx = provider.verify_verified(&token).unwrap();
        AuthorizedRepository::from_verified_context(ctx, TokenScope::Write).unwrap()
    })
}

#[test]
fn scope_namespace_none_returns_global() {
    assert_eq!(scope_namespace(None), "global");
}

#[test]
fn scope_namespace_is_stable_64_char_hex() {
    let scope = test_scope();
    let ns = scope_namespace(Some(&scope));
    assert_eq!(ns, scope_namespace(Some(&scope)));
    assert_eq!(ns.len(), 64);
    assert!(ns.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn scope_namespace_differs_with_revision() {
    let scope_no_rev =
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap();
    let scope_rev =
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", Some("abc123")).unwrap();
    assert_ne!(
        scope_namespace(Some(&scope_no_rev)),
        scope_namespace(Some(&scope_rev))
    );
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

// --- LFS_CONTENT_TYPE ---

#[test]
fn lfs_content_type_is_correct() {
    assert_eq!(LFS_CONTENT_TYPE, "application/vnd.git-lfs+json");
}

// --- Cross-protocol namespace isolation ---

#[test]
fn cross_protocol_namespace_isolation() {
    let hash = "a".repeat(64);
    let lfs_key = lfs_object_key(&hash, &test_capability(None)).unwrap();
    let bazel_key =
        bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None)).unwrap();

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
fn deterministic_object_key() {
    let key1 = object_key("some/path").unwrap();
    let key2 = object_key("some/path").unwrap();
    assert_eq!(key1, key2);
}

// --- Scope isolation ---

#[test]
fn object_keys_isolate_repositories_and_global_access() {
    let hash = "a".repeat(64);
    let scope_a = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-a", None).unwrap();
    let scope_b = RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo-b", None).unwrap();
    for (case, left, right) in [
        ("different repositories", Some(scope_a), Some(scope_b)),
        ("scoped versus global", Some(test_scope()), None),
    ] {
        let left = test_capability(left);
        let right = test_capability(right);
        assert_ne!(
            lfs_object_key(&hash, &left).unwrap(),
            lfs_object_key(&hash, &right).unwrap(),
            "LFS: {case}"
        );
        for kind in [BazelCacheKind::Cas, BazelCacheKind::Ac] {
            assert_ne!(
                bazel_cache_object_key(kind, &hash, &left).unwrap(),
                bazel_cache_object_key(kind, &hash, &right).unwrap(),
                "{kind:?}: {case}"
            );
        }
    }
}

// Shared validation must agree across both Bazel kinds and LFS.
#[test]
fn content_hash_validation_agrees_across_protocols() {
    let auth = test_capability(None);
    for (hash, valid) in [
        ("a".repeat(64), true),
        (String::new(), false),
        ("short".to_owned(), false),
        ("abc123".to_owned(), false),
        ("not-a-hash".to_owned(), false),
        ("A".repeat(64), false),
        (format!("{}g{}", "a".repeat(32), "a".repeat(31)), false),
        (format!("{}aa\0", "a".repeat(61)), false),
    ] {
        for (protocol, result) in [
            ("validator", validate_content_hash(&hash)),
            ("lfs", lfs_object_key(&hash, &auth).map(|_| ())),
            (
                "bazel-cas",
                bazel_cache_object_key(BazelCacheKind::Cas, &hash, &auth).map(|_| ()),
            ),
            (
                "bazel-ac",
                bazel_cache_object_key(BazelCacheKind::Ac, &hash, &auth).map(|_| ()),
            ),
        ] {
            if valid {
                assert!(result.is_ok(), "{protocol}: {hash:?}: {result:?}");
            } else {
                assert!(
                    matches!(result, Err(ProtocolError::InvalidContentHash)),
                    "{protocol}: {hash:?}: {result:?}"
                );
            }
        }
    }
}
