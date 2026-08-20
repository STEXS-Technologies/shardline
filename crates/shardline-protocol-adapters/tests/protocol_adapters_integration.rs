//! Integration tests for `shardline-protocol-adapters`.
//!
//! These tests exercise the public API — [`lfs_object_key`],
//! [`bazel_cache_object_key`], [`scope_namespace`], and
//! [`validate_content_hash`] — with various scope configurations and
//! valid/invalid inputs.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::shadow_unrelated
)]

use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
use shardline_protocol_adapters::{
    BazelCacheKind, ProtocolError, bazel_cache_object_key, lfs_object_key, scope_namespace,
    validate_content_hash,
};
use shardline_server_core::{AuthProvider, AuthorizedRepository, LocalHmacProvider};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns a 64-character lowercase hex string.
fn valid_oid() -> String {
    "a".repeat(64)
}

/// Builds a capability carrying the given repository scope (or a permissive
/// anonymous capability when `None`), mirroring how the auth layer mints
/// capabilities: the claims are verified through a real provider so the
/// type-level seal is satisfied.
fn test_capability(scope: Option<RepositoryScope>) -> AuthorizedRepository {
    scope.map_or_else(AuthorizedRepository::anonymous_full_access, |repo| {
        let claims = TokenClaims::new("local", "test", TokenScope::Write, repo, u64::MAX).unwrap();
        let provider = LocalHmacProvider::new(b"test-signing-key-32-bytes-long!!").unwrap();
        let token = provider.mint_token(&claims).unwrap();
        let ctx = provider.verify_verified(&token).unwrap();
        AuthorizedRepository::from_verified_context(ctx, TokenScope::Write).unwrap()
    })
}

/// Builds a [`RepositoryScope`] with the given provider, owner, name, and
/// optional revision.
fn make_scope(
    provider: RepositoryProvider,
    owner: &str,
    name: &str,
    revision: Option<&str>,
) -> RepositoryScope {
    RepositoryScope::new(provider, owner, name, revision).expect("valid scope")
}

/// A default test scope for "acme/repo" on GitHub.
fn test_scope() -> RepositoryScope {
    make_scope(RepositoryProvider::GitHub, "acme", "repo", None)
}

/// A test scope with a revision.
fn test_scope_with_revision() -> RepositoryScope {
    make_scope(
        RepositoryProvider::GitLab,
        "my-team",
        "my-project",
        Some("main"),
    )
}

// ============================================================================
// lfs_object_key
// ============================================================================

#[test]
fn lfs_object_key_global_namespace() {
    let oid = valid_oid();
    let key = lfs_object_key(&oid, &test_capability(None)).expect("LFS key");
    assert!(key.as_str().contains("protocols/lfs/global/objects/"));
    assert!(key.as_str().ends_with(&oid));
}

#[test]
fn lfs_object_key_scoped_namespace() {
    let oid = valid_oid();
    let scope = test_scope();
    let key =
        lfs_object_key(&oid, &test_capability(Some(scope.clone()))).expect("LFS key with scope");
    assert!(key.as_str().starts_with("protocols/lfs/"));
    assert!(key.as_str().ends_with(&oid));
    // The namespace is deterministic (SHA-256 hash) and should NOT contain "global".
    let namespace = scope_namespace(Some(&scope));
    assert!(key.as_str().contains(&namespace));
}

#[test]
fn lfs_object_key_with_revision() {
    let oid = valid_oid();
    let scope = test_scope_with_revision();
    let key = lfs_object_key(&oid, &test_capability(Some(scope))).expect("LFS key with revision");
    assert!(key.as_str().starts_with("protocols/lfs/"));
    assert!(key.as_str().ends_with(&oid));
}

#[test]
fn lfs_object_key_invalid_oid_too_short() {
    let result = lfs_object_key("short", &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn lfs_object_key_invalid_oid_uppercase() {
    let oid = "A".repeat(64);
    let result = lfs_object_key(&oid, &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn lfs_object_key_empty_oid() {
    let result = lfs_object_key("", &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn lfs_object_key_non_hex_chars() {
    let oid = "z".repeat(64);
    let result = lfs_object_key(&oid, &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn lfs_object_key_different_providers_produce_different_keys() {
    let oid = valid_oid();
    let gh_scope = make_scope(RepositoryProvider::GitHub, "org", "repo", None);
    let gl_scope = make_scope(RepositoryProvider::GitLab, "org", "repo", None);
    let key_gh = lfs_object_key(&oid, &test_capability(Some(gh_scope))).expect("gh key");
    let key_gl = lfs_object_key(&oid, &test_capability(Some(gl_scope))).expect("gl key");
    assert_ne!(key_gh.as_str(), key_gl.as_str());
}

// ============================================================================
// bazel_cache_object_key
// ============================================================================

#[test]
fn bazel_cache_object_key_ac_global() {
    let hash = valid_oid();
    let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(None))
        .expect("bazel AC key");
    assert!(key.as_str().contains("protocols/bazel/global/ac/"));
    assert!(key.as_str().ends_with(&hash));
}

#[test]
fn bazel_cache_object_key_cas_global() {
    let hash = valid_oid();
    let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None))
        .expect("bazel CAS key");
    assert!(key.as_str().contains("protocols/bazel/global/cas/"));
    assert!(key.as_str().ends_with(&hash));
}

#[test]
fn bazel_cache_object_key_ac_with_scope() {
    let hash = valid_oid();
    let scope = test_scope();
    let namespace = scope_namespace(Some(&scope));
    let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(Some(scope)))
        .expect("bazel AC scoped key");
    assert!(
        key.as_str()
            .contains(&format!("protocols/bazel/{namespace}/ac/"))
    );
    assert!(key.as_str().ends_with(&hash));
}

#[test]
fn bazel_cache_object_key_cas_with_scope() {
    let hash = valid_oid();
    let scope = test_scope();
    let namespace = scope_namespace(Some(&scope));
    let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(Some(scope)))
        .expect("bazel CAS scoped key");
    assert!(
        key.as_str()
            .contains(&format!("protocols/bazel/{namespace}/cas/"))
    );
    assert!(key.as_str().ends_with(&hash));
}

#[test]
fn bazel_cache_object_key_with_revision_scope() {
    let hash = valid_oid();
    let scope = test_scope_with_revision();
    let namespace = scope_namespace(Some(&scope));
    let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(Some(scope)))
        .expect("bazel CAS key with revision");
    assert!(key.as_str().contains(&format!("/{namespace}/cas/")));
}

#[test]
fn bazel_cache_object_key_different_kinds_differ() {
    let hash = valid_oid();
    let key_ac =
        bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(None)).expect("AC key");
    let key_cas = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None))
        .expect("CAS key");
    assert_ne!(key_ac.as_str(), key_cas.as_str());
    assert!(key_ac.as_str().contains("/ac/"));
    assert!(key_cas.as_str().contains("/cas/"));
}

#[test]
fn bazel_cache_object_key_invalid_hash() {
    let result = bazel_cache_object_key(BazelCacheKind::Ac, "bad", &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn bazel_cache_object_key_uppercase_hash() {
    let hash = "F".repeat(64);
    let result = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

#[test]
fn bazel_cache_object_key_empty_hash() {
    let result = bazel_cache_object_key(BazelCacheKind::Ac, "", &test_capability(None));
    assert!(matches!(result, Err(ProtocolError::InvalidContentHash)));
}

// ============================================================================
// scope_namespace
// ============================================================================

#[test]
fn scope_namespace_none_returns_global() {
    assert_eq!(scope_namespace(None), "global");
}

#[test]
fn scope_namespace_scoped_returns_hash() {
    let scope = test_scope();
    let ns = scope_namespace(Some(&scope));
    // Should be a 64-char hex string (SHA-256).
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
fn scope_namespace_different_providers_differ() {
    let gh = make_scope(RepositoryProvider::GitHub, "org", "repo", None);
    let gl = make_scope(RepositoryProvider::GitLab, "org", "repo", None);
    assert_ne!(scope_namespace(Some(&gh)), scope_namespace(Some(&gl)));
}

#[test]
fn scope_namespace_different_owners_differ() {
    let a = make_scope(RepositoryProvider::GitHub, "owner-a", "repo", None);
    let b = make_scope(RepositoryProvider::GitHub, "owner-b", "repo", None);
    assert_ne!(scope_namespace(Some(&a)), scope_namespace(Some(&b)));
}

#[test]
fn scope_namespace_different_repos_differ() {
    let a = make_scope(RepositoryProvider::GitHub, "org", "repo-a", None);
    let b = make_scope(RepositoryProvider::GitHub, "org", "repo-b", None);
    assert_ne!(scope_namespace(Some(&a)), scope_namespace(Some(&b)));
}

#[test]
fn scope_namespace_with_revision_differs_from_without() {
    let without_rev = make_scope(RepositoryProvider::GitHub, "org", "repo", None);
    let with_rev = make_scope(RepositoryProvider::GitHub, "org", "repo", Some("main"));
    assert_ne!(
        scope_namespace(Some(&without_rev)),
        scope_namespace(Some(&with_rev))
    );
}

#[test]
fn scope_namespace_different_revisions_differ() {
    let main = make_scope(RepositoryProvider::GitHub, "org", "repo", Some("main"));
    let develop = make_scope(RepositoryProvider::GitHub, "org", "repo", Some("develop"));
    assert_ne!(
        scope_namespace(Some(&main)),
        scope_namespace(Some(&develop))
    );
}

#[test]
fn scope_namespace_codeberg_provider() {
    let scope = make_scope(RepositoryProvider::Codeberg, "user", "project", None);
    let ns = scope_namespace(Some(&scope));
    assert_eq!(ns.len(), 64);
}

#[test]
fn scope_namespace_generic_provider() {
    let scope = make_scope(RepositoryProvider::Generic, "any", "thing", None);
    let ns = scope_namespace(Some(&scope));
    assert_eq!(ns.len(), 64);
}

// ============================================================================
// validate_content_hash
// ============================================================================

#[test]
fn validate_content_hash_valid() {
    assert!(validate_content_hash(&"a".repeat(64)).is_ok());
    assert!(validate_content_hash(&"0".repeat(64)).is_ok());
    assert!(validate_content_hash(&"f".repeat(64)).is_ok());
}

#[test]
fn validate_content_hash_too_short() {
    assert!(matches!(
        validate_content_hash(&"a".repeat(63)),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn validate_content_hash_too_long() {
    assert!(matches!(
        validate_content_hash(&"a".repeat(65)),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn validate_content_hash_empty() {
    assert!(matches!(
        validate_content_hash(""),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn validate_content_hash_uppercase() {
    assert!(matches!(
        validate_content_hash(&"A".repeat(64)),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn validate_content_hash_non_hex() {
    assert!(matches!(
        validate_content_hash(&"z".repeat(64)),
        Err(ProtocolError::InvalidContentHash)
    ));
    assert!(matches!(
        validate_content_hash(&"g".repeat(64)),
        Err(ProtocolError::InvalidContentHash)
    ));
}

// ============================================================================
// object_key (parsing utility)
// ============================================================================

#[test]
fn object_key_valid_paths() {
    let key = shardline_protocol_adapters::object_key("simple/path.txt").expect("valid path");
    assert_eq!(key.as_str(), "simple/path.txt");
}

#[test]
fn object_key_rejects_empty() {
    assert!(matches!(
        shardline_protocol_adapters::object_key(""),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn object_key_rejects_traversal() {
    assert!(matches!(
        shardline_protocol_adapters::object_key("../escape"),
        Err(ProtocolError::InvalidContentHash)
    ));
}

#[test]
fn object_key_rejects_absolute() {
    assert!(matches!(
        shardline_protocol_adapters::object_key("/absolute/path"),
        Err(ProtocolError::InvalidContentHash)
    ));
}

// ============================================================================
// LFS / Bazel key format consistency
// ============================================================================

#[test]
fn lfs_and_bazel_keys_use_same_namespace() {
    let oid = valid_oid();
    let scope = test_scope();

    let lfs_key = lfs_object_key(&oid, &test_capability(Some(scope.clone()))).expect("LFS key");
    let bazel_key = bazel_cache_object_key(
        BazelCacheKind::Cas,
        &oid,
        &test_capability(Some(scope.clone())),
    )
    .expect("bazel key");

    let namespace = scope_namespace(Some(&scope));
    assert!(lfs_key.as_str().contains(&namespace));
    assert!(bazel_key.as_str().contains(&namespace));
}

#[test]
fn lfs_key_format_structure() {
    let oid = valid_oid();
    // Without scope: protocols/lfs/global/objects/<hash>
    let key = lfs_object_key(&oid, &test_capability(None)).expect("LFS global key");
    let parts: Vec<&str> = key.as_str().split('/').collect();
    assert_eq!(parts.len(), 5, "expected 5 parts: {parts:?}");
    assert_eq!(parts[0], "protocols");
    assert_eq!(parts[1], "lfs");
    assert_eq!(parts[2], "global");
    assert_eq!(parts[3], "objects");
    assert_eq!(parts[4], &oid);
}

#[test]
fn bazel_key_format_structure() {
    let hash = valid_oid();
    // Without scope: protocols/bazel/global/ac/<hash>
    let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(None))
        .expect("bazel AC global key");
    let parts: Vec<&str> = key.as_str().split('/').collect();
    assert_eq!(parts.len(), 5, "expected 5 parts: {parts:?}");
    assert_eq!(parts[0], "protocols");
    assert_eq!(parts[1], "bazel");
    assert_eq!(parts[2], "global");
    assert_eq!(parts[3], "ac");
    assert_eq!(parts[4], &hash);
}

#[test]
fn bazel_cas_key_format_structure() {
    let hash = valid_oid();
    // Without scope: protocols/bazel/global/cas/<hash>
    let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None))
        .expect("bazel CAS global key");
    let parts: Vec<&str> = key.as_str().split('/').collect();
    assert_eq!(parts.len(), 5);
    assert_eq!(parts[3], "cas");
}
