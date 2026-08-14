use shardline_server_core::AuthorizedRepository;
use shardline_storage::ObjectKey;

use crate::{ProtocolError, object_key, scope_namespace, validate_content_hash};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BazelCacheKind {
    Ac,
    Cas,
}

impl BazelCacheKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ac => "ac",
            Self::Cas => "cas",
        }
    }
}

/// Returns the storage object key for a Bazel cache entry.
///
/// The namespace is derived from the verified [`AuthorizedRepository`]
/// capability: `None` (permissive, anonymous full-access) resolves to the
/// global namespace, a scoped capability to the repository's SHA-256 namespace.
///
/// # Errors
///
/// Returns [`ProtocolError::InvalidContentHash`] when `hash_hex` is malformed
/// or the constructed key is invalid.
pub fn bazel_cache_object_key(
    kind: BazelCacheKind,
    hash_hex: &str,
    auth: &AuthorizedRepository,
) -> Result<ObjectKey, ProtocolError> {
    validate_content_hash(hash_hex)?;
    object_key(&format!(
        "protocols/bazel/{}/{}/{}",
        scope_namespace(auth.namespace()),
        kind.as_str(),
        hash_hex
    ))
}

#[cfg(test)]
mod tests {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthContext, AuthorizedRepository};

    use super::*;

    fn valid_hash() -> String {
        "a".repeat(64)
    }

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "acme", "repo", None).unwrap()
    }

    /// Builds a capability carrying the given repository scope (or a
    /// permissive anonymous capability when `None`), mirroring how the auth
    /// layer mints capabilities from verified token claims.
    fn test_capability(scope: Option<RepositoryScope>) -> AuthorizedRepository {
        scope.map_or_else(AuthorizedRepository::anonymous_full_access, |repo| {
            let claims =
                TokenClaims::new("local", "test", TokenScope::Write, repo, u64::MAX).unwrap();
            AuthorizedRepository::from_verified_context(AuthContext::new(claims), TokenScope::Write)
                .unwrap()
        })
    }

    // --- BazelCacheKind ---

    #[test]
    fn bazel_cache_kind_ac_as_str() {
        assert_eq!(BazelCacheKind::Ac.as_str(), "ac");
    }

    #[test]
    fn bazel_cache_kind_cas_as_str() {
        assert_eq!(BazelCacheKind::Cas.as_str(), "cas");
    }

    #[test]
    fn bazel_cache_kind_variants_are_distinct() {
        assert_ne!(BazelCacheKind::Ac, BazelCacheKind::Cas);
    }

    // --- bazel_cache_object_key ---

    #[test]
    fn bazel_cache_object_key_ac_valid_hash() {
        let hash = valid_hash();
        let key =
            bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(None)).unwrap();
        assert!(key.as_str().contains("protocols/bazel/global/ac/"));
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn bazel_cache_object_key_cas_valid_hash() {
        let hash = valid_hash();
        let key =
            bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(None)).unwrap();
        assert!(key.as_str().contains("protocols/bazel/global/cas/"));
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn bazel_cache_object_key_with_scope() {
        let hash = valid_hash();
        let scope = test_scope();
        let key = bazel_cache_object_key(BazelCacheKind::Cas, &hash, &test_capability(Some(scope)))
            .unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("protocols/bazel/"));
        assert!(key.as_str().contains("/cas/"));
        assert!(key.as_str().ends_with(&hash));
    }

    #[test]
    fn bazel_cache_object_key_ac_with_scope() {
        let hash = valid_hash();
        let scope = test_scope();
        let key = bazel_cache_object_key(BazelCacheKind::Ac, &hash, &test_capability(Some(scope)))
            .unwrap();
        assert!(!key.as_str().contains("global"));
        assert!(key.as_str().contains("/ac/"));
    }

    #[test]
    fn bazel_cache_object_key_invalid_hash_too_short() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Cas, "short", &test_capability(None)),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn bazel_cache_object_key_invalid_hash_uppercase() {
        let uppercase = "A".repeat(64);
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Cas, &uppercase, &test_capability(None)),
            Err(ProtocolError::InvalidContentHash)
        ));
    }

    #[test]
    fn bazel_cache_object_key_invalid_hash_empty() {
        assert!(matches!(
            bazel_cache_object_key(BazelCacheKind::Ac, "", &test_capability(None)),
            Err(ProtocolError::InvalidContentHash)
        ));
    }
}
