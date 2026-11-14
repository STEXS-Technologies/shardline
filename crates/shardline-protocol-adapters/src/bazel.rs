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
    use crate::tests::{test_capability, test_scope};

    use super::*;

    fn valid_hash() -> String {
        "a".repeat(64)
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
}
