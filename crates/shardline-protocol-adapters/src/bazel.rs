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

    // --- bazel_cache_object_key ---

    #[test]
    fn bazel_cache_object_keys_cover_both_kinds_and_scopes() {
        let hash = valid_hash();
        for (kind, segment) in [(BazelCacheKind::Ac, "ac"), (BazelCacheKind::Cas, "cas")] {
            for scope in [None, Some(test_scope())] {
                let scoped = scope.is_some();
                let auth = test_capability(scope);
                let key = bazel_cache_object_key(kind, &hash, &auth).unwrap();
                assert_eq!(key, bazel_cache_object_key(kind, &hash, &auth).unwrap());
                assert!(
                    key.as_str().contains("protocols/bazel/"),
                    "{kind:?}, {scoped}"
                );
                assert!(
                    key.as_str().contains(&format!("/{segment}/")),
                    "{kind:?}, {scoped}"
                );
                assert!(key.as_str().ends_with(&hash), "{kind:?}, {scoped}");
                if scoped {
                    assert!(!key.as_str().contains("global"), "{kind:?}");
                } else {
                    assert_eq!(
                        key.as_str(),
                        format!("protocols/bazel/global/{segment}/{hash}")
                    );
                }
            }
        }
    }
}
