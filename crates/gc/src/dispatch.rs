use shardline_server_core::ServerObjectStore;
use shardline_storage::ObjectKey;
use shardline_xet_adapter::{
    XetAdapterError, shard_hash_from_object_key_if_present, visit_stored_xorb_chunk_hashes,
    xorb_hash_from_object_key_if_present, xorb_object_key,
};

use crate::{GcError, ServerFrontend};

pub(super) fn optional_chunk_container_keys(
    frontends: &[ServerFrontend],
    chunk_hash: &str,
) -> Result<Vec<ObjectKey>, GcError> {
    let mut object_keys = Vec::new();
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                let object_key = xorb_object_key(chunk_hash)?;
                if !object_keys.contains(&object_key) {
                    object_keys.push(object_key);
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(object_keys)
}

pub(super) fn referenced_term_object_key(
    frontends: &[ServerFrontend],
    term_hash: &str,
) -> Result<ObjectKey, GcError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return Ok(xorb_object_key(term_hash)?);
    }

    Err(GcError::InvalidContentHash)
}

pub(super) fn managed_protocol_object_identity(
    frontends: &[ServerFrontend],
    key: &ObjectKey,
) -> Result<Option<String>, GcError> {
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if let Some(hash) = xorb_hash_from_object_key_if_present(key)? {
                    return Ok(Some(hash.to_owned()));
                }
                if let Some(hash) = shard_hash_from_object_key_if_present(key)? {
                    return Ok(Some(hash.to_owned()));
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(None)
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), GcError>
where
    Visitor: FnMut(String) -> Result<(), GcError>,
{
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if xorb_hash_from_object_key_if_present(object_key)?.is_some() {
                    let mut result = Ok(());
                    visit_stored_xorb_chunk_hashes(object_store, object_key, |chunk_hash_hex| {
                        match visitor(chunk_hash_hex) {
                            Ok(()) => Ok(()),
                            Err(e) => {
                                result = Err(e);
                                Err(XetAdapterError::NotFound)
                            }
                        }
                    })?;
                    return result;
                }
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shardline_storage::ObjectKey;

    const VALID_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn optional_chunk_container_keys_with_xet_returns_xorb_key() {
        let keys = optional_chunk_container_keys(&[ServerFrontend::Xet], VALID_HASH).unwrap();
        assert_eq!(keys.len(), 1);
        // xorb_object_key produces xorbs/default/{prefix}/{hash}.xorb
        assert!(keys[0].as_str().starts_with("xorbs/default/"));
        assert!(keys[0].as_str().ends_with(".xorb"));
    }

    #[test]
    fn optional_chunk_container_keys_with_lfs_only_returns_empty() {
        let keys = optional_chunk_container_keys(&[ServerFrontend::Lfs], VALID_HASH).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn optional_chunk_container_keys_with_multiple_frontends_returns_one_xorb_key() {
        let frontends = [
            ServerFrontend::Xet,
            ServerFrontend::Lfs,
            ServerFrontend::Hub,
        ];
        let keys = optional_chunk_container_keys(&frontends, VALID_HASH).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].as_str().starts_with("xorbs/default/"));
    }

    #[test]
    fn referenced_term_object_key_with_xet_returns_ok() {
        let result = referenced_term_object_key(&[ServerFrontend::Xet], VALID_HASH);
        assert!(result.is_ok());
        let key = result.unwrap();
        // Should produce a valid xorb key
        assert!(key.as_str().starts_with("xorbs/default/"));
        assert!(key.as_str().ends_with(".xorb"));
    }

    #[test]
    fn referenced_term_object_key_without_xet_returns_error() {
        let result = referenced_term_object_key(&[ServerFrontend::Lfs], VALID_HASH);
        assert!(matches!(result, Err(GcError::InvalidContentHash)));
    }

    #[test]
    fn managed_protocol_object_identity_with_xet_and_xorb_key_returns_hash() {
        let xorb_key = xorb_object_key(VALID_HASH).unwrap();
        let result = managed_protocol_object_identity(&[ServerFrontend::Xet], &xorb_key).unwrap();
        assert_eq!(result.as_deref(), Some(VALID_HASH));
    }

    #[test]
    fn managed_protocol_object_identity_with_non_xorb_key_returns_none() {
        // A key that doesn't match xorbs/ or shards/ pattern
        let key = ObjectKey::parse("other/somevalue").unwrap();
        let result = managed_protocol_object_identity(&[ServerFrontend::Xet], &key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn managed_protocol_object_identity_without_xet_returns_none() {
        let xorb_key = xorb_object_key(VALID_HASH).unwrap();
        let result = managed_protocol_object_identity(&[ServerFrontend::Lfs], &xorb_key).unwrap();
        assert_eq!(result, None);
    }

    // --- GC safety guarantee tests ---

    #[test]
    fn optional_chunk_container_keys_xet_and_lfs_no_duplicates() {
        // When both Xet and Lfs are present, only the Xet xorb key should be
        // returned — no duplicate entries.
        let frontends = [ServerFrontend::Xet, ServerFrontend::Lfs];
        let keys = optional_chunk_container_keys(&frontends, VALID_HASH).unwrap();
        assert_eq!(keys.len(), 1, "should return exactly one xorb key");
        assert!(keys[0].as_str().starts_with("xorbs/default/"));
        assert!(keys[0].as_str().ends_with(".xorb"));
    }

    #[test]
    fn optional_chunk_container_keys_non_xet_frontends_returns_empty() {
        // Frontends that don't contribute chunk container keys should yield an
        // empty vec.
        let frontends = [
            ServerFrontend::Lfs,
            ServerFrontend::BazelHttp,
            ServerFrontend::Oci,
            ServerFrontend::Hub,
        ];
        let keys = optional_chunk_container_keys(&frontends, VALID_HASH).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn referenced_term_object_key_empty_frontends_returns_invalid_content_hash() {
        let result = referenced_term_object_key(&[], VALID_HASH);
        assert!(
            matches!(result, Err(GcError::InvalidContentHash)),
            "empty frontends should yield InvalidContentHash"
        );
    }

    #[test]
    fn referenced_term_object_key_with_xet_and_invalid_hash_returns_error() {
        // A hash shorter than 64 hex characters should be rejected.
        let short_hash = "abc123";
        let result = referenced_term_object_key(&[ServerFrontend::Xet], short_hash);
        assert!(result.is_err());
    }

    #[test]
    fn managed_protocol_object_identity_with_shard_key_returns_hash() {
        // Shard keys follow the format shards/<prefix>/<hash>.shard
        let shard_key = shardline_xet_adapter::shard_object_key(VALID_HASH).unwrap();
        let result = managed_protocol_object_identity(&[ServerFrontend::Xet], &shard_key).unwrap();
        assert_eq!(
            result.as_deref(),
            Some(VALID_HASH),
            "shard key should yield the embedded hash"
        );
    }

    #[test]
    fn managed_protocol_object_identity_with_chunk_key_returns_none() {
        // Chunk keys use the format <prefix>/<hash> which is neither an xorb nor
        // a shard key, so managed_protocol_object_identity should return None.
        let chunk_key = shardline_server_core::chunk_object_key(VALID_HASH).unwrap();
        let result = managed_protocol_object_identity(&[ServerFrontend::Xet], &chunk_key).unwrap();
        assert_eq!(result, None, "chunk key is not an xorb or shard");
    }

    // ── visit_protocol_object_member_chunks tests ────────────────────────

    #[test]
    fn visit_protocol_object_member_chunks_without_xet_returns_ok() {
        // When the frontend list doesn't include Xet, the function should
        // return Ok(()) without calling the visitor.
        let object_store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("some/key").unwrap();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(
            &[ServerFrontend::Lfs],
            &object_store,
            &key,
            |_hash| {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert!(!visited, "visitor should not be called without Xet frontend");
    }

    #[test]
    fn visit_protocol_object_member_chunks_with_non_xorb_key_returns_ok() {
        // When the key is not an xorb, the function should return Ok(())
        // without calling the visitor.
        let object_store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("some/key").unwrap();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(
            &[ServerFrontend::Xet],
            &object_store,
            &key,
            |_hash| {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert!(!visited, "visitor should not be called for non-xorb key");
    }

    #[test]
    fn visit_protocol_object_member_chunks_with_empty_frontends_returns_ok() {
        let object_store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("some/key").unwrap();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(
            &[],
            &object_store,
            &key,
            |_hash| {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert!(!visited);
    }

    #[test]
    fn visit_protocol_object_member_chunks_multiple_frontends_xet_first() {
        // With multiple frontends including Xet, and a non-xorb key,
        // the function should still complete without error.
        let object_store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("some/key").unwrap();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(
            &[ServerFrontend::Xet, ServerFrontend::Lfs],
            &object_store,
            &key,
            |_hash| {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok());
        assert!(!visited);
    }

    // ── optional_chunk_container_keys additional coverage ───────────────

    #[test]
    fn optional_chunk_container_keys_with_duplicate_xet_key_dedup() {
        // When Xet appears twice in the frontend list, the result should
        // still contain only one xorb key (deduplication via contains check).
        let frontends = [ServerFrontend::Xet, ServerFrontend::Xet];
        let keys = optional_chunk_container_keys(&frontends, VALID_HASH).unwrap();
        assert_eq!(keys.len(), 1, "duplicate Xet frontends must not produce duplicates");
    }
}
