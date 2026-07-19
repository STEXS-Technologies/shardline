use shardline_index::FileChunkRecord;
use shardline_storage::ObjectKey;

use crate::{ServerError, object_store::ServerObjectStore};

use super::{ServerFrontend, xet};

pub(crate) fn optional_chunk_container_keys(
    frontends: &[ServerFrontend],
    chunk_hash: &str,
) -> Result<Vec<ObjectKey>, ServerError> {
    let mut object_keys = Vec::new();
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                xet::push_optional_chunk_container_key(&mut object_keys, chunk_hash)?
            }
            ServerFrontend::Lfs
            | ServerFrontend::BazelHttp
            | ServerFrontend::Oci
            | ServerFrontend::Hub => {}
        }
    }

    Ok(object_keys)
}

pub(crate) fn referenced_term_object_key(
    frontends: &[ServerFrontend],
    term_hash: &str,
) -> Result<ObjectKey, ServerError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return xet::referenced_term_object_key(term_hash);
    }

    Err(ServerError::InvalidContentHash)
}

pub(crate) fn visit_protocol_object_member_chunks<Visitor>(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    for frontend in frontends {
        match frontend {
            ServerFrontend::Xet => {
                if xet::owns_protocol_object(object_key)? {
                    return xet::visit_protocol_object_member_chunks(
                        object_store,
                        object_key,
                        visitor,
                    );
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

pub(crate) fn append_referenced_term_bytes(
    frontends: &[ServerFrontend],
    object_store: &ServerObjectStore,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    if frontends.contains(&ServerFrontend::Xet) {
        return xet::append_referenced_term_bytes(object_store, term, output);
    }

    Err(ServerError::InvalidContentHash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServerObjectStore;
    use crate::xet_adapter::xorb_object_key;

    // -----------------------------------------------------------------------
    // optional_chunk_container_keys
    // -----------------------------------------------------------------------

    #[test]
    fn optional_keys_with_xet_frontend_and_valid_hash() {
        let hash = "ab".repeat(32);
        let frontends = vec![ServerFrontend::Xet];
        let result = optional_chunk_container_keys(&frontends, &hash);
        assert!(result.is_ok());
        let keys = result.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], xorb_object_key(&hash).unwrap());
    }

    #[test]
    fn optional_keys_with_xet_frontend_and_invalid_hash() {
        let frontends = vec![ServerFrontend::Xet];
        let result = optional_chunk_container_keys(&frontends, "bad");
        assert!(result.is_err());
    }

    #[test]
    fn optional_keys_without_xet_frontend_returns_empty() {
        let hash = "ab".repeat(32);
        let frontends = vec![
            ServerFrontend::Lfs,
            ServerFrontend::BazelHttp,
            ServerFrontend::Oci,
            ServerFrontend::Hub,
        ];
        let result = optional_chunk_container_keys(&frontends, &hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn optional_keys_with_mixed_frontends_xet_alone_produces_keys() {
        let hash = "cd".repeat(32);
        let frontends = vec![
            ServerFrontend::Lfs,
            ServerFrontend::Xet,
            ServerFrontend::Oci,
        ];
        let result = optional_chunk_container_keys(&frontends, &hash);
        assert!(result.is_ok());
        let keys = result.unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], xorb_object_key(&hash).unwrap());
    }

    #[test]
    fn optional_keys_without_any_frontends_returns_empty() {
        let hash = "ab".repeat(32);
        let frontends: Vec<ServerFrontend> = Vec::new();
        let result = optional_chunk_container_keys(&frontends, &hash);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // referenced_term_object_key
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_term_key_with_xet_frontend_and_valid_hash() {
        let hash = "ef".repeat(32);
        let frontends = vec![ServerFrontend::Xet];
        let result = referenced_term_object_key(&frontends, &hash);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), xorb_object_key(&hash).unwrap());
    }

    #[test]
    fn referenced_term_key_with_xet_frontend_and_invalid_hash() {
        let frontends = vec![ServerFrontend::Xet];
        let result = referenced_term_object_key(&frontends, "bad-hash");
        assert!(result.is_err());
    }

    #[test]
    fn referenced_term_key_without_xet_frontend_returns_error() {
        let hash = "ef".repeat(32);
        let frontends = vec![ServerFrontend::Lfs];
        let result = referenced_term_object_key(&frontends, &hash);
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[test]
    fn referenced_term_key_with_empty_frontends_returns_error() {
        let hash = "ef".repeat(32);
        let frontends: Vec<ServerFrontend> = Vec::new();
        let result = referenced_term_object_key(&frontends, &hash);
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    // -----------------------------------------------------------------------
    // visit_protocol_object_member_chunks
    // -----------------------------------------------------------------------

    #[test]
    fn visit_protocol_object_member_chunks_with_non_xet_frontends_returns_ok() {
        let hash = "ef".repeat(32);
        let key = ObjectKey::parse(&format!("xorbs/default/{h}/{h}", h = &hash[..2])).unwrap();
        // Use a blackhole store (never returns metadata)
        let store = ServerObjectStore::blackhole();
        let frontends = vec![
            ServerFrontend::Lfs,
            ServerFrontend::Oci,
            ServerFrontend::BazelHttp,
            ServerFrontend::Hub,
        ];
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(&frontends, &store, &key, |_hash| {
            visited = true;
            Ok(())
        });
        // Non-Xet frontends should return Ok without calling the visitor
        assert!(result.is_ok());
        assert!(!visited);
    }

    #[test]
    fn visit_protocol_object_member_chunks_with_empty_frontends_returns_ok() {
        let key = ObjectKey::parse("some/object").unwrap();
        let store = ServerObjectStore::blackhole();
        let frontends: Vec<ServerFrontend> = Vec::new();
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(&frontends, &store, &key, |_hash| {
            visited = true;
            Ok(())
        });
        assert!(result.is_ok());
        assert!(!visited);
    }

    #[test]
    fn visit_protocol_object_member_chunks_with_xet_but_non_xorb_key_skips_visit() {
        // Xet frontend with a non-xorb key → owns_protocol_object returns false
        // → falls through without visiting.
        let key = ObjectKey::parse("shards/ab/some.shard").unwrap();
        let store = ServerObjectStore::blackhole();
        let frontends = vec![ServerFrontend::Xet];
        let mut visited = false;
        let result = visit_protocol_object_member_chunks(&frontends, &store, &key, |_hash| {
            visited = true;
            Ok(())
        });
        assert!(result.is_ok());
        assert!(!visited);
    }

    // -----------------------------------------------------------------------
    // append_referenced_term_bytes
    // -----------------------------------------------------------------------

    #[test]
    fn append_referenced_term_bytes_with_non_xet_frontends_returns_error() {
        use shardline_index::FileChunkRecord;
        let store = ServerObjectStore::blackhole();
        let term = FileChunkRecord {
            hash: "ab".repeat(32),
            offset: 0,
            length: 1024,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 1024,
        };
        let frontends = vec![ServerFrontend::Lfs];
        let mut output = Vec::new();
        let result = append_referenced_term_bytes(&frontends, &store, &term, &mut output);
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[test]
    fn append_referenced_term_bytes_with_empty_frontends_returns_error() {
        use shardline_index::FileChunkRecord;
        let store = ServerObjectStore::blackhole();
        let term = FileChunkRecord {
            hash: "ab".repeat(32),
            offset: 0,
            length: 1024,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 1024,
        };
        let frontends: Vec<ServerFrontend> = Vec::new();
        let mut output = Vec::new();
        let result = append_referenced_term_bytes(&frontends, &store, &term, &mut output);
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }
}
