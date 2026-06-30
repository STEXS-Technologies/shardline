use shardline_index::xet_hash_hex_string;
use shardline_protocol::ShardlineHash;
use shardline_storage::ObjectKey;

use crate::ServerError;

pub(crate) fn chunk_object_key(hash_hex: &str) -> Result<ObjectKey, ServerError> {
    Ok(shardline_server_core::chunk_object_key(hash_hex)?)
}

pub(crate) fn chunk_object_key_for_computed_hash(
    hash: ShardlineHash,
) -> Result<(String, ObjectKey), ServerError> {
    let hash_hex = xet_hash_hex_string(hash);
    let object_key = shardline_server_core::chunk_object_key(&hash_hex)?;
    Ok((hash_hex, object_key))
}

pub(crate) fn chunk_hash_from_chunk_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerError> {
    Ok(shardline_server_core::chunk_hash_from_chunk_object_key_if_present(key)?)
}

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;
    use shardline_storage::ObjectKey;

    use super::{
        chunk_hash_from_chunk_object_key_if_present, chunk_object_key,
        chunk_object_key_for_computed_hash,
    };

    #[test]
    fn chunk_object_key_maps_hash_into_prefix_layout() {
        let hash = "de".repeat(32);
        let key = chunk_object_key(&hash);

        assert!(key.is_ok());
        if let Ok(key) = key {
            assert_eq!(key.as_str(), format!("de/{hash}"));
        }
    }

    #[test]
    fn computed_hash_object_key_maps_hash_into_prefix_layout_without_reparsing_hash() {
        let hash = ShardlineHash::from_bytes([0xde; 32]);
        let key = chunk_object_key_for_computed_hash(hash);

        assert!(key.is_ok());
        if let Ok((hash_hex, key)) = key {
            assert_eq!(hash_hex, "de".repeat(32));
            assert_eq!(key.as_str(), format!("de/{hash_hex}"));
        }
    }

    #[test]
    fn chunk_object_key_rejects_uppercase_hashes() {
        let hash = "AA".repeat(32);
        let key = chunk_object_key(&hash);

        assert!(key.is_err());
    }

    #[test]
    fn chunk_hash_from_chunk_object_key_if_present_skips_non_chunk_namespaces() {
        let key = ObjectKey::parse("shards/ab/example.shard");

        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let extracted = chunk_hash_from_chunk_object_key_if_present(&key);

        assert!(matches!(extracted, Ok(None)));
    }
}
