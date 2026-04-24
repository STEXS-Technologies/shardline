use shardline_index::xet_hash_hex_string;
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectKey, ObjectKeyError};

use crate::{ServerError, validation::validate_content_hash};

pub(crate) fn chunk_object_key(hash_hex: &str) -> Result<ObjectKey, ServerError> {
    validate_content_hash(hash_hex)?;
    chunk_object_key_from_valid_hash_hex(hash_hex)
}

pub(crate) fn chunk_object_key_for_computed_hash(
    hash: ShardlineHash,
) -> Result<(String, ObjectKey), ServerError> {
    let hash_hex = xet_hash_hex_string(hash);
    let object_key = chunk_object_key_from_valid_hash_hex(&hash_hex)?;
    Ok((hash_hex, object_key))
}

fn chunk_object_key_from_valid_hash_hex(hash_hex: &str) -> Result<ObjectKey, ServerError> {
    let prefix = hash_hex.get(..2).ok_or(ServerError::InvalidContentHash)?;
    let mut key = String::with_capacity(
        prefix
            .len()
            .checked_add(1)
            .and_then(|length| length.checked_add(hash_hex.len()))
            .ok_or(ServerError::Overflow)?,
    );
    key.push_str(prefix);
    key.push('/');
    key.push_str(hash_hex);
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

pub(crate) fn chunk_hash_from_chunk_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerError> {
    let mut segments = key.as_str().split('/');
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(candidate_hash_hex) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    if !candidate_hash_hex.starts_with(prefix) {
        return Ok(None);
    }
    validate_content_hash(candidate_hash_hex)?;
    Ok(Some(candidate_hash_hex))
}

const fn map_object_key_error(error: ObjectKeyError) -> ServerError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => ServerError::InvalidContentHash,
    }
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
