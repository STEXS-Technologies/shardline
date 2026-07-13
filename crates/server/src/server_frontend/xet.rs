use std::io::Cursor;

use shardline_index::{FileChunkRecord, parse_xet_hash_hex};
use shardline_storage::{ObjectKey, ObjectStore};

use crate::{
    InvalidSerializedShardError, ServerError,
    object_store::{ServerObjectStore, read_full_object},
    xet_adapter::{
        XorbVisitError, try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
        visit_stored_xorb_chunk_hashes, xorb_hash_from_object_key_if_present, xorb_object_key,
    },
};

fn map_xorb_visit_error_server(error: XorbVisitError<ServerError>) -> ServerError {
    match error {
        XorbVisitError::Parse(error) => ServerError::from(error),
        XorbVisitError::Visitor(error) => error,
    }
}

pub(super) fn push_optional_chunk_container_key(
    object_keys: &mut Vec<ObjectKey>,
    chunk_hash: &str,
) -> Result<(), ServerError> {
    let object_key = xorb_object_key(chunk_hash)?;
    if !object_keys.contains(&object_key) {
        object_keys.push(object_key);
    }
    Ok(())
}

pub(super) fn referenced_term_object_key(term_hash: &str) -> Result<ObjectKey, ServerError> {
    Ok(xorb_object_key(term_hash)?)
}

pub(super) fn owns_protocol_object(key: &ObjectKey) -> Result<bool, ServerError> {
    Ok(xorb_hash_from_object_key_if_present(key)?.is_some())
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    let mut result = Ok(());
    visit_stored_xorb_chunk_hashes(object_store, object_key, |chunk_hash_hex| {
        match visitor(chunk_hash_hex) {
            Ok(()) => Ok(()),
            Err(e) => {
                result = Err(e);
                Err(crate::xet_adapter::XetAdapterError::NotFound)
            }
        }
    })?;
    result
}

pub(super) fn append_referenced_term_bytes(
    object_store: &ServerObjectStore,
    term: &FileChunkRecord,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    if term.range_end <= term.range_start {
        return Err(InvalidSerializedShardError::NativeXetTermEmptyOrInvertedChunkRange.into());
    }

    let xorb_key = xorb_object_key(&term.hash)?;
    let Some(metadata) = object_store.metadata(&xorb_key)? else {
        return Err(ServerError::MissingReferencedXorb);
    };
    let xorb_bytes = read_full_object(object_store, &xorb_key, metadata.length())?;
    let expected_hash = parse_xet_hash_hex(&term.hash)?;
    let mut reader = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    let range_start = usize::try_from(term.range_start)?;
    let range_end = usize::try_from(term.range_end)?;
    if range_end > validated.chunks().len() {
        return Err(InvalidSerializedShardError::NativeXetTermRangeExceededXorbChunkCount.into());
    }

    let mut chunk_index = 0_usize;
    try_for_each_serialized_xorb_chunk(&mut reader, &validated, |decoded_chunk| {
        if chunk_index >= range_start && chunk_index < range_end {
            output.extend_from_slice(decoded_chunk.data());
        }
        chunk_index = chunk_index.checked_add(1).ok_or(ServerError::Overflow)?;
        Ok::<(), ServerError>(())
    })
    .map_err(map_xorb_visit_error_server)
}

#[cfg(test)]
mod tests {
    use shardline_storage::ObjectKey;

    use super::*;
    use crate::xet_adapter::{XorbParseError, XorbVisitError};

    // -----------------------------------------------------------------------
    // map_xorb_visit_error_server
    // -----------------------------------------------------------------------

    #[test]
    fn map_xorb_visit_error_parse_hash_mismatch() {
        let err = XorbVisitError::Parse(XorbParseError::HashMismatch);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::XorbHashMismatch));
    }

    #[test]
    fn map_xorb_visit_error_parse_other_error() {
        // Non-HashMismatch parse errors map to InvalidSerializedXorb
        let io_err = std::io::Error::other("test io");
        let err = XorbVisitError::Parse(XorbParseError::Io(io_err));
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::InvalidSerializedXorb));
    }

    #[test]
    fn map_xorb_visit_error_visitor_passthrough() {
        let err = XorbVisitError::Visitor(ServerError::NotFound);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::NotFound));
    }

    #[test]
    fn map_xorb_visit_error_visitor_other_error() {
        let err = XorbVisitError::Visitor(ServerError::Overflow);
        let result = map_xorb_visit_error_server(err);
        assert!(matches!(result, ServerError::Overflow));
    }

    // -----------------------------------------------------------------------
    // push_optional_chunk_container_key
    // -----------------------------------------------------------------------

    #[test]
    fn push_optional_chunk_key_valid_hash() {
        let hash = "ab".repeat(32);
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, &hash);
        assert!(result.is_ok());
        assert_eq!(keys.len(), 1);
        let expected_key = xorb_object_key(&hash).unwrap();
        assert_eq!(keys[0], expected_key);
    }

    #[test]
    fn push_optional_chunk_key_deduplicates() {
        let hash = "cd".repeat(32);
        let mut keys = Vec::new();
        push_optional_chunk_container_key(&mut keys, &hash).unwrap();
        push_optional_chunk_container_key(&mut keys, &hash).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn push_optional_chunk_key_rejects_invalid_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, "not-a-hex-hash");
        assert!(result.is_err());
    }

    #[test]
    fn push_optional_chunk_key_rejects_uppercase_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, &"AA".repeat(32));
        assert!(result.is_err());
    }

    #[test]
    fn push_optional_chunk_key_rejects_short_hash() {
        let mut keys = Vec::new();
        let result = push_optional_chunk_container_key(&mut keys, "abc");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // referenced_term_object_key
    // -----------------------------------------------------------------------

    #[test]
    fn referenced_term_object_key_valid_hash() {
        let hash = "ef".repeat(32);
        let result = referenced_term_object_key(&hash);
        assert!(result.is_ok());
        let expected = xorb_object_key(&hash).unwrap();
        assert_eq!(result.unwrap(), expected);
    }

    #[test]
    fn referenced_term_object_key_invalid_hash() {
        let result = referenced_term_object_key("invalid");
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // owns_protocol_object
    // -----------------------------------------------------------------------

    #[test]
    fn owns_protocol_object_with_xorb_key() {
        let hash = "01".repeat(32);
        let key = xorb_object_key(&hash).unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_non_xorb_key() {
        let key = ObjectKey::parse("shards/ab/some.shard").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_chunk_key() {
        let key = ObjectKey::parse("ab/abcdef1234567890").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn owns_protocol_object_with_random_key() {
        let key = ObjectKey::parse("some/arbitrary/path").unwrap();
        let result = owns_protocol_object(&key);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
