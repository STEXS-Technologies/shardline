use std::io::Cursor;

use shardline_index::FileChunkRecord;
use shardline_protocol::{
    ShardlineHash, try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
};
use shardline_storage::ObjectKey;

use crate::{
    InvalidSerializedShardError, ServerError,
    object_store::{ServerObjectStore, read_full_object},
    xet_adapter::{
        map_xorb_visit_error, shard_hash_from_object_key_if_present,
        visit_stored_xorb_chunk_hashes, xorb_hash_from_object_key_if_present, xorb_object_key,
    },
};

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
    xorb_object_key(term_hash)
}

pub(super) fn managed_protocol_object_identity(
    key: &ObjectKey,
) -> Result<Option<String>, ServerError> {
    if let Some(hash) = xorb_hash_from_object_key_if_present(key)? {
        return Ok(Some(hash.to_owned()));
    }
    if let Some(hash) = shard_hash_from_object_key_if_present(key)? {
        return Ok(Some(hash.to_owned()));
    }

    Ok(None)
}

pub(super) fn owns_protocol_object(key: &ObjectKey) -> Result<bool, ServerError> {
    Ok(xorb_hash_from_object_key_if_present(key)?.is_some())
}

pub(super) fn visit_protocol_object_member_chunks<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    visit_stored_xorb_chunk_hashes(object_store, object_key, visitor)
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
    let expected_hash = ShardlineHash::parse_api_hex(&term.hash)?;
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
    .map_err(map_xorb_visit_error)
}
