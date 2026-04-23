use std::{borrow::Cow, io::Cursor};

use shardline_protocol::{
    ShardlineHash, ValidatedXorb, try_for_each_serialized_xorb_chunk, validate_serialized_xorb,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectKeyError, PutOutcome};
use xet_core_structures::xorb_object::reconstruct_xorb_with_footer;

use crate::{
    ServerError,
    chunk_store::chunk_object_key,
    local_backend::chunk_hash,
    object_store::{ServerObjectStore, read_full_object},
    validation::validate_content_hash,
};

use super::map_xorb_visit_error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StoredXorbUpload {
    pub(crate) was_inserted: bool,
    pub(crate) stored_bytes: u64,
}

pub(crate) fn xorb_object_key(hash_hex: &str) -> Result<ObjectKey, ServerError> {
    validate_content_hash(hash_hex)?;
    let prefix = hash_hex.get(..2).ok_or(ServerError::InvalidContentHash)?;
    let key = format!("xorbs/default/{prefix}/{hash_hex}.xorb");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

pub(crate) fn xorb_hash_from_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerError> {
    let mut segments = key.as_str().split('/');
    let Some(namespace) = segments.next() else {
        return Ok(None);
    };
    let Some(default_namespace) = segments.next() else {
        return Ok(None);
    };
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(file_name) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if namespace != "xorbs" || default_namespace != "default" {
        return Ok(None);
    }
    if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    let Some(hash_hex) = file_name.strip_suffix(".xorb") else {
        return Ok(None);
    };
    if !hash_hex.starts_with(prefix) {
        return Ok(None);
    }
    validate_content_hash(hash_hex)?;
    Ok(Some(hash_hex))
}

pub(crate) fn visit_stored_xorb_chunk_hashes<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), ServerError>
where
    Visitor: FnMut(String) -> Result<(), ServerError>,
{
    let Some(metadata) = object_store.metadata(object_key)? else {
        return Ok(());
    };
    let Some(xorb_hash_hex) = xorb_hash_from_object_key_if_present(object_key)? else {
        return Ok(());
    };
    let expected_hash = ShardlineHash::parse_api_hex(xorb_hash_hex)?;
    let xorb_bytes = read_full_object(object_store, object_key, metadata.length())?;
    let mut cursor = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut cursor, expected_hash)?;
    try_for_each_serialized_xorb_chunk(&mut cursor, &validated, |decoded_chunk| {
        visitor(decoded_chunk.descriptor().hash().api_hex_string())
    })
    .map_err(map_xorb_visit_error)
}

pub(crate) fn store_uploaded_xorb(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
) -> Result<StoredXorbUpload, ServerError> {
    let expected_hash_value = ShardlineHash::parse_api_hex(expected_hash)?;
    let (canonical_bytes, validated) =
        canonicalize_uploaded_xorb(expected_hash_value, uploaded_bytes)?;
    let canonical_length = u64::try_from(canonical_bytes.len())?;
    let mut cursor = Cursor::new(canonical_bytes.as_ref());
    let mut unpacked_length = 0_u64;
    let mut stored_bytes = 0_u64;

    try_for_each_serialized_xorb_chunk(&mut cursor, &validated, |decoded_chunk| {
        let chunk_hash_hex = decoded_chunk.descriptor().hash().api_hex_string();
        let chunk_length = u64::try_from(decoded_chunk.data().len())?;
        unpacked_length = unpacked_length
            .checked_add(chunk_length)
            .ok_or(ServerError::Overflow)?;
        let chunk_integrity = ObjectIntegrity::new(chunk_hash(decoded_chunk.data()), chunk_length);
        let chunk_key = chunk_object_key(&chunk_hash_hex)?;
        let outcome = object_store.put_if_absent(
            &chunk_key,
            ObjectBody::from_slice(decoded_chunk.data()),
            &chunk_integrity,
        )?;
        if matches!(outcome, PutOutcome::Inserted) {
            stored_bytes = stored_bytes
                .checked_add(chunk_length)
                .ok_or(ServerError::Overflow)?;
        }
        Ok::<(), ServerError>(())
    })
    .map_err(map_xorb_visit_error)?;

    if unpacked_length != validated.unpacked_length() {
        return Err(ServerError::InvalidSerializedXorb);
    }

    let serialized_key = xorb_object_key(expected_hash)?;
    let serialized_integrity =
        ObjectIntegrity::new(chunk_hash(canonical_bytes.as_ref()), canonical_length);
    let serialized_outcome = object_store.put_if_absent(
        &serialized_key,
        ObjectBody::from_vec(canonical_bytes.into_owned()),
        &serialized_integrity,
    )?;
    if matches!(serialized_outcome, PutOutcome::Inserted) {
        stored_bytes = stored_bytes
            .checked_add(canonical_length)
            .ok_or(ServerError::Overflow)?;
    }

    Ok(StoredXorbUpload {
        was_inserted: matches!(serialized_outcome, PutOutcome::Inserted),
        stored_bytes,
    })
}

fn canonicalize_uploaded_xorb<'bytes>(
    expected_hash: ShardlineHash,
    uploaded_bytes: &'bytes [u8],
) -> Result<(Cow<'bytes, [u8]>, ValidatedXorb), ServerError> {
    let mut uploaded_cursor = Cursor::new(uploaded_bytes);
    match validate_serialized_xorb(&mut uploaded_cursor, expected_hash) {
        Ok(validated) => Ok((Cow::Borrowed(uploaded_bytes), validated)),
        Err(_error) => {
            let normalized = normalize_serialized_xorb(expected_hash, uploaded_bytes)?;
            let mut normalized_cursor = Cursor::new(normalized.as_slice());
            let validated = validate_serialized_xorb(&mut normalized_cursor, expected_hash)
                .map_err(ServerError::from)?;
            Ok((Cow::Owned(normalized), validated))
        }
    }
}

pub(crate) fn normalize_serialized_xorb(
    expected_hash: ShardlineHash,
    bytes: &[u8],
) -> Result<Vec<u8>, ServerError> {
    let mut normalized = Vec::with_capacity(bytes.len());
    let (_xorb, computed_hash) = reconstruct_xorb_with_footer(&mut normalized, bytes)
        .map_err(|_error| ServerError::InvalidSerializedXorb)?;
    let computed_hash = ShardlineHash::parse_api_hex(&computed_hash.hex())?;
    if computed_hash != expected_hash {
        return Err(ServerError::XorbHashMismatch);
    }

    Ok(normalized)
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
    use std::{borrow::Cow, io::Cursor};

    use shardline_protocol::{ShardlineHash, validate_serialized_xorb};
    use xet_core_structures::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };

    use super::{
        canonicalize_uploaded_xorb, normalize_serialized_xorb,
        xorb_hash_from_object_key_if_present, xorb_object_key,
    };
    use crate::ServerError;

    #[test]
    fn normalize_serialized_xorb_accepts_footerless_uploads() {
        let raw = build_raw_xorb(4, ChunkSize::Fixed(1024));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false);
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        assert!(serialized.footer_start.is_none());
        let expected_hash = ShardlineHash::parse_api_hex(&serialized.hash.hex());
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };

        let normalized = normalize_serialized_xorb(expected_hash, &serialized.serialized_data);
        assert!(normalized.is_ok());
        let Ok(normalized) = normalized else {
            return;
        };
        assert!(normalized.len() > serialized.serialized_data.len());

        let validated = validate_serialized_xorb(&mut Cursor::new(normalized), expected_hash);
        assert!(validated.is_ok());
    }

    #[test]
    fn normalize_serialized_xorb_rejects_wrong_hash() {
        let raw = build_raw_xorb(2, ChunkSize::Fixed(768));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, false);
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let wrong_hash = ShardlineHash::from_bytes([9; 32]);

        let normalized = normalize_serialized_xorb(wrong_hash, &serialized.serialized_data);
        assert!(matches!(normalized, Err(ServerError::XorbHashMismatch)));
    }

    #[test]
    fn canonicalize_uploaded_xorb_borrows_already_canonical_bytes() {
        let raw = build_raw_xorb(3, ChunkSize::Fixed(640));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true);
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = ShardlineHash::parse_api_hex(&serialized.hash.hex());
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };

        let canonicalized = canonicalize_uploaded_xorb(expected_hash, &serialized.serialized_data);

        assert!(canonicalized.is_ok());
        let Ok((canonicalized, validated)) = canonicalized else {
            return;
        };
        assert!(matches!(canonicalized, Cow::Borrowed(_)));
        assert_eq!(
            canonicalized.as_ref(),
            serialized.serialized_data.as_slice()
        );
        assert_eq!(validated.hash(), expected_hash);
    }

    #[test]
    fn canonicalize_uploaded_xorb_normalizes_footerless_bytes() {
        let raw = build_raw_xorb(3, ChunkSize::Fixed(640));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false);
        assert!(serialized.is_ok());
        let Ok(serialized) = serialized else {
            return;
        };
        let expected_hash = ShardlineHash::parse_api_hex(&serialized.hash.hex());
        assert!(expected_hash.is_ok());
        let Ok(expected_hash) = expected_hash else {
            return;
        };

        let canonicalized = canonicalize_uploaded_xorb(expected_hash, &serialized.serialized_data);

        assert!(canonicalized.is_ok());
        let Ok((canonicalized, validated)) = canonicalized else {
            return;
        };
        assert!(matches!(canonicalized, Cow::Owned(_)));
        assert!(canonicalized.len() > serialized.serialized_data.len());
        assert_eq!(validated.hash(), expected_hash);
    }

    #[test]
    fn xorb_object_key_rejects_uppercase_hashes() {
        let hash = "AA".repeat(32);
        let key = xorb_object_key(&hash);

        assert!(key.is_err());
    }

    #[test]
    fn xorb_hash_from_object_key_extracts_hash_for_native_xorb_layout() {
        let hash = "ab".repeat(32);
        let key = xorb_object_key(&hash);

        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let extracted = xorb_hash_from_object_key_if_present(&key);

        assert!(extracted.is_ok());
        if let Ok(extracted) = extracted {
            assert_eq!(extracted, Some(hash.as_str()));
        }
    }
}
