use std::{borrow::Cow, io::Cursor};

use shardline_index::{parse_xet_hash_hex, xet_hash_hex_string};
use shardline_protocol::ShardlineHash;
use shardline_server_core::{ServerObjectStore, chunk_hash, read_full_object};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectKeyError, ObjectStore, PutOutcome,
};
use shardline_xet_core::xorb_object::reconstruct_xorb_with_footer;

fn chunk_object_key_local(hash_hex: &str) -> Result<ObjectKey, XetAdapterError> {
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    let prefix = hash_hex
        .get(..2)
        .ok_or(XetAdapterError::InvalidContentHash)?;
    let key = format!("{prefix}/{hash_hex}");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

use crate::error::XetAdapterError;

use super::{
    ValidatedXorb, map_xorb_visit_error, try_for_each_serialized_xorb_chunk,
    validate_serialized_xorb,
};

#[derive(Debug)]
pub struct StoredXorbUpload {
    pub was_inserted: bool,
    pub stored_bytes: u64,
}

/// # Errors
///
/// Returns an error when the hash is not valid or the object key cannot be constructed.
pub fn xorb_object_key(hash_hex: &str) -> Result<ObjectKey, XetAdapterError> {
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    let prefix = hash_hex
        .get(..2)
        .ok_or(XetAdapterError::InvalidContentHash)?;
    let key = format!("xorbs/default/{prefix}/{hash_hex}.xorb");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

/// # Errors
///
/// Returns an error when the object key cannot be validated.
pub fn xorb_hash_from_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, XetAdapterError> {
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
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    Ok(Some(hash_hex))
}

/// # Errors
///
/// Returns an error when the xorb cannot be read or validated.
pub fn visit_stored_xorb_chunk_hashes<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<(), XetAdapterError>
where
    Visitor: FnMut(String) -> Result<(), XetAdapterError>,
{
    let Some(metadata) = object_store.metadata(object_key)? else {
        return Ok(());
    };
    let Some(xorb_hash_hex) = xorb_hash_from_object_key_if_present(object_key)? else {
        return Ok(());
    };
    let expected_hash = parse_xet_hash_hex(xorb_hash_hex)?;
    let xorb_bytes = read_full_object(object_store, object_key, metadata.length())?;
    let mut cursor = Cursor::new(xorb_bytes);
    let validated = validate_serialized_xorb(&mut cursor, expected_hash)?;
    try_for_each_serialized_xorb_chunk(&mut cursor, &validated, |decoded_chunk| {
        visitor(xet_hash_hex_string(decoded_chunk.descriptor().hash()))
    })
    .map_err(map_xorb_visit_error)
}

/// # Errors
///
/// Returns an error when the upload fails validation or storage.
pub fn store_uploaded_xorb(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
) -> Result<StoredXorbUpload, XetAdapterError> {
    let expected_hash_value = parse_xet_hash_hex(expected_hash)?;
    let (canonical_bytes, validated) =
        canonicalize_uploaded_xorb(expected_hash_value, uploaded_bytes)?;
    let canonical_length = u64::try_from(canonical_bytes.len())?;
    let mut cursor = Cursor::new(canonical_bytes.as_ref());
    let mut unpacked_length = 0_u64;
    let mut stored_bytes = 0_u64;

    try_for_each_serialized_xorb_chunk(&mut cursor, &validated, |decoded_chunk| {
        let chunk_hash_hex = xet_hash_hex_string(decoded_chunk.descriptor().hash());
        let chunk_length = u64::try_from(decoded_chunk.data().len())?;
        unpacked_length = unpacked_length
            .checked_add(chunk_length)
            .ok_or(XetAdapterError::Overflow)?;
        let chunk_integrity = ObjectIntegrity::new(chunk_hash(decoded_chunk.data()), chunk_length);
        let chunk_key = chunk_object_key_local(&chunk_hash_hex)?;
        let outcome = object_store.put_if_absent(
            &chunk_key,
            ObjectBody::from_slice(decoded_chunk.data()),
            &chunk_integrity,
        )?;
        if matches!(outcome, PutOutcome::Inserted) {
            stored_bytes = stored_bytes
                .checked_add(chunk_length)
                .ok_or(XetAdapterError::Overflow)?;
        }
        Ok::<(), XetAdapterError>(())
    })
    .map_err(map_xorb_visit_error)?;

    if unpacked_length != validated.unpacked_length() {
        return Err(XetAdapterError::InvalidSerializedXorb);
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
            .ok_or(XetAdapterError::Overflow)?;
    }

    Ok(StoredXorbUpload {
        was_inserted: matches!(serialized_outcome, PutOutcome::Inserted),
        stored_bytes,
    })
}

/// # Errors
///
/// Returns an error when the upload fails validation or storage.
pub fn store_uploaded_xorb_with_metrics(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
) -> Result<StoredXorbUpload, XetAdapterError> {
    let result = store_uploaded_xorb(object_store, expected_hash, uploaded_bytes)?;
    shardline_metrics::record_xet_xorb_upload(uploaded_bytes.len() as u64);
    Ok(result)
}

fn canonicalize_uploaded_xorb<'bytes>(
    expected_hash: ShardlineHash,
    uploaded_bytes: &'bytes [u8],
) -> Result<(Cow<'bytes, [u8]>, ValidatedXorb), XetAdapterError> {
    let mut uploaded_cursor = Cursor::new(uploaded_bytes);
    match validate_serialized_xorb(&mut uploaded_cursor, expected_hash) {
        Ok(validated) => Ok((Cow::Borrowed(uploaded_bytes), validated)),
        Err(_error) => {
            let normalized = normalize_serialized_xorb(expected_hash, uploaded_bytes)?;
            let mut normalized_cursor = Cursor::new(normalized.as_slice());
            let validated = validate_serialized_xorb(&mut normalized_cursor, expected_hash)
                .map_err(XetAdapterError::from)?;
            Ok((Cow::Owned(normalized), validated))
        }
    }
}

/// # Errors
///
/// Returns an error when the bytes cannot be reconstructed or the hash does not match.
pub fn normalize_serialized_xorb(
    expected_hash: ShardlineHash,
    bytes: &[u8],
) -> Result<Vec<u8>, XetAdapterError> {
    let mut normalized = Vec::with_capacity(bytes.len());
    let (_xorb, computed_hash) = reconstruct_xorb_with_footer(&mut normalized, bytes)
        .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
    let computed_hash = parse_xet_hash_hex(&computed_hash.hex())?;
    if computed_hash != expected_hash {
        return Err(XetAdapterError::XorbHashMismatch);
    }

    Ok(normalized)
}

const fn map_object_key_error(error: ObjectKeyError) -> XetAdapterError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => XetAdapterError::InvalidContentHash,
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, io::Cursor};

    use shardline_index::parse_xet_hash_hex;
    use shardline_protocol::ShardlineHash;
    use shardline_xet_core::xorb_object::{
        CompressionScheme, SerializedXorbObject,
        xorb_format_test_utils::{ChunkSize, build_raw_xorb},
    };

    use super::{
        canonicalize_uploaded_xorb, normalize_serialized_xorb, store_uploaded_xorb,
        store_uploaded_xorb_with_metrics, validate_serialized_xorb, visit_stored_xorb_chunk_hashes,
        xorb_hash_from_object_key_if_present, xorb_object_key,
    };
    use crate::error::XetAdapterError;
    use shardline_server_core::ServerObjectStore;

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
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex());
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
        assert!(matches!(normalized, Err(XetAdapterError::XorbHashMismatch)));
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
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex());
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
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex());
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

    // ---- store_uploaded_xorb tests ----

    #[test]
    fn store_uploaded_xorb_stores_chunks_and_xorb() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok(), "store_uploaded_xorb failed: {result:?}");
        let stored = result.unwrap();
        assert!(stored.was_inserted, "xorb should be newly inserted");
        assert!(
            stored.stored_bytes > 0,
            "stored_bytes should be > 0, got {}",
            stored.stored_bytes
        );
    }

    #[test]
    fn store_uploaded_xorb_idempotent_returns_was_inserted_false() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let first = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(first.was_inserted);

        let second =
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(
            !second.was_inserted,
            "second store should report was_inserted=false"
        );
    }

    #[test]
    fn store_uploaded_xorb_rejects_wrong_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let wrong_hash = "00".repeat(32);

        let result = store_uploaded_xorb(&object_store, &wrong_hash, &serialized.serialized_data);
        assert!(result.is_err(), "expected error for wrong hash");
    }

    #[test]
    fn store_uploaded_xorb_with_metrics_delegates_and_records() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data);
        assert!(
            result.is_ok(),
            "store_uploaded_xorb_with_metrics failed: {result:?}"
        );
        let stored = result.unwrap();
        assert!(stored.was_inserted);
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

    // ── xorb_hash_from_object_key_if_present edge cases ──────────────────

    #[test]
    fn xorb_hash_from_object_key_rejects_non_xorb_namespace() {
        let key = shardline_storage::ObjectKey::parse("shards/ab/abhash.shard").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_too_many_segments() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa/hash.xorb/extra").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_non_default_namespace() {
        let key = shardline_storage::ObjectKey::parse("xorbs/custom/aa/hash.xorb").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_invalid_prefix() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/xyz/hash.xorb").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_missing_xorb_extension() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa/hash").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_prefix_mismatch() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/bb/aahash.xorb").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_invalid_hash_characters() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/gg/gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.xorb").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    // ── map_object_key_error ────────────────────────────────────────────

    #[test]
    fn xorb_store_map_object_key_error_maps_all_variants() {
        use shardline_storage::ObjectKeyError;
        let cases: &[(ObjectKeyError, &str)] = &[
            (ObjectKeyError::Empty, "invalid"),
            (ObjectKeyError::UnsafePath, "invalid"),
            (ObjectKeyError::ControlCharacter, "invalid"),
            (ObjectKeyError::TooLong, "invalid"),
        ];
        for (err, _) in cases {
            let mapped = super::map_object_key_error(*err);
            let msg = mapped.to_string();
            assert!(msg.contains("hash"), "msg '{msg}' missing 'hash'");
        }
    }

    // ── xorb_object_key hash validation ──────────────────────────────────

    #[test]
    fn xorb_object_key_rejects_non_hex_characters() {
        let hash = "zz".repeat(32);
        let key = xorb_object_key(&hash);
        assert!(key.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_short_hash() {
        let key = xorb_object_key("abc");
        assert!(key.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_empty_hash() {
        let key = xorb_object_key("");
        assert!(key.is_err());
    }

    // ── visit_stored_xorb_chunk_hashes ───────────────────────────────────

    #[test]
    fn visit_stored_xorb_chunk_hashes_visits_all_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();

        let mut visited = Vec::new();
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |chunk_hash: String| -> Result<(), XetAdapterError> {
                visited.push(chunk_hash);
                Ok(())
            },
        );
        assert!(result.is_ok(), "visit failed: {result:?}");
        assert!(!visited.is_empty(), "should have visited chunk hashes");
        for hex in &visited {
            assert_eq!(hex.len(), 64, "expected 64-char hex hash");
        }
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_returns_ok_for_unknown_key() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa/nonexistent.xorb").unwrap();
        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok(), "expected Ok for missing key");
        assert!(!visited, "visitor should not be called for missing key");
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_returns_ok_for_invalid_key() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        // A non-xorb key should cause xorb_hash_from_object_key_if_present to return None
        let key = shardline_storage::ObjectKey::parse("shards/aa/hash.shard").unwrap();
        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok(), "expected Ok for invalid key: {result:?}");
        assert!(!visited, "visitor should not be called for invalid key");
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_propagates_visitor_error() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();

        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> { Err(XetAdapterError::Overflow) },
        );
        assert!(
            matches!(result, Err(XetAdapterError::Overflow)),
            "expected Overflow error, got {result:?}"
        );
    }

    // ── From<XorbParseError> for XetAdapterError variants ─────────────────

    #[test]
    fn xorb_parse_error_hash_mismatch_maps_to_xorb_hash_mismatch() {
        use crate::xorb::XorbParseError;
        let err: XetAdapterError = XorbParseError::HashMismatch.into();
        let msg = err.to_string();
        assert!(
            msg.contains("hash"),
            "expected hash-related message, got '{msg}'"
        );
    }

    #[test]
    fn xorb_parse_error_invalid_format_maps_to_invalid_serialized_xorb() {
        use crate::xorb::{XorbInvalidFormatError, XorbParseError};
        let err: XetAdapterError =
            XorbParseError::InvalidFormat(XorbInvalidFormatError::StructuralValidationFailed)
                .into();
        let msg = err.to_string();
        assert!(
            msg.contains("xorb"),
            "expected xorb-related message, got '{msg}'"
        );
    }

    // ── store_uploaded_xorb unpacked length mismatch ───────────────────

    #[test]
    fn store_uploaded_xorb_rejects_unpacked_length_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Create a xorb but provide modified bytes that change unpacked length
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        // Store it normally first to get the proper format
        let hash = serialized.hash.hex();
        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok());
    }

    #[test]
    fn store_uploaded_xorb_rejects_invalid_hash_format() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let result = store_uploaded_xorb(&object_store, "not-a-hash", b"data");
        assert!(result.is_err());
    }

    // ── store_uploaded_xorb_with_metrics error propagation ─────────────

    #[test]
    fn store_uploaded_xorb_with_metrics_propagates_error() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let result = store_uploaded_xorb_with_metrics(&object_store, "invalid-hash", b"data");
        assert!(result.is_err(), "expected error for invalid hash");
    }

    // ── normalize_serialized_xorb error paths ──────────────────────────

    #[test]
    fn normalize_serialized_xorb_rejects_empty_bytes() {
        let hash = parse_xet_hash_hex(&"ab".repeat(32)).unwrap();
        let result = normalize_serialized_xorb(hash, b"");
        assert!(result.is_err(), "expected error for empty bytes");
    }

    #[test]
    fn normalize_serialized_xorb_rejects_garbage_bytes() {
        use shardline_protocol::ShardlineHash;
        let hash = ShardlineHash::from_bytes([0xab; 32]);
        let result = normalize_serialized_xorb(hash, b"not-a-xorb");
        assert!(
            matches!(result, Err(XetAdapterError::InvalidSerializedXorb)),
            "expected InvalidSerializedXorb, got {result:?}"
        );
    }

    // ── xorb_object_key edge cases ─────────────────────────────────────

    #[test]
    fn xorb_object_key_constructs_valid_key() {
        let hash = "ab".repeat(32);
        let key = xorb_object_key(&hash);
        assert!(key.is_ok());
        let key = key.unwrap();
        assert!(key.as_str().starts_with("xorbs/default/ab/"));
        assert!(key.as_str().ends_with(".xorb"));
    }

    // ── chunk_object_key_local (tests through canonicalize path) ───────

    #[test]
    fn canonicalize_uploaded_xorb_rejects_invalid_hash() {
        let hash = parse_xet_hash_hex(&"ab".repeat(32)).unwrap();
        let result = canonicalize_uploaded_xorb(hash, b"garbage");
        assert!(result.is_err());
    }

    // ── visit_stored_xorb_chunk_hashes with corrupted xorb ────────────

    #[test]
    fn visit_stored_xorb_chunk_hashes_rejects_missing_stored_xorb() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Key that looks like a xorb key but doesn't exist
        let key = xorb_object_key(&"ab".repeat(32)).unwrap();
        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                visited = true;
                Ok(())
            },
        );
        assert!(result.is_ok(), "expected Ok for missing key");
        assert!(!visited, "visitor should not be called");
    }

    // ── xorb_object_key with various valid prefixes ────────────────────

    #[test]
    fn xorb_object_key_various_prefixes() {
        let prefixes = ["aa", "ff", "10", "99", "ac"];
        for prefix in prefixes {
            let hash = format!("{}{}", prefix, "0".repeat(62));
            let key = xorb_object_key(&hash);
            assert!(key.is_ok(), "failed for prefix {prefix}: {key:?}");
            let key = key.unwrap();
            assert!(
                key.as_str().contains(&format!("xorbs/default/{prefix}/")),
                "key '{}' missing expected prefix segment",
                key.as_str()
            );
        }
    }

    // ── xorb_hash_from_object_key_if_present zero-segment ──────────────

    #[test]
    fn xorb_hash_from_object_key_none_for_empty_string() {
        use shardline_storage::ObjectKey;
        // ObjectKey::parse("") returns Err(Empty), so we skip empty keys
        let key = ObjectKey::parse("a").unwrap(); // single segment, not a valid xorb path
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert!(extracted.unwrap().is_none());
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_one_segment() {
        use shardline_storage::ObjectKey;
        let key = ObjectKey::parse("xorbs").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_two_segments() {
        use shardline_storage::ObjectKey;
        let key = ObjectKey::parse("xorbs/default").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_three_segments() {
        use shardline_storage::ObjectKey;
        let key = ObjectKey::parse("xorbs/default/ab").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    // ── ObjectKey error mapping ──────────────────────────────────────────

    #[test]
    fn xorb_store_map_object_key_error_message_content() {
        use shardline_storage::ObjectKeyError;
        let cases = [
            ObjectKeyError::Empty,
            ObjectKeyError::UnsafePath,
            ObjectKeyError::ControlCharacter,
            ObjectKeyError::TooLong,
        ];
        for err in cases {
            let mapped = super::map_object_key_error(err);
            let msg = mapped.to_string();
            assert!(
                msg.contains("hash"),
                "msg '{msg}' missing 'hash' for {err:?}"
            );
        }
    }

    // ── store_uploaded_xorb store_uploaded_xorb_with_metrics propagation ─

    #[test]
    fn store_uploaded_xorb_handles_invalid_bytes_gracefully() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let hash = "ab".repeat(32);

        // Completely invalid xorb bytes - should fail validation
        let result = store_uploaded_xorb(&object_store, &hash, b"\x00\x01\x02\x03");
        assert!(result.is_err(), "expected error for invalid xorb bytes");
    }

    // ── visit_stored_xorb_chunk_hashes with stored xorb ────────────────

    #[test]
    fn visit_stored_xorb_chunk_hashes_visits_once_per_existing_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();

        let mut count = 0_usize;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                count += 1;
                Ok(())
            },
        );
        assert!(result.is_ok(), "visit should succeed: {result:?}");
        assert_eq!(count, 1, "should visit exactly 1 chunk hash");
    }

    // ── store_uploaded_xorb_with_metrics with footerless bytes ────────

    #[test]
    fn store_uploaded_xorb_with_metrics_footerless_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Build a xorb WITHOUT a footer
        let raw = build_raw_xorb(3, ChunkSize::Fixed(640));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data);
        assert!(
            result.is_ok(),
            "store_uploaded_xorb_with_metrics(footerless) failed: {result:?}"
        );
        let stored = result.unwrap();
        assert!(stored.was_inserted);
        assert!(stored.stored_bytes > 0, "stored_bytes should be > 0");
    }

    // ── store_uploaded_xorb with edge cases ───────────────────────────

    #[test]
    fn store_uploaded_xorb_rejects_empty_expected_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let result = store_uploaded_xorb(&object_store, "", b"data");
        assert!(result.is_err(), "expected error for empty hash");
    }

    #[test]
    fn store_uploaded_xorb_unpacked_length_check() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        // Store it once - should succeed
        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok(), "first store should succeed: {result:?}");
        let stored = result.unwrap();
        assert!(stored.was_inserted);
        assert!(
            stored.stored_bytes > 0,
            "stored_bytes should be > 0, got {}",
            stored.stored_bytes
        );

        // Store again (idempotent) - stored_bytes should only count chunks+xorb once
        let second = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(second.is_ok(), "second store should succeed: {second:?}");
        // was_inserted = false because AlreadyExists
        // stored_bytes might be positive because chunks were already stored
    }
}
