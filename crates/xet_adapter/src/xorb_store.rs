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
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

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

    // ── visit_stored_xorb_chunk_hashes with existing object but invalid hash ─

    #[test]
    fn visit_stored_xorb_chunk_hashes_returns_ok_when_hash_missing_from_key() {
        // Create a key that looks like a valid xorb path but where the
        // hash part doesn't start with the prefix (so
        // xorb_hash_from_object_key_if_present returns Ok(None)).
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Key with a non-matching prefix: "bb" in filename but "aa" is the directory prefix
        let key = shardline_storage::ObjectKey::parse(
            "xorbs/default/aa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.xorb",
        )
        .unwrap();
        let integrity = ObjectIntegrity::new(shardline_server_core::chunk_hash(b"some data"), 9);
        object_store
            .put_if_absent(&key, ObjectBody::from_slice(b"some data"), &integrity)
            .unwrap();

        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                visited = true;
                Ok(())
            },
        );
        // metadata() finds the object, but xorb_hash_from_object_key_if_present
        // returns Ok(None) because hash doesn't start with prefix -> early return Ok(())
        assert!(result.is_ok(), "expected Ok for non-matching hash prefix");
        assert!(!visited, "visitor should not be called");
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_rejects_hash_with_invalid_characters() {
        // Store at a key where the hash has non-hex chars
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let key = shardline_storage::ObjectKey::parse(
            "xorbs/default/aa/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.xorb",
        )
        .unwrap();
        let integrity = ObjectIntegrity::new(shardline_server_core::chunk_hash(b"dummy data"), 10);
        object_store
            .put_if_absent(&key, ObjectBody::from_slice(b"dummy data"), &integrity)
            .unwrap();

        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(
            &object_store,
            &key,
            |_hash: String| -> Result<(), XetAdapterError> {
                visited = true;
                Ok(())
            },
        );
        // metadata() finds the object, xorb_hash_from_object_key_if_present
        // returns Ok(None) because the prefix "zz" is not hex
        assert!(result.is_ok(), "expected Ok for non-hex prefix");
        assert!(!visited, "visitor should not be called");
    }

    // ── store_uploaded_xorb unpacked_length sanity checks ────────────────

    #[test]
    fn store_uploaded_xorb_stores_zero_stored_bytes_for_existing_chunks() {
        // Store same xorb twice: first time inserts chunks, second time
        // finds them already present so stored_bytes doesn't count them again.
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
        assert!(!second.was_inserted);
    }

    // ── canonicalize_uploaded_xorb with complete error path ──────────────

    #[test]
    fn canonicalize_uploaded_xorb_rejects_garbage_bytes_directly() {
        use shardline_protocol::ShardlineHash;
        let hash = ShardlineHash::from_bytes([0xab; 32]);
        // Test that canonicalize with garbage -> normalization attempt -> error
        let result = super::canonicalize_uploaded_xorb(hash, b"garbage data");
        assert!(result.is_err());
    }

    // ── normalize_serialized_xorb with valid but wrong footer xorb ──────

    #[test]
    fn normalize_serialized_xorb_accepts_valid_footerless_xorb() {
        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        assert!(serialized.footer_start.is_none());
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();

        let normalized = normalize_serialized_xorb(expected_hash, &serialized.serialized_data);
        assert!(normalized.is_ok());
        let normalized = normalized.unwrap();
        assert!(normalized.len() > serialized.serialized_data.len());

        // Verify the normalized data validates
        let validated = validate_serialized_xorb(&mut Cursor::new(normalized), expected_hash);
        assert!(validated.is_ok());
    }

    // ── store_uploaded_xorb with stored_bytes accumulation ─────────────

    #[test]
    fn store_uploaded_xorb_stored_bytes_sum_chunks_and_xorb() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(result.was_inserted);
        assert!(result.stored_bytes > 0, "should count stored bytes");

        // Second store: chunks exist, xorb exists -> stored_bytes unchanged
        let second =
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(!second.was_inserted);
    }

    #[test]
    fn store_uploaded_xorb_with_lz4_compression() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(3, ChunkSize::Fixed(768));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok(), "lz4 xorb store failed: {result:?}");
        let stored = result.unwrap();
        assert!(stored.was_inserted);

        // Verify stored xorb can be retrieved via chunk hashes
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        let visit = visit_stored_xorb_chunk_hashes(&object_store, &key, |h| {
            assert_eq!(h.len(), 64);
            count += 1;
            Ok(())
        });
        assert!(visit.is_ok());
        assert_eq!(count, 3);
    }

    #[test]
    fn store_uploaded_xorb_with_bg4lz4_compression() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(256));
        let serialized = SerializedXorbObject::from_xorb_with_compression(
            raw,
            CompressionScheme::ByteGrouping4LZ4,
            true,
        )
        .unwrap();
        let hash = serialized.hash.hex();

        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok(), "bg4lz4 xorb store failed: {result:?}");
        let stored = result.unwrap();
        assert!(stored.was_inserted);
    }

    // ── visit_stored_xorb_chunk_hashes with various xorb sizes ─────────

    #[test]
    fn visit_stored_xorb_chunk_hashes_visits_correct_number_of_hashes() {
        for num_chunks in [1, 3, 5] {
            let temp = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(256));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::None,
                true,
            )
            .unwrap();
            let hash = serialized.hash.hex();
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
            let key = xorb_object_key(&hash).unwrap();

            let mut count = 0_usize;
            let result = visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
                count += 1;
                Ok(())
            });
            assert!(result.is_ok(), "visit failed for {num_chunks} chunks");
            assert_eq!(
                count, num_chunks as usize,
                "expected {num_chunks} hashes, got {count}"
            );
        }
    }

    // ── canonicalize_uploaded_xorb with footerless + wrong hash ────────

    #[test]
    fn canonicalize_uploaded_xorb_rejects_wrong_hash_for_normalized() {
        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        assert!(serialized.footer_start.is_none());

        // Provide a wrong hash - normalization succeeds but hash check fails
        let wrong_hash = ShardlineHash::from_bytes([0xab; 32]);
        let result = super::canonicalize_uploaded_xorb(wrong_hash, &serialized.serialized_data);
        assert!(
            result.is_err(),
            "expected error for wrong hash on normalized xorb"
        );
    }

    // ── xorb_object_key with edge cases ────────────────────────────────

    #[test]
    fn xorb_object_key_boundary_prefixes() {
        for prefix in ["00", "ff", "0a", "f0"] {
            let hash = format!("{}{}", prefix, "0".repeat(62));
            let key = xorb_object_key(&hash).unwrap();
            assert!(
                key.as_str().contains(&format!("xorbs/default/{prefix}/")),
                "key missing prefix {prefix}: {}",
                key.as_str()
            );
        }
    }

    #[test]
    fn xorb_hash_from_object_key_none_for_empty_segments_after_namespace() {
        let key = shardline_storage::ObjectKey::parse("xorbs").unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
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

    // ── store_uploaded_xorb_with_metrics full round-trip ─────────────────

    #[test]
    fn store_uploaded_xorb_with_metrics_single_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok());
        let stored = result.unwrap();
        assert!(stored.was_inserted);
        assert!(stored.stored_bytes > 0);

        // Verify chunk visit returns exactly 1 chunk hash
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 1);
    }

    // ── store_uploaded_xorb with footerless + LZ4 ───────────────────────

    #[test]
    fn store_uploaded_xorb_footerless_lz4_normalization() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        assert!(serialized.footer_start.is_none());
        let hash = serialized.hash.hex();

        // Store footerless bytes -> triggers normalization
        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data);
        assert!(result.is_ok(), "footerless store failed: {result:?}");
        let stored = result.unwrap();
        assert!(stored.was_inserted);

        // The stored bytes now have a footer, verify via visit
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        let visit = visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        });
        assert!(visit.is_ok());
        assert_eq!(count, 2);
    }

    // ── xorb_hash_from_object_key_if_present with valid error check ─────

    #[test]
    fn xorb_hash_from_object_key_with_uppercase_hash_returns_err() {
        // A key where the hash has uppercase chars after a valid lowercase prefix
        // This passes the starts_with check but fails validate_content_hash_with
        let hash_body = format!("aa{}", "AA".repeat(31)); // 64 chars, starts with "aa", but has uppercase
        assert_eq!(hash_body.len(), 64);
        let key_str = format!("xorbs/default/aa/{}.xorb", hash_body);
        let key = shardline_storage::ObjectKey::parse(&key_str).unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(
            result.is_err(),
            "expected error for uppercase hash, got {result:?}"
        );
    }

    #[test]
    fn xorb_hash_from_object_key_with_non_hex_second_char_returns_none() {
        // Prefix "ag" is not valid hex
        let key = shardline_storage::ObjectKey::parse(
            "xorbs/default/ag/aggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.xorb",
        )
        .unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ── store_uploaded_xorb errors with various invalid inputs ──────────

    #[test]
    fn store_uploaded_xorb_rejects_tiny_invalid_body_with_valid_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let hash = "ab".repeat(32);
        let result = store_uploaded_xorb(&object_store, &hash, b"\x00");
        assert!(result.is_err(), "expected error for tiny body");
    }

    #[test]
    fn store_uploaded_xorb_rejects_large_garbage_body() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let hash = "ab".repeat(32);
        let garbage = vec![0xFFu8; 4096];
        let result = store_uploaded_xorb(&object_store, &hash, &garbage);
        assert!(result.is_err(), "expected error for garbage body");
    }

    // ── normalize_serialized_xorb edge cases ────────────────────────────

    #[test]
    fn normalize_serialized_xorb_rejects_already_normalized_wrong_hash() {
        // Create a valid xorb WITH footer, then check with wrong hash
        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let wrong_hash = ShardlineHash::from_bytes([0x99; 32]);

        let result = normalize_serialized_xorb(wrong_hash, &serialized.serialized_data);
        assert!(
            matches!(result, Err(XetAdapterError::XorbHashMismatch)),
            "expected XorbHashMismatch, got {result:?}"
        );
    }

    // ── Varying chunk counts and storage verification ────────────────────

    #[test]
    fn store_uploaded_multiple_chunks_visits_all_correctly() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(5, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();

        // Collect all chunk hashes
        let mut hashes = Vec::new();
        visit_stored_xorb_chunk_hashes(&object_store, &key, |h| {
            hashes.push(h);
            Ok(())
        })
        .unwrap();
        assert_eq!(hashes.len(), 5);
        // All hashes should be 64-char hex strings
        for h in &hashes {
            assert_eq!(h.len(), 64);
            assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }

    // ── visit_stored_xorb_chunk_hashes with missing object (no metadata) ─

    #[test]
    fn visit_stored_xorb_chunk_hashes_non_existent_namespace() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // A key in a non-existent namespace
        let key =
            shardline_storage::ObjectKey::parse("xorbs/missing/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb")
                .unwrap();
        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            visited = true;
            Ok(())
        });
        // metadata() should return None since the key doesn't exist
        assert!(result.is_ok());
        assert!(!visited);
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_without_xorb_suffix() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Store at a key that lacks .xorb suffix but otherwise looks valid
        let key = shardline_storage::ObjectKey::parse(
            "xorbs/default/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let integrity = ObjectIntegrity::new(shardline_server_core::chunk_hash(b"test bytes"), 10);
        object_store
            .put_if_absent(&key, ObjectBody::from_slice(b"test bytes"), &integrity)
            .unwrap();

        let mut visited = false;
        let result = visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            visited = true;
            Ok(())
        });
        // metadata() returns Some, but xorb_hash_from_object_key_if_present
        // returns Ok(None) because key lacks .xorb suffix
        assert!(result.is_ok());
        assert!(!visited);
    }

    // ── xorb_hash_from_object_key_if_present edge cases ─────────────────

    #[test]
    fn xorb_hash_from_object_key_rejects_prefix_not_two_chars() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/a/ahash.xorb").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_prefix_one_char() {
        let key = shardline_storage::ObjectKey::parse(
            "xorbs/default/a/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
        )
        .unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_accepts_valid_xorb_key_round_trip() {
        let hash = "ab".repeat(32);
        let key = xorb_object_key(&hash).unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key).unwrap();
        assert_eq!(extracted, Some(hash.as_str()));
    }

    // ── store_uploaded_xorb with varied setup ────────────────────────────

    #[test]
    fn store_uploaded_xorb_with_none_compression_single_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(1, ChunkSize::Fixed(64));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let result = store_uploaded_xorb(
            &object_store,
            &serialized.hash.hex(),
            &serialized.serialized_data,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().was_inserted);
    }

    #[test]
    fn store_uploaded_xorb_with_none_compression_two_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(2, ChunkSize::Fixed(64));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let result = store_uploaded_xorb(
            &object_store,
            &serialized.hash.hex(),
            &serialized.serialized_data,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().was_inserted);
    }

    #[test]
    fn store_uploaded_xorb_with_none_compression_many_chunks() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(10, ChunkSize::Fixed(32));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let result = store_uploaded_xorb(
            &object_store,
            &serialized.hash.hex(),
            &serialized.serialized_data,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().was_inserted);
    }

    // ── store_uploaded_xorb_with_metrics round trip with verify ─────────

    #[test]
    fn store_uploaded_xorb_with_metrics_then_visit_all_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data)
            .unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 3);
    }

    // ── xorb_object_key with edge case hash lengths ─────────────────────

    #[test]
    fn xorb_object_key_constructs_correct_format() {
        let tests = vec![
            ("aa".repeat(32), "xorbs/default/aa/"),
            ("ff".repeat(32), "xorbs/default/ff/"),
            ("00".repeat(32), "xorbs/default/00/"),
        ];
        for (hash, expected_prefix) in tests {
            let key = xorb_object_key(&hash).unwrap();
            assert!(
                key.as_str().starts_with(expected_prefix),
                "key {} should start with {}",
                key.as_str(),
                expected_prefix
            );
            assert!(
                key.as_str().ends_with(".xorb"),
                "key {} should end with .xorb",
                key.as_str()
            );
            assert_eq!(
                key.as_str().len(),
                expected_prefix.len() + 64 + ".xorb".len()
            );
        }
    }

    // ─── xorb_object_key round-trip with xorb_hash_from_object_key_if_present ─

    #[test]
    fn xorb_object_key_round_trip_reconstructs_hash() {
        let hash = "cd".repeat(32);
        let key = xorb_object_key(&hash).unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key).unwrap();
        assert_eq!(extracted, Some(hash.as_str()));
    }

    #[test]
    fn xorb_object_key_round_trip_reconstructs_hash_other_prefix() {
        let hash = "ef".repeat(32);
        let key = xorb_object_key(&hash).unwrap();
        let extracted = xorb_hash_from_object_key_if_present(&key).unwrap();
        assert_eq!(extracted, Some(hash.as_str()));
    }

    // ── store_uploaded_xorb idempotent detailed check ──────────────────

    #[test]
    fn store_uploaded_xorb_first_inserts_second_already_exists() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(2, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        let first = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(first.was_inserted);
        let second =
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(!second.was_inserted);
    }

    #[test]
    fn store_uploaded_xorb_different_xorbs_independent() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw1 = build_raw_xorb(1, ChunkSize::Fixed(256));
        let s1 =
            SerializedXorbObject::from_xorb_with_compression(raw1, CompressionScheme::None, true)
                .unwrap();
        let raw2 = build_raw_xorb(1, ChunkSize::Fixed(512));
        let s2 =
            SerializedXorbObject::from_xorb_with_compression(raw2, CompressionScheme::None, true)
                .unwrap();

        let r1 = store_uploaded_xorb(&object_store, &s1.hash.hex(), &s1.serialized_data).unwrap();
        assert!(r1.was_inserted);
        let r2 = store_uploaded_xorb(&object_store, &s2.hash.hex(), &s2.serialized_data).unwrap();
        assert!(r2.was_inserted);

        // Both should be visitable
        let k1 = xorb_object_key(&s1.hash.hex()).unwrap();
        let k2 = xorb_object_key(&s2.hash.hex()).unwrap();
        let mut c1 = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &k1, |_h| {
            c1 += 1;
            Ok(())
        })
        .unwrap();
        let mut c2 = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &k2, |_h| {
            c2 += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(c1, 1);
        assert_eq!(c2, 1);
    }

    // ── normalize_serialized_xorb edge cases ────────────────────────────

    #[test]
    fn normalize_serialized_xorb_with_valid_footerless_bytes_succeeds() {
        let raw = build_raw_xorb(3, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let hash = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();
        let normalized = normalize_serialized_xorb(hash, &serialized.serialized_data).unwrap();
        assert!(!normalized.is_empty());
        assert!(normalized.len() > serialized.serialized_data.len());
    }

    #[test]
    fn normalize_serialized_xorb_rejects_bad_hash_after_normalization() {
        let raw = build_raw_xorb(2, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, false)
                .unwrap();
        let wrong_hash = ShardlineHash::from_bytes([0x42; 32]);
        let result = normalize_serialized_xorb(wrong_hash, &serialized.serialized_data);
        assert!(result.is_err());
    }

    // ── xorb_hash_from_object_key_if_present returns None for wrong ns ─

    #[test]
    fn xorb_hash_from_object_key_rejects_shards_namespace() {
        let key = shardline_storage::ObjectKey::parse(
            "shards/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
        )
        .unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_non_default_namespace_variant() {
        let key = shardline_storage::ObjectKey::parse(
            "xorbs/custom/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
        )
        .unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ── additional xorb_object_key rejection tests ─────────────────────

    #[test]
    fn xorb_object_key_rejects_string_with_special_chars() {
        let hash = format!("ab{}cd", "!@#");
        let result = xorb_object_key(&hash);
        assert!(result.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_hash_with_whitespace() {
        let hash = format!("ab {} cd", "  ");
        let result = xorb_object_key(&hash);
        assert!(result.is_err());
    }

    // ── visit_stored_xorb_chunk_hashes with stored xorb (coverage boost) ─

    #[test]
    fn visit_stored_xorb_chunk_hashes_two_chunks_visits_both() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(2, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut visited = Vec::new();
        visit_stored_xorb_chunk_hashes(&object_store, &key, |h| {
            visited.push(h);
            Ok(())
        })
        .unwrap();
        assert_eq!(visited.len(), 2);
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_four_chunks_visits_all() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(4, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 4);
    }

    // ── store_uploaded_xorb with LZ4 and footer ─────────────────────────

    #[test]
    fn store_uploaded_xorb_lz4_with_footer_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(512));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        let hash = serialized.hash.hex();
        let first = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(first.was_inserted);
        let second =
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        assert!(!second.was_inserted);
    }

    // ── canonicalize_uploaded_xorb with LZ4 footerless bytes ──────────

    #[test]
    fn canonicalize_uploaded_xorb_lz4_footerless_normalizes() {
        let raw = build_raw_xorb(4, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();
        let result = super::canonicalize_uploaded_xorb(expected_hash, &serialized.serialized_data);
        assert!(result.is_ok());
        let (cow, validated) = result.unwrap();
        assert!(matches!(cow, Cow::Owned(_)));
        assert_eq!(validated.hash(), expected_hash);
    }

    // ── xorb_hash_from_object_key_if_present with edge case paths ──────

    #[test]
    fn xorb_hash_from_object_key_rejects_extra_segment_after_filename() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa/ahash.xorb/extra").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_missing_file_extension() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa/ahash").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_non_hex_prefix_chars() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/gh/ghash.xorb").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_non_hex_prefix_char_first() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/z1/z1hash.xorb").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    // ── normalize_serialized_xorb with empty/invalid input ─────────────

    #[test]
    fn normalize_serialized_xorb_rejects_garbage() {
        let hash = parse_xet_hash_hex(&"ab".repeat(32)).unwrap();
        let result = normalize_serialized_xorb(hash, b"garbage bytes here");
        assert!(result.is_err());
    }

    // ── chunk_object_key_local invoked through store path ──────────────

    #[test]
    fn store_uploaded_xorb_multiple_chunks_counts_stored_bytes() {
        for num_chunks in [1, 2, 4] {
            let temp = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(128));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::None,
                true,
            )
            .unwrap();
            let hash = serialized.hash.hex();
            let result =
                store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
            assert!(result.was_inserted, "chunks={num_chunks}");
            assert!(result.stored_bytes > 0, "chunks={num_chunks}");
        }
    }

    // ── store_uploaded_xorb_with_metrics error handling ───────────────

    #[test]
    fn store_uploaded_xorb_with_metrics_rejects_bad_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let result = store_uploaded_xorb_with_metrics(&object_store, "invalid", b"test");
        assert!(result.is_err());
    }

    #[test]
    fn store_uploaded_xorb_with_metrics_rejects_wrong_hash_format() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let result = store_uploaded_xorb_with_metrics(&object_store, "", b"data");
        assert!(result.is_err());
    }

    // ── xorb_object_key edge behavior ─────────────────────────────────

    #[test]
    fn xorb_object_key_rejects_hash_with_underscore() {
        let result = xorb_object_key("ab_".repeat(21).as_str());
        assert!(result.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_too_long_hash() {
        let result = xorb_object_key(&"ab".repeat(33));
        assert!(result.is_err());
    }

    #[test]
    fn xorb_object_key_rejects_too_short_hash() {
        let result = xorb_object_key("a");
        assert!(result.is_err());
    }

    // ── idempotent store with visit verification ────────────────────────

    #[test]
    fn store_and_visit_xorb_multiple_times_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        // Visit once
        let key = xorb_object_key(&hash).unwrap();
        let mut v1 = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            v1 += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(v1, 3);
        // Visit again
        let mut v2 = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            v2 += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(v2, 3);
    }

    #[test]
    fn store_xorb_with_lz4_then_visit_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(5, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 5);
    }

    // ── Footerless LZ4 store with visit ─────────────────────────────────

    #[test]
    fn store_footerless_xorb_then_visit_normalized() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 3);
    }

    // ── xorb_hash_from_object_key_if_present more edge cases ────────────

    #[test]
    fn xorb_hash_from_object_key_rejects_no_prefix_segment() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_only_namespace() {
        let key = shardline_storage::ObjectKey::parse("xorbs").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn xorb_hash_from_object_key_rejects_no_file_name() {
        let key = shardline_storage::ObjectKey::parse("xorbs/default/aa").unwrap();
        let result = xorb_hash_from_object_key_if_present(&key);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }
}
