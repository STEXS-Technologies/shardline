use std::{
    borrow::Cow,
    io::{BufReader, Cursor},
    path::Path,
};

use shardline_index::{parse_xet_hash_hex, xet_hash_hex_string};
use shardline_protocol::ShardlineHash;
use shardline_server_core::ServerObjectStore;
use shardline_storage::{ObjectKey, PutOutcome};
use shardline_xet_core::xorb_object::reconstruct_xorb_with_footer;

use crate::{
    error::XetAdapterError,
    xorb::{MAX_XORB_UNPACKED_BYTES, declared_footerless_unpacked_length},
};

use super::{ValidatedXorb, validate_serialized_xorb};

mod blocking;
mod encoding;
mod file_preparation;
mod member_read;
mod normalization;
mod owned_preparation;
mod prepared_memory;
pub use member_read::read_verified_xorb_chunk;
pub use normalization::{
    normalize_owned_uploaded_xorb_file, normalize_owned_uploaded_xorb_file_with_checkpoint,
    normalize_uploaded_xorb_file,
};
pub use owned_preparation::{
    PreparedXorbFile, prepare_owned_xorb_file_with_checkpoint, store_prepared_xorb_file,
};
pub use prepared_memory::{
    IncrementalXorbMemory, PreparedXorbMemory, prepare_owned_xorb_memory_with_checkpoint,
    prepare_xorb_memory, store_prepared_xorb_memory,
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
    ObjectKey::parse(&key).map_err(XetAdapterError::from)
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
    visitor: Visitor,
) -> Result<(), XetAdapterError>
where
    Visitor: FnMut(String) -> Result<(), XetAdapterError>,
{
    visit_stored_xorb_chunk_hashes_if_present(object_store, object_key, visitor).map(|_| ())
}

/// Visits members only after fully validating their stored container.
///
/// Returns `true` when the source was present, validated and visited successfully;
/// returns `false` for a missing source or a key outside the Xorb namespace.
/// Callers can reuse this observation instead of probing the same source first.
///
/// # Errors
///
/// Returns an error when the source cannot be read or validated, or a visitor fails.
pub fn visit_stored_xorb_chunk_hashes_if_present<Visitor>(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    mut visitor: Visitor,
) -> Result<bool, XetAdapterError>
where
    Visitor: FnMut(String) -> Result<(), XetAdapterError>,
{
    let Some(xorb_hash_hex) = xorb_hash_from_object_key_if_present(object_key)? else {
        return Ok(false);
    };

    // Persisted member sidecars are derived data without an authenticated leaf
    // structure. Always validate the source container before exposing members.
    let expected_hash = parse_xet_hash_hex(xorb_hash_hex)?;
    // Keep small verification snapshots in a fixed budget instead of writing
    // every remote container to scratch. Larger sources still spill to disk;
    // this is request-local buffering, never a cache of source health.
    const MAX_VERIFICATION_MEMORY_BYTES: u64 = 2 * 1024 * 1024;
    let Some((cursor, _length)) = object_store
        .materialize_bounded_object_if_present(object_key, MAX_VERIFICATION_MEMORY_BYTES)?
    else {
        return Ok(false);
    };
    // Bound scratch-read buffering independently of container size. Validation
    // still reads and authenticates every payload and footer from this snapshot.
    let mut cursor = BufReader::with_capacity(65536, cursor);
    let validated = validate_serialized_xorb(&mut cursor, expected_hash)?;

    // Validation has already decoded and authenticated every member against
    // the expected container root. Reachability needs only those hashes, not
    // a second decompression/allocation pass over the same private snapshot.
    for chunk in validated.chunks() {
        visitor(xet_hash_hex_string(chunk.hash()))?;
    }

    Ok(true)
}

/// Extracts a Xorb hash from a legacy member-cache sidecar key.
/// Returns `None` when the key does not match the cache-namespace pattern.
///
/// # Errors
///
/// Never returns an error: keys that do not match the cache-namespace pattern
/// yield `Ok(None)`.
pub fn xorb_chunks_cache_hash_from_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, XetAdapterError> {
    let mut segments = key.as_str().split('/');
    let Some(namespace) = segments.next() else {
        return Ok(None);
    };
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(hash) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if namespace != "_xorb_chunks" {
        return Ok(None);
    }
    if prefix.len() != 2 {
        return Ok(None);
    }
    if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(None);
    }
    if &hash[..2] != prefix {
        return Ok(None);
    }
    Ok(Some(hash))
}

/// # Errors
///
/// Returns an error when the upload fails validation or storage.
pub async fn store_uploaded_xorb(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
) -> Result<StoredXorbUpload, XetAdapterError> {
    store_xorb_bytes_with_resources(object_store, expected_hash, uploaded_bytes, ()).await
}

/// Stores verified in-memory input while retaining admission through immutable
/// persistence and any full verification of an equivalent retained encoding.
///
/// # Errors
/// Returns the same validation and storage errors as [`store_uploaded_xorb`].
pub async fn store_xorb_bytes_with_resources<R: Send + 'static>(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let prepared = prepare_xorb_memory(expected_hash, uploaded_bytes)?;
    store_prepared_xorb_memory(object_store, prepared, resources).await
}

/// Stores only the canonical serialized Xorb from a seekable file. Decoded
/// members are authenticated during validation, without a second stored copy.
/// Footerless inputs use verified file-backed normalization with bounded memory.
///
/// # Errors
///
/// Returns an error when the file cannot be opened, validated, or stored.
pub async fn store_uploaded_xorb_file(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    path: &Path,
) -> Result<StoredXorbUpload, XetAdapterError> {
    store_xorb_file_with_resources(
        object_store,
        expected_hash,
        path,
        (),
        file_preparation::SourceOwnership::Borrowed,
    )
    .await
}

/// Persists an owned staged container, retaining application admission through
/// any synchronous write that outlives cancellation of its caller.
///
/// # Errors
/// Returns the same validation and storage errors as [`store_uploaded_xorb_file`].
pub async fn store_owned_xorb_file<R: Send + 'static>(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    file: tempfile::NamedTempFile,
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let path = file.path().to_owned();
    store_xorb_file_with_resources(
        object_store,
        expected_hash,
        &path,
        (file, resources),
        file_preparation::SourceOwnership::OwnedStaging,
    )
    .await
}

async fn store_xorb_file_with_resources<R: Send + 'static>(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    path: &Path,
    resources: R,
    ownership: file_preparation::SourceOwnership,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let hash_for_preparation = expected_hash.to_owned();
    let path_for_preparation = path.to_owned();
    // Hold both staging and admission in the worker, including after caller
    // cancellation. Preparation performs full synchronous reads and hashing.
    let (prepared, resources) = blocking::cancellable_read((), move |checkpoint| {
        Ok((
            file_preparation::prepare_source(
                &hash_for_preparation,
                path_for_preparation,
                ownership,
                checkpoint,
            )?,
            resources,
        ))
    })
    .await?;
    store_prepared_file_with_resources(object_store, expected_hash, prepared, resources).await
}

async fn store_prepared_file_with_resources<R: Send + 'static>(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    prepared: file_preparation::PreparedFile,
    resources: R,
) -> Result<StoredXorbUpload, XetAdapterError> {
    let file_preparation::PreparedFile {
        normalized,
        path: path_for_store,
        validated,
        integrity,
        length: canonical_length,
    } = prepared;
    let key = xorb_object_key(expected_hash)?;
    tracing::debug!(xorb_hash = expected_hash, "persisting verified Xorb");
    let store = object_store.clone();
    let outcome = blocking::cancellable_write((normalized, resources), move || {
        let result = store.put_content_addressed_file(&key, &path_for_store, &integrity);
        encoding::resolve_conflict(&store, &key, &validated, result)
    })
    .await?;
    Ok(StoredXorbUpload {
        was_inserted: matches!(outcome, PutOutcome::Inserted),
        stored_bytes: if matches!(outcome, PutOutcome::Inserted) {
            canonical_length
        } else {
            0
        },
    })
}

/// Returns the stored length after the same normalization used by xorb uploads.
///
/// # Errors
/// Returns an error for invalid containers or hashes.
pub fn canonical_uploaded_xorb_length(
    expected_hash: &str,
    bytes: &[u8],
) -> Result<u64, XetAdapterError> {
    let expected = parse_xet_hash_hex(expected_hash)?;
    let (canonical, _) = canonicalize_uploaded_xorb(expected, bytes)?;
    Ok(u64::try_from(canonical.len())?)
}

/// Returns a staged file's stored length without buffering canonical containers.
/// Footerless inputs use bounded file-backed normalization.
///
/// # Errors
/// Returns an error for unreadable, invalid, or oversized compatibility inputs.
pub fn canonical_uploaded_xorb_file_length(
    expected_hash: &str,
    path: &Path,
) -> Result<u64, XetAdapterError> {
    let normalized = normalize_uploaded_xorb_file(expected_hash, path)?;
    Ok(std::fs::metadata(normalized.as_ref().map_or(path, |file| file.path()))?.len())
}

/// # Errors
///
/// Returns an error when the upload fails validation or storage.
pub async fn store_uploaded_xorb_with_metrics(
    object_store: &ServerObjectStore,
    expected_hash: &str,
    uploaded_bytes: &[u8],
) -> Result<StoredXorbUpload, XetAdapterError> {
    let result = store_uploaded_xorb(object_store, expected_hash, uploaded_bytes).await?;
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
    match validate_serialized_xorb(&mut Cursor::new(bytes), expected_hash) {
        Ok(_validated) => return Ok(bytes.to_vec()),
        Err(crate::xorb::XorbParseError::HashMismatch) => {
            return Err(XetAdapterError::XorbHashMismatch);
        }
        Err(_error) => {}
    }
    // Reject a decompression bomb BEFORE the reconstruct pass allocates: scan
    // the chunk headers and bound the total declared unpacked size upfront. The
    // reconstruct pass itself is streaming (at most one bounded chunk in memory
    // at a time), but this check avoids paying decompression work on obviously
    // oversized inputs and keeps the declared-total cap consistent with the
    // footer-bearing validation path.
    // A failed footer-bearing validation must not be repaired by ignoring the
    // footer. This scan requires chunk framing to consume every input byte.
    let declared_total = declared_footerless_unpacked_length(bytes)?;
    if declared_total > MAX_XORB_UNPACKED_BYTES {
        return Err(XetAdapterError::XorbUnpackedLengthExceedsCap);
    }

    let mut normalized = Vec::with_capacity(bytes.len());
    let (_xorb, computed_hash) = reconstruct_xorb_with_footer(&mut normalized, bytes)
        .map_err(|_error| XetAdapterError::InvalidSerializedXorb)?;
    let computed_hash = parse_xet_hash_hex(&computed_hash.hex())?;
    if computed_hash != expected_hash {
        return Err(XetAdapterError::XorbHashMismatch);
    }

    Ok(normalized)
}

#[cfg(test)]
mod encoding_tests;

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
    use crate::xorb::MAX_XORB_UNPACKED_BYTES;
    use shardline_server_core::ServerObjectStore;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

    #[test]
    fn normalize_serialized_xorb_validates_reconstructed_footers() {
        for (num_chunks, chunk_bytes) in [(4, 1024), (2, 512), (3, 512)] {
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_bytes));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::LZ4,
                false,
            )
            .unwrap();
            assert!(serialized.footer_start.is_none());
            let expected = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();
            let normalized =
                normalize_serialized_xorb(expected, &serialized.serialized_data).unwrap();
            assert!(normalized.len() > serialized.serialized_data.len());
            let validated =
                validate_serialized_xorb(&mut Cursor::new(normalized), expected).unwrap();
            assert_eq!(validated.hash(), expected);
        }
    }

    #[test]
    fn normalize_serialized_xorb_rejects_wrong_hash_with_or_without_footer() {
        for (chunk_bytes, footer, wrong_byte) in
            [(768, false, 9), (512, true, 0x99), (512, false, 0x42)]
        {
            let raw = build_raw_xorb(2, ChunkSize::Fixed(chunk_bytes));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::None,
                footer,
            )
            .unwrap();
            let wrong_hash = ShardlineHash::from_bytes([wrong_byte; 32]);
            let result = normalize_serialized_xorb(wrong_hash, &serialized.serialized_data);
            assert!(
                matches!(result, Err(XetAdapterError::XorbHashMismatch)),
                "footer {footer}: {result:?}"
            );
        }
    }

    #[test]
    fn canonicalize_uploaded_xorb_borrows_already_canonical_bytes() {
        let raw = build_raw_xorb(3, ChunkSize::Fixed(640));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true);
        let serialized = serialized.expect("expected valid serialized");
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex());
        let expected_hash = expected_hash.expect("expected valid expected_hash");

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
    fn canonicalize_uploaded_xorb_owns_normalized_footerless_bytes() {
        for (num_chunks, chunk_bytes) in [(3, 640), (4, 256)] {
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_bytes));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::LZ4,
                false,
            )
            .unwrap();
            let expected = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();
            let (canonical, validated) =
                canonicalize_uploaded_xorb(expected, &serialized.serialized_data).unwrap();
            assert!(matches!(canonical, Cow::Owned(_)));
            assert!(canonical.len() > serialized.serialized_data.len());
            assert_eq!(validated.hash(), expected);
        }
    }

    // ---- store_uploaded_xorb tests ----

    #[tokio::test]
    async fn store_uploaded_xorb_accounts_only_for_new_verified_containers() {
        for (num_chunks, chunk_bytes, compression) in [
            (1, 256, CompressionScheme::None),
            (2, 256, CompressionScheme::None),
            (3, 512, CompressionScheme::LZ4),
            (2, 512, CompressionScheme::None),
            (1, 128, CompressionScheme::None),
            (2, 128, CompressionScheme::None),
            (4, 128, CompressionScheme::None),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_bytes));
            let serialized =
                SerializedXorbObject::from_xorb_with_compression(raw, compression, true).unwrap();
            let hash = serialized.hash.hex();
            let first = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
                .await
                .unwrap();
            assert!(first.was_inserted);
            assert_eq!(first.stored_bytes, serialized.serialized_data.len() as u64);
            let second = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
                .await
                .unwrap();
            assert!(!second.was_inserted);
            assert_eq!(second.stored_bytes, 0);
            let objects = ObjectStore::list_prefix(
                &object_store,
                &shardline_storage::ObjectPrefix::parse("").unwrap(),
            )
            .unwrap();
            assert_eq!(
                objects.len(),
                1,
                "only the verified container should be retained"
            );
        }
    }

    #[tokio::test]
    async fn store_uploaded_xorb_rejects_wrong_hash() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let wrong_hash = "00".repeat(32);

        let result =
            store_uploaded_xorb(&object_store, &wrong_hash, &serialized.serialized_data).await;
        assert!(result.is_err(), "expected error for wrong hash");
    }

    #[tokio::test]
    async fn store_uploaded_xorb_with_metrics_delegates_and_records() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data)
                .await;
        assert!(
            result.is_ok(),
            "store_uploaded_xorb_with_metrics failed: {result:?}"
        );
        let stored = result.unwrap();
        assert!(stored.was_inserted);
    }

    #[test]
    fn xorb_object_key_rejects_invalid_hashes() {
        for hash in [
            "AA".repeat(32),
            "zz".repeat(32),
            "abc".to_owned(),
            String::new(),
            "ab!@#cd".to_owned(),
            "ab    cd".to_owned(),
            "ab_".repeat(21),
            "ab".repeat(33),
            "a".to_owned(),
        ] {
            assert!(
                xorb_object_key(&hash).is_err(),
                "accepted invalid hash {hash:?}"
            );
        }
    }

    #[test]
    fn xorb_hash_from_object_key_ignores_unrelated_layouts() {
        for path in [
            "shards/ab/abhash.shard",
            "xorbs/default/aa/hash.xorb/extra",
            "xorbs/custom/aa/hash.xorb",
            "xorbs/default/xyz/hash.xorb",
            "xorbs/default/aa/hash",
            "xorbs/default/bb/aahash.xorb",
            "xorbs/default/gg/gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.xorb",
            "a",
            "xorbs",
            "xorbs/default",
            "xorbs/default/ab",
            "xorbs/default/ag/aggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.xorb",
            "xorbs/default/a/ahash.xorb",
            "xorbs/default/a/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
            "shards/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
            "xorbs/custom/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
            "xorbs/default/aa/ahash.xorb/extra",
            "xorbs/default/aa/ahash",
            "xorbs/default/gh/ghash.xorb",
            "xorbs/default/z1/z1hash.xorb",
            "xorbs/default/aa",
        ] {
            let key = shardline_storage::ObjectKey::parse(path).unwrap();
            assert_eq!(
                xorb_hash_from_object_key_if_present(&key).unwrap(),
                None,
                "unrelated layout {path:?}"
            );
        }
    }

    #[tokio::test]
    async fn visit_stored_xorb_chunk_hashes_visits_every_member() {
        for (num_chunks, chunk_bytes) in
            [(2, 512), (1, 256), (3, 256), (5, 256), (2, 256), (4, 128)]
        {
            let temp = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_bytes));
            let serialized = SerializedXorbObject::from_xorb_with_compression(
                raw,
                CompressionScheme::None,
                true,
            )
            .unwrap();
            let hash = serialized.hash.hex();
            store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
                .await
                .unwrap();
            let key = xorb_object_key(&hash).unwrap();
            let mut visited = Vec::new();
            visit_stored_xorb_chunk_hashes(&object_store, &key, |hash| {
                visited.push(hash);
                Ok(())
            })
            .unwrap();
            assert_eq!(
                visited.len(),
                num_chunks as usize,
                "{num_chunks} chunks of {chunk_bytes} bytes"
            );
            assert!(visited.iter().all(|hash| hash.len() == 64));
        }
    }

    #[test]
    fn visit_stored_xorb_chunk_hashes_skips_missing_or_unrelated_objects() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let missing = xorb_object_key(&"ab".repeat(32)).unwrap();
        let cases = [
            ("xorbs/default/aa/nonexistent.xorb", None),
            ("shards/aa/hash.shard", None),
            (missing.as_str(), None),
            (
                "xorbs/default/aa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.xorb",
                Some(b"some data".as_slice()),
            ),
            (
                "xorbs/default/aa/zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.xorb",
                Some(b"dummy data".as_slice()),
            ),
            (
                "xorbs/missing/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.xorb",
                None,
            ),
            (
                "xorbs/default/aa/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                Some(b"test bytes".as_slice()),
            ),
        ];
        for (path, body) in cases {
            let key = shardline_storage::ObjectKey::parse(path).unwrap();
            if let Some(body) = body {
                let integrity = ObjectIntegrity::new(
                    shardline_server_core::chunk_hash(body),
                    body.len() as u64,
                );
                object_store
                    .put_if_absent(&key, ObjectBody::from_slice(body), &integrity)
                    .unwrap();
            }
            let mut visited = false;
            visit_stored_xorb_chunk_hashes(&object_store, &key, |_| {
                visited = true;
                Ok(())
            })
            .unwrap();
            assert!(!visited, "unexpected member callback for {path:?}");
        }
    }

    #[tokio::test]
    async fn visit_stored_xorb_chunk_hashes_propagates_visitor_error() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn store_uploaded_xorb_rejects_unpacked_length_mismatch() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Create a xorb but provide modified bytes that change unpacked length
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        // Store it normally first to get the proper format
        let hash = serialized.hash.hex();
        let result = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn store_uploaded_xorb_rejects_malformed_uploads() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let hash = "ab".repeat(32);
        let garbage = vec![0xFFu8; 4096];
        for (case, expected_hash, body) in [
            ("invalid hash", "not-a-hash", b"data".as_slice()),
            ("empty hash", "", b"data".as_slice()),
            (
                "invalid body",
                hash.as_str(),
                b"\x00\x01\x02\x03".as_slice(),
            ),
            ("tiny body", hash.as_str(), b"\x00".as_slice()),
            ("large garbage", hash.as_str(), garbage.as_slice()),
        ] {
            let result = store_uploaded_xorb(&object_store, expected_hash, body).await;
            assert!(
                result.is_err(),
                "{case}: expected rejection, got {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn store_uploaded_xorb_with_metrics_rejects_invalid_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        for (hash, body) in [
            ("invalid-hash", b"data".as_slice()),
            ("invalid", b"test".as_slice()),
            ("", b"data".as_slice()),
        ] {
            let result = store_uploaded_xorb_with_metrics(&object_store, hash, body).await;
            assert!(
                result.is_err(),
                "hash {hash:?}: expected rejection, got {result:?}"
            );
        }
    }

    #[test]
    fn normalize_serialized_xorb_rejects_malformed_bytes() {
        let hash = ShardlineHash::from_bytes([0xab; 32]);
        assert!(normalize_serialized_xorb(hash, b"").is_err());
        for body in [b"not-a-xorb".as_slice(), b"garbage bytes here"] {
            let result = normalize_serialized_xorb(hash, body);
            assert!(
                matches!(result, Err(XetAdapterError::InvalidSerializedXorb)),
                "body {body:?}: expected InvalidSerializedXorb, got {result:?}"
            );
        }
    }

    #[test]
    fn normalize_serialized_xorb_rejects_declared_unpacked_bomb() {
        use shardline_xet_core::xorb_object::constants::MAX_CHUNK_SIZE;
        use shardline_xet_core::xorb_object::{XorbChunkHeader, write_chunk_header};

        // A footerless stream of chunk headers, each declaring a maximum-size
        // uncompressed length. The declared total exceeds the cap before any
        // decompression work, so normalize rejects it upfront instead of
        // allocating a multi-GiB output.
        let max_declared = (MAX_CHUNK_SIZE.load(std::sync::atomic::Ordering::Relaxed) - 1) as u32;
        let headers_needed = (MAX_XORB_UNPACKED_BYTES / u64::from(max_declared)) as usize + 1;
        let mut stream = Vec::with_capacity(headers_needed * 8);
        for _ in 0..headers_needed {
            let header = XorbChunkHeader::new(CompressionScheme::None, 0, max_declared);
            write_chunk_header(&mut stream, &header).unwrap();
        }

        let hash = parse_xet_hash_hex(&"ab".repeat(32)).unwrap();
        let result = normalize_serialized_xorb(hash, &stream);
        assert!(
            matches!(result, Err(XetAdapterError::XorbUnpackedLengthExceedsCap)),
            "expected XorbUnpackedLengthExceedsCap, got {result:?}"
        );
    }

    #[tokio::test]
    async fn store_uploaded_xorb_rejects_forged_footer_without_publication() {
        // Invalid footer-bearing input is not a footerless compatibility case.
        // Reject the forged declaration before publishing any replacement.
        use shardline_xet_core::xorb_object::XorbObject;

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let expected_hash = parse_xet_hash_hex(&serialized.hash.hex()).unwrap();

        // Forge the footer's unpacked chunk offsets above the cap.
        let mut parsed =
            XorbObject::deserialize(&mut Cursor::new(serialized.serialized_data.as_slice()))
                .unwrap();
        *parsed.info.unpacked_chunk_offsets.last_mut().unwrap() = MAX_XORB_UNPACKED_BYTES + 1;
        parsed.info.fill_in_boundary_offsets();
        let footer_start = serialized.serialized_data.len() as u64
            - parsed.info_length
            - std::mem::size_of::<u64>() as u64;
        let mut forged = serialized.serialized_data[..footer_start as usize].to_vec();
        let info_len = parsed.info.serialize(&mut forged).unwrap();
        forged.extend_from_slice(&(info_len as u64).to_le_bytes());

        // The forged declaration is rejected outright by validation.
        assert!(
            validate_serialized_xorb(&mut Cursor::new(forged.as_slice()), expected_hash).is_err()
        );

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let result = store_uploaded_xorb(&object_store, &serialized.hash.hex(), &forged).await;
        assert!(result.is_err(), "forged footer was accepted: {result:?}");
        let key = xorb_object_key(&serialized.hash.hex()).unwrap();
        assert!(
            ObjectStore::metadata(&object_store, &key)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn xorb_object_key_preserves_layout_and_round_trips() {
        let hashes = ["ab", "aa", "ff", "00", "cd", "ef"]
            .into_iter()
            .map(|pair| pair.repeat(32))
            .chain(
                ["aa", "ff", "10", "99", "ac", "00", "0a", "f0"]
                    .into_iter()
                    .map(|prefix| format!("{prefix}{}", "0".repeat(62))),
            );
        for hash in hashes {
            let key = xorb_object_key(&hash).unwrap();
            assert_eq!(
                key.as_str(),
                format!("xorbs/default/{}/{hash}.xorb", &hash[..2])
            );
            assert_eq!(
                xorb_hash_from_object_key_if_present(&key).unwrap(),
                Some(hash.as_str())
            );
        }
    }

    #[test]
    fn canonicalize_uploaded_xorb_rejects_garbage() {
        let expected = ShardlineHash::from_bytes([0xab; 32]);
        for body in [b"garbage".as_slice(), b"garbage data"] {
            assert!(canonicalize_uploaded_xorb(expected, body).is_err());
        }
    }

    #[tokio::test]
    async fn store_uploaded_xorb_with_metrics_footerless_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Build a xorb WITHOUT a footer
        let raw = build_raw_xorb(3, ChunkSize::Fixed(640));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data)
                .await;
        assert!(
            result.is_ok(),
            "store_uploaded_xorb_with_metrics(footerless) failed: {result:?}"
        );
        let stored = result.unwrap();
        assert!(stored.was_inserted);
        assert!(stored.stored_bytes > 0, "stored_bytes should be > 0");
    }

    #[tokio::test]
    async fn store_uploaded_xorb_round_trips_compressions_and_footerless_input() {
        for (num_chunks, chunk_bytes, compression, footer) in [
            (3, 768, CompressionScheme::LZ4, true),
            (2, 256, CompressionScheme::ByteGrouping4LZ4, true),
            (1, 64, CompressionScheme::None, true),
            (2, 64, CompressionScheme::None, true),
            (10, 32, CompressionScheme::None, true),
            (2, 512, CompressionScheme::LZ4, false),
        ] {
            let temp = tempfile::tempdir().unwrap();
            let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
            let raw = build_raw_xorb(num_chunks, ChunkSize::Fixed(chunk_bytes));
            let serialized =
                SerializedXorbObject::from_xorb_with_compression(raw, compression, footer).unwrap();
            assert_eq!(serialized.footer_start.is_some(), footer);
            let hash = serialized.hash.hex();
            let stored = store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
                .await
                .unwrap();
            assert!(stored.was_inserted);
            let key = xorb_object_key(&hash).unwrap();
            let mut count = 0;
            visit_stored_xorb_chunk_hashes(&object_store, &key, |hash| {
                assert_eq!(hash.len(), 64);
                count += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(count, num_chunks);
        }
    }

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

    #[tokio::test]
    async fn store_uploaded_xorb_with_metrics_single_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        let result =
            store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data)
                .await;
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

    #[tokio::test]
    async fn store_uploaded_multiple_chunks_visits_all_correctly() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(5, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();

        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn store_uploaded_xorb_with_metrics_then_visit_all_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb_with_metrics(&object_store, &hash, &serialized.serialized_data)
            .await
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

    #[tokio::test]
    async fn store_uploaded_xorb_different_xorbs_independent() {
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

        let r1 = store_uploaded_xorb(&object_store, &s1.hash.hex(), &s1.serialized_data)
            .await
            .unwrap();
        assert!(r1.was_inserted);
        let r2 = store_uploaded_xorb(&object_store, &s2.hash.hex(), &s2.serialized_data)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn persisted_member_cache_cannot_hide_members_or_bypass_container_validation() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
            .unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let cache_key =
            shardline_storage::ObjectKey::parse(&format!("_xorb_chunks/{}/{}", &hash[..2], hash))
                .unwrap();
        let mut expected = Vec::new();
        visit_stored_xorb_chunk_hashes(&object_store, &key, |hash| {
            expected.push(hash);
            Ok(())
        })
        .unwrap();
        assert_eq!(expected.len(), 3);
        assert!(
            object_store.metadata(&cache_key).unwrap().is_none(),
            "a scan must not write an unverified sidecar"
        );
        for cache in [
            Vec::new(),
            expected.first().unwrap().as_bytes().to_vec(),
            b"invalid cache".to_vec(),
        ] {
            object_store
                .put_overwrite(
                    &cache_key,
                    ObjectBody::Borrowed(&cache),
                    &ObjectIntegrity::new(
                        shardline_server_core::chunk_hash(&cache),
                        cache.len() as u64,
                    ),
                )
                .unwrap();
            let mut actual = Vec::new();
            visit_stored_xorb_chunk_hashes(&object_store, &key, |hash| {
                actual.push(hash);
                Ok(())
            })
            .unwrap();
            assert_eq!(actual, expected);
        }
        let corrupt = b"invalid container";
        object_store
            .put_overwrite(
                &key,
                ObjectBody::Borrowed(corrupt),
                &ObjectIntegrity::new(
                    shardline_server_core::chunk_hash(corrupt),
                    corrupt.len() as u64,
                ),
            )
            .unwrap();
        let mut visited = 0;
        assert!(
            visit_stored_xorb_chunk_hashes(&object_store, &key, |_hash| {
                visited += 1;
                Ok(())
            })
            .is_err()
        );
        assert_eq!(visited, 0);
    }

    #[tokio::test]
    async fn store_and_visit_xorb_multiple_times_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(128));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
            .unwrap();
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

    #[tokio::test]
    async fn store_xorb_with_lz4_then_visit_hashes() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(5, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, true)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
            .unwrap();
        let key = xorb_object_key(&hash).unwrap();
        let mut count = 0_usize;
        visit_stored_xorb_chunk_hashes(&object_store, &key, |_h| {
            count += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn store_footerless_xorb_then_visit_normalized() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let raw = build_raw_xorb(3, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::LZ4, false)
                .unwrap();
        let hash = serialized.hash.hex();
        store_uploaded_xorb(&object_store, &hash, &serialized.serialized_data)
            .await
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
}
