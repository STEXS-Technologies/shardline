use std::io::{Cursor, Read};

use shardline_index::xet_hash_hex_string;
use shardline_protocol::ShardlineHash;
use shardline_xet_core::{
    merklehash::MerkleHash,
    xorb_object::{
        Chunk, CompressionScheme, RawXorbData, SerializedXorbObject,
        xorb_chunk_format::{XorbChunkHeader, XORB_CHUNK_HEADER_LENGTH, deserialize_chunk_header},
    },
};

use crate::{ServerError, object_store::ServerObjectStore};

/// Metadata for one chunk packed inside a xorb.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read in tests
pub(crate) struct XorbChunkEntry {
    /// Index of this chunk within the xorb (0-based).
    pub chunk_index: u32,
    /// Byte offset in the original file (uncompressed).
    pub raw_offset: u64,
    /// Uncompressed chunk length.
    pub raw_length: u64,
    /// Byte offset of this chunk in the serialized xorb (including header).
    pub packed_offset: u32,
    /// Compressed length of this chunk in the serialized xorb (including header).
    pub packed_length: u32,
}

/// A fully packed xorb container with metadata.
#[derive(Debug, Clone)]
pub(crate) struct PackedXorb {
    /// Complete serialized xorb bytes (chunks + footer).
    pub serialized: Vec<u8>,
    /// Xorb content hash in Xet hexadecimal format.
    pub xorb_hash_hex: String,
    /// Per-chunk metadata within this xorb.
    pub chunk_entries: Vec<XorbChunkEntry>,
}

/// Packs CDC chunks into a xorb container using BG4+LZ4 compression
/// and stores it in the object store.
///
/// Takes raw (uncompressed) chunk data paired with their file offsets
/// and produces a serialized xorb with footer metadata. Returns the
/// packed xorb together with per-chunk entry metadata.
///
/// # Errors
///
/// Returns [`ServerError::Overflow`] when chunk offsets or buffer
/// lengths overflow, or [`ServerError::Io`] when serialization fails.
pub(crate) fn pack_chunks_into_xorb(
    chunks: &[(Vec<u8>, u64)], // (raw_data, file_offset)
) -> Result<PackedXorb, ServerError> {
    if chunks.is_empty() {
        return Err(ServerError::Overflow);
    }

    // Build Chunk objects with content hashes.
    let xorb_chunks: Vec<Chunk> = chunks
        .iter()
        .map(|(data, _)| Chunk {
            hash: shardline_xet_core::merklehash::compute_data_hash(data),
            data: std::borrow::Cow::Owned(data.clone()),
        })
        .collect();

    let file_boundaries: Vec<usize> = chunks.iter().map(|(_, offset)| *offset as usize).collect();

    let raw = RawXorbData::from_chunks(&xorb_chunks, file_boundaries);

    // Serialize with BG4+LZ4 compression and footer.
    let serialized = SerializedXorbObject::from_xorb_with_compression(
        raw,
        CompressionScheme::ByteGrouping4LZ4,
        true, // include footer
    )
    .map_err(|e| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("xorb serialization failed: {e}"),
        ))
    })?;

    // Compute the xorb hash in Xet hex format.
    let merkle_hash: MerkleHash = serialized.hash;
    let hash_bytes: [u8; 32] = merkle_hash.into();
    let shardline_hash = ShardlineHash::from_bytes(hash_bytes);
    let xorb_hash_hex = xet_hash_hex_string(shardline_hash);

    // Walk the serialized xorb to record per-chunk byte offsets.
    let mut chunk_entries = Vec::with_capacity(chunks.len());
    let mut cursor = Cursor::new(serialized.serialized_data.as_slice());
    let mut packed_offset: u32 = 0;

    for (i, (raw_data, raw_offset)) in chunks.iter().enumerate() {
        let header: XorbChunkHeader = deserialize_chunk_header(&mut cursor).map_err(|e| {
            ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read xorb chunk header at index {i}: {e}"),
            ))
        })?;

        let compressed_len = usize::try_from(header.get_compressed_length())
            .map_err(|e| {
                ServerError::NumericConversion(e)
            })?;
        let chunk_total_len =
            u32::try_from(XORB_CHUNK_HEADER_LENGTH.checked_add(compressed_len).ok_or(ServerError::Overflow)?)
                .map_err(ServerError::NumericConversion)?;

        // Skip the compressed payload bytes.
        let mut skip_buf = vec![0u8; compressed_len];
        cursor.read_exact(&mut skip_buf).map_err(|e| {
            ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("truncated xorb chunk payload at index {i}: {e}"),
            ))
        })?;

        chunk_entries.push(XorbChunkEntry {
            chunk_index: i as u32,
            raw_offset: *raw_offset,
            raw_length: raw_data.len() as u64,
            packed_offset,
            packed_length: chunk_total_len,
        });

        packed_offset = packed_offset
            .checked_add(chunk_total_len)
            .ok_or(ServerError::Overflow)?;
    }

    Ok(PackedXorb {
        serialized: serialized.serialized_data,
        xorb_hash_hex,
        chunk_entries,
    })
}

/// Stores a serialized xorb in the object store using content-addressed
/// xorb key (`xorbs/default/{prefix}/{hash}.xorb`).
///
/// Uses `put_if_absent` — if the xorb already exists the store is not
/// modified.
///
/// # Errors
///
/// Returns [`ServerError`] when the key cannot be constructed, the store
/// operation fails, or integrity computation fails.
pub(crate) async fn store_xorb(
    object_store: &ServerObjectStore,
    xorb_hash_hex: &str,
    xorb_bytes: &[u8],
) -> Result<bool, ServerError> {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore, PutOutcome};

    let object_key = crate::xet_adapter::xorb_object_key(xorb_hash_hex)?;
    let integrity = ObjectIntegrity::new(
        crate::local_backend::chunk_hash(xorb_bytes),
        u64::try_from(xorb_bytes.len())?,
    );
    let outcome = object_store.put_if_absent(
        &object_key,
        ObjectBody::from_bytes(axum::body::Bytes::copy_from_slice(xorb_bytes)),
        &integrity,
    )?;
    Ok(matches!(outcome, PutOutcome::Inserted))
}

#[cfg(test)]
mod tests {
    use shardline_protocol::ShardlineHash;
    use shardline_xet_core::{
        merklehash::{compute_data_hash, xorb_hash},
        xorb_object::xorb_chunk_format::deserialize_chunk,
    };

    use super::*;

    /// Helper to validate that a packed xorb round-trips correctly.
    fn verify_xorb_roundtrip(packed: &PackedXorb, expected_chunks: &[(Vec<u8>, u64)]) {
        // Validate all chunks can be decoded from the serialized xorb.
        let mut cursor = Cursor::new(packed.serialized.as_slice());

        for (i, (expected_data, expected_offset)) in expected_chunks.iter().enumerate() {
            let (decoded, compressed_size, uncompressed_len) =
                deserialize_chunk(&mut cursor).expect("chunk should deserialize");

            assert_eq!(
                decoded, *expected_data,
                "chunk {i} data mismatch after round-trip"
            );
            assert_eq!(
                u64::from(uncompressed_len),
                expected_data.len() as u64,
                "chunk {i} length mismatch"
            );

            let entry = &packed.chunk_entries[i];
            assert_eq!(entry.chunk_index, i as u32, "chunk {i} index mismatch");
            assert_eq!(entry.raw_offset, *expected_offset, "chunk {i} offset mismatch");
            assert_eq!(entry.raw_length, expected_data.len() as u64, "chunk {i} raw length mismatch");
            assert_eq!(entry.packed_length as usize, compressed_size, "chunk {i} packed length mismatch");
        }

        // Verify xorb hash is deterministic.
        let hash_bytes: [u8; 32] = {
            let merkle: MerkleHash = {
                // Re-compute the xorb hash from chunk hashes.
                let chunks_and_sizes: Vec<_> = expected_chunks
                    .iter()
                    .map(|(data, _)| {
                        let h = compute_data_hash(data);
                        (h, data.len() as u64)
                    })
                    .collect();
                shardline_xet_core::merklehash::xorb_hash(&chunks_and_sizes)
            };
            merkle.into()
        };
        let expected_xorb_hash_hex = xet_hash_hex_string(ShardlineHash::from_bytes(hash_bytes));
        assert_eq!(
            packed.xorb_hash_hex, expected_xorb_hash_hex,
            "xorb hash should match computed hash"
        );
    }

    #[test]
    fn pack_single_chunk() {
        let chunks = vec![(b"hello world".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 1);
        assert!(!packed.xorb_hash_hex.is_empty());
        assert!(packed.serialized.len() > 20);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn pack_multiple_chunks() {
        let chunks = vec![
            (b"hello ".to_vec(), 0u64),
            (b"world".to_vec(), 6u64),
            (b"!".to_vec(), 11u64),
        ];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 3);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn pack_large_chunks() {
        // 64KiB chunks of compressible data.
        let data1 = vec![0xABu8; 65536];
        let data2 = vec![0xCDu8; 65536];
        let chunks = vec![(data1, 0u64), (data2, 65536u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 2);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn pack_empty_chunks_errors() {
        let result = pack_chunks_into_xorb(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn pack_deterministic_hash() {
        let chunks = vec![(b"test data".to_vec(), 0u64)];
        let packed1 = pack_chunks_into_xorb(&chunks).unwrap();
        let packed2 = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed1.xorb_hash_hex, packed2.xorb_hash_hex);
        assert_eq!(packed1.serialized, packed2.serialized);
    }

    #[test]
    fn pack_chunk_offsets_are_monotonic() {
        let chunks = vec![
            (b"chunk a".to_vec(), 0u64),
            (b"chunk b".to_vec(), 7u64),
            (b"chunk c".to_vec(), 14u64),
        ];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        let mut prev_end = 0u32;
        for entry in &packed.chunk_entries {
            assert!(
                entry.packed_offset >= prev_end,
                "packed offsets must be monotonic"
            );
            prev_end = entry
                .packed_offset
                .checked_add(entry.packed_length)
                .expect("offset overflow");
        }
    }

    #[test]
    fn store_and_read_xorb() {
        use shardline_storage::ObjectStore;
        use crate::object_store::ServerObjectStore;

        let tmp = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(tmp.path().join("objects")).unwrap();

        let chunks = vec![(b"xorb store test".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let was_inserted = rt
            .block_on(store_xorb(&store, &packed.xorb_hash_hex, &packed.serialized))
            .unwrap();
        assert!(was_inserted, "first store should insert");

        // Verify the xorb exists in the store.
        let object_key =
            crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
        assert!(store.contains(&object_key).unwrap());

        // Read back and verify using metadata length.
        let metadata = store.metadata(&object_key).unwrap().unwrap();
        let stored_bytes = store.read_full_object(&object_key, metadata.length()).unwrap();
        assert_eq!(stored_bytes, packed.serialized);
    }

    #[test]
    fn store_xorb_idempotent() {
        use crate::object_store::ServerObjectStore;

        let tmp = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(tmp.path().join("objects")).unwrap();

        let chunks = vec![(b"idempotent test".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let first = rt
            .block_on(store_xorb(&store, &packed.xorb_hash_hex, &packed.serialized))
            .unwrap();
        assert!(first);

        let second = rt
            .block_on(store_xorb(&store, &packed.xorb_hash_hex, &packed.serialized))
            .unwrap();
        assert!(!second, "second store should report not inserted");
    }

    #[test]
    fn pack_and_unpack_round_trip_preserves_content() {
        // Pack 3 chunks of varying sizes, then unpack via
        // validate_serialized_xorb + decode and verify every chunk's
        // hash matches the original raw data.
        let chunks = vec![
            (b"hello".to_vec(), 0u64),
            (b" large world ".to_vec(), 5u64),
            (b"!!".to_vec(), 19u64),
        ];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 3);

        let expected_hash =
            shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let mut cursor = std::io::Cursor::new(packed.serialized.as_slice());
        let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, expected_hash)
            .expect("xorb validation should succeed");

        std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(0)).unwrap();
        let decoded =
            crate::xet_adapter::decode_serialized_xorb_chunks(&mut cursor, &validated)
                .expect("xorb decode should succeed");

        assert_eq!(decoded.len(), chunks.len());
        for (i, (expected_data, _offset)) in chunks.iter().enumerate() {
            let decoded_chunk = decoded.get(i).expect("decoded chunk should exist");
            assert_eq!(
                decoded_chunk.data(),
                expected_data.as_slice(),
                "chunk {i} data mismatch after full round trip"
            );
            // Verify per-chunk content hash matches original raw data.
            let expected_hash: [u8; 32] =
                compute_data_hash(expected_data).into();
            let actual_hash = *decoded_chunk.descriptor().hash().as_bytes();
            assert_eq!(
                expected_hash, actual_hash,
                "chunk {i} content hash mismatch"
            );
        }
    }

    #[test]
    fn xorb_hash_is_deterministic() {
        // Same chunks produce the same xorb hash every time.
        let chunks = vec![
            (b"alpha".to_vec(), 0u64),
            (b"beta".to_vec(), 5u64),
            (b"gamma".to_vec(), 9u64),
        ];
        let packed1 = pack_chunks_into_xorb(&chunks).unwrap();
        let packed2 = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed1.xorb_hash_hex, packed2.xorb_hash_hex);
        assert_eq!(packed1.serialized, packed2.serialized);
    }

    #[test]
    fn xorb_hash_depends_on_chunk_order() {
        // Different chunk order produces a different xorb hash.
        let chunks_a = vec![
            (b"first".to_vec(), 0u64),
            (b"second".to_vec(), 5u64),
            (b"third".to_vec(), 11u64),
        ];
        let chunks_b = vec![
            (b"first".to_vec(), 0u64),
            (b"third".to_vec(), 5u64),
            (b"second".to_vec(), 11u64),
        ];
        let packed_a = pack_chunks_into_xorb(&chunks_a).unwrap();
        let packed_b = pack_chunks_into_xorb(&chunks_b).unwrap();
        assert_ne!(
            packed_a.xorb_hash_hex, packed_b.xorb_hash_hex,
            "reordered chunks should produce different xorb hashes"
        );
        assert_ne!(
            packed_a.serialized, packed_b.serialized,
            "reordered chunks should produce different serialized data"
        );
    }

    #[test]
    fn single_chunk_xorb_round_trip() {
        // Pack a single chunk, unpack, and verify content.
        let data = b"single chunk payload".to_vec();
        let chunks = vec![(data.clone(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 1);

        // Unpack and verify.
        let expected_hash =
            shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let mut cursor = std::io::Cursor::new(packed.serialized.as_slice());
        let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, expected_hash)
            .expect("single-chunk xorb validation should succeed");

        std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(0)).unwrap();
        let decoded =
            crate::xet_adapter::decode_serialized_xorb_chunks(&mut cursor, &validated)
                .expect("single-chunk xorb decode should succeed");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].data(), data.as_slice());
    }

    #[test]
    fn large_chunk_xorb_round_trip() {
        // Pack a 1 MiB chunk, unpack, and verify content.
        let data = vec![0xABu8; 1024 * 1024]; // 1 MiB
        let chunks = vec![(data.clone(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 1);

        // Unpack via validate + decode.
        let expected_hash =
            shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let mut cursor = std::io::Cursor::new(packed.serialized.as_slice());
        let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, expected_hash)
            .expect("large-chunk xorb validation should succeed");

        std::io::Seek::seek(&mut cursor, std::io::SeekFrom::Start(0)).unwrap();
        let decoded =
            crate::xet_adapter::decode_serialized_xorb_chunks(&mut cursor, &validated)
                .expect("large-chunk xorb decode should succeed");

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].data(), data.as_slice());
    }

    #[test]
    fn xorb_stored_and_retrieved_from_local_store() {
        // Store xorb in a local object store, read it back, verify bytes match.
        use shardline_storage::ObjectStore;

        let tmp = tempfile::tempdir().unwrap();
        let store = crate::object_store::ServerObjectStore::local(tmp.path().join("objects"))
            .unwrap();

        let chunks = vec![(b"stored xorb test data".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let was_inserted = rt
            .block_on(store_xorb(&store, &packed.xorb_hash_hex, &packed.serialized))
            .unwrap();
        assert!(was_inserted, "first store should insert");

        // Read back and verify bytes match.
        let object_key =
            crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
        let metadata = store.metadata(&object_key).unwrap().unwrap();
        let stored_bytes = store.read_full_object(&object_key, metadata.length()).unwrap();
        assert_eq!(stored_bytes, packed.serialized);
    }

    #[test]
    fn pack_xorb_hash_matches_xet_format() {
        // Verify that the xorb hash hex produced by our packer can be
        // parsed back by the Xet adapter's parse_xet_hash_hex function.
        let data = b"hash format test".to_vec();
        let chunks = vec![(data, 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let parsed =
            shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let expected: [u8; 32] = {
            let hash = compute_data_hash(b"hash format test");
            let xorb_h = xorb_hash(&[(hash, 16u64)]);
            xorb_h.into()
        };
        assert_eq!(parsed.as_bytes(), &expected);
    }
}
