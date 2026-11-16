use std::io::Cursor;

use shardline_index::xet_hash_hex_string;
use shardline_protocol::ShardlineHash;
use shardline_xet_core::xorb_object::{
    SerializedXorbObject,
    xorb_chunk_format::{XORB_CHUNK_HEADER_LENGTH, XorbChunkHeader, deserialize_chunk_header},
};

use crate::{ServerError, object_store::ServerObjectStore};

mod raw_batch;
pub(super) use raw_batch::RecoverableXorbBatch;

/// Metadata for one chunk packed inside a xorb.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct XorbChunkEntry {
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
pub struct PackedXorb {
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
pub fn pack_chunks_into_xorb(
    chunks: &[(Vec<u8>, u64)], // (raw_data, file_offset)
) -> Result<PackedXorb, ServerError> {
    pack_owned_chunks_into_xorb(chunks.to_vec())
}

/// Packs owned CDC chunks without cloning their raw payloads.
pub(crate) fn pack_owned_chunks_into_xorb(
    chunks: Vec<(Vec<u8>, u64)>,
) -> Result<PackedXorb, ServerError> {
    RecoverableXorbBatch::new(chunks).pack()
}

fn packed_xorb_from_serialized(
    serialized: SerializedXorbObject,
    chunk_metadata: impl ExactSizeIterator<Item = (usize, u64)>,
) -> Result<PackedXorb, ServerError> {
    let xorb_hash_hex = xet_hash_hex_string(ShardlineHash::from_bytes(serialized.hash.into()));

    // Walk the serialized xorb to record per-chunk byte offsets.
    let mut chunk_entries = Vec::with_capacity(chunk_metadata.len());
    let mut cursor = Cursor::new(serialized.serialized_data.as_slice());
    let mut packed_offset: u32 = 0;

    for (i, (raw_length, raw_offset)) in chunk_metadata.enumerate() {
        let header: XorbChunkHeader = deserialize_chunk_header(&mut cursor).map_err(|e| {
            ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to read xorb chunk header at index {i}: {e}"),
            ))
        })?;

        let compressed_len = usize::try_from(header.get_compressed_length())
            .map_err(ServerError::NumericConversion)?;
        let chunk_total_len = u32::try_from(
            XORB_CHUNK_HEADER_LENGTH
                .checked_add(compressed_len)
                .ok_or(ServerError::Overflow)?,
        )
        .map_err(ServerError::NumericConversion)?;

        // Skip the compressed payload without allocating a temporary buffer,
        // while retaining the old truncated-payload rejection semantics.
        let payload_end = cursor
            .position()
            .checked_add(u64::try_from(compressed_len)?)
            .ok_or(ServerError::Overflow)?;
        if payload_end > u64::try_from(serialized.serialized_data.len())? {
            return Err(ServerError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("truncated xorb chunk payload at index {i}"),
            )));
        }
        cursor.set_position(payload_end);

        chunk_entries.push(XorbChunkEntry {
            chunk_index: i as u32,
            raw_offset,
            raw_length: u64::try_from(raw_length)?,
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
    use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, PutOutcome};

    let object_key = crate::xet_adapter::xorb_object_key(xorb_hash_hex)?;
    let integrity = ObjectIntegrity::new(
        crate::local_backend::chunk_hash(xorb_bytes),
        u64::try_from(xorb_bytes.len())?,
    );
    let outcome = object_store
        .put_if_absent(&object_key, ObjectBody::from_slice(xorb_bytes), &integrity)
        .await?;
    Ok(matches!(outcome, PutOutcome::Inserted))
}

#[cfg(test)]
mod async_store_tests;

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
        assert_eq!(packed.chunk_entries.len(), expected_chunks.len());
        // Check both the chunk stream and the complete native container.
        let mut cursor = Cursor::new(packed.serialized.as_slice());

        for (i, (expected_data, expected_offset)) in expected_chunks.iter().enumerate() {
            let chunk_start = cursor.position();
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
            assert_eq!(u64::from(entry.packed_offset), chunk_start);
            assert_eq!(entry.chunk_index, i as u32, "chunk {i} index mismatch");
            assert_eq!(
                entry.raw_offset, *expected_offset,
                "chunk {i} offset mismatch"
            );
            assert_eq!(
                entry.raw_length,
                expected_data.len() as u64,
                "chunk {i} raw length mismatch"
            );
            assert_eq!(
                entry.packed_length as usize, compressed_size,
                "chunk {i} packed length mismatch"
            );
        }

        let chunks_and_sizes: Vec<_> = expected_chunks
            .iter()
            .map(|(data, _)| (compute_data_hash(data), data.len() as u64))
            .collect();
        let expected_xorb_hash_hex = xet_hash_hex_string(ShardlineHash::from_bytes(
            xorb_hash(&chunks_and_sizes).into(),
        ));
        assert_eq!(
            packed.xorb_hash_hex, expected_xorb_hash_hex,
            "xorb hash should match computed hash"
        );

        cursor.set_position(0);
        let expected_hash = shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, expected_hash)
            .expect("packed container and footer should validate");
        cursor.set_position(0);
        let mut remaining = expected_chunks.iter();
        crate::xet_adapter::try_for_each_serialized_xorb_chunk_trusted(
            &mut cursor,
            &validated,
            |chunk| {
                let (expected_data, _) = remaining.next().expect("unexpected decoded chunk");
                assert_eq!(chunk.data(), expected_data.as_slice());
                let expected_hash: [u8; 32] = compute_data_hash(expected_data).into();
                assert_eq!(chunk.descriptor().hash().as_bytes(), &expected_hash);
                Ok::<(), ServerError>(())
            },
        )
        .expect("validated container should decode");
        assert!(remaining.next().is_none(), "missing decoded chunks");
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
    fn owned_packing_matches_borrowed_packing() {
        let chunks = vec![
            (b"first chunk".to_vec(), 0u64),
            (b"second chunk".to_vec(), 11u64),
        ];
        let borrowed = pack_chunks_into_xorb(&chunks).unwrap();
        let owned = pack_owned_chunks_into_xorb(chunks).unwrap();

        assert_eq!(owned.serialized, borrowed.serialized);
        assert_eq!(owned.xorb_hash_hex, borrowed.xorb_hash_hex);
        assert_eq!(owned.chunk_entries, borrowed.chunk_entries);
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
        use crate::object_store::ServerObjectStore;
        use shardline_storage::ObjectStore;

        let tmp = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(tmp.path().join("objects")).unwrap();

        let chunks = vec![(b"xorb store test".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let was_inserted = rt
            .block_on(store_xorb(
                &store,
                &packed.xorb_hash_hex,
                &packed.serialized,
            ))
            .unwrap();
        assert!(was_inserted, "first store should insert");

        // Verify the xorb exists in the store.
        let object_key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
        assert!(store.contains(&object_key).unwrap());

        // Read back and verify using metadata length.
        let metadata = store.metadata(&object_key).unwrap().unwrap();
        let stored_bytes = store
            .read_full_object(&object_key, metadata.length())
            .unwrap();
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
            .block_on(store_xorb(
                &store,
                &packed.xorb_hash_hex,
                &packed.serialized,
            ))
            .unwrap();
        assert!(first);

        let second = rt
            .block_on(store_xorb(
                &store,
                &packed.xorb_hash_hex,
                &packed.serialized,
            ))
            .unwrap();
        assert!(!second, "second store should report not inserted");
    }

    #[test]
    fn validated_round_trips_preserve_payloads_and_noncontiguous_file_offsets() {
        for chunks in [
            vec![(b"single chunk payload".to_vec(), 0)],
            vec![(vec![0xAB; 1024 * 1024], 0)],
            vec![
                (b"hello".to_vec(), 0),
                (b" large world ".to_vec(), 5),
                (b"!!".to_vec(), 19),
            ],
        ] {
            let packed = pack_chunks_into_xorb(&chunks).unwrap();
            verify_xorb_roundtrip(&packed, &chunks);
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
    fn pack_xorb_hash_matches_xet_format() {
        // Verify that the xorb hash hex produced by our packer can be
        // parsed back by the Xet adapter's parse_xet_hash_hex function.
        let data = b"hash format test".to_vec();
        let chunks = vec![(data, 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let parsed = shardline_index::parse_xet_hash_hex(&packed.xorb_hash_hex).unwrap();
        let expected: [u8; 32] = {
            let hash = compute_data_hash(b"hash format test");
            let xorb_h = xorb_hash(&[(hash, 16u64)]);
            xorb_h.into()
        };
        assert_eq!(parsed.as_bytes(), &expected);
    }

    #[test]
    fn pack_max_chunks_succeeds() {
        // Create exactly 8192 chunks of varying unique content.
        let chunks: Vec<(Vec<u8>, u64)> = (0..8192)
            .map(|i| (vec![(i & 0xFF) as u8; 64], i as u64 * 64))
            .collect();
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 8192);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn pack_single_very_large_chunk() {
        let data = b"A".repeat(2_000_000);
        let chunks = vec![(data, 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 1);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn pack_chunks_with_binary_data() {
        // Chunks with binary data including null bytes and high bytes 0x00-0xFF.
        let chunks: Vec<(Vec<u8>, u64)> = (0..256)
            .map(|i| {
                let data: Vec<u8> = (0..64).map(|j| ((i + j) & 0xFF) as u8).collect();
                (data, i as u64 * 64)
            })
            .collect();
        let packed = pack_chunks_into_xorb(&chunks).unwrap();
        assert_eq!(packed.chunk_entries.len(), 256);
        verify_xorb_roundtrip(&packed, &chunks);
    }

    #[test]
    fn store_xorb_idempotent_already_exists() {
        use crate::object_store::ServerObjectStore;
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore, PutOutcome};

        let tmp = tempfile::tempdir().unwrap();
        let store = ServerObjectStore::local(tmp.path().join("objects")).unwrap();

        let chunks = vec![(b"idempotent already exists test".to_vec(), 0u64)];
        let packed = pack_chunks_into_xorb(&chunks).unwrap();

        let object_key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
        let integrity = ObjectIntegrity::new(
            crate::local_backend::chunk_hash(&packed.serialized),
            packed.serialized.len() as u64,
        );

        let outcome1 = store
            .put_if_absent(
                &object_key,
                ObjectBody::from_bytes(axum::body::Bytes::copy_from_slice(&packed.serialized)),
                &integrity,
            )
            .unwrap();
        assert!(
            matches!(outcome1, PutOutcome::Inserted),
            "first store should return Inserted, not AlreadyExists"
        );

        let outcome2 = store
            .put_if_absent(
                &object_key,
                ObjectBody::from_bytes(axum::body::Bytes::copy_from_slice(&packed.serialized)),
                &integrity,
            )
            .unwrap();
        assert!(
            matches!(outcome2, PutOutcome::AlreadyExists),
            "second store should return AlreadyExists, not Inserted"
        );
    }
}
