use axum::body::Bytes;
use bytes::BytesMut;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
use tokio::task;
use tracing::{debug, trace};

use super::body_reader::ChunkBuffer;
use crate::{
    ServerError,
    chunk_store::chunk_object_key_for_computed_hash,
    local_backend::chunk_hash,
    metrics::{record_compression_saved, record_dedup_saves},
    object_store::ServerObjectStore,
};

pub(super) struct SequencedStoredChunkOutcome {
    pub(super) sequence: u64,
    pub(super) offset: u64,
    pub(super) stored: StoredChunkOutcome,
}

pub(super) struct SequencedStoredChunkTaskOutcome {
    pub(super) sequence: u64,
    pub(super) offset: u64,
    pub(super) stored: StoredChunkOutcome,
    pub(super) reusable_buffer: Option<BytesMut>,
}

pub(super) struct StoredChunkOutcome {
    pub(super) hash_hex: String,
    pub(super) chunk_length: u64,
    pub(super) compressed_length: u64,
    pub(super) inserted: bool,
}

struct ChunkStorageRequest {
    key: ObjectKey,
    integrity: ObjectIntegrity,
    hash_hex: String,
    chunk_length: u64,
    compressed_length: u64,
}

fn chunk_object_key_and_integrity(chunk: &ChunkBuffer) -> Result<ChunkStorageRequest, ServerError> {
    let raw = chunk.as_slice();
    let compressed = lz4_flex::compress_prepend_size(raw);
    let raw_hash = chunk_hash(raw); // content identity: hash of raw bytes
    let compressed_hash = chunk_hash(&compressed); // storage integrity: hash of stored bytes
    let (hash_hex, object_key) = chunk_object_key_for_computed_hash(raw_hash)?;
    let chunk_length = u64::try_from(raw.len())?;
    let compressed_length = u64::try_from(compressed.len())?;
    #[allow(clippy::float_arithmetic)]
    let ratio = format!("{:.2}", compressed.len() as f64 / raw.len().max(1) as f64);
    trace!(
        raw_len = raw.len(),
        compressed_len = compressed.len(),
        ratio = ratio,
        "LZ4 compressed chunk"
    );
    record_compression_saved(chunk_length, compressed_length);
    let integrity = ObjectIntegrity::new(compressed_hash, compressed_length);
    Ok(ChunkStorageRequest {
        key: object_key,
        integrity,
        hash_hex,
        chunk_length,
        compressed_length,
    })
}

/// Records dedup savings when a chunk already exists in the store.
fn record_dedup_on_already_exists(outcome: PutOutcome, chunk_length: u64) {
    if matches!(outcome, PutOutcome::AlreadyExists) {
        debug!(chunk_length, "dedup hit — chunk already stored");
        record_dedup_saves(chunk_length);
    }
}

pub(super) async fn put_if_absent_pooled_chunk_buffer(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
) -> Result<(StoredChunkOutcome, Option<BytesMut>), ServerError> {
    let request = chunk_object_key_and_integrity(&chunk)?;
    let ChunkBuffer::Pooled(bytes) = chunk;
    let compressed = lz4_flex::compress_prepend_size(&bytes);
    let compressed_bytes = Bytes::from(compressed);
    let (outcome, _compressed_bytes) =
        put_if_absent_pooled_bytes(object_store, &request, compressed_bytes).await?;
    record_dedup_on_already_exists(outcome, request.chunk_length);
    // Return the original (uncompressed) bytes for reuse as a pending buffer
    let reusable_buffer = bytes.try_into_mut().ok();
    Ok((
        StoredChunkOutcome {
            hash_hex: request.hash_hex,
            chunk_length: request.chunk_length,
            compressed_length: request.compressed_length,
            inserted: matches!(outcome, PutOutcome::Inserted),
        },
        reusable_buffer,
    ))
}

async fn put_if_absent_pooled_bytes(
    object_store: &ServerObjectStore,
    request: &ChunkStorageRequest,
    bytes: Bytes,
) -> Result<(PutOutcome, Bytes), ServerError> {
    match object_store {
        ServerObjectStore::Local(store) => {
            let store = store.clone();
            let key = request.key.clone();
            let integrity = request.integrity;
            task::spawn_blocking(move || {
                let outcome = store
                    .put_if_absent(&key, ObjectBody::from_bytes(bytes.clone()), &integrity)
                    .map_err(ServerError::from)?;
                Ok((outcome, bytes))
            })
            .await
            .map_err(ServerError::BlockingTask)?
        }
        ServerObjectStore::S3(store) => {
            let store = store.clone();
            let key = request.key.clone();
            let integrity = request.integrity;
            task::spawn_blocking(move || {
                let outcome = store
                    .put_if_absent(&key, ObjectBody::from_bytes(bytes.clone()), &integrity)
                    .map_err(ServerError::from)?;
                Ok((outcome, bytes))
            })
            .await
            .map_err(ServerError::BlockingTask)?
        }
        ServerObjectStore::Blackhole => {
            let outcome = object_store.put_if_absent(
                &request.key,
                ObjectBody::from_bytes(bytes.clone()),
                &request.integrity,
            )?;
            Ok((outcome, bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;

    use super::{
        ChunkBuffer, chunk_object_key_and_integrity, put_if_absent_pooled_chunk_buffer,
    };
    use crate::local_backend::chunk_hash;
    use crate::object_store::ServerObjectStore;
    use shardline_index::xet_hash_hex_string;
    use shardline_storage::ObjectStore;

    // ------------------------------------------------------------------
    // chunk_object_key_and_integrity
    // ------------------------------------------------------------------

    #[test]
    fn pooled_chunk_produces_valid_content_addressed_key() {
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"hello world"));
        let result = chunk_object_key_and_integrity(&chunk);
        assert!(result.is_ok());
        let request = result.unwrap();
        // Key should contain the hash prefix layout
        assert!(!request.hash_hex.is_empty());
        assert_eq!(request.chunk_length, 11);
    }

    #[test]
    fn integrity_hash_matches_blake3_of_raw_bytes() {
        let data = b"test data for blake3 verification";
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(data));
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        let expected_hash = chunk_hash(data); // CHANGED: hash raw bytes, not compressed
        let expected_hex = xet_hash_hex_string(expected_hash);
        assert_eq!(request.hash_hex, expected_hex);
        // Small data may not compress below raw length (LZ4 adds size prefix overhead),
        // but the compressed length must be consistent
        assert!(request.compressed_length > 0);
    }

    #[test]
    fn hash_is_of_raw_bytes_not_compressed() {
        // Two different raw chunks must produce different hashes
        // even if they compress to the same output (unlikely but proves the model)
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"hello world"));
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"hello worle"));
        let req1 = chunk_object_key_and_integrity(&chunk1).unwrap();
        let req2 = chunk_object_key_and_integrity(&chunk2).unwrap();
        assert_ne!(req1.hash_hex, req2.hash_hex);

        // Verify hash is computed on raw data, not compressed
        let raw_hash = chunk_hash(b"hello world");
        let raw_hex = xet_hash_hex_string(raw_hash);
        assert_eq!(req1.hash_hex, raw_hex);
    }

    #[test]
    fn chunk_length_matches_buffer_length() {
        let data = b"short";
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(data));
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        assert_eq!(request.chunk_length, 5);
    }

    #[test]
    fn different_chunks_produce_different_hashes() {
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"alpha"));
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"beta"));
        let req1 = chunk_object_key_and_integrity(&chunk1).unwrap();
        let req2 = chunk_object_key_and_integrity(&chunk2).unwrap();
        assert_ne!(req1.hash_hex, req2.hash_hex);
    }

    #[test]
    fn same_content_produces_same_hash() {
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"identical"));
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"identical"));
        let req1 = chunk_object_key_and_integrity(&chunk1).unwrap();
        let req2 = chunk_object_key_and_integrity(&chunk2).unwrap();
        assert_eq!(req1.hash_hex, req2.hash_hex);
        assert_eq!(req1.key.as_str(), req2.key.as_str());
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer with Blackhole store
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn blackhole_store_inserted_returns_true() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"test data"));
        let (outcome, _reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 9);
    }

    #[tokio::test]
    async fn blackhole_store_same_hash_inserted_returns_true() {
        let store = ServerObjectStore::blackhole();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"same data"));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"same data"));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        // Blackhole always returns Inserted
        assert!(outcome1.inserted);
        assert!(outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer with Local store
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn local_store_first_insert_returns_inserted() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"local test"));
        let (outcome, _reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 10);
    }

    #[tokio::test]
    async fn local_store_same_hash_returns_not_inserted() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"dedup test"));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        assert!(outcome1.inserted);

        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"dedup test"));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(!outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    #[tokio::test]
    async fn local_store_stored_bytes_match_original() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = b"verify stored content matches";
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(data));
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);

        // The hash is computed on raw bytes (content-addressed on raw content)
        let object_key = crate::chunk_store::chunk_object_key(&outcome.hash_hex).unwrap();
        let hash = chunk_hash(data);
        let expected_hex = xet_hash_hex_string(hash);
        assert_eq!(outcome.hash_hex, expected_hex);

        // Verify the object exists in the store
        assert!(store.contains(&object_key).unwrap());
    }

    #[tokio::test]
    async fn local_store_different_chunks_store_separately() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"chunk one"));
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"chunk two"));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(outcome1.inserted);
        assert!(outcome2.inserted);
        assert_ne!(outcome1.hash_hex, outcome2.hash_hex);
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer with Local store — dedup
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn pooled_local_store_returns_inserted_first_time() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"pooled test"));
        let (outcome, _reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
    }

    #[tokio::test]
    async fn pooled_local_store_returns_not_inserted_second_time() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"pooled dedup"));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        assert!(outcome1.inserted);

        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"pooled dedup"));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(!outcome2.inserted);
    }

    #[tokio::test]
    async fn dedup_records_savings_metric_on_already_exists() {
        // Verify that storing a duplicate chunk triggers the dedup_saves_bytes_total
        // metric, which is wired through record_dedup_on_already_exists.
        // NOTE: We use only relative assertions because the global metric counter
        // is shared across all tests in the process.
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = b"dedup-metric-test-content!";

        // First insert — should be stored as new
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        assert!(outcome1.inserted);

        // Second insert with same content — should trigger dedup and record savings
        let before_dedup = shardline_metrics::metrics()
            .storage
            .dedup_saves_bytes_total
            .get();
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(!outcome2.inserted);
        let after_dedup = shardline_metrics::metrics()
            .storage
            .dedup_saves_bytes_total
            .get();
        assert!(
            after_dedup > before_dedup,
            "dedup_saves_bytes_total should increase by chunk_length ({}) on dedup (before: {before_dedup}, after: {after_dedup})",
            outcome2.chunk_length
        );
    }

    #[tokio::test]
    async fn pooled_local_store_returns_reusable_buffer_for_pooled_bytes() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        // Use Bytes::from(Vec) so the buffer is Arc-backed and convertible
        // back to BytesMut via try_into_mut().
        let chunk = ChunkBuffer::Pooled(Bytes::from(vec![0u8; 8]));
        let (_outcome, reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        // When Bytes has a single reference, try_into_mut() succeeds
        assert!(reusable.is_some());
        let buf = reusable.unwrap();
        assert_eq!(&*buf, &[0u8; 8]);
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn empty_chunk_buffer_round_trip() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Pooled(Bytes::new());
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 0);
    }

    #[tokio::test]
    async fn empty_chunk_blackhole_round_trip() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::new());
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 0);
    }

    // ------------------------------------------------------------------
    // ChunkBuffer length via chunk_object_key_and_integrity edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_chunk_produces_valid_key() {
        let chunk = ChunkBuffer::Pooled(Bytes::new());
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        assert_eq!(request.chunk_length, 0);
        assert!(!request.hash_hex.is_empty());
    }

    #[tokio::test]
    async fn blackhole_store_pooled_chunk_buffer_returns_inserted_with_reusable() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::from(vec![0u8; 16]));
        let (outcome, reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        // Blackhole's put_if_absent returns true (Inserted)
        // and the Bytes may be reusable if it has a single ref
        assert_eq!(outcome.chunk_length, 16);
        // reusable is Some if try_into_mut succeeds, which depends on ref count
        // at minimum verify the function doesn't panic
        let _ = reusable;
    }

    #[tokio::test]
    async fn local_store_pooled_chunk_buffer_inserts_and_returns_reusable() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Pooled(Bytes::from(vec![0xABu8; 32]));
        let (outcome, reusable) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 32);
        // Buffer should be reusable since we have unique ownership
        assert!(reusable.is_some());
        if let Some(buf) = reusable {
            assert_eq!(buf.len(), 32);
            assert_eq!(&*buf, &[0xABu8; 32]);
        }
    }

    // ------------------------------------------------------------------
    // Large data edge cases
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn local_store_put_if_absent_large_chunk() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = vec![0x42u8; 65536]; // 64KB chunk
        let chunk = ChunkBuffer::Pooled(Bytes::from(data));
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 65536);
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer with Blackhole — all store types
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn blackhole_put_if_absent_pooled_same_content_inserted() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"dedup pool blackhole"));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome1.inserted);
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"dedup pool blackhole"));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    // ------------------------------------------------------------------
    // Compression tests
    // ------------------------------------------------------------------

    #[test]
    fn compress_decompress_round_trip_preserves_data() {
        let data = b"hello world this is test data for lz4 compression round trip";
        let compressed = lz4_flex::compress_prepend_size(data);
        let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compressible_data_is_smaller_after_compression() {
        // Highly repetitive data compresses well
        let data = vec![0xABu8; 4096];
        let compressed = lz4_flex::compress_prepend_size(&data);
        assert!(
            compressed.len() < data.len(),
            "expected compressible data to shrink (compressed={} vs raw={})",
            compressed.len(),
            data.len()
        );
    }

    #[test]
    fn incompressible_random_data_does_not_expand_significantly() {
        // LZ4 stores incompressible data with minimal overhead.
        // Use somewhat random data (not perfectly random, but enough to be incompressible).
        let data: Vec<u8> = (0..4096)
            .map(|i: i32| (i.wrapping_mul(37) ^ 0xFF) as u8)
            .collect();
        let compressed = lz4_flex::compress_prepend_size(&data);
        // LZ4 may add a small amount of overhead for incompressible data plus the size prefix.
        // Allow up to 8 bytes of overhead for the size prefix and LZ4 block header.
        let overhead = (compressed.len() as i64) - (data.len() as i64);
        assert!(
            overhead <= 8,
            "expected minimal overhead for incompressible data, got {overhead}"
        );
    }

    #[allow(clippy::panic)]
    #[tokio::test]
    async fn compressed_chunk_stored_and_recoverable() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();

        // Store compressible content
        let data = vec![0x42u8; 8192];
        let chunk = ChunkBuffer::Pooled(Bytes::from(data.clone()));
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 8192);
        assert!(outcome.compressed_length < outcome.chunk_length);

        // Read the stored (compressed) bytes back from the store
        let object_key = crate::chunk_store::chunk_object_key(&outcome.hash_hex).unwrap();
        let local_store = match &store {
            ServerObjectStore::Local(store) => store,
            ServerObjectStore::S3(_) | ServerObjectStore::Blackhole => panic!("expected local store"),
        };
        let read_end = outcome.compressed_length.saturating_sub(1);
        let range = shardline_protocol::ByteRange::new(0, read_end).unwrap();
        let stored_compressed = local_store.read_range(&object_key, range).unwrap();
        assert_eq!(stored_compressed.len() as u64, outcome.compressed_length);

        // Decompress and verify
        let decompressed = lz4_flex::decompress_size_prepended(&stored_compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[tokio::test]
    async fn compressed_chunk_dedup_with_local_store() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();

        let data = b"compressible dedup test data";
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let (outcome1, _) = put_if_absent_pooled_chunk_buffer(&store, chunk1)
            .await
            .unwrap();
        assert!(outcome1.inserted);

        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let (outcome2, _) = put_if_absent_pooled_chunk_buffer(&store, chunk2)
            .await
            .unwrap();
        assert!(!outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
        assert_eq!(outcome1.compressed_length, outcome2.compressed_length);
    }

    #[allow(clippy::panic)]
    #[tokio::test]
    async fn compressed_length_in_stored_outcome_matches_stored_bytes() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();

        let data = vec![0x42u8; 1024];
        let chunk = ChunkBuffer::Pooled(Bytes::from(data));
        let (outcome, _) = put_if_absent_pooled_chunk_buffer(&store, chunk)
            .await
            .unwrap();
        assert!(outcome.inserted);

        let object_key = crate::chunk_store::chunk_object_key(&outcome.hash_hex).unwrap();
        let local_store = match &store {
            ServerObjectStore::Local(store) => store,
            ServerObjectStore::S3(_) | ServerObjectStore::Blackhole => panic!("expected local store"),
        };
        let read_end = outcome.compressed_length.saturating_sub(1);
        let range = shardline_protocol::ByteRange::new(0, read_end).unwrap();
        let stored = local_store.read_range(&object_key, range).unwrap();
        assert_eq!(
            stored.len() as u64,
            outcome.compressed_length,
            "stored byte count should match compressed_length"
        );
    }

    // ------------------------------------------------------------------
    // LZ4 compression additional tests
    // ------------------------------------------------------------------

    #[test]
    fn lz4_compress_reduces_size_for_compressible_data() {
        let data = vec![0u8; 65536]; // 64KB of zeros — highly compressible
        let compressed = lz4_flex::compress_prepend_size(&data);
        assert!(
            compressed.len() < data.len(),
            "compression should reduce size for zeros (compressed={} vs raw={})",
            compressed.len(),
            data.len()
        );
        let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn lz4_round_trip_various_sizes() {
        for size in [0, 1, 63, 64, 65, 128, 1024, 65536] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let compressed = lz4_flex::compress_prepend_size(&data);
            let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
            assert_eq!(decompressed, data, "round-trip failed for size {size}");
        }
    }

    #[test]
    fn lz4_zero_length_round_trip() {
        let data: Vec<u8> = vec![];
        let compressed = lz4_flex::compress_prepend_size(&data);
        let decompressed = lz4_flex::decompress_size_prepended(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compressed_length_leq_chunk_length_for_compressible() {
        let chunk = ChunkBuffer::Pooled(Bytes::from(vec![0u8; 4096]));
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        assert!(
            request.compressed_length <= request.chunk_length,
            "compressible data should not expand (compressed={} vs raw={})",
            request.compressed_length,
            request.chunk_length
        );
    }

    #[test]
    fn compressed_length_may_exceed_for_incompressible() {
        let data: Vec<u8> = (0..128).map(|i| (i * 37 + 13) as u8).collect();
        let chunk = ChunkBuffer::Pooled(Bytes::from(data));
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        // Small data with LZ4 prepend_size overhead may be larger than raw
        assert!(request.compressed_length > 0);
        assert_eq!(request.chunk_length, 128);
    }
}
