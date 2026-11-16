use axum::body::Bytes;
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey, PutOutcome};
use tracing::{debug, trace};

use super::body_reader::ChunkBuffer;
use crate::{
    ServerError,
    chunk_store::chunk_object_key_for_computed_hash,
    local_backend::chunk_hash,
    metrics::{record_compression_saved, record_dedup_saves},
    object_store::ServerObjectStore,
};

pub(super) struct CompletedChunk {
    pub(super) offset: u64,
    pub(super) stored: StoredChunkOutcome,
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
    compressed: Bytes,
    hash_hex: String,
    chunk_length: u64,
}

fn chunk_object_key_and_integrity(chunk: &ChunkBuffer) -> Result<ChunkStorageRequest, ServerError> {
    let raw = chunk.as_slice();
    let compressed = lz4_flex::compress_prepend_size(raw);
    let raw_hash = chunk_hash(raw); // content identity: hash of raw bytes
    let compressed_hash = chunk_hash(&compressed); // storage integrity: hash of stored bytes
    let (hash_hex, object_key) = chunk_object_key_for_computed_hash(raw_hash)?;
    let chunk_length = u64::try_from(raw.len())?;
    let compressed_length = u64::try_from(compressed.len())?;
    if tracing::enabled!(tracing::Level::TRACE) {
        #[allow(clippy::float_arithmetic)]
        let ratio = format!("{:.2}", compressed.len() as f64 / raw.len().max(1) as f64);
        trace!(
            raw_len = raw.len(),
            compressed_len = compressed.len(),
            ratio = ratio,
            "LZ4 compressed chunk"
        );
    }
    record_compression_saved(chunk_length, compressed_length);
    let integrity = ObjectIntegrity::new(compressed_hash, compressed_length);
    let ChunkBuffer(raw_owner) = chunk;
    Ok(ChunkStorageRequest {
        key: object_key,
        integrity,
        // Compression creates a new allocation. Carry the raw owner's
        // admission into every encoded clone retained by storage/transport.
        compressed: super::buffer_admission::into_bytes(compressed, raw_owner.clone()),
        hash_hex,
        chunk_length,
    })
}

/// Records dedup savings when a chunk already exists in the store.
fn record_dedup_on_already_exists(outcome: PutOutcome, chunk_length: u64) {
    if matches!(outcome, PutOutcome::AlreadyExists) {
        debug!(chunk_length, "dedup hit — chunk already stored");
        record_dedup_saves(chunk_length);
    }
}

pub(super) async fn put_chunk_if_absent(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
) -> Result<StoredChunkOutcome, ServerError> {
    put_if_absent_with_verified_cache(object_store, chunk, None).await
}

pub(super) async fn put_if_absent_with_verified_cache(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
    cache: Option<&super::durable_chunks::SharedReadCache>,
) -> Result<StoredChunkOutcome, ServerError> {
    let request = chunk_object_key_and_integrity(&chunk)?;
    let ChunkBuffer(bytes) = chunk;
    let outcome = AsyncObjectStore::put_if_absent(
        object_store,
        &request.key,
        ObjectBody::from_bytes(request.compressed.clone()),
        &request.integrity,
    )
    .await?;
    record_dedup_on_already_exists(outcome, request.chunk_length);
    if outcome == PutOutcome::AlreadyExists
        && matches!(object_store, ServerObjectStore::S3(_))
        && let Some(cache) = cache
    {
        cache
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remember_verified_existing(&request.hash_hex, &bytes, request.integrity.length())?;
    }
    Ok(StoredChunkOutcome {
        hash_hex: request.hash_hex,
        chunk_length: request.chunk_length,
        compressed_length: request.integrity.length(),
        inserted: matches!(outcome, PutOutcome::Inserted),
    })
}

#[cfg(test)]
mod async_store_tests;

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use shardline_index::xet_hash_hex_string;
    use shardline_protocol::ByteRange;
    use shardline_storage::ObjectStore;

    use super::{ChunkBuffer, chunk_object_key_and_integrity, put_chunk_if_absent};
    use crate::{local_backend::chunk_hash, object_store::ServerObjectStore};

    fn payloads() -> Vec<Bytes> {
        let mut cases: Vec<_> = [
            "hello world",
            "hello worle",
            "test data for blake3 verification",
            "short",
            "alpha",
            "beta",
            "identical",
            "test data",
            "same data",
            "local test",
            "dedup test",
            "verify stored content matches",
            "chunk one",
            "chunk two",
            "pooled test",
            "pooled dedup",
            "dedup pool blackhole",
            "compressible dedup test data",
            "hello world this is test data for lz4 compression round trip",
        ]
        .into_iter()
        .map(|value| Bytes::from_static(value.as_bytes()))
        .collect();
        cases.extend([8, 16, 4096, 65536].map(|size| Bytes::from(vec![0; size])));
        cases.extend([1024, 8192, 65536].map(|size| Bytes::from(vec![0x42; size])));
        cases.extend([32, 4096].map(|size| Bytes::from(vec![0xAB; size])));
        cases.extend([0, 1, 63, 64, 65, 128, 1024, 65536].map(|size| {
            Bytes::from(
                (0..size)
                    .map(|index| (index % 256) as u8)
                    .collect::<Vec<_>>(),
            )
        }));
        cases.push(Bytes::from(
            (0..4096)
                .map(|index: i32| (index.wrapping_mul(37) ^ 0xFF) as u8)
                .collect::<Vec<_>>(),
        ));
        cases.push(Bytes::from(
            (0..128)
                .map(|index: u8| index.wrapping_mul(37).wrapping_add(13))
                .collect::<Vec<_>>(),
        ));
        cases
    }

    #[test]
    fn requests_keep_raw_identity_and_compressed_storage_integrity_separate() {
        for raw in payloads() {
            let request = chunk_object_key_and_integrity(&ChunkBuffer(raw.clone())).unwrap();
            let hash = xet_hash_hex_string(chunk_hash(&raw));
            let compressed = lz4_flex::compress_prepend_size(&raw);
            assert_eq!(request.hash_hex, hash);
            assert_eq!(
                request.key,
                crate::chunk_store::chunk_object_key(&hash).unwrap()
            );
            assert_eq!(request.chunk_length, raw.len() as u64);
            assert_eq!(request.compressed.as_ref(), compressed);
            assert_eq!(request.integrity.hash(), chunk_hash(&compressed));
            assert_eq!(request.integrity.length(), compressed.len() as u64);
            assert_eq!(
                lz4_flex::decompress_size_prepended(&request.compressed).unwrap(),
                raw
            );
            assert!(compressed.len() <= raw.len().saturating_add(8));
            if raw.len() >= 4096
                && raw
                    .first()
                    .is_some_and(|first| raw.iter().all(|byte| byte == first))
            {
                assert!(compressed.len() < raw.len());
            }
        }
    }

    #[tokio::test]
    async fn storage_preserves_bytes_and_lengths_on_first_and_duplicate_uploads() {
        for raw in payloads() {
            let tmp = shardline_test_support::TempStorage::new();
            for (store, blackhole) in [
                (ServerObjectStore::local(tmp.path()).unwrap(), false),
                (ServerObjectStore::blackhole(), true),
            ] {
                let compressed = lz4_flex::compress_prepend_size(&raw);
                let hash = xet_hash_hex_string(chunk_hash(&raw));
                let key = crate::chunk_store::chunk_object_key(&hash).unwrap();
                for inserted in [true, blackhole] {
                    let outcome = put_chunk_if_absent(&store, ChunkBuffer(raw.clone()))
                        .await
                        .unwrap();
                    assert_eq!(outcome.inserted, inserted);
                    assert_eq!(outcome.hash_hex, hash);
                    assert_eq!(outcome.chunk_length, raw.len() as u64);
                    assert_eq!(outcome.compressed_length, compressed.len() as u64);
                    if !blackhole {
                        assert!(store.contains(&key).unwrap());
                        let range =
                            ByteRange::new(0, outcome.compressed_length.checked_sub(1).unwrap())
                                .unwrap();
                        let stored = store.read_range(&key, range).unwrap();
                        assert_eq!(stored, compressed);
                        assert_eq!(lz4_flex::decompress_size_prepended(&stored).unwrap(), raw);
                    }
                }
            }
        }
    }

    #[test]
    fn compressed_transport_retains_raw_payload_admission() {
        let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
        let permit = pool.try_acquire().unwrap();
        let payload = super::super::buffer_admission::into_bytes(vec![0x5a; 4096], permit);
        let chunk = ChunkBuffer(payload);
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        let transport = request.compressed.clone();
        drop(request);
        drop(chunk);
        assert!(
            pool.try_acquire().is_none(),
            "compressed transport must retain admission after request cancellation"
        );
        assert_eq!(
            lz4_flex::decompress_size_prepended(&transport).unwrap(),
            vec![0x5a; 4096]
        );
        drop(transport);
        assert!(pool.try_acquire().is_some());
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
        let chunk1 = ChunkBuffer(Bytes::from_static(data));
        let outcome1 = put_chunk_if_absent(&store, chunk1).await.unwrap();
        assert!(outcome1.inserted);

        // Second insert with same content — should trigger dedup and record savings
        let before_dedup = shardline_metrics::metrics()
            .storage
            .dedup_saves_bytes_total
            .get();
        let chunk2 = ChunkBuffer(Bytes::from_static(data));
        let outcome2 = put_chunk_if_absent(&store, chunk2).await.unwrap();
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
}
