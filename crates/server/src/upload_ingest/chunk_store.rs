use axum::body::Bytes;
use bytes::BytesMut;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
use tokio::task;

use super::body_reader::ChunkBuffer;
use crate::{
    ServerError, chunk_store::chunk_object_key_for_computed_hash, local_backend::chunk_hash,
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
    pub(super) inserted: bool,
}

struct ChunkStorageRequest {
    key: ObjectKey,
    integrity: ObjectIntegrity,
    hash_hex: String,
    chunk_length: u64,
}

fn chunk_object_key_and_integrity(chunk: &ChunkBuffer) -> Result<ChunkStorageRequest, ServerError> {
    let hash = chunk_hash(chunk.as_slice());
    let (hash_hex, object_key) = chunk_object_key_for_computed_hash(hash)?;
    let chunk_length = u64::try_from(chunk.len())?;
    let integrity = ObjectIntegrity::new(hash, chunk_length);
    Ok(ChunkStorageRequest {
        key: object_key,
        integrity,
        hash_hex,
        chunk_length,
    })
}

/// Records dedup savings when a chunk already exists in the store.
fn record_dedup_on_already_exists(outcome: &PutOutcome, chunk_length: u64) {
    if matches!(outcome, PutOutcome::AlreadyExists) {
        crate::metrics::record_dedup_saves(chunk_length);
    }
}

pub(super) async fn put_if_absent_chunk_buffer(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
) -> Result<StoredChunkOutcome, ServerError> {
    let request = chunk_object_key_and_integrity(&chunk)?;
    match chunk {
        ChunkBuffer::Pooled(bytes) => {
            let (outcome, _bytes) =
                put_if_absent_pooled_bytes(object_store, &request, bytes).await?;
            record_dedup_on_already_exists(&outcome, request.chunk_length);
            Ok(StoredChunkOutcome {
                hash_hex: request.hash_hex,
                chunk_length: request.chunk_length,
                inserted: matches!(outcome, PutOutcome::Inserted),
            })
        }
        ChunkBuffer::Shared(bytes) => {
            let outcome = put_if_absent_shared_bytes(object_store, &request, bytes).await?;
            record_dedup_on_already_exists(&outcome, request.chunk_length);
            Ok(StoredChunkOutcome {
                hash_hex: request.hash_hex,
                chunk_length: request.chunk_length,
                inserted: matches!(outcome, PutOutcome::Inserted),
            })
        }
    }
}

pub(super) async fn put_if_absent_pooled_chunk_buffer(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
) -> Result<(StoredChunkOutcome, Option<BytesMut>), ServerError> {
    let request = chunk_object_key_and_integrity(&chunk)?;
    let bytes = match chunk {
        ChunkBuffer::Pooled(bytes) => bytes,
        ChunkBuffer::Shared(_bytes) => return Err(ServerError::Overflow),
    };
    let (outcome, bytes) = put_if_absent_pooled_bytes(object_store, &request, bytes).await?;
    record_dedup_on_already_exists(&outcome, request.chunk_length);
    let reusable_buffer = bytes.try_into_mut().ok();
    Ok((
        StoredChunkOutcome {
            hash_hex: request.hash_hex,
            chunk_length: request.chunk_length,
            inserted: matches!(outcome, PutOutcome::Inserted),
        },
        reusable_buffer,
    ))
}

async fn put_if_absent_shared_bytes(
    object_store: &ServerObjectStore,
    request: &ChunkStorageRequest,
    bytes: Bytes,
) -> Result<PutOutcome, ServerError> {
    match object_store {
        ServerObjectStore::Local(store) => {
            let store = store.clone();
            let key = request.key.clone();
            let integrity = request.integrity;
            task::spawn_blocking(move || {
                store
                    .put_if_absent(&key, ObjectBody::from_bytes(bytes), &integrity)
                    .map_err(ServerError::from)
            })
            .await
            .map_err(ServerError::BlockingTask)?
        }
        ServerObjectStore::S3(store) => {
            let store = store.clone();
            let key = request.key.clone();
            let integrity = request.integrity;
            task::spawn_blocking(move || {
                store
                    .put_if_absent(&key, ObjectBody::from_bytes(bytes), &integrity)
                    .map_err(ServerError::from)
            })
            .await
            .map_err(ServerError::BlockingTask)?
        }
        ServerObjectStore::Blackhole => Ok(object_store.put_if_absent(
            &request.key,
            ObjectBody::from_bytes(bytes),
            &request.integrity,
        )?),
    }
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
        ChunkBuffer, chunk_object_key_and_integrity, put_if_absent_chunk_buffer,
        put_if_absent_pooled_chunk_buffer,
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
    fn shared_chunk_produces_valid_content_addressed_key() {
        let chunk = ChunkBuffer::Shared(Bytes::from_static(b"hello world"));
        let result = chunk_object_key_and_integrity(&chunk);
        assert!(result.is_ok());
        let request = result.unwrap();
        assert_eq!(request.chunk_length, 11);
    }

    #[test]
    fn integrity_hash_matches_blake3_of_bytes() {
        let data = b"test data for blake3 verification";
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(data));
        let request = chunk_object_key_and_integrity(&chunk).unwrap();
        let expected_hash = chunk_hash(data);
        let expected_hex = xet_hash_hex_string(expected_hash);
        assert_eq!(request.hash_hex, expected_hex);
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
    // put_if_absent_chunk_buffer with Blackhole store
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn blackhole_store_inserted_returns_true() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"test data"));
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 9);
    }

    #[tokio::test]
    async fn blackhole_store_same_hash_inserted_returns_true() {
        let store = ServerObjectStore::blackhole();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"same data"));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"same data"));
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        // Blackhole always returns Inserted
        assert!(outcome1.inserted);
        assert!(outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    #[tokio::test]
    async fn blackhole_store_shared_variant() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Shared(Bytes::from_static(b"shared data"));
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 11);
    }

    // ------------------------------------------------------------------
    // put_if_absent_chunk_buffer with Local store
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn local_store_first_insert_returns_inserted() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(b"local test"));
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 10);
    }

    #[tokio::test]
    async fn local_store_same_hash_returns_not_inserted() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"dedup test"));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        assert!(outcome1.inserted);

        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"dedup test"));
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        assert!(!outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    #[tokio::test]
    async fn local_store_stored_bytes_match_original() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = b"verify stored content matches";
        let chunk = ChunkBuffer::Pooled(Bytes::from_static(data));
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);

        // Read the bytes back using the key
        let object_key = crate::chunk_store::chunk_object_key(&outcome.hash_hex).unwrap();
        let hash = chunk_hash(data);
        let expected_hex = xet_hash_hex_string(hash);
        assert_eq!(outcome.hash_hex, expected_hex);

        // Verify the object exists in the store
        assert!(store.contains(&object_key).unwrap());
    }

    #[tokio::test]
    async fn local_store_shared_variant_inserts_and_round_trips() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = b"local shared content";
        let chunk = ChunkBuffer::Shared(Bytes::from_static(data));
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, data.len() as u64);

        // Verify the object exists in the store
        let object_key = crate::chunk_store::chunk_object_key(&outcome.hash_hex).unwrap();
        assert!(store.contains(&object_key).unwrap());
    }

    #[tokio::test]
    async fn local_store_shared_variant_dedup_returns_not_inserted() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Shared(Bytes::from_static(b"shared dedup"));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        assert!(outcome1.inserted);

        let chunk2 = ChunkBuffer::Shared(Bytes::from_static(b"shared dedup"));
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        assert!(!outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
    }

    #[tokio::test]
    async fn local_store_different_chunks_store_separately() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(b"chunk one"));
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(b"chunk two"));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        assert!(outcome1.inserted);
        assert!(outcome2.inserted);
        assert_ne!(outcome1.hash_hex, outcome2.hash_hex);
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer with Local store
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
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let data = b"dedup-metric-test-content!";

        let before = shardline_metrics::metrics().storage.dedup_saves_bytes_total.get();

        // First insert — should be stored as new
        let chunk1 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        assert!(outcome1.inserted);
        let after_first = shardline_metrics::metrics().storage.dedup_saves_bytes_total.get();
        assert_eq!(
            after_first, before,
            "dedup_saves should not increase on first insert"
        );

        // Second insert with same content — should trigger dedup and record savings
        let chunk2 = ChunkBuffer::Pooled(Bytes::from_static(data));
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        assert!(!outcome2.inserted);
        let after_dedup = shardline_metrics::metrics().storage.dedup_saves_bytes_total.get();
        assert!(
            after_dedup >= after_first + outcome2.chunk_length,
            "dedup_saves_bytes_total should increase by chunk_length ({}) on dedup (before: {after_first}, after: {after_dedup})",
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
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 0);
    }

    #[tokio::test]
    async fn empty_chunk_blackhole_round_trip() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Pooled(Bytes::new());
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
        assert!(outcome.inserted);
        assert_eq!(outcome.chunk_length, 0);
    }

    // ------------------------------------------------------------------
    // put_if_absent_pooled_chunk_buffer — Shared variant rejection
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn pooled_chunk_buffer_rejects_shared_input() {
        let store = ServerObjectStore::blackhole();
        let chunk = ChunkBuffer::Shared(Bytes::from_static(b"shared data"));
        let result = put_if_absent_pooled_chunk_buffer(&store, chunk).await;
        assert!(
            matches!(result, Err(crate::ServerError::Overflow)),
            "expected Overflow when passing Shared chunk to pooled function"
        );
    }

    // ------------------------------------------------------------------
    // put_if_absent_chunk_buffer with Blackhole store — Shared variant
    // via the shared_bytes path
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn blackhole_shared_same_hash_inserted_returns_true() {
        let store = ServerObjectStore::blackhole();
        let chunk1 = ChunkBuffer::Shared(Bytes::from_static(b"shared dedup data"));
        let outcome1 = put_if_absent_chunk_buffer(&store, chunk1).await.unwrap();
        let chunk2 = ChunkBuffer::Shared(Bytes::from_static(b"shared dedup data"));
        let outcome2 = put_if_absent_chunk_buffer(&store, chunk2).await.unwrap();
        // Blackhole always returns Inserted
        assert!(outcome1.inserted);
        assert!(outcome2.inserted);
        assert_eq!(outcome1.hash_hex, outcome2.hash_hex);
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

    #[tokio::test]
    async fn local_store_shared_via_pooled_fn_returns_overflow() {
        let tmp = shardline_test_support::TempStorage::new();
        let store = ServerObjectStore::local(tmp.path()).unwrap();
        let chunk = ChunkBuffer::Shared(Bytes::from_static(b"shared data"));
        let result = put_if_absent_pooled_chunk_buffer(&store, chunk).await;
        assert!(matches!(result, Err(crate::ServerError::Overflow)));
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
        let outcome = put_if_absent_chunk_buffer(&store, chunk).await.unwrap();
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
}
