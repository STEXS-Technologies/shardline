use axum::body::Bytes;
use bytes::BytesMut;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
use tokio::task;

use super::ChunkBuffer;
use crate::{
    ServerError,
    chunk_store::chunk_object_key_for_computed_hash,
    local_backend::chunk_hash,
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

pub(super) async fn put_if_absent_chunk_buffer(
    object_store: &ServerObjectStore,
    chunk: ChunkBuffer,
) -> Result<StoredChunkOutcome, ServerError> {
    let request = chunk_object_key_and_integrity(&chunk)?;
    match chunk {
        ChunkBuffer::Pooled(bytes) => {
            let (outcome, _bytes) =
                put_if_absent_pooled_bytes(object_store, &request, bytes).await?;
            Ok(StoredChunkOutcome {
                hash_hex: request.hash_hex,
                chunk_length: request.chunk_length,
                inserted: matches!(outcome, PutOutcome::Inserted),
            })
        }
        ChunkBuffer::Shared(bytes) => {
            let outcome = put_if_absent_shared_bytes(object_store, &request, bytes).await?;
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
