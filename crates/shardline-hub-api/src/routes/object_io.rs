//! Bounded and streaming object access for Hub handlers.
//!
//! The storage adapters expose range reads, not whole-object reads.  Keep that
//! boundary visible here: previews consume a bounded prefix and downloads emit
//! fixed-size ranges as an HTTP body stream.
use std::{io, sync::Arc};

use axum::body::Body;
use bytes::Bytes;
use futures_util::stream;
use shardline_protocol::ByteRange;
use shardline_server_core::{ServerObjectStore, ServerObjectStoreError};
use shardline_storage::{ObjectKey, ObjectStore};

/// Range size used for Hub reads.  This bounds one storage response and one
/// resident body chunk; it is deliberately independent of file size.
pub(crate) const OBJECT_STREAM_CHUNK_BYTES: u64 = 64 * 1024;

/// Reads at most `limit` bytes from the beginning of an object.
///
/// This is intended for metadata/front-matter and bounded dataset previews. It
/// never asks the backend for a range larger than the limit and never allocates
/// based on the complete object length.
pub(crate) fn read_object_prefix(
    store: &ServerObjectStore,
    key: &ObjectKey,
    object_length: u64,
    limit: usize,
) -> Result<Vec<u8>, ServerObjectStoreError> {
    let target = object_length.min(limit as u64);
    let mut output = Vec::with_capacity(target as usize);
    let mut offset = 0_u64;
    while offset < target {
        let end_exclusive = offset
            .checked_add(OBJECT_STREAM_CHUNK_BYTES)
            .ok_or(ServerObjectStoreError::Overflow)?
            .min(target);
        let end = end_exclusive
            .checked_sub(1)
            .ok_or(ServerObjectStoreError::Overflow)?;
        let range =
            ByteRange::new(offset, end).map_err(|_error| ServerObjectStoreError::Overflow)?;
        let chunk = store.read_range(key, range)?;
        if chunk.is_empty() {
            return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
        }
        let expected_u64 = end
            .checked_sub(offset)
            .and_then(|value| value.checked_add(1))
            .ok_or(ServerObjectStoreError::Overflow)?;
        let expected =
            usize::try_from(expected_u64).map_err(|_error| ServerObjectStoreError::Overflow)?;
        if chunk.len() != expected {
            return Err(ServerObjectStoreError::StoredObjectLengthMismatch);
        }
        output.extend_from_slice(&chunk);
        offset = end_exclusive;
    }
    Ok(output)
}

/// Builds an HTTP body that reads an object through fixed-size ranged reads.
///
/// Storage access is moved to Tokio's blocking pool because the current local
/// and S3 adapters expose synchronous range reads through `ServerObjectStore`.
pub(crate) fn stream_object(store: &ServerObjectStore, key: ObjectKey, object_length: u64) -> Body {
    let initial = (Arc::new(store.clone()), key, 0_u64);
    let stream = stream::try_unfold(
        initial,
        move |(store_state, key_state, offset)| async move {
            if offset >= object_length {
                return Ok(None);
            }
            let end_exclusive = offset
                .checked_add(OBJECT_STREAM_CHUNK_BYTES)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
                })?
                .min(object_length);
            let end = end_exclusive.checked_sub(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
            })?;
            let range = ByteRange::new(offset, end).map_err(|_error| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid object range")
            })?;
            let expected_u64 = end
                .checked_sub(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
                })?;
            let expected = usize::try_from(expected_u64).map_err(|_error| {
                io::Error::new(io::ErrorKind::InvalidInput, "object range overflow")
            })?;
            let store_for_read = Arc::clone(&store_state);
            let key_for_read = key_state.clone();
            let chunk = tokio::task::spawn_blocking(move || {
                store_for_read
                    .read_range(&key_for_read, range)
                    .map_err(|error| io::Error::other(error.to_string()))
            })
            .await
            .map_err(|error| io::Error::other(error.to_string()))??;
            if chunk.len() != expected {
                return Err(io::Error::other(
                    "object range returned an unexpected length",
                ));
            }
            Ok(Some((
                Bytes::from(chunk),
                (store_state, key_state, end_exclusive),
            )))
        },
    );
    Body::from_stream(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use shardline_protocol::ShardlineHash;
    use shardline_storage::{ObjectBody, ObjectIntegrity};

    fn local_store() -> (tempfile::TempDir, ServerObjectStore) {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = ServerObjectStore::local(temp.path().join("objects")).expect("local store");
        (temp, store)
    }

    fn put(store: &ServerObjectStore, key: &ObjectKey, bytes: &[u8]) {
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
            bytes.len() as u64,
        );
        store
            .put_overwrite(key, ObjectBody::from_slice(bytes), &integrity)
            .expect("put object");
    }

    #[test]
    fn prefix_read_is_bounded_to_requested_bytes() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/large.bin").expect("key");
        let bytes = vec![0xA5; (OBJECT_STREAM_CHUNK_BYTES as usize * 3) + 17];
        put(&store, &key, &bytes);

        let prefix =
            read_object_prefix(&store, &key, bytes.len() as u64, 100_003).expect("prefix read");
        assert_eq!(prefix.len(), 100_003);
        assert_eq!(prefix, bytes[..100_003]);
    }

    #[tokio::test]
    async fn stream_surfaces_mid_object_storage_failure_and_recovers() {
        let (_temp, store) = local_store();
        let key = ObjectKey::parse("hub/recover.bin").expect("key");
        let bytes = vec![0x3C; (OBJECT_STREAM_CHUNK_BYTES as usize * 2) + 11];
        put(&store, &key, &bytes);
        let path = store.local_path_for_key(&key).expect("local path");

        let mut body = stream_object(&store, key.clone(), bytes.len() as u64);
        let first = body
            .frame()
            .await
            .expect("first frame")
            .expect("first body frame");
        assert_eq!(
            first.into_data().expect("first data").len(),
            OBJECT_STREAM_CHUNK_BYTES as usize
        );

        std::fs::remove_file(&path).expect("inject object loss");
        let failure = body
            .frame()
            .await
            .expect("stream frame")
            .expect_err("expected read failure");
        assert!(!failure.to_string().is_empty());

        // Recovery is a fresh stream after the durable object is restored.
        put(&store, &key, &bytes);
        let mut recovered = stream_object(&store, key, bytes.len() as u64);
        let mut recovered_bytes = Vec::new();
        while let Some(frame) = recovered.frame().await {
            recovered_bytes.extend_from_slice(
                &frame
                    .expect("recovered frame")
                    .into_data()
                    .expect("recovered data"),
            );
        }
        assert_eq!(recovered_bytes, bytes);
    }

    #[tokio::test]
    async fn blackhole_stream_fails_without_buffering_or_panicking() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("hub/missing.bin").expect("key");
        let mut body = stream_object(&store, key, 1);
        let frame = body.frame().await.expect("stream frame");
        assert!(frame.is_err(), "blackhole read must be surfaced to caller");
    }
}
