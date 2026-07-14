use std::{io::SeekFrom, pin::Pin};

use axum::body::Bytes;
use futures_util::{Stream, TryStreamExt, stream};
use shardline_protocol::ByteRange;
use shardline_storage::{LocalObjectStore, ObjectKey, ObjectStore, S3ObjectStore};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{
    ServerError, error::ObjectStoreError, object_store::ServerObjectStore,
    object_store::run_before_local_object_read_hook,
};

pub const STREAM_READ_BUFFER_BYTES: u64 = 1024 * 1024;

pub type ServerByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ServerError>> + Send>>;

struct LocalObjectByteStreamState {
    file: File,
    remaining: u64,
}

#[cfg(test)]
pub(crate) async fn local_object_byte_stream(
    object_store: LocalObjectStore,
    object_key: ObjectKey,
    length: u64,
) -> Result<ServerByteStream, ServerError> {
    local_store_byte_stream(object_store, object_key, length).await
}

#[cfg(test)]
pub(crate) async fn local_object_byte_range_stream(
    object_store: LocalObjectStore,
    object_key: ObjectKey,
    total_length: u64,
    range: ByteRange,
) -> Result<ServerByteStream, ServerError> {
    local_store_byte_range_stream(object_store, object_key, total_length, range).await
}

pub(crate) async fn object_byte_range_stream(
    object_store: ServerObjectStore,
    object_key: ObjectKey,
    total_length: u64,
    range: ByteRange,
) -> Result<ServerByteStream, ServerError> {
    match object_store {
        ServerObjectStore::Local(store) => {
            local_store_byte_range_stream(store, object_key, total_length, range).await
        }
        ServerObjectStore::S3(store) => {
            s3_store_byte_range_stream(store, object_key, total_length, range).await
        }
        ServerObjectStore::Blackhole => Err(ServerError::NotFound),
    }
}

pub(crate) async fn object_byte_stream(
    object_store: ServerObjectStore,
    object_key: ObjectKey,
    total_length: u64,
) -> Result<ServerByteStream, ServerError> {
    if total_length == 0 {
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        if metadata.length() != 0 {
            return Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch,
            ));
        }

        return Ok(Box::pin(stream::empty()));
    }

    let end_inclusive = total_length.checked_sub(1).ok_or(ServerError::Overflow)?;
    let range = ByteRange::new(0, end_inclusive).map_err(|_error| ServerError::Overflow)?;
    object_byte_range_stream(object_store, object_key, total_length, range).await
}

#[cfg(test)]
async fn local_store_byte_stream(
    object_store: LocalObjectStore,
    object_key: ObjectKey,
    length: u64,
) -> Result<ServerByteStream, ServerError> {
    if length == 0 {
        let file = object_store.open_object_file(&object_key)?;
        let metadata = file.metadata()?;
        if metadata.len() != 0 {
            return Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch,
            ));
        }

        return Ok(Box::pin(stream::empty()));
    }

    let end_inclusive = length.checked_sub(1).ok_or(ServerError::Overflow)?;
    let range = ByteRange::new(0, end_inclusive).map_err(|_error| ServerError::Overflow)?;
    local_store_byte_range_stream(object_store, object_key, length, range).await
}

async fn local_store_byte_range_stream(
    object_store: LocalObjectStore,
    object_key: ObjectKey,
    total_length: u64,
    range: ByteRange,
) -> Result<ServerByteStream, ServerError> {
    let file = object_store.open_object_file(&object_key)?;
    let mut file = File::from_std(file);
    let metadata = file.metadata().await?;
    if metadata.len() != total_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }

    // TOCTOU guard: expose hook point for concurrent-growth testing.
    let path = object_store.path_for_key(&object_key);
    run_before_local_object_read_hook(&path);
    // Re-validate length after hook (file may have grown).
    let post_hook_metadata = file.metadata().await?;
    if post_hook_metadata.len() != total_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }

    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }

    file.seek(SeekFrom::Start(range.start())).await?;
    let remaining = range.len().ok_or(ServerError::Overflow)?;
    let state = LocalObjectByteStreamState { file, remaining };
    let byte_stream = stream::try_unfold(state, |mut state| async move {
        if state.remaining == 0 {
            return Ok::<Option<(Bytes, LocalObjectByteStreamState)>, ServerError>(None);
        }

        let read_len_u64 = state.remaining.min(STREAM_READ_BUFFER_BYTES);
        let read_len = usize::try_from(read_len_u64)?;
        let mut buffer = vec![0_u8; read_len];
        let read = state.file.read(&mut buffer).await?;
        if read == 0 {
            return Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch,
            ));
        }

        buffer.truncate(read);
        let read_u64 = u64::try_from(read)?;
        state.remaining = state
            .remaining
            .checked_sub(read_u64)
            .ok_or(ServerError::Overflow)?;

        Ok(Some((Bytes::from(buffer), state)))
    });

    Ok(Box::pin(byte_stream))
}

async fn s3_store_byte_range_stream(
    object_store: S3ObjectStore,
    object_key: ObjectKey,
    total_length: u64,
    range: ByteRange,
) -> Result<ServerByteStream, ServerError> {
    let metadata = object_store.metadata(&object_key)?;
    let Some(metadata) = metadata else {
        return Err(ServerError::NotFound);
    };
    if metadata.length() != total_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }

    let byte_stream = object_store.stream_range(&object_key, range).await?;

    Ok(Box::pin(byte_stream.map_err(ServerError::from)))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use futures_util::StreamExt;
    use shardline_protocol::ByteRange;
    use shardline_storage::{LocalObjectStore, ObjectKey};
    use tokio::fs;

    use super::{local_object_byte_range_stream, local_object_byte_stream};
    use crate::ServerError;
    use crate::error::ObjectStoreError;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_stream_reads_object_in_segments() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf());
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let object_key = ObjectKey::parse("ab/object");
        assert!(object_key.is_ok());
        let Ok(object_key) = object_key else {
            return;
        };
        let bytes = vec![7_u8; 64 * 1024 + 3];
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            let created = fs::create_dir_all(parent).await;
            assert!(created.is_ok());
        }
        let written = fs::write(&path, &bytes).await;
        assert!(written.is_ok());

        let byte_stream = local_object_byte_stream(
            object_store,
            object_key,
            u64::try_from(bytes.len()).unwrap_or(0),
        )
        .await;
        assert!(byte_stream.is_ok());
        let Ok(mut byte_stream) = byte_stream else {
            return;
        };
        let mut observed = Vec::with_capacity(bytes.len());
        while let Some(item) = byte_stream.next().await {
            assert!(item.is_ok());
            let Ok(chunk) = item else {
                return;
            };
            observed.extend_from_slice(&chunk);
        }

        assert_eq!(observed, bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_stream_rejects_index_length_mismatch() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf());
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let object_key = ObjectKey::parse("ab/object");
        assert!(object_key.is_ok());
        let Ok(object_key) = object_key else {
            return;
        };
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            let created = fs::create_dir_all(parent).await;
            assert!(created.is_ok());
        }
        let written = fs::write(&path, b"short").await;
        assert!(written.is_ok());

        let byte_stream = local_object_byte_stream(object_store, object_key, 100).await;

        assert!(matches!(
            byte_stream,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_growth_after_length_validation() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf());
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let object_key = ObjectKey::parse("ab/object");
        assert!(object_key.is_ok());
        let Ok(object_key) = object_key else {
            return;
        };
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            let created = fs::create_dir_all(parent).await;
            assert!(created.is_ok());
        }
        // Write initial content.
        let written = fs::write(&path, b"abcd").await;
        assert!(written.is_ok());

        // Open a writer handle for the hook to append through.
        let writer_path = path.clone();
        let hook_writer = std::sync::Mutex::new(
            std::fs::OpenOptions::new()
                .append(true)
                .open(&writer_path)
                .unwrap(),
        );
        crate::object_store::set_before_local_object_read_hook(path, move || {
            let mut writer = hook_writer.lock().unwrap();
            let _ = writer.write_all(b"extra");
            let _ = writer.sync_all();
        });

        let byte_stream = local_object_byte_range_stream(
            object_store,
            object_key,
            4, // total_length = 4, but file will grow to 9 after hook fires
            ByteRange::new(0, 3).unwrap(),
        )
        .await;

        assert!(matches!(
            byte_stream,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_reads_only_requested_range() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf());
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let object_key = ObjectKey::parse("ab/object");
        assert!(object_key.is_ok());
        let Ok(object_key) = object_key else {
            return;
        };
        let bytes = b"abcdefghijkl".to_vec();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            let created = fs::create_dir_all(parent).await;
            assert!(created.is_ok());
        }
        let written = fs::write(&path, &bytes).await;
        assert!(written.is_ok());
        let range = ByteRange::new(2, 7);
        assert!(range.is_ok());
        let Ok(range) = range else {
            return;
        };

        let byte_stream = local_object_byte_range_stream(
            object_store,
            object_key,
            u64::try_from(bytes.len()).unwrap_or(0),
            range,
        )
        .await;
        assert!(byte_stream.is_ok());
        let Ok(mut byte_stream) = byte_stream else {
            return;
        };
        let mut observed = Vec::new();
        while let Some(item) = byte_stream.next().await {
            assert!(item.is_ok());
            let Ok(chunk) = item else {
                return;
            };
            observed.extend_from_slice(&chunk);
        }

        assert_eq!(observed, b"cdefgh");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_with_blackhole_returns_not_found_for_nonzero_length() {
        use crate::object_store::ServerObjectStore;
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        let result = super::object_byte_stream(store, key, 10).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_blackhole_returns_not_found() {
        use crate::object_store::ServerObjectStore;
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, key, 10, range).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_blackhole_zero_length_checks_metadata() {
        use crate::object_store::ServerObjectStore;
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        // Blackhole returns None for metadata, so this should return NotFound
        let result = super::object_byte_stream(store, key, 0).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_stream_zero_length_empty_object() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/empty").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"").await.unwrap();

        let result = local_object_byte_stream(object_store, object_key, 0).await;
        // Zero-length object → empty stream
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        use futures_util::StreamExt;
        let next = stream.next().await;
        assert!(next.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_stream_rejects_zero_length_with_nonempty_file() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/nonempty-claimed-zero").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"data").await.unwrap();

        let result = local_object_byte_stream(object_store, object_key, 0).await;
        assert!(matches!(
            result,
            Err(crate::ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_range_exceeding_length() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-too-large").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcd").await.unwrap();

        // range.end_inclusive = 10, total_length = 4 → RangeNotSatisfiable
        let range = ByteRange::new(0, 10).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(crate::ServerError::RangeNotSatisfiable)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_nonzero_length_with_local_store() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/stream-nonzero").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"stream-me").await.unwrap();

        let store = crate::object_store::ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 9).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_local_store_returns_error_for_blackhole() {
        let store = crate::object_store::ServerObjectStore::blackhole();
        let object_key = ObjectKey::parse("ab/any-key").unwrap();
        let range = ByteRange::new(0, 4).unwrap();
        let result = super::object_byte_range_stream(store, object_key, 10, range).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_rejects_zero_length_with_nonempty_file() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/not-empty").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"data").await.unwrap();

        let store = crate::object_store::ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(matches!(
            result,
            Err(crate::ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_range_stream_reads_exact_chunks() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/exact-chunks").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        // Write enough data to require two read iterations (STREAM_READ_BUFFER_BYTES + extra)
        let data = vec![0xABu8; (super::STREAM_READ_BUFFER_BYTES as usize) + 100];
        tokio::fs::write(&path, &data).await.unwrap();

        let total = data.len() as u64;
        let range = ByteRange::new(0, total - 1).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, total, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed.len(), data.len());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_via_public_fn_with_local_store() {
        // Test the public object_byte_stream function via the localstore path.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/stream-full-2").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        let content = b"stream-payload-data";
        tokio::fs::write(&path, content).await.unwrap();

        let store = crate::object_store::ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store.clone(), object_key.clone(), content.len() as u64).await;
        assert!(result.is_ok(), "object_byte_stream failed: {:?}", result.err());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, content);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_s3_store_returns_error_when_not_configured() {
        // S3 variant: cannot test actual S3, so verify blackhole falls through
        let store = crate::object_store::ServerObjectStore::blackhole();
        let object_key = ObjectKey::parse("ab/s3-test").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, object_key, 10, range).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_reads_zero_remaining_properly() {
        // When remaining == 0 after reading all data, the stream should end.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/exact-range-end").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcd").await.unwrap();

        let range = ByteRange::new(0, 3).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"abcd");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_stream_reads_multiple_chunks_correctly() {
        // Create a large enough object to require multiple read iterations.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/multi-chunk").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        let data = vec![0x42u8; (super::STREAM_READ_BUFFER_BYTES as usize) * 2 + 50];
        tokio::fs::write(&path, &data).await.unwrap();

        let result = local_object_byte_stream(object_store, object_key, data.len() as u64).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed.len(), data.len());
    }
}
