use std::{io::SeekFrom, pin::Pin};

use axum::body::Bytes;
use futures_util::{Stream, TryStreamExt, stream};
use shardline_protocol::ByteRange;
use shardline_storage::{LocalObjectStore, ObjectKey, ObjectStore, S3ObjectStore};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

use crate::{ServerError, object_store::ServerObjectStore};

pub(crate) const STREAM_READ_BUFFER_BYTES: u64 = 1024 * 1024;

pub(crate) type ServerByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ServerError>> + Send>>;

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
            return Err(ServerError::StoredObjectLengthMismatch);
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
            return Err(ServerError::StoredObjectLengthMismatch);
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
        return Err(ServerError::StoredObjectLengthMismatch);
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
            return Err(ServerError::StoredObjectLengthMismatch);
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
        return Err(ServerError::StoredObjectLengthMismatch);
    }
    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }

    let byte_stream = object_store.stream_range(&object_key, range).await?;

    Ok(Box::pin(byte_stream.map_err(ServerError::from)))
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use shardline_protocol::ByteRange;
    use shardline_storage::{LocalObjectStore, ObjectKey};
    use tokio::fs;

    use super::{local_object_byte_range_stream, local_object_byte_stream};
    use crate::ServerError;
    #[tokio::test]
    async fn local_object_byte_stream_reads_object_in_segments() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = LocalObjectStore::new(storage.path().to_path_buf());
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

    #[tokio::test]
    async fn local_object_byte_stream_rejects_index_length_mismatch() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = LocalObjectStore::new(storage.path().to_path_buf());
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
            Err(ServerError::StoredObjectLengthMismatch)
        ));
    }

    #[tokio::test]
    async fn local_object_byte_range_stream_reads_only_requested_range() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = LocalObjectStore::new(storage.path().to_path_buf());
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
}
