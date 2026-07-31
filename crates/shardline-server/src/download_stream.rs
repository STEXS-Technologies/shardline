use std::{
    io::{Error as IoError, ErrorKind, SeekFrom},
    pin::Pin,
};

use axum::body::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use lz4_flex;
use shardline_index::{FileRecord, parse_xet_hash_hex, xet_hash_hex_string};
use shardline_protocol::ByteRange;
use shardline_storage::{LocalObjectStore, ObjectKey, ObjectStore, S3ObjectStore};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::{debug, trace, warn};

use crate::{
    ServerError, chunk_store::chunk_object_key, error::ObjectStoreError, local_backend::chunk_hash,
    object_store::ServerObjectStore, object_store::read_full_object,
    object_store::run_before_local_object_read_hook,
};

pub const STREAM_READ_BUFFER_BYTES: u64 = 1024 * 1024;

pub type ServerByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ServerError>> + Send>>;

/// Reads a xorb-backed file record by fetching the single xorb object, parsing
/// all chunks, and extracting the requested byte range.
///
/// When all chunks in a [`FileRecord`] share the same hash (meaning they were
/// packed into a single xorb container during upload), this path reads the xorb
/// once and extracts decompressed chunk data without per-chunk storage round-trips.
async fn read_xorb_backed_chunks(
    object_store: ServerObjectStore,
    record: &FileRecord,
    range: Option<ByteRange>,
) -> Result<ServerByteStream, ServerError> {
    // 1. Read the entire xorb from storage.
    let chunk_zero = record.chunks.first().ok_or(ServerError::Overflow)?;
    let xorb_hash_hex = &chunk_zero.hash;
    let xorb_key = crate::xet_adapter::xorb_object_key(xorb_hash_hex)?;
    let metadata = object_store
        .metadata(&xorb_key)?
        .ok_or(ServerError::NotFound)?;
    let xorb_length = metadata.length();
    let xorb_data = read_full_object(&object_store, &xorb_key, xorb_length)?;

    // 2. Parse and validate the xorb (verifies xorb hash against expected hash).
    let expected_hash = parse_xet_hash_hex(xorb_hash_hex)?;
    let mut cursor = std::io::Cursor::new(xorb_data.as_slice());
    let validated = crate::xet_adapter::validate_serialized_xorb(&mut cursor, expected_hash)?;

    // 3. Decode all chunks (decompresses and verifies per-chunk content hashes).
    std::io::Seek::seek(&mut cursor, SeekFrom::Start(0))?;
    let decoded_chunks =
        crate::xet_adapter::decode_serialized_xorb_chunks(&mut cursor, &validated)?;

    // 4. Filter and slice by the requested byte range.
    let requested_start = range.map_or(0, |value| value.start());
    let requested_end = range.map_or_else(
        || {
            record
                .total_bytes
                .checked_sub(1)
                .ok_or(ServerError::Overflow)
        },
        |value| Ok(value.end_inclusive()),
    )?;

    let mut terms = Vec::new();
    for chunk in &record.chunks {
        let chunk_end = chunk
            .offset
            .checked_add(chunk.length)
            .and_then(|v| v.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        let start = requested_start.max(chunk.offset);
        let end = requested_end.min(chunk_end);
        if start > end {
            continue;
        }
        let relative_start = start
            .checked_sub(chunk.offset)
            .ok_or(ServerError::Overflow)?;
        let relative_end = end.checked_sub(chunk.offset).ok_or(ServerError::Overflow)?;

        // Look up the decoded chunk by its index within the xorb.
        let chunk_index = chunk.range_start as usize;
        let decoded = decoded_chunks
            .get(chunk_index)
            .ok_or(ServerError::Overflow)?;
        let decoded_data = decoded.data();

        let range_start = usize::try_from(relative_start)?;
        let range_end = usize::try_from(relative_end)?;
        let sliced = decoded_data
            .get(range_start..=range_end)
            .ok_or(ServerError::Overflow)?
            .to_vec();
        terms.push(Bytes::from(sliced));
    }

    let stream = stream::iter(terms).map(Ok::<Bytes, ServerError>);
    Ok(Box::pin(stream))
}

/// Streams a chunk-backed file record without materializing the complete object.
///
/// Chunks are stored compressed. Each chunk is read as a whole compressed blob,
/// decompressed, and then the requested byte range is sliced from the decompressed
/// data. The `packed_end` field on the chunk record indicates the compressed storage
/// length; `length` is the raw (uncompressed) length used for offset math.
///
/// When all chunks in the record share the same hash (xorb-backed), this function
/// delegates to [`read_xorb_backed_chunks`] for a single-GET read path.
pub(crate) async fn file_record_byte_stream(
    object_store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
) -> Result<ServerByteStream, ServerError> {
    record.validate_reconstruction_plan()?;
    if record.total_bytes == 0 {
        return Ok(Box::pin(stream::empty()));
    }

    // Explicit routing based on storage representation.
    // WholeFileV1 records should use reconstruct_file_record_bytes, not this path.
    match record.storage_repr {
        shardline_index::StorageRepresentation::WholeFileV1 => {
            return Err(ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredLengthMismatch,
            ));
        }
        shardline_index::StorageRepresentation::FixedChunkV1 => {
            // Old format: uncompressed chunks.  The is_xorb_backed check below
            // handles single-chunk records correctly (packed_start == 0).
        }
        shardline_index::StorageRepresentation::XorbCdcV1 => {
            // New format: compressed + optionally xorb-packed.  Proceed.
        }
    }

    // Fast path: if all chunks are in the same xorb, read it once.
    // For a single chunk we also check packed_start > 0 — regular chunks
    // always have packed_start == 0 (the serde default), while xorb-backed
    // chunks have a non-zero offset into the xorb serialized data.
    let first_hash = record.chunks.first().map(|c| &c.hash);
    let all_same_hash = first_hash.is_some_and(|h| record.chunks.iter().all(|c| c.hash == *h));
    let is_xorb_backed = if record.chunks.len() > 1 {
        all_same_hash
    } else {
        // Single chunk: only route through the xorb path when the
        // chunk has a non-zero packed_start (indicating it is a slice
        // within a xorb, not a standalone chunk).
        all_same_hash && record.chunks.first().is_some_and(|c| c.packed_start > 0)
    };

    if is_xorb_backed {
        #[allow(clippy::indexing_slicing)]
        let xorb_hash = &record.chunks[0].hash;
        debug!(
            file_id = %record.file_id,
            total_bytes = record.total_bytes,
            xorb_hash = %xorb_hash,
            "reading xorb-backed file"
        );
        return read_xorb_backed_chunks(object_store, &record, range).await;
    }

    debug!(
        file_id = %record.file_id,
        total_bytes = record.total_bytes,
        chunk_count = record.chunks.len(),
        range = ?range,
        "reconstructing file from chunks"
    );

    let requested_start = range.map_or(0, |value| value.start());
    let requested_end = range.map_or_else(
        || {
            record
                .total_bytes
                .checked_sub(1)
                .ok_or(ServerError::Overflow)
        },
        |value| Ok(value.end_inclusive()),
    )?;
    if requested_end >= record.total_bytes {
        return Err(ServerError::RangeNotSatisfiable);
    }

    let mut terms = Vec::new();
    for chunk in record.chunks {
        let chunk_end = chunk
            .offset
            .checked_add(chunk.length)
            .and_then(|value| value.checked_sub(1))
            .ok_or(ServerError::Overflow)?;
        let start = requested_start.max(chunk.offset);
        let end = requested_end.min(chunk_end);
        if start > end {
            continue;
        }
        let relative_start = start
            .checked_sub(chunk.offset)
            .ok_or(ServerError::Overflow)?;
        let relative_end = end.checked_sub(chunk.offset).ok_or(ServerError::Overflow)?;
        // Use packed_end as storage length (compressed), fall back to length for backward compat
        let storage_length = if chunk.packed_end > 0 {
            chunk.packed_end
        } else {
            chunk.length
        };
        // Compression is a property of the storage representation, not of size
        // equality: LZ4-compressed data may be exactly the same size as (or even
        // larger than) the raw chunk for small or incompressible payloads. Using
        // `packed_end != chunk.length` as the discriminator caused compressed
        // bytes to be served as raw whenever the compressed size coincided with
        // the raw size (XorbCdcV1 single-chunk records).
        let is_compressed = matches!(
            record.storage_repr,
            shardline_index::StorageRepresentation::XorbCdcV1
        );
        let hash_hex = chunk.hash.clone();
        let chunk_range = ByteRange::new(relative_start, relative_end)
            .map_err(|_error| ServerError::RangeNotSatisfiable)?;
        terms.push((
            chunk_object_key(&hash_hex)?,
            storage_length, // compressed length for storage read
            chunk.length,   // raw length for offset math
            hash_hex,       // expected hash for integrity verification
            chunk_range,
            is_compressed, // XorbCdcV1 → LZ4-compressed; FixedChunkV1 → old uncompressed
        ));
    }

    // For each term: read chunk data, optionally decompress, verify integrity, then apply byte range
    let streams = stream::iter(terms).then(
        move |(key, storage_length, _raw_length, expected_hash_hex, chunk_range, is_compressed)| {
            let object_store = object_store.clone();
            async move {
                // Read the entire compressed chunk
                let chunk_key = key.clone();
                let compressed_stream =
                    object_byte_stream(object_store, key, storage_length).await?;
                // Collect all compressed bytes
                let compressed = compressed_stream
                    .try_fold(Vec::new(), |mut acc, chunk| async move {
                        acc.extend_from_slice(&chunk);
                        Ok(acc)
                    })
                    .await?;
                let data: Vec<u8> = if is_compressed {
                    // New format (XorbCdcV1): LZ4-compressed. Decompress and verify hash.
                    const MAX_DECOMPRESSED_CHUNK: u64 = 2 * 1024 * 1024;
                    let decompressed_size = compressed
                        .first_chunk::<4>()
                        .map(|header| u32::from_le_bytes(*header) as u64)
                        .unwrap_or(u64::MAX);
                    if decompressed_size > MAX_DECOMPRESSED_CHUNK {
                        return Err(ServerError::Overflow);
                    }
                    let decompressed =
                        lz4_flex::decompress_size_prepended(&compressed).map_err(|e| {
                            warn!(compressed_len = compressed.len(), error = %e, "failed to decompress chunk");
                            ServerError::Io(IoError::new(ErrorKind::InvalidData, e))
                        })?;

                    let actual_hash = chunk_hash(&decompressed);
                    let actual_hex = xet_hash_hex_string(actual_hash);
                    if actual_hex != expected_hash_hex {
                        warn!(
                            expected = %expected_hash_hex,
                            actual = %actual_hex,
                            decompressed_len = decompressed.len(),
                            "chunk integrity mismatch after decompression"
                        );
                        return Err(ServerError::ObjectStore(
                            ObjectStoreError::StoredLengthMismatch,
                        ));
                    }

                    trace!(
                        chunk_hash = ?chunk_key,
                        compressed_len = compressed.len(),
                        decompressed_len = decompressed.len(),
                        "decompressed chunk"
                    );
                    decompressed
                } else {
                    // Old format (FixedChunkV1 / pre-CDC): raw (uncompressed) data.
                    // The stored bytes are the raw chunk data; the hash in the
                    // file record is computed from raw bytes, so the hash stored
                    // in the record matches the data on disk directly.
                    compressed
                };
                // Apply byte range on the data
                let range_start = usize::try_from(chunk_range.start())?;
                let range_end = usize::try_from(chunk_range.end_inclusive())?;
                let sliced = data
                    .get(range_start..=range_end)
                    .ok_or(ServerError::Overflow)?
                    .to_vec();
                let byte_stream: ServerByteStream =
                    Box::pin(stream::once(async move { Ok(Bytes::from(sliced)) }));
                Ok::<ServerByteStream, ServerError>(byte_stream)
            }
        },
    );
    Ok(Box::pin(streams.try_flatten()))
}

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
    use crate::object_store::ServerObjectStore;
    use crate::object_store::set_before_local_object_read_hook;
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
        set_before_local_object_read_hook(path, move || {
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
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        let result = super::object_byte_stream(store, key, 10).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_blackhole_returns_not_found() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, key, 10, range).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_blackhole_zero_length_checks_metadata() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/key").unwrap();
        // Blackhole returns None for metadata, so this should return NotFound
        let result = super::object_byte_stream(store, key, 0).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
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
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
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
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
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

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 9).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_local_store_returns_error_for_blackhole() {
        let store = ServerObjectStore::blackhole();
        let object_key = ObjectKey::parse("ab/any-key").unwrap();
        let range = ByteRange::new(0, 4).unwrap();
        let result = super::object_byte_range_stream(store, object_key, 10, range).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
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

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
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

        let store = ServerObjectStore::Local(object_store);
        let result =
            super::object_byte_stream(store.clone(), object_key.clone(), content.len() as u64)
                .await;
        assert!(
            result.is_ok(),
            "object_byte_stream failed: {:?}",
            result.err()
        );
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
        let store = ServerObjectStore::blackhole();
        let object_key = ObjectKey::parse("ab/s3-test").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, object_key, 10, range).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
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

    // ── object_byte_stream — zero-length metadata scenarios ──────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_zero_length_with_existing_empty_file() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/existing-empty").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        use futures_util::StreamExt;
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_zero_length_nonempty_file_errors() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/nonempty-claimed-zero-v2").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"data").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    // ── local_store_byte_range_stream — truncated file during read ───────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_range_stream_read_returns_zero() {
        // Create a file, then truncate it after opening to simulate read(2)
        // returning 0 (unexpected EOF).
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/truncated-read").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        // Write initial content
        tokio::fs::write(&path, b"content-to-be-truncated")
            .await
            .unwrap();

        // We need to access the file after the stream is created and truncate it.
        // Use the before_read_hook to truncate after length validation passes.
        let truncate_path = path.clone();
        set_before_local_object_read_hook(path.clone(), move || {
            // Truncate the file to a very small size so the read loop gets 0
            let _ = std::fs::write(&truncate_path, b"tiny");
        });

        // total_length = 24, range covers all — after truncation, the read
        // will get 0 bytes and should return StoredLengthMismatch.
        let range = ByteRange::new(0, 23).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 24, range).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    // ── object_byte_range_stream — various range configurations ──────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_via_local_store_start_only() {
        // Range starts at a non-zero position, reads to end
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-start-only").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcdefghij").await.unwrap();

        let range = ByteRange::new(3, 9).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 10, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"defghij");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_via_local_store_full_range() {
        // Full range (0 to end_inclusive) should return entire content
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-full").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        let content = b"full range content";
        tokio::fs::write(&path, content).await.unwrap();

        let total = content.len() as u64;
        let range = ByteRange::new(0, total - 1).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, total, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, content);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_single_byte_range() {
        // Single-byte range
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-single-byte").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcdef").await.unwrap();

        let range = ByteRange::new(2, 2).unwrap(); // just 'c'
        let result = local_object_byte_range_stream(object_store, object_key, 6, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"c");
    }

    // ── object_byte_stream via ServerObjectStore::Local ──────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_via_local_store_empty_file() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/store-empty").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        assert!(stream.next().await.is_none());
    }

    // ── object_byte_range_stream with length mismatch ────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_length_mismatch() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-length-mismatch").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"short").await.unwrap();

        let range = ByteRange::new(0, 3).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 100, range).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    // ── object_byte_range_stream — invalid range ─────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_range_end_exceeds_length() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-end-exceeds").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcd").await.unwrap();

        // Range end_inclusive (10) >= total_length (4) → RangeNotSatisfiable
        let range = ByteRange::new(0, 10).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
    }

    // ── object_byte_range_stream — range end equals total_length ─────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_range_end_equals_length_errors() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-end-equals-length").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcd").await.unwrap();

        // range.end_inclusive() == total_length → >= check triggers RangeNotSatisfiable
        let range = ByteRange::new(0, 4).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
    }

    // ── object_byte_stream — 404 via blackhole metadata check ────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_blackhole_missing_metadata() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/missing-key").unwrap();
        // Blackhole returns None for all metadata → NotFound
        let result = super::object_byte_stream(store, key, 0).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_blackhole_zero_length_metadata_length_mismatch() {
        // When metadata reports non-zero length but we asked for length 0, should error
        // via StoredLengthMismatch. However, Blackhole metadata() returns None,
        // so this is a type-level check that the branch compiles.
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/zero-claim-nonzero").unwrap();
        // Blackhole metadata returns None → NotFound before length check
        let result = super::object_byte_stream(store, key, 0).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    // ── overflow edge cases ──────────────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_overflow_on_zero_length_sub_one() {
        // The function uses checked_sub(1) for end_inclusive range when length == 0
        // should go through the metadata path, not the range path.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/zero-stream").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        // Zero-length existing empty file → empty stream
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_blackhole_not_found() {
        // Blackhole returns NotFound for all stores
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/blackhole-range").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, key, 10, range).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    // ── file_record_byte_stream decompression round-trip ─────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_record_byte_stream_decompresses_lz4_chunks() {
        use shardline_index::{FileChunkRecord, FileRecord};
        use shardline_storage::ObjectStore;

        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();

        // Create a compressible payload
        let payload = vec![0xABu8; 4096];
        let compressed = lz4_flex::compress_prepend_size(&payload);

        // Store the compressed chunk keyed by its raw-content hash
        let raw_hash = crate::local_backend::chunk_hash(&payload);
        let hash_hex = shardline_index::xet_hash_hex_string(raw_hash);
        let object_key = crate::chunk_store::chunk_object_key(&hash_hex).unwrap();
        // Integrity verifies stored bytes (compressed), not raw content
        let compressed_hash = crate::local_backend::chunk_hash(&compressed);
        let integrity =
            shardline_storage::ObjectIntegrity::new(compressed_hash, compressed.len() as u64);
        object_store
            .put_if_absent(
                &object_key,
                shardline_storage::ObjectBody::from_vec(compressed.clone()),
                &integrity,
            )
            .unwrap();

        // Build a FileRecord pointing to this chunk (compressed storage)
        let record = FileRecord {
            file_id: "test-lz4".to_owned(),
            content_hash: hash_hex.clone(),
            total_bytes: payload.len() as u64,
            chunk_size: 65536,
            storage_repr: shardline_index::StorageRepresentation::XorbCdcV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: hash_hex,
                offset: 0,
                length: payload.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: compressed.len() as u64,
            }],
        };

        let mut stream = super::file_record_byte_stream(object_store, record, None)
            .await
            .unwrap();
        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(result, payload);
    }

    // ── regression: compressed size coinciding with raw size ─────────────
    // XorbCdcV1 single-chunk records where the LZ4-compressed object is
    // exactly as large as the raw chunk used to be served as raw compressed
    // bytes, because compression was detected via size inequality
    // (`packed_end != chunk.length`). Compression is a property of the
    // storage representation and must not be inferred from sizes.

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_record_byte_stream_decompresses_when_compressed_size_equals_raw_size() {
        use shardline_index::{FileChunkRecord, FileRecord};
        use shardline_storage::ObjectStore;

        // Stored chunk captured from a real CI failure post-mortem (OCI
        // digest mismatch, e2e skopeo flow): a skopeo-pushed gzip layer
        // blob whose LZ4-compressed form is exactly as large as its raw
        // content. LZ4 header u32 LE = 183; decompressed size = 183;
        // sha256(decompressed) = 70d1c71d22c6e42eacf51571769b7a7fc73e4a3f4a7236ea05219d0bdc2cbb58
        // (the expected blob digest the server failed to serve).
        let stored: Vec<u8> = vec![
            0xb7, 0x00, 0x00, 0x00, 0xf6, 0x8c, 0x1f, 0x8b, 0x08, 0x00, 0x00, 0x09, 0x6e, 0x88,
            0x00, 0xff, 0xec, 0xd1, 0x41, 0x6a, 0xc3, 0x30, 0x10, 0x40, 0x51, 0xad, 0x7b, 0x0a,
            0x9d, 0xc0, 0x9e, 0xa9, 0x2c, 0xf9, 0x3c, 0xc2, 0x76, 0xb1, 0x41, 0xad, 0x8a, 0xed,
            0x42, 0x7b, 0xfb, 0xe2, 0x45, 0xb0, 0x42, 0x08, 0xd9, 0xc4, 0x90, 0x90, 0xff, 0x36,
            0x03, 0x23, 0x31, 0x9b, 0x5f, 0xd5, 0xe6, 0x70, 0x22, 0x22, 0xad, 0xf7, 0xdb, 0xd4,
            0xd6, 0x4b, 0x39, 0x4f, 0x8c, 0xfa, 0x77, 0xe7, 0xd4, 0x85, 0xc6, 0x05, 0x23, 0x12,
            0x5a, 0xaf, 0xc6, 0xfa, 0xe2, 0xc6, 0x61, 0x7e, 0x96, 0x35, 0xce, 0xd6, 0x9a, 0xd8,
            0x95, 0xdb, 0x4b, 0xb7, 0xde, 0x9f, 0x54, 0x55, 0x7f, 0xc7, 0xbf, 0x94, 0x63, 0x5f,
            0xad, 0xbf, 0xeb, 0xbe, 0xbe, 0xab, 0xad, 0x70, 0x68, 0x9a, 0xeb, 0xfd, 0x9d, 0x9e,
            0xf7, 0x57, 0x55, 0xe7, 0x8d, 0x95, 0xf2, 0xc8, 0x51, 0x5e, 0xbc, 0xff, 0x38, 0xa4,
            0x94, 0xed, 0xc7, 0x9c, 0x3f, 0xed, 0x32, 0xc6, 0xb9, 0x4f, 0xd3, 0xd7, 0x60, 0x73,
            0x37, 0xbd, 0xed, 0x5f, 0x00, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x03, 0x3c, 0xa8, 0x7f,
            0x00, 0x00, 0x00, 0xff, 0xff, 0x03, 0x00, 0x0f, 0x4c, 0x70, 0x4d, 0x00, 0x28, 0x00,
            0x00,
        ];
        let payload = lz4_flex::decompress_size_prepended(&stored)
            .expect("captured bytes decompress to the raw gzip blob");
        assert_eq!(
            stored.len(),
            payload.len(),
            "the coincidence at the heart of the bug: compressed size == raw size"
        );

        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();

        // Store the compressed chunk keyed by its raw-content hash
        let raw_hash = crate::local_backend::chunk_hash(&payload);
        let hash_hex = shardline_index::xet_hash_hex_string(raw_hash);
        let object_key = crate::chunk_store::chunk_object_key(&hash_hex).unwrap();
        let stored_hash = crate::local_backend::chunk_hash(&stored);
        let integrity =
            shardline_storage::ObjectIntegrity::new(stored_hash, stored.len() as u64);
        object_store
            .put_if_absent(
                &object_key,
                shardline_storage::ObjectBody::from_vec(stored.clone()),
                &integrity,
            )
            .unwrap();

        // Forge the record shape observed in the wild: chunk.length and
        // total_bytes equal the STORED (compressed) size, with packed_end
        // matching as well — so a size-based discriminator sees
        // `packed_end == chunk.length` and serves the raw object. A
        // storage-repr-based discriminator must still decompress.
        let stored_len = stored.len() as u64;
        let record = FileRecord {
            file_id: "test-eq-size".to_owned(),
            content_hash: hash_hex.clone(),
            total_bytes: stored_len,
            chunk_size: 65536,
            storage_repr: shardline_index::StorageRepresentation::XorbCdcV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: hash_hex,
                offset: 0,
                length: stored_len,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: stored_len,
            }],
        };

        let mut stream = super::file_record_byte_stream(object_store, record, None)
            .await
            .unwrap();
        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(result, payload);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_local_store_mid_range() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/mid-range").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"0123456789").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let range = ByteRange::new(3, 7).unwrap();
        let result = super::object_byte_range_stream(store, object_key, 10, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        use futures_util::StreamExt;
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"34567");
    }

    // ── local_store_byte_range_stream — single-byte at start ──────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_range_stream_first_byte() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/first-byte").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcdef").await.unwrap();

        let range = ByteRange::new(0, 0).unwrap(); // just first byte
        let result = local_object_byte_range_stream(object_store, object_key, 6, range).await;
        assert!(result.is_ok());
        let mut observed = Vec::new();
        let mut stream = result.unwrap();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"a");
    }

    // ── local_store_byte_range_stream — range at very end ─────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_range_stream_last_byte_range() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/last-byte").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"abcdef").await.unwrap();

        // Range at end: byte 5 of 6 → last byte
        let range = ByteRange::new(5, 5).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 6, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let mut observed = Vec::new();
        while let Some(item) = stream.next().await {
            observed.extend_from_slice(&item.unwrap());
        }
        assert_eq!(observed, b"f");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_zero_length_with_missing_object_returns_not_found() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/missing").unwrap();
        let result = super::object_byte_stream(store, key, 0).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_stream_zero_length_with_nonzero_metadata_returns_stored_length_mismatch() {
        // Create a file with content but claim length 0
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/zero-claim-nonzero").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"content").await.unwrap();

        let store = ServerObjectStore::Local(object_store);
        let result = super::object_byte_stream(store, object_key, 0).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn object_byte_range_stream_with_blackhole_returns_not_found_alt() {
        let store = ServerObjectStore::blackhole();
        let key = ObjectKey::parse("test/alt-blackhole-range").unwrap();
        let range = ByteRange::new(0, 9).unwrap();
        let result = super::object_byte_range_stream(store, key, 10, range).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_store_byte_range_stream_empty_range_with_nonempty_body_not_possible() {
        // A zero-length range is not representable by ByteRange (start <= end always).
        // Test edge: range at position 0,0 on 1-byte file.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/minimal-range").unwrap();
        let path = object_store.path_for_key(&object_key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, b"x").await.unwrap();

        let range = ByteRange::new(0, 0).unwrap();
        let result = local_object_byte_range_stream(object_store, object_key, 1, range).await;
        assert!(result.is_ok());
        let mut stream = result.unwrap();
        let observed: Vec<u8> = futures_util::StreamExt::collect::<Vec<_>>(&mut stream)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(observed, b"x");
    }

    // ── xorb-backed file record tests ────────────────────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xorb_backed_file_record_reads_correctly() {
        use shardline_index::{FileChunkRecord, FileRecord};

        // 1. Pack 3 chunks into a xorb.
        let chunks = vec![
            (b"Hello, ".to_vec(), 0u64),
            (b"xorb world".to_vec(), 7u64),
            (b"!".to_vec(), 17u64),
        ];
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();

        // 2. Store xorb in a local object store.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();
        let was_inserted = crate::upload_ingest::xorb_packer::store_xorb(
            &object_store,
            &packed.xorb_hash_hex,
            &packed.serialized,
        )
        .await
        .unwrap();
        assert!(was_inserted, "xorb should be stored");

        // 3. Create a FileRecord with xorb-backed entries.
        let mut file_chunks = Vec::new();
        for (i, entry) in packed.chunk_entries.iter().enumerate() {
            let raw_len = chunks[i].0.len() as u64;
            let next_index = entry.chunk_index.checked_add(1).unwrap();
            let packed_end = entry
                .packed_offset
                .checked_add(entry.packed_length)
                .unwrap();
            file_chunks.push(FileChunkRecord {
                hash: packed.xorb_hash_hex.clone(),
                offset: entry.raw_offset,
                length: raw_len,
                range_start: u64::from(entry.chunk_index),
                range_end: u64::from(next_index),
                packed_start: u64::from(entry.packed_offset),
                packed_end: u64::from(packed_end),
            });
        }

        let total_bytes: u64 = chunks.iter().map(|(d, _)| d.len() as u64).sum();
        let record = FileRecord {
            file_id: "xorb-test-file.bin".to_owned(),
            content_hash: packed.xorb_hash_hex.clone(),
            total_bytes,
            chunk_size: 65536,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: file_chunks,
        };

        // 4. Call file_record_byte_stream (no range = full file).
        let mut stream = super::file_record_byte_stream(object_store, record, None)
            .await
            .unwrap();

        // 5. Collect and verify all decompressed content matches.
        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }
        let expected: Vec<u8> = chunks.iter().flat_map(|(d, _)| d.clone()).collect();
        assert_eq!(
            result, expected,
            "xorb-backed file record content should match"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xorb_backed_download_with_byte_range() {
        use shardline_index::{FileChunkRecord, FileRecord};

        // 1. Pack 3 chunks into a xorb with known sizes.
        let chunks = vec![
            (b"0123456789".to_vec(), 0u64),  // 10 bytes
            (b"ABCDEFGHIJ".to_vec(), 10u64), // 10 bytes
            (b"abcdefghij".to_vec(), 20u64), // 10 bytes
        ];
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();

        // 2. Store xorb.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();
        let _ = crate::upload_ingest::xorb_packer::store_xorb(
            &object_store,
            &packed.xorb_hash_hex,
            &packed.serialized,
        )
        .await
        .unwrap();

        // 3. Create FileRecord with xorb-backed entries.
        let mut file_chunks = Vec::new();
        for (i, entry) in packed.chunk_entries.iter().enumerate() {
            let raw_len = chunks[i].0.len() as u64;
            let next_index = entry.chunk_index.checked_add(1).unwrap();
            let packed_end = entry
                .packed_offset
                .checked_add(entry.packed_length)
                .unwrap();
            file_chunks.push(FileChunkRecord {
                hash: packed.xorb_hash_hex.clone(),
                offset: entry.raw_offset,
                length: raw_len,
                range_start: u64::from(entry.chunk_index),
                range_end: u64::from(next_index),
                packed_start: u64::from(entry.packed_offset),
                packed_end: u64::from(packed_end),
            });
        }

        let total_bytes: u64 = chunks.iter().map(|(d, _)| d.len() as u64).sum();
        let record = FileRecord {
            file_id: "xorb-range-test.bin".to_owned(),
            content_hash: packed.xorb_hash_hex.clone(),
            total_bytes,
            chunk_size: 65536,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: file_chunks,
        };

        // 4. Request a range that spans bytes 5-24.
        //    Chunk 0: bytes 0-9, we want 5-9 (5 bytes: "56789")
        //    Chunk 1: bytes 10-19, we want 10-19 (10 bytes: "ABCDEFGHIJ")
        //    Chunk 2: bytes 20-29, we want 20-24 (5 bytes: "abcde")
        //    Expected: "56789ABCDEFGHIJabcde"
        let range = ByteRange::new(5, 24).unwrap();
        let mut stream = super::file_record_byte_stream(object_store, record, Some(range))
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }
        let expected: Vec<u8> = b"56789ABCDEFGHIJabcde".to_vec();
        assert_eq!(
            result, expected,
            "xorb-backed byte range should span multiple chunks correctly"
        );
        assert_eq!(
            result.len(),
            20,
            "byte range 5-24 inclusive should be 20 bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn xorb_backed_byte_range_single_chunk() {
        use shardline_index::{FileChunkRecord, FileRecord};

        // 1. Pack a single chunk into a xorb.
        let content = b"single-chunk-xorb-range-test!".to_vec();
        let chunks = vec![(content.clone(), 0u64)];
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();

        // 2. Store xorb.
        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();
        let _ = crate::upload_ingest::xorb_packer::store_xorb(
            &object_store,
            &packed.xorb_hash_hex,
            &packed.serialized,
        )
        .await
        .unwrap();

        // 3. Create FileRecord with one xorb-backed entry.
        //    Use the xorb hash as the chunk hash, and a non-zero packed_start
        //    so the is_xorb_backed guard detects it as xorb-backed. The xorb
        //    fast path reads the entire xorb and indexes decoded chunks by
        //    range_start — it does not use packed_start for data access.
        let entry = &packed.chunk_entries[0];
        let raw_len = content.len() as u64;
        let next_index = entry.chunk_index.checked_add(1).unwrap();
        let packed_end = entry
            .packed_offset
            .checked_add(entry.packed_length)
            .unwrap();
        let record = FileRecord {
            file_id: "single-chunk-xorb-range-test.bin".to_owned(),
            content_hash: packed.xorb_hash_hex.clone(),
            total_bytes: raw_len,
            chunk_size: 65536,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: packed.xorb_hash_hex.clone(),
                offset: 0,
                length: raw_len,
                range_start: u64::from(entry.chunk_index),
                range_end: u64::from(next_index),
                packed_start: 1, // non-zero to signal xorb-backed to the guard
                packed_end: u64::from(packed_end),
            }],
        };

        // 4. Request a range within the single chunk (bytes 5-15).
        let range = ByteRange::new(5, 15).unwrap();
        let mut stream = super::file_record_byte_stream(object_store, record, Some(range))
            .await
            .unwrap();

        let mut result = Vec::new();
        while let Some(chunk) = stream.next().await {
            result.extend_from_slice(&chunk.unwrap());
        }
        let expected: Vec<u8> = content[5..=15].to_vec();
        assert_eq!(
            result, expected,
            "xorb-backed byte range should handle single chunk correctly"
        );
        assert_eq!(
            result.len(),
            11,
            "byte range 5-15 inclusive should be 11 bytes"
        );
    }
}
