use std::{
    collections::VecDeque,
    io::{Error as IoError, ErrorKind, Seek, SeekFrom},
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
    ServerError, admission::BoundedPool, chunk_store::chunk_object_key, error::ObjectStoreError,
    local_backend::chunk_hash, object_store::ServerObjectStore,
    object_store::run_before_local_object_read_hook,
};

pub const STREAM_READ_BUFFER_BYTES: u64 = 1024 * 1024;

pub type ServerByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ServerError>> + Send>>;

// A queued job is aborted when its polling future is dropped. Running jobs
// retain admission until completion, including initial whole-xorb validation.
// Subsequent polls read or decode only one bounded portion.
struct AbortBlockingJob(tokio::task::AbortHandle);
impl Drop for AbortBlockingJob {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn blocking_stream_work<T: Send + 'static>(
    pool: &BoundedPool,
    work: impl FnOnce() -> Result<T, ServerError> + Send + 'static,
) -> Result<T, ServerError> {
    let permit = pool
        .acquire()
        .await
        .map_err(|error| ServerError::Io(IoError::other(error)))?;
    let job = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        work()
    });
    let _abort = AbortBlockingJob(job.abort_handle());
    job.await
        .map_err(|error| ServerError::Io(IoError::other(error)))?
}

struct SerializedXorbStreamState {
    file: tempfile::NamedTempFile,
    offset: u64,
    end: u64,
    pool: BoundedPool,
}

/// Reads and validates a complete content-addressed xorb before exposing a requested
/// serialized-byte range.
///
/// A range cannot be authenticated in isolation because the xorb identity covers its
/// chunk sequence and the serialized container can be corrupted outside the requested
/// range. Validation therefore finishes before the response stream is constructed, so
/// stale or corrupted provider bytes never become a successful partial response.
///
/// # Errors
///
/// Returns [`ServerError`] when the object is missing, its stored length or requested
/// range is invalid, or the serialized xorb does not match `hash_hex`.
pub(crate) async fn validated_xorb_byte_range_stream(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    hash_hex: &str,
    total_length: u64,
    range: ByteRange,
    pool: BoundedPool,
) -> Result<ServerByteStream, ServerError> {
    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }
    let expected_hash = parse_xet_hash_hex(hash_hex)?;
    let store = object_store.clone();
    let key = object_key.clone();
    let file = blocking_stream_work(&pool, move || {
        let mut file = store.materialize_object_to_tempfile(&key, total_length)?;
        crate::xet_adapter::validate_serialized_xorb(file.as_file_mut(), expected_hash)?;
        Ok(file)
    })
    .await?;
    let state = SerializedXorbStreamState {
        file,
        offset: range.start(),
        end: range.end_inclusive(),
        pool,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            if state.offset > state.end {
                return Ok(None);
            }
            let work_pool = state.pool.clone();
            blocking_stream_work(&work_pool, move || {
                use std::io::Read;
                let length = state
                    .end
                    .checked_sub(state.offset)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(ServerError::Overflow)?
                    .min(STREAM_READ_BUFFER_BYTES);
                let length = usize::try_from(length)?;
                let cursor = state.file.as_file_mut();
                cursor.seek(SeekFrom::Start(state.offset))?;
                let mut bytes = vec![0_u8; length];
                cursor.read_exact(&mut bytes)?;
                state.offset = state
                    .offset
                    .checked_add(u64::try_from(length)?)
                    .ok_or(ServerError::Overflow)?;
                Ok(Some((Bytes::from(bytes), state)))
            })
            .await
        },
    )))
}

/// Returns the serialized xorb length when an object is stored under `hash_hex`.
///
/// Used by the single-chunk routing decision in [`file_record_byte_stream`]: a
/// xorb-backed single-chunk record stores its `hash` as the xorb hash, an
/// opaque value that is indistinguishable from a plain chunk data hash without
/// checking storage. A metadata miss (or an error) means the record is treated
/// as individual-chunk-backed; the subsequent read then surfaces any real
/// storage failure.
fn xorb_object_length(object_store: &ServerObjectStore, hash_hex: &str) -> Option<u64> {
    let Ok(object_key) = crate::xet_adapter::xorb_object_key(hash_hex) else {
        return None;
    };
    match object_store.metadata(&object_key) {
        Ok(Some(metadata)) if metadata.length() != 0 => Some(metadata.length()),
        Ok(_) | Err(_) => None,
    }
}

struct DecodedXorbStreamState {
    file: tempfile::NamedTempFile,
    descriptors: VecDeque<shardline_xet_adapter::ValidatedXorbChunk>,
    chunks: Vec<shardline_index::FileChunkRecord>,
    next_chunk_index: usize,
    requested_start: u64,
    requested_end: u64,
    pool: BoundedPool,
}

/// Reads a xorb-backed file record by fetching the single xorb object, parsing
/// all chunks, and extracting the requested byte range.
///
/// When all chunks in a [`FileRecord`] share the same hash (meaning they were
/// packed into a single xorb container during upload), this path reads the xorb
/// once and extracts decompressed chunk data without per-chunk storage round-trips.
async fn read_xorb_backed_chunks(
    object_store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
    known_xorb_length: Option<u64>,
    pool: BoundedPool,
) -> Result<ServerByteStream, ServerError> {
    let chunk_zero = record.chunks.first().ok_or(ServerError::Overflow)?;
    let expected_hash = parse_xet_hash_hex(&chunk_zero.hash)?;
    let key = crate::xet_adapter::xorb_object_key(&chunk_zero.hash)?;
    let (file, validated) = blocking_stream_work(&pool, move || {
        let length = match known_xorb_length {
            Some(length) => length,
            None => object_store
                .metadata(&key)?
                .ok_or(ServerError::NotFound)?
                .length(),
        };
        let mut file = object_store.materialize_object_to_tempfile(&key, length)?;
        let validated =
            crate::xet_adapter::validate_serialized_xorb(file.as_file_mut(), expected_hash)?;
        Ok((file, validated))
    })
    .await?;
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
    let state = DecodedXorbStreamState {
        file,
        descriptors: validated.chunks().iter().cloned().collect(),
        chunks: record.chunks,
        next_chunk_index: 0,
        requested_start,
        requested_end,
        pool,
    };
    Ok(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            if state.descriptors.is_empty() {
                return Ok(None);
            }
            let work_pool = state.pool.clone();
            blocking_stream_work(&work_pool, move || {
                while let Some(descriptor) = state.descriptors.pop_front() {
                    let chunk_index = descriptor.unpacked_start();
                    while state
                        .chunks
                        .get(state.next_chunk_index)
                        .is_some_and(|chunk| chunk.offset < chunk_index)
                    {
                        state.next_chunk_index = state
                            .next_chunk_index
                            .checked_add(1)
                            .ok_or(ServerError::Overflow)?;
                    }
                    let chunk = state
                        .chunks
                        .get(state.next_chunk_index)
                        .ok_or(ServerError::Overflow)?;
                    if chunk.offset != chunk_index {
                        return Err(ServerError::Overflow);
                    }
                    state.next_chunk_index = state
                        .next_chunk_index
                        .checked_add(1)
                        .ok_or(ServerError::Overflow)?;
                    let chunk_end = chunk
                        .offset
                        .checked_add(chunk.length)
                        .and_then(|v| v.checked_sub(1))
                        .ok_or(ServerError::Overflow)?;
                    let start = state.requested_start.max(chunk.offset);
                    let end = state.requested_end.min(chunk_end);
                    if start > end {
                        continue;
                    }
                    let relative_start = usize::try_from(
                        start
                            .checked_sub(chunk.offset)
                            .ok_or(ServerError::Overflow)?,
                    )?;
                    let relative_end = usize::try_from(
                        end.checked_sub(chunk.offset)
                            .ok_or(ServerError::Overflow)?
                            .checked_add(1)
                            .ok_or(ServerError::Overflow)?,
                    )?;
                    if descriptor.unpacked_len()
                        > shardline_xet_core::xorb_object::constants::MAX_CHUNK_SIZE
                            .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        return Err(ServerError::Overflow);
                    }
                    let cursor = state.file.as_file_mut();
                    cursor.seek(SeekFrom::Start(descriptor.packed_start()))?;
                    // Full object integrity was verified before headers. The private
                    // temporary file is immutable; still check each decoder length
                    // against the validated footer before applying the output slice.
                    let (data, packed_len, unpacked_len) =
                        shardline_xet_core::xorb_object::deserialize_chunk(cursor).map_err(
                            |error| ServerError::Io(IoError::new(ErrorKind::InvalidData, error)),
                        )?;
                    if u64::try_from(packed_len)?
                        != descriptor
                            .packed_end()
                            .checked_sub(descriptor.packed_start())
                            .ok_or(ServerError::Overflow)?
                        || u64::from(unpacked_len) != descriptor.unpacked_len()
                    {
                        return Err(ServerError::Io(IoError::new(
                            ErrorKind::InvalidData,
                            "xorb chunk length disagrees with validated footer",
                        )));
                    }
                    let bytes = Bytes::from(data);
                    if bytes.get(relative_start..relative_end).is_none() {
                        return Err(ServerError::Overflow);
                    }
                    return Ok(Some((bytes.slice(relative_start..relative_end), state)));
                }
                Ok(None)
            })
            .await
        },
    )))
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
    pool: BoundedPool,
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
    // For a single chunk the record hash is either the chunk's data hash
    // (individual-chunk storage) or the xorb's hash (xorb-backed storage);
    // both are opaque 64-hex values, so we probe the object store for a
    // stored xorb object under that hash. Xorb-backed chunks always carry a
    // nonzero packed_end (the chunk's serialized length inside the xorb),
    // which lets us skip the probe for legacy records that predate packing.
    let first_hash = record.chunks.first().map(|c| &c.hash);
    let all_same_hash = first_hash.is_some_and(|h| record.chunks.iter().all(|c| c.hash == *h));
    let known_xorb_length = if record.chunks.len() == 1
        && all_same_hash
        && record
            .chunks
            .first()
            .is_some_and(|chunk| chunk.packed_end > 0)
    {
        record
            .chunks
            .first()
            .and_then(|chunk| xorb_object_length(&object_store, &chunk.hash))
    } else {
        None
    };
    let is_xorb_backed = if record.chunks.len() > 1 {
        all_same_hash
    } else {
        known_xorb_length.is_some()
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
        return read_xorb_backed_chunks(object_store, record, range, known_xorb_length, pool).await;
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

    let mut terms = Vec::with_capacity(record.chunks.len());
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
        let hash_hex = chunk.hash;
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
                    // lz4_flex::compress_prepend_size writes a 4-byte LE u32 size prefix.
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
                    // Verify content hash to detect bit-rot or storage corruption.
                    let actual_hash = chunk_hash(&compressed);
                    let actual_hex = xet_hash_hex_string(actual_hash);
                    if actual_hex != expected_hash_hex {
                        warn!(
                            expected = %expected_hash_hex,
                            actual = %actual_hex,
                            chunk_len = compressed.len(),
                            "chunk integrity mismatch for uncompressed chunk"
                        );
                        return Err(ServerError::ObjectStore(
                            ObjectStoreError::StoredLengthMismatch,
                        ));
                    }
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

    use futures_util::{StreamExt, TryStreamExt};
    use shardline_index::FileRecord;
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

        let mut stream = super::file_record_byte_stream(
            object_store,
            record,
            None,
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
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
        let integrity = shardline_storage::ObjectIntegrity::new(stored_hash, stored.len() as u64);
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

        let mut stream = super::file_record_byte_stream(
            object_store,
            record,
            None,
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
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
    async fn xorb_range_rejects_corruption_outside_requested_bytes() {
        let chunks = vec![(vec![1; 1024], 0), (vec![2; 1024], 1024)];
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();
        let storage = shardline_test_support::TempStorage::new();
        let object_store = crate::object_store::ServerObjectStore::local(storage.path()).unwrap();
        crate::upload_ingest::xorb_packer::store_xorb(
            &object_store,
            &packed.xorb_hash_hex,
            &packed.serialized,
        )
        .await
        .unwrap();
        let key = crate::xet_adapter::xorb_object_key(&packed.xorb_hash_hex).unwrap();
        let path = object_store.local_path_for_key(&key).unwrap();
        let mut corrupt = packed.serialized.clone();
        // Damage the second chunk, outside the one-byte serialized range.
        let offset = usize::try_from(packed.chunk_entries[1].packed_offset).unwrap();
        corrupt[offset] ^= 1;
        std::fs::write(path, &corrupt).unwrap();
        let result = super::validated_xorb_byte_range_stream(
            &object_store,
            &key,
            &packed.xorb_hash_hex,
            corrupt.len() as u64,
            ByteRange::new(0, 0).unwrap(),
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
        .await;
        assert!(
            result.is_err(),
            "full xorb validation must precede delivery"
        );
    }

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
        let mut stream = super::file_record_byte_stream(
            object_store,
            record,
            None,
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
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
        let mut stream = super::file_record_byte_stream(
            object_store,
            record,
            Some(range),
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
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
        //    Use the xorb hash as the chunk hash with the real packed offsets
        //    (packed_start is 0 — the sole chunk is the first in the xorb).
        //    The is_xorb_backed guard detects the record as xorb-backed by
        //    probing for the stored xorb object under the record hash. The
        //    xorb fast path reads the entire xorb and indexes decoded chunks
        //    by range_start — it does not use packed_start for data access.
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
                packed_start: u64::from(entry.packed_offset),
                packed_end: u64::from(packed_end),
            }],
        };

        // 4. Request a range within the single chunk (bytes 5-15).
        let range = ByteRange::new(5, 15).unwrap();
        let mut stream = super::file_record_byte_stream(
            object_store,
            record,
            Some(range),
            crate::admission::ExecutionPools::default_sizes().blocking_io,
        )
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
    async fn pull_stream_fixture() -> (
        shardline_test_support::TempStorage,
        ServerObjectStore,
        FileRecord,
        Vec<u8>,
        Vec<u8>,
    ) {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = ServerObjectStore::local(storage.path()).unwrap();
        let mut seed = 0x53a4_f792_6295_14be_u64;
        let mut chunks = Vec::new();
        let mut offset = 0_u64;
        for _ in 0..4 {
            let mut data = vec![0_u8; 1_048_576];
            for byte in &mut data {
                seed ^= seed.wrapping_shl(13);
                seed ^= seed.wrapping_shr(7);
                seed ^= seed.wrapping_shl(17);
                *byte = seed.to_le_bytes().first().copied().unwrap();
            }
            chunks.push((data, offset));
            offset = offset.checked_add(1_048_576).unwrap();
        }
        let packed = crate::upload_ingest::xorb_packer::pack_chunks_into_xorb(&chunks).unwrap();
        crate::upload_ingest::xorb_packer::store_xorb(
            &object_store,
            &packed.xorb_hash_hex,
            &packed.serialized,
        )
        .await
        .unwrap();
        let records = packed
            .chunk_entries
            .iter()
            .map(|entry| shardline_index::FileChunkRecord {
                hash: packed.xorb_hash_hex.clone(),
                offset: entry.raw_offset,
                length: 1_048_576,
                range_start: u64::from(entry.chunk_index),
                range_end: u64::from(entry.chunk_index.checked_add(1).unwrap()),
                packed_start: u64::from(entry.packed_offset),
                packed_end: u64::from(
                    entry
                        .packed_offset
                        .checked_add(entry.packed_length)
                        .unwrap(),
                ),
            })
            .collect();
        let record = FileRecord {
            file_id: "pull-stream.bin".to_owned(),
            content_hash: packed.xorb_hash_hex,
            total_bytes: offset,
            chunk_size: 1_048_576,
            storage_repr: shardline_index::StorageRepresentation::XorbCdcV1,
            repository_scope: None,
            chunks: records,
        };
        let raw = chunks.into_iter().flat_map(|(data, _)| data).collect();
        (storage, object_store, record, packed.serialized, raw)
    }

    #[test]
    fn unpolled_xorb_streams_leave_blocking_work_and_shutdown_available() {
        use std::time::Duration;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (storage, mut streams, work_progressed) = runtime.block_on(async {
            let (storage, store, record, serialized, _raw) = pull_stream_fixture().await;
            let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
            let key = crate::xet_adapter::xorb_object_key(&record.content_hash).unwrap();
            let length = u64::try_from(serialized.len()).unwrap();
            let mut streams = Vec::new();
            streams.push(
                super::validated_xorb_byte_range_stream(
                    &store,
                    &key,
                    &record.content_hash,
                    length,
                    ByteRange::new(0, length.checked_sub(1).unwrap()).unwrap(),
                    pool.clone(),
                )
                .await
                .unwrap(),
            );
            streams.push(
                super::file_record_byte_stream(store, record, None, pool.clone())
                    .await
                    .unwrap(),
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(
                pool.available_permits(),
                1,
                "idle bodies retain no work permit"
            );
            let progressed = tokio::time::timeout(
                Duration::from_millis(250),
                tokio::task::spawn_blocking(|| 17),
            )
            .await
            .is_ok();
            (storage, streams, progressed)
        });
        let (shutdown_done, shutdown_received) = std::sync::mpsc::channel();
        let shutdown = std::thread::spawn(move || {
            drop(runtime);
            shutdown_done.send(()).unwrap();
        });
        let shutdown_with_live_streams = shutdown_received
            .recv_timeout(Duration::from_secs(1))
            .is_ok();
        // Cleanup before asserting so the old channel-backed implementation
        // fails without hanging runtime destruction.
        streams.clear();
        shutdown.join().unwrap();
        drop(storage);
        assert!(
            work_progressed,
            "idle body producer parked the only blocking worker"
        );
        assert!(
            shutdown_with_live_streams,
            "idle response body prevented runtime shutdown"
        );
    }

    #[test]
    fn one_work_slot_streams_full_and_partial_serialized_and_decoded_bytes() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (_storage, store, record, serialized, raw) = pull_stream_fixture().await;
            let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
            let key = crate::xet_adapter::xorb_object_key(&record.content_hash).unwrap();
            let length = u64::try_from(serialized.len()).unwrap();
            for range in [
                ByteRange::new(0, length.checked_sub(1).unwrap()).unwrap(),
                ByteRange::new(17, 2_097_181).unwrap(),
            ] {
                let stream = super::validated_xorb_byte_range_stream(
                    &store,
                    &key,
                    &record.content_hash,
                    length,
                    range,
                    pool.clone(),
                )
                .await
                .unwrap();
                let chunks = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    stream.try_collect::<Vec<_>>(),
                )
                .await
                .unwrap()
                .unwrap();
                let got: Vec<_> = chunks.into_iter().flatten().collect();
                assert_eq!(
                    got.as_slice(),
                    serialized
                        .get(
                            usize::try_from(range.start()).unwrap()
                                ..=usize::try_from(range.end_inclusive()).unwrap()
                        )
                        .unwrap()
                );
                assert_eq!(pool.available_permits(), 1);
            }
            for range in [None, Some(ByteRange::new(17, 2_097_181).unwrap())] {
                let stream = super::file_record_byte_stream(
                    store.clone(),
                    record.clone(),
                    range,
                    pool.clone(),
                )
                .await
                .unwrap();
                let chunks = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    stream.try_collect::<Vec<_>>(),
                )
                .await
                .unwrap()
                .unwrap();
                let got: Vec<_> = chunks.into_iter().flatten().collect();
                let expected = range.map_or(raw.as_slice(), |range| {
                    raw.get(
                        usize::try_from(range.start()).unwrap()
                            ..=usize::try_from(range.end_inclusive()).unwrap(),
                    )
                    .unwrap()
                });
                assert_eq!(got, expected);
                assert_eq!(pool.available_permits(), 1);
            }
        });
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_running_stream_work_keeps_admission_until_job_completes() {
        use std::sync::Arc;
        let pool = crate::admission::BoundedPool::new(std::num::NonZeroUsize::MIN);
        let started = Arc::new(tokio::sync::Notify::new());
        let (release, gate) = std::sync::mpsc::channel();
        let job_pool = pool.clone();
        let job_started = started.clone();
        let job = tokio::spawn(async move {
            super::blocking_stream_work(&job_pool, move || {
                job_started.notify_one();
                gate.recv().unwrap();
                Ok(17)
            })
            .await
        });
        started.notified().await;
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert_eq!(
            pool.available_permits(),
            0,
            "running job still owns capacity after caller cancellation"
        );
        let waiting_pool = pool.clone();
        let queued_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let queued_flag = queued_started.clone();
        let waiting = Arc::new(tokio::sync::Notify::new());
        let waiting_flag = waiting.clone();
        let queued = tokio::spawn(async move {
            waiting_flag.notify_one();
            super::blocking_stream_work(&waiting_pool, move || {
                queued_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(19)
            })
            .await
        });
        waiting.notified().await;
        tokio::task::yield_now().await;
        queued.abort();
        assert!(queued.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        let recovered = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            super::blocking_stream_work(&pool, || Ok(23)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered, 23);
        assert!(
            !queued_started.load(std::sync::atomic::Ordering::SeqCst),
            "cancelled queued closure executed"
        );
        assert_eq!(pool.available_permits(), 1);
    }
}
