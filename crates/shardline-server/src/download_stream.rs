use std::{
    io::{Error as IoError, ErrorKind, Seek, SeekFrom},
    pin::Pin,
    sync::{Arc, OnceLock},
};

use axum::body::Bytes;
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use shardline_index::{FileRecord, FileRecordStorageLayout, parse_xet_hash_hex};
use shardline_protocol::ByteRange;
use shardline_server_core::MaterializedObject;
use shardline_storage::{LocalObjectStore, ObjectKey, ObjectStore, S3ObjectStore};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tracing::debug;

use crate::{
    ServerError,
    admission::{BoundedPool, blocking_work as blocking_stream_work},
    chunk_store::chunk_object_key,
    error::ObjectStoreError,
    object_store::ServerObjectStore,
    object_store::run_before_local_object_read_hook,
};

pub(crate) mod authenticated_range;
pub(crate) mod checkpoint_reader;
mod collect;
mod fallback_chunks;
mod xorb_files;
#[cfg(test)]
pub(crate) use collect::collect_file_record_bytes;
pub(crate) use collect::download_file_record_bytes;

pub const STREAM_READ_BUFFER_BYTES: u64 = 1024 * 1024;

const MAX_IN_MEMORY_S3_XORB_BYTES: u64 = 1024 * 1024;
const MAX_IN_MEMORY_S3_XORB_READS: usize = 16;

static IN_MEMORY_S3_XORB_READS: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn try_acquire_s3_xorb_memory() -> Option<OwnedSemaphorePermit> {
    IN_MEMORY_S3_XORB_READS
        .get_or_init(|| Arc::new(Semaphore::new(MAX_IN_MEMORY_S3_XORB_READS)))
        .clone()
        .try_acquire_owned()
        .ok()
}

fn retain_memory_permit_for_materialized_object(
    object: &MaterializedObject,
    permit: Option<OwnedSemaphorePermit>,
) -> Option<OwnedSemaphorePermit> {
    if matches!(object, MaterializedObject::Memory(_)) {
        permit
    } else {
        drop(permit);
        None
    }
}

pub type ServerByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, ServerError>> + Send>>;

/// Selects the download representation after the backend resolves an authorized record.
pub(crate) async fn file_record_response_stream(
    object_store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
) -> Result<(ServerByteStream, u64), ServerError> {
    let total_bytes = record.total_bytes;
    crate::metrics::record_object_read_by_repr(record.storage_repr.as_str(), total_bytes);
    let stream = if record.storage_layout() == FileRecordStorageLayout::ReferencedObjectTerms {
        if range.is_some() {
            return Err(ServerError::RangeNotSatisfiable);
        }
        crate::server_frontend::xet::referenced_file_record_byte_stream(object_store, record, pool)?
    } else {
        file_record_byte_stream(object_store, record, range, pool).await?
    };
    Ok((stream, total_bytes))
}

struct SerializedXorbStreamState {
    file: XorbReadFile,
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
#[cfg(test)]
pub(crate) async fn validated_xorb_byte_range_stream(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    hash_hex: &str,
    total_length: u64,
    range: ByteRange,
    pool: BoundedPool,
) -> Result<ServerByteStream, ServerError> {
    authorized_xorb_byte_range_stream(
        object_store,
        object_key,
        hash_hex,
        total_length,
        range,
        pool,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn authorized_xorb_byte_range_stream(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    hash_hex: &str,
    total_length: u64,
    range: ByteRange,
    pool: BoundedPool,
    coverage: Option<&crate::record_store::XorbRangeCoverage>,
) -> Result<ServerByteStream, ServerError> {
    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }
    prepare_xorb_byte_range(object_store, object_key, hash_hex, Some(total_length), pool)
        .await?
        .into_stream(range, coverage)
}

/// A completely verified source held privately until range authorization ends.
pub(crate) struct VerifiedXorb {
    file: XorbReadFile,
    pool: BoundedPool,
    total_length: u64,
    validated: shardline_xet_adapter::ValidatedXorb,
}

pub(crate) async fn prepare_xorb_byte_range(
    object_store: &ServerObjectStore,
    object_key: &ObjectKey,
    hash_hex: &str,
    total_length: Option<u64>,
    pool: BoundedPool,
) -> Result<VerifiedXorb, ServerError> {
    let expected_hash = parse_xet_hash_hex(hash_hex)?;
    let store = object_store.clone();
    let key = object_key.clone();
    let memory_permit = matches!(&store, ServerObjectStore::S3(_))
        .then(try_acquire_s3_xorb_memory)
        .flatten();
    let maximum_memory_length = if memory_permit.is_some() {
        MAX_IN_MEMORY_S3_XORB_BYTES
    } else {
        0
    };
    let (file, observed_length, validated) = crate::admission::cancellable_work_with_setup(
        &pool,
        || (),
        move |(), cancel| {
            let (materialized, observed_length) = match &store {
                ServerObjectStore::Local(local) => {
                    let metadata = store
                        .metadata(&key)?
                        .ok_or(crate::object_store::ServerObjectStoreError::NotFound)?;
                    run_before_local_object_read_hook(&local.path_for_key(&key));
                    cancel.check()?;
                    let file = local.open_object_file(&key)?;
                    let observed_length = file.metadata()?.len();
                    if observed_length != metadata.length() {
                        return Err(
                            crate::object_store::ServerObjectStoreError::StoredObjectLengthMismatch
                                .into(),
                        );
                    }
                    (MaterializedObject::Local(file), observed_length)
                }
                ServerObjectStore::S3(_) => store
                    .materialize_bounded_object_if_present_with_checkpoint(
                        &key,
                        maximum_memory_length,
                        &|| cancel.check_io(),
                    )?
                    .ok_or(crate::object_store::ServerObjectStoreError::NotFound)?,
                ServerObjectStore::Blackhole => {
                    return Err(crate::object_store::ServerObjectStoreError::NotFound.into());
                }
            };
            if total_length.is_some_and(|expected| observed_length != expected) {
                return Err(
                    crate::object_store::ServerObjectStoreError::StoredObjectLengthMismatch.into(),
                );
            }
            let memory_permit =
                retain_memory_permit_for_materialized_object(&materialized, memory_permit);
            let mut file = XorbReadFile {
                object: materialized,
                _memory_permit: memory_permit,
            };
            let validated = crate::xet_adapter::validate_serialized_xorb(
                &mut checkpoint_reader::CheckpointReader {
                    reader: &mut file,
                    checkpoint: &|| cancel.check_io(),
                },
                expected_hash,
            );
            cancel.check()?;
            let validated = validated?;
            Ok((file, observed_length, validated))
        },
    )
    .await?;
    Ok(VerifiedXorb {
        file,
        pool,
        total_length: observed_length,
        validated,
    })
}

impl VerifiedXorb {
    pub(crate) const fn total_length(&self) -> u64 {
        self.total_length
    }

    pub(crate) const fn packed_content_length(&self) -> u64 {
        self.validated.packed_content_length()
    }

    pub(crate) fn into_stream(
        self,
        range: ByteRange,
        coverage: Option<&crate::record_store::XorbRangeCoverage>,
    ) -> Result<ServerByteStream, ServerError> {
        if range.end_inclusive() >= self.total_length {
            return Err(ServerError::RangeNotSatisfiable);
        }
        if coverage.as_ref().is_some_and(|allowed| {
            !allowed
                .for_layout(self.validated.chunks())
                .permits(range, self.validated.packed_content_length())
        }) {
            return Err(ServerError::NotFound);
        }
        let state = SerializedXorbStreamState {
            file: self.file,
            offset: range.start(),
            end: range.end_inclusive(),
            pool: self.pool,
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
                    state.file.seek(SeekFrom::Start(state.offset))?;
                    let mut bytes = vec![0_u8; length];
                    state.file.read_exact(&mut bytes)?;
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
}

/// Decodes logical file terms from a fully verified container snapshot.
struct DecodedXorbStreamState {
    file: XorbReadFile,
    validated: shardline_xet_adapter::ValidatedXorb,
    chunks: std::iter::Peekable<std::vec::IntoIter<shardline_index::FileChunkRecord>>,
    next_descriptor_index: Option<usize>,
    requested_start: u64,
    requested_end: u64,
    pool: BoundedPool,
}

struct XorbReadFile {
    object: MaterializedObject,
    _memory_permit: Option<OwnedSemaphorePermit>,
}

impl std::io::Read for XorbReadFile {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IoError> {
        std::io::Read::read(&mut self.object, buffer)
    }
}

impl Seek for XorbReadFile {
    fn seek(&mut self, position: SeekFrom) -> Result<u64, IoError> {
        Seek::seek(&mut self.object, position)
    }
}

/// Reads logical terms from one authenticated container, decoding one bounded
/// physical chunk per poll. Terms may select or repeat any container chunk range.
///
/// When all chunks in a [`FileRecord`] share the same hash (meaning they were
/// packed into a single xorb container during upload), this path reads the xorb
/// once. Logical file offsets do not have to equal container-relative offsets.
async fn read_xorb_backed_chunks(
    object_store: ServerObjectStore,
    record: &mut FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
    decoded_byte_budget: usize,
) -> Result<Option<ServerByteStream>, ServerError> {
    let chunk_zero = record.chunks.first().ok_or(ServerError::Overflow)?;
    let expected_hash = parse_xet_hash_hex(&chunk_zero.hash)?;
    let key = crate::xet_adapter::xorb_object_key(&chunk_zero.hash)?;
    let memory_permit = matches!(&object_store, ServerObjectStore::S3(_))
        .then(try_acquire_s3_xorb_memory)
        .flatten();
    let maximum_memory_length = if memory_permit.is_some() {
        MAX_IN_MEMORY_S3_XORB_BYTES
    } else {
        0
    };
    // Allocate the caller's result on its runtime worker, rather than growing
    // large retained allocator arenas across blocking validation workers.
    let Some((file, validated, decoded)) = crate::admission::cancellable_work_with_setup(
        &pool,
        move || Vec::with_capacity(decoded_byte_budget),
        move |decoded_buffer, cancel| {
            let mut file = match &object_store {
                ServerObjectStore::Local(local) => {
                    let Some(metadata) = object_store.metadata(&key)? else {
                        return Ok(None);
                    };
                    let length = metadata.length();
                    // Local objects are immutable and atomically published. Keep
                    // the already-open inode as the validation/read snapshot rather
                    // than copying the full container into a second scratch file.
                    run_before_local_object_read_hook(&local.path_for_key(&key));
                    cancel.check()?;
                    let file = local.open_object_file(&key)?;
                    if file.metadata()?.len() != length {
                        return Err(ServerError::ObjectStore(
                            ObjectStoreError::StoredLengthMismatch,
                        ));
                    }
                    XorbReadFile {
                        object: MaterializedObject::Local(file),
                        _memory_permit: None,
                    }
                }
                ServerObjectStore::S3(_) => {
                    let Some((object, _length)) = object_store
                        .materialize_bounded_object_if_present_with_checkpoint(
                            &key,
                            maximum_memory_length,
                            &|| cancel.check_io(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let memory_permit =
                        retain_memory_permit_for_materialized_object(&object, memory_permit);
                    XorbReadFile {
                        object,
                        _memory_permit: memory_permit,
                    }
                }
                ServerObjectStore::Blackhole => return Ok(None),
            };
            let validated = shardline_xet_adapter::validate_serialized_xorb_with_data(
                &mut checkpoint_reader::CheckpointReader {
                    reader: &mut file,
                    checkpoint: &|| cancel.check_io(),
                },
                expected_hash,
                decoded_byte_budget,
                decoded_buffer,
            );
            cancel.check()?;
            let (validated, decoded) = validated?;
            Ok(Some((file, validated, decoded)))
        },
    )
    .await?
    else {
        return Ok(None);
    };
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
    if let Some(decoded) = decoded
        && collect::covers_complete_xorb(record, &validated)?
    {
        let start = usize::try_from(requested_start)?;
        let end = usize::try_from(requested_end.checked_add(1).ok_or(ServerError::Overflow)?)?;
        let bytes = Bytes::from(decoded);
        if bytes.get(start..end).is_none() {
            return Err(ServerError::Overflow);
        }
        return Ok(Some(Box::pin(stream::once(async move {
            Ok(bytes.slice(start..end))
        }))));
    }
    let state = DecodedXorbStreamState {
        file,
        validated,
        chunks: std::mem::take(&mut record.chunks).into_iter().peekable(),
        next_descriptor_index: None,
        requested_start,
        requested_end,
        pool,
    };
    Ok(Some(Box::pin(stream::try_unfold(
        state,
        |mut state| async move {
            if state.chunks.peek().is_none() {
                return Ok(None);
            }
            let work_pool = state.pool.clone();
            blocking_stream_work(&work_pool, move || {
                while let Some(chunk) = state.chunks.peek() {
                    let first = usize::try_from(chunk.range_start)?;
                    let limit = usize::try_from(chunk.range_end)?;
                    let selected = state
                        .validated
                        .chunks()
                        .get(first..limit)
                        .ok_or(ServerError::Overflow)?;
                    let first_descriptor = selected.first().ok_or(ServerError::Overflow)?;
                    let last_descriptor = selected.last().ok_or(ServerError::Overflow)?;
                    if last_descriptor
                        .unpacked_end()
                        .checked_sub(first_descriptor.unpacked_start())
                        != Some(chunk.length)
                        || first_descriptor.packed_start() != chunk.packed_start
                        || last_descriptor.packed_end() != chunk.packed_end
                    {
                        return Err(ServerError::ObjectStore(
                            ObjectStoreError::StoredLengthMismatch,
                        ));
                    }
                    let index = state.next_descriptor_index.unwrap_or(first);
                    let descriptor = state
                        .validated
                        .chunks()
                        .get(index)
                        .ok_or(ServerError::Overflow)?;
                    let chunk_offset = chunk
                        .offset
                        .checked_add(
                            descriptor
                                .unpacked_start()
                                .checked_sub(first_descriptor.unpacked_start())
                                .ok_or(ServerError::Overflow)?,
                        )
                        .ok_or(ServerError::Overflow)?;
                    let chunk_end = chunk_offset
                        .checked_add(descriptor.unpacked_len())
                        .and_then(|value| value.checked_sub(1))
                        .ok_or(ServerError::Overflow)?;
                    let next = index.checked_add(1).ok_or(ServerError::Overflow)?;
                    if next == limit {
                        state.chunks.next();
                        state.next_descriptor_index = None;
                    } else {
                        state.next_descriptor_index = Some(next);
                    }
                    let start = state.requested_start.max(chunk_offset);
                    let end = state.requested_end.min(chunk_end);
                    if start > end {
                        continue;
                    }
                    let relative_start = usize::try_from(
                        start
                            .checked_sub(chunk_offset)
                            .ok_or(ServerError::Overflow)?,
                    )?;
                    let relative_end = usize::try_from(
                        end.checked_sub(chunk_offset)
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
                    state
                        .file
                        .seek(SeekFrom::Start(descriptor.packed_start()))?;
                    // Full container integrity was verified before these bytes. The private
                    // temporary file is immutable; still check each decoder length
                    // against the validated footer before applying the output slice.
                    let (data, packed_len, unpacked_len) =
                        shardline_xet_core::xorb_object::deserialize_chunk(&mut state.file)
                            .map_err(|error| {
                                ServerError::Io(IoError::new(ErrorKind::InvalidData, error))
                            })?;
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
    ))))
}

/// Streams a chunk-backed file record without materializing the complete object.
///
/// Chunks are stored compressed. Each chunk is read as a whole compressed blob,
/// decompressed, and then the requested byte range is sliced from the decompressed
/// data. The `packed_end` field on the chunk record indicates the compressed storage
/// length; `length` is the raw (uncompressed) length used for offset math.
///
/// Adjacent terms in the same container share one validated read. Containers
/// and individual fallback chunks may coexist in the same file.
pub(crate) async fn file_record_byte_stream(
    object_store: ServerObjectStore,
    record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
) -> Result<ServerByteStream, ServerError> {
    xorb_files::file_stream(object_store, record, range, pool).await
}

async fn single_container_file_stream(
    object_store: ServerObjectStore,
    mut record: FileRecord,
    range: Option<ByteRange>,
    pool: BoundedPool,
    decoded_byte_budget: usize,
) -> Result<ServerByteStream, ServerError> {
    record.validate_reconstruction_plan()?;
    if record.total_bytes == 0 {
        return Ok(Box::pin(stream::empty()));
    }

    // WholeFileV1 records use reconstruct_file_record_bytes instead.
    if record.storage_repr == shardline_index::StorageRepresentation::WholeFileV1 {
        return Err(ServerError::ObjectStore(
            crate::error::ObjectStoreError::StoredLengthMismatch,
        ));
    }

    // Fast path: when all chunks reference one packed xorb, read and validate
    // that object once. The direct GET also establishes object existence, so
    // S3 avoids a separate HEAD. If the xorb is absent, preserve compatibility
    // by falling back to individually stored chunks.
    if let Some(first) = record.chunks.first().filter(|first| {
        first.packed_end > 0 && record.chunks.iter().all(|chunk| chunk.hash == first.hash)
    }) {
        let xorb_hash = &first.hash;
        debug!(
            file_id = %record.file_id,
            total_bytes = record.total_bytes,
            xorb_hash = %xorb_hash,
            "reading xorb-backed file"
        );
        if let Some(stream) = read_xorb_backed_chunks(
            object_store.clone(),
            &mut record,
            range,
            pool.clone(),
            decoded_byte_budget,
        )
        .await?
        {
            return Ok(stream);
        }
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
        terms.push(fallback_chunks::Term {
            key: chunk_object_key(&hash_hex)?,
            storage_length,
            raw_length: chunk.length,
            expected_hash: hash_hex,
            range: chunk_range,
            compressed: is_compressed,
        });
    }

    // Validate the first fallback chunk before the caller constructs HTTP
    // success headers. Later chunks remain lazy; no complete file is buffered.
    let mut remaining = fallback_chunks::byte_stream(object_store, terms);
    let first = remaining.try_next().await?.ok_or(ServerError::Overflow)?;
    Ok(Box::pin(
        stream::once(async move { Ok(first) }).chain(remaining),
    ))
}

struct LocalObjectByteStreamState {
    file: File,
    remaining: u64,
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
    if range.end_inclusive() >= total_length {
        return Err(ServerError::RangeNotSatisfiable);
    }
    let byte_stream = object_store
        .stream_range_if_present(&object_key, range, total_length)
        .await
        .map_err(|error| {
            if matches!(
                error,
                shardline_storage::S3ObjectStoreError::IntegrityLengthMismatch
            ) {
                ServerError::ObjectStore(ObjectStoreError::StoredLengthMismatch)
            } else {
                ServerError::from(error)
            }
        })?
        .ok_or(ServerError::NotFound)?;

    Ok(Box::pin(byte_stream.map_err(ServerError::from)))
}

#[cfg(test)]
mod tests {
    use std::{io::Write, path::Path, sync::Arc};

    use futures_util::{StreamExt, TryStreamExt};
    use shardline_index::FileRecord;
    use shardline_protocol::ByteRange;
    use shardline_storage::{LocalObjectStore, ObjectKey};
    use tokio::fs;

    use super::{
        MaterializedObject, local_store_byte_range_stream,
        retain_memory_permit_for_materialized_object,
    };
    use crate::ServerError;
    use crate::error::ObjectStoreError;
    use crate::object_store::ServerObjectStore;
    use crate::object_store::set_before_local_object_read_hook;

    async fn write_fixture_file(path: &Path, bytes: impl AsRef<[u8]>) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.unwrap();
        }
        fs::write(path, bytes).await.unwrap();
    }

    #[test]
    fn spilled_xorb_releases_its_memory_admission_permit() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let object = MaterializedObject::Memory(std::io::Cursor::new(Vec::new()));
        let retained = retain_memory_permit_for_materialized_object(&object, Some(permit));
        assert!(retained.is_some());
        assert_eq!(semaphore.available_permits(), 0);
        drop(retained);
        assert_eq!(semaphore.available_permits(), 1);

        let permit = semaphore.clone().try_acquire_owned().unwrap();
        let temporary = tempfile::NamedTempFile::new().unwrap();
        let object = MaterializedObject::Temporary(temporary);
        assert!(retain_memory_permit_for_materialized_object(&object, Some(permit)).is_none());
        assert_eq!(semaphore.available_permits(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_object_streams_preserve_bytes_and_bound_each_read() {
        for (key, bytes) in [
            ("ab/object", vec![7; 64 * 1024 + 3]),
            ("ab/stream-nonzero", b"stream-me".to_vec()),
            ("ab/stream-full-2", b"stream-payload-data".to_vec()),
            (
                "ab/multi-chunk",
                vec![0x42; super::STREAM_READ_BUFFER_BYTES as usize * 2 + 50],
            ),
            ("ab/empty", Vec::new()),
            ("ab/existing-empty", Vec::new()),
            ("ab/store-empty", Vec::new()),
            ("ab/zero-stream", Vec::new()),
        ] {
            let storage = shardline_test_support::TempStorage::new();
            let store = LocalObjectStore::new(storage.path_buf()).unwrap();
            let key = ObjectKey::parse(key).unwrap();
            write_fixture_file(&store.path_for_key(&key), &bytes).await;
            let mut stream = super::object_byte_stream(
                ServerObjectStore::Local(store),
                key.clone(),
                bytes.len() as u64,
            )
            .await
            .unwrap();
            let mut observed = Vec::new();
            while let Some(chunk) = stream.try_next().await.unwrap() {
                assert!(!chunk.is_empty(), "{key:?}");
                assert!(
                    chunk.len() as u64 <= super::STREAM_READ_BUFFER_BYTES,
                    "{key:?}"
                );
                observed.extend_from_slice(&chunk);
            }
            assert_eq!(observed, bytes, "{key:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_object_streams_reject_incorrect_stored_lengths() {
        for (key, bytes, claimed) in [
            ("ab/object", b"short".as_slice(), 100),
            ("ab/nonempty-claimed-zero", b"data".as_slice(), 0),
            ("ab/not-empty", b"data".as_slice(), 0),
            ("ab/nonempty-claimed-zero-v2", b"data".as_slice(), 0),
            ("ab/zero-claim-nonzero", b"content".as_slice(), 0),
        ] {
            let storage = shardline_test_support::TempStorage::new();
            let store = LocalObjectStore::new(storage.path_buf()).unwrap();
            let key = ObjectKey::parse(key).unwrap();
            write_fixture_file(&store.path_for_key(&key), bytes).await;
            let result =
                super::object_byte_stream(ServerObjectStore::Local(store), key.clone(), claimed)
                    .await;
            assert!(
                matches!(
                    result,
                    Err(ServerError::ObjectStore(
                        ObjectStoreError::StoredLengthMismatch
                    ))
                ),
                "{key:?}, claimed length {claimed}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_growth_after_length_validation() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/object").unwrap();
        let path = object_store.path_for_key(&object_key);
        // Write initial content.
        write_fixture_file(&path, b"abcd").await;

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

        let byte_stream = local_store_byte_range_stream(
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
    async fn blackhole_full_object_streams_return_not_found() {
        for (key, length) in [
            ("test/key", 10),
            ("test/key", 0),
            ("test/missing-key", 0),
            ("test/zero-claim-nonzero", 0),
            ("test/missing", 0),
        ] {
            let object_key = ObjectKey::parse(key).unwrap();
            let result =
                super::object_byte_stream(ServerObjectStore::blackhole(), object_key, length).await;
            assert!(
                matches!(result, Err(ServerError::NotFound)),
                "{key}, length {length}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blackhole_object_range_streams_return_not_found() {
        for (key, length, start, end) in [
            ("test/key", 10, 0, 9),
            ("ab/any-key", 10, 0, 4),
            ("ab/s3-test", 10, 0, 9),
            ("test/blackhole-range", 10, 0, 9),
            ("test/alt-blackhole-range", 10, 0, 9),
        ] {
            let object_key = ObjectKey::parse(key).unwrap();
            let range = ByteRange::new(start, end).unwrap();
            let result = super::object_byte_range_stream(
                ServerObjectStore::blackhole(),
                object_key,
                length,
                range,
            )
            .await;
            assert!(
                matches!(result, Err(ServerError::NotFound)),
                "{key}, length {length}, range {start}..={end}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_range_exceeding_length() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-too-large").unwrap();
        let path = object_store.path_for_key(&object_key);
        write_fixture_file(&path, b"abcd").await;

        // range.end_inclusive = 10, total_length = 4 → RangeNotSatisfiable
        let range = ByteRange::new(0, 10).unwrap();
        let result = local_store_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
    }

    #[tokio::test]
    async fn local_range_rejects_truncation_during_length_validation() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let key = ObjectKey::parse("ab/truncated-validation").unwrap();
        let path = store.path_for_key(&key);
        let payload = b"content-to-be-truncated";
        write_fixture_file(&path, payload).await;
        let truncate_path = path.clone();
        set_before_local_object_read_hook(path, move || {
            std::fs::write(&truncate_path, b"tiny").unwrap();
        });
        let length = payload.len() as u64;
        let result = local_store_byte_range_stream(
            store,
            key,
            length,
            ByteRange::new(0, length - 1).unwrap(),
        )
        .await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test]
    async fn local_range_reports_unexpected_eof_after_stream_creation() {
        let storage = shardline_test_support::TempStorage::new();
        let store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let key = ObjectKey::parse("ab/truncated-read").unwrap();
        let path = store.path_for_key(&key);
        let payload = b"content-to-be-truncated";
        write_fixture_file(&path, payload).await;
        let length = payload.len() as u64;
        let mut stream = super::object_byte_range_stream(
            ServerObjectStore::Local(store),
            key,
            length,
            ByteRange::new(0, length - 1).unwrap(),
        )
        .await
        .unwrap();
        fs::write(&path, b"tiny").await.unwrap();
        assert_eq!(
            stream.try_next().await.unwrap().unwrap(),
            b"tiny".as_slice()
        );
        assert!(matches!(
            stream.try_next().await,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_rejects_length_mismatch() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-length-mismatch").unwrap();
        let path = object_store.path_for_key(&object_key);
        write_fixture_file(&path, b"short").await;

        let range = ByteRange::new(0, 3).unwrap();
        let result = local_store_byte_range_stream(object_store, object_key, 100, range).await;
        assert!(matches!(
            result,
            Err(ServerError::ObjectStore(
                ObjectStoreError::StoredLengthMismatch
            ))
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_range_end_exceeds_length() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-end-exceeds").unwrap();
        let path = object_store.path_for_key(&object_key);
        write_fixture_file(&path, b"abcd").await;

        // Range end_inclusive (10) >= total_length (4) → RangeNotSatisfiable
        let range = ByteRange::new(0, 10).unwrap();
        let result = local_store_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_object_byte_range_stream_range_end_equals_length_errors() {
        let storage = shardline_test_support::TempStorage::new();
        let object_store = LocalObjectStore::new(storage.path_buf()).unwrap();
        let object_key = ObjectKey::parse("ab/range-end-equals-length").unwrap();
        let path = object_store.path_for_key(&object_key);
        write_fixture_file(&path, b"abcd").await;

        // range.end_inclusive() == total_length → >= check triggers RangeNotSatisfiable
        let range = ByteRange::new(0, 4).unwrap();
        let result = local_store_byte_range_stream(object_store, object_key, 4, range).await;
        assert!(matches!(result, Err(ServerError::RangeNotSatisfiable)));
    }

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
            deduplication_domain: None,
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
            deduplication_domain: None,
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

    #[tokio::test]
    async fn local_range_entry_points_read_middle_and_boundary_bytes() {
        let large = vec![0xAB; super::STREAM_READ_BUFFER_BYTES as usize + 100];
        for (payload, start, end, expected) in [
            (
                large.as_slice(),
                0,
                large.len() as u64 - 1,
                large.as_slice(),
            ),
            (b"abcdefghijkl".as_slice(), 2, 7, b"cdefgh".as_slice()),
            (b"abcd".as_slice(), 0, 3, b"abcd".as_slice()),
            (b"abcdefghij".as_slice(), 3, 9, b"defghij".as_slice()),
            (
                b"full range content".as_slice(),
                0,
                17,
                b"full range content".as_slice(),
            ),
            (b"abcdef".as_slice(), 2, 2, b"c".as_slice()),
            (b"0123456789".as_slice(), 3, 7, b"34567".as_slice()),
            (b"abcdef".as_slice(), 0, 0, b"a".as_slice()),
            (b"abcdef".as_slice(), 5, 5, b"f".as_slice()),
            (b"x".as_slice(), 0, 0, b"x".as_slice()),
        ] {
            let storage = shardline_test_support::TempStorage::new();
            let store = LocalObjectStore::new(storage.path_buf()).unwrap();
            let key = ObjectKey::parse("ab/range").unwrap();
            write_fixture_file(&store.path_for_key(&key), payload).await;
            let length = u64::try_from(payload.len()).unwrap();
            let range = ByteRange::new(start, end).unwrap();
            for dispatch in [false, true] {
                let stream = if dispatch {
                    super::object_byte_range_stream(
                        ServerObjectStore::Local(store.clone()),
                        key.clone(),
                        length,
                        range,
                    )
                    .await
                } else {
                    local_store_byte_range_stream(store.clone(), key.clone(), length, range).await
                }
                .unwrap();
                let mut stream = stream;
                let mut observed = Vec::new();
                while let Some(chunk) = stream.try_next().await.unwrap() {
                    assert!(!chunk.is_empty());
                    assert!(chunk.len() as u64 <= super::STREAM_READ_BUFFER_BYTES);
                    observed.extend_from_slice(&chunk);
                }
                assert_eq!(
                    observed, expected,
                    "range {start}..={end}, dispatch={dispatch}"
                );
            }
        }
    }

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
            deduplication_domain: None,
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
            deduplication_domain: None,
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
        //    The xorb candidate path fetches and validates the whole object;
        //    the recorded chunk index selects the requested logical bytes.
        let entry = &packed.chunk_entries[0];
        let raw_len = content.len() as u64;
        let next_index = entry.chunk_index.checked_add(1).unwrap();
        let packed_end = entry
            .packed_offset
            .checked_add(entry.packed_length)
            .unwrap();
        let record = FileRecord {
            deduplication_domain: None,
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
            deduplication_domain: None,
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
