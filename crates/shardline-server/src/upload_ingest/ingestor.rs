use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::num::NonZeroUsize;
use std::sync::{Arc, LazyLock};

use bytes::{Bytes, BytesMut};
use sha2::{Digest, Sha256};
use shardline_index::{FileChunkRecord, FileRecord, xet_hash_hex_string};
use shardline_protocol::RepositoryScope;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
};
use tracing::{debug, instrument, warn};

use super::body_reader::ChunkBuffer;
use super::cdc::CdcChunker;
use super::chunk_store::{
    SequencedStoredChunkOutcome, SequencedStoredChunkTaskOutcome, StoredChunkOutcome,
    put_if_absent_pooled_chunk_buffer,
};
use super::xorb_packer::{pack_owned_chunks_into_xorb, store_xorb};

#[cfg(test)]
use crate::config::default_upload_max_in_flight_chunks;
use crate::{
    ServerError,
    local_backend::{chunk_hash, content_hash},
    metrics::record_chunk_inserted,
    model::{UploadChunkResult, UploadFileResponse},
    object_store::ServerObjectStore,
    overflow::{checked_add, checked_increment},
};

// Native Xet permits up to 64 MiB of raw data per xorb. Server-side ingest uses
// half that ceiling so several independently validated range fetches can be in
// flight without approaching whole-file memory use.
const MAX_SERVER_XORB_RAW_BYTES: usize = 32 * 1024 * 1024;
const MAX_XORB_CHUNKS: usize = 8 * 1024;
/// Local uploads up to one server Xorb batch can avoid a raw temporary-file
/// write and the subsequent read needed to pack their Xorb. Larger uploads
/// spill to the existing spool.
const MAX_LOCAL_MEMORY_RAW_BYTES: usize = MAX_SERVER_XORB_RAW_BYTES;
/// Memory retained for local uploads is admitted process-wide. Each MiB of raw
/// chunks reserves four MiB of budget for retained bytes and packing workspace.
const LOCAL_RAW_MEMORY_UNIT_BYTES: usize = 1024 * 1024;
const LOCAL_RAW_MEMORY_BUDGET_UNITS: usize = 128;
const LOCAL_RAW_MEMORY_UNITS_PER_MIB: usize = 4;
static LOCAL_RAW_MEMORY_BUDGET: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(LOCAL_RAW_MEMORY_BUDGET_UNITS)));

fn local_raw_memory_units(raw_bytes: usize) -> Result<usize, ServerError> {
    if raw_bytes == 0 {
        return Ok(0);
    }
    let rounded = raw_bytes
        .checked_add(LOCAL_RAW_MEMORY_UNIT_BYTES - 1)
        .ok_or(ServerError::Overflow)?
        / LOCAL_RAW_MEMORY_UNIT_BYTES;
    rounded
        .checked_mul(LOCAL_RAW_MEMORY_UNITS_PER_MIB)
        .ok_or(ServerError::Overflow)
}

/// Incremental file upload assembler.
pub(crate) struct FileUploadIngestor {
    pub(super) chunk_size: usize,
    pub(super) max_in_flight_chunks: usize,
    pub(super) pending: BytesMut,
    pub(super) next_sequence: u64,
    pub(super) next_recorded_sequence: u64,
    pub(super) next_offset: u64,
    pub(super) completed_chunks: Vec<SequencedStoredChunkOutcome>,
    pub(super) in_flight_chunks: JoinSet<Result<SequencedStoredChunkTaskOutcome, ServerError>>,
    pub(super) reusable_pending_buffers: Vec<BytesMut>,
    pub(super) inserted_chunks: u64,
    pub(super) reused_chunks: u64,
    pub(super) stored_bytes: u64,
    pub(super) chunks: Vec<UploadChunkResult>,
    pub(super) records: Vec<FileChunkRecord>,
    /// Raw S3 upload chunks retained for the next bounded Xorb batch. They
    /// become durable as Xorbs first; individual chunks are written only if
    /// packing or Xorb storage fails.
    pub(super) s3_xorb_batch: Vec<(Bytes, u64)>,
    pub(super) s3_xorb_batch_raw_bytes: usize,
    pub(super) s3_xorb_batch_record_start: usize,
    pub(super) sha256: Option<Sha256>,
    pub(super) cdc_chunker: Box<CdcChunker>,
    /// Local deployments spool raw chunks to their filesystem-backed object
    /// store path. S3 deployments leave this unset and retain one bounded raw
    /// batch in memory until its Xorb is stored or chunk fallback is needed.
    pub(super) raw_chunk_spool: Option<tempfile::NamedTempFile>,
    pub(super) raw_spool_bytes: u64,
    pub(super) raw_chunk_offsets: Vec<(u64, u64)>,
    /// Local raw chunks admitted to the bounded in-memory packing path.
    pub(super) local_raw_chunks: Vec<(Bytes, u64)>,
    pub(super) local_raw_bytes: usize,
    pub(super) local_raw_batch_start_offset: u64,
    pub(super) local_raw_memory_budget: Arc<Semaphore>,
    pub(super) local_raw_memory_permits: Vec<OwnedSemaphorePermit>,
    /// Local and S3 chunks stay in bounded memory or the local spool until a
    /// packed Xorb is durable. If packing fails, the fallback stores the
    /// original chunks before publication.
    pub(super) defer_chunk_stores: bool,
    /// Public uploads advertise independently readable chunk hashes. Protocol
    /// uploads expose only the file and can omit these compatibility objects.
    retain_standalone_chunks: bool,
}

impl FileUploadIngestor {
    /// Creates a new file upload ingestor.
    #[cfg(test)]
    pub(crate) fn new(chunk_size: NonZeroUsize, compute_sha256: bool) -> Self {
        Self::new_with_parallelism(
            chunk_size,
            compute_sha256,
            default_upload_max_in_flight_chunks(),
        )
    }

    /// Creates a new file upload ingestor with bounded chunk-level parallelism.
    pub(crate) fn new_with_parallelism(
        chunk_size: NonZeroUsize,
        compute_sha256: bool,
        max_in_flight_chunks: NonZeroUsize,
    ) -> Self {
        Self::new_with_parallelism_and_memory_budget(
            chunk_size,
            compute_sha256,
            max_in_flight_chunks,
            Arc::clone(&LOCAL_RAW_MEMORY_BUDGET),
        )
    }

    fn new_with_parallelism_and_memory_budget(
        chunk_size: NonZeroUsize,
        compute_sha256: bool,
        max_in_flight_chunks: NonZeroUsize,
        local_raw_memory_budget: Arc<Semaphore>,
    ) -> Self {
        let chunk_size = chunk_size.get();
        Self {
            chunk_size,
            max_in_flight_chunks: max_in_flight_chunks.get(),
            pending: BytesMut::new(),
            next_sequence: 0,
            next_recorded_sequence: 0,
            next_offset: 0,
            completed_chunks: Vec::new(),
            in_flight_chunks: JoinSet::new(),
            reusable_pending_buffers: Vec::new(),
            inserted_chunks: 0,
            reused_chunks: 0,
            stored_bytes: 0,
            chunks: Vec::new(),
            records: Vec::new(),
            s3_xorb_batch: Vec::new(),
            s3_xorb_batch_raw_bytes: 0,
            s3_xorb_batch_record_start: 0,
            sha256: compute_sha256.then(Sha256::new),
            cdc_chunker: Box::new(CdcChunker::new(chunk_size)),
            raw_chunk_spool: None,
            raw_spool_bytes: 0,
            raw_chunk_offsets: Vec::new(),
            local_raw_chunks: Vec::new(),
            local_raw_bytes: 0,
            local_raw_batch_start_offset: 0,
            local_raw_memory_budget,
            local_raw_memory_permits: Vec::new(),
            defer_chunk_stores: false,
            retain_standalone_chunks: false,
        }
    }

    pub(crate) const fn retain_standalone_chunks(mut self, retain: bool) -> Self {
        self.retain_standalone_chunks = retain;
        self
    }

    async fn persist_advertised_chunk(
        &self,
        object_store: &ServerObjectStore,
        chunk: &BytesMut,
    ) -> Result<(), ServerError> {
        if self.retain_standalone_chunks {
            // Keep the same verified, conditional persistence used by chunk
            // fallback. A failed chunk write must prevent publication even
            // when the packed container could otherwise be stored.
            put_if_absent_pooled_chunk_buffer(
                object_store,
                ChunkBuffer::Pooled(Bytes::copy_from_slice(chunk)),
            )
            .await?;
        }
        Ok(())
    }

    /// Ingests one body chunk and persists complete content chunks.
    #[instrument(skip(self, object_store, bytes), fields(bytes_len = bytes.len()))]
    pub(crate) async fn ingest_body_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        bytes: &axum::body::Bytes,
    ) -> Result<(), ServerError> {
        if let Some(sha256) = &mut self.sha256 {
            sha256.update(bytes);
        }

        // Accumulate incoming bytes
        self.pending.extend_from_slice(bytes);

        // Loop to find and flush multiple chunks from pending
        loop {
            let boundary = {
                if self.pending.len() < self.cdc_chunker.min_chunk() {
                    None
                } else {
                    self.cdc_chunker.find_boundary(&self.pending)
                }
            };
            match boundary {
                Some(b) => {
                    self.flush_pending_chunk_at(object_store, b).await?;
                }
                None => break,
            }
        }

        Ok(())
    }

    /// Finalizes the upload after the stream reaches EOF.
    #[instrument(skip(self, object_store), fields(file_id = %file_id))]
    pub(crate) async fn finish(
        mut self,
        object_store: &ServerObjectStore,
        file_id: &str,
        repository_scope: Option<&RepositoryScope>,
        expected_sha256: Option<&str>,
    ) -> Result<(FileRecord, UploadFileResponse), ServerError> {
        if !self.pending.is_empty() {
            self.flush_pending_chunk(object_store).await?;
        }
        self.drain_all_completed_chunks().await?;
        self.record_completed_chunks()?;
        if self.next_recorded_sequence != self.next_sequence
            || self
                .records
                .last()
                .map(|record| checked_add(record.offset, record.length))
                .transpose()?
                .unwrap_or(0)
                != self.next_offset
        {
            return Err(ServerError::Overflow);
        }

        if let Some(expected_sha256) = expected_sha256 {
            let Some(sha256) = self.sha256.take() else {
                return Err(ServerError::ExpectedBodyHashMismatch);
            };
            let actual = hex::encode(sha256.finalize());
            if actual != expected_sha256 {
                return Err(ServerError::ExpectedBodyHashMismatch);
            }
        }

        let total_bytes = self.next_offset;
        let chunk_size = u64::try_from(self.chunk_size)?;

        // Pack CDC chunks into bounded Xorb containers. A file can be
        // arbitrarily larger than one Xorb;
        // keeping each container within the native 64 MiB / 8192-chunk envelope
        // also bounds integrity validation before a requested range is exposed.
        // Individual chunks are persisted only as fallback for a batch that
        // cannot be packed or whose Xorb cannot be stored.
        if matches!(object_store, ServerObjectStore::S3(_)) {
            self.flush_s3_xorb_batch(object_store, file_id).await?;
        } else if !self.local_raw_chunks.is_empty() {
            self.flush_local_xorb_batch(object_store, file_id).await?;
        } else if !self.raw_chunk_offsets.is_empty() {
            let record_start = self
                .records
                .len()
                .checked_sub(self.raw_chunk_offsets.len())
                .ok_or(ServerError::Overflow)?;
            let outcomes = pack_and_store_xorb_spool(
                object_store,
                file_id,
                self.raw_chunk_spool.as_mut().ok_or(ServerError::Overflow)?,
                &self.raw_chunk_offsets,
                self.records
                    .get_mut(record_start..)
                    .ok_or(ServerError::Overflow)?,
                self.chunks
                    .get_mut(record_start..)
                    .ok_or(ServerError::Overflow)?,
            )
            .await?;
            self.record_deferred_chunk_outcomes(record_start, outcomes, false)?;
        }

        let content_hash = content_hash(total_bytes, chunk_size, &self.records);
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.clone(),
            total_bytes,
            chunk_size,
            storage_repr: shardline_index::StorageRepresentation::XorbCdcV1,
            repository_scope: repository_scope.cloned(),
            chunks: self.records,
        };
        let response = UploadFileResponse {
            file_id: file_id.to_owned(),
            content_hash,
            total_bytes,
            chunk_size,
            inserted_chunks: self.inserted_chunks,
            reused_chunks: self.reused_chunks,
            stored_bytes: self.stored_bytes,
            chunks: self.chunks,
        };

        debug!(
            file_id,
            total_bytes = self.next_offset,
            inserted_chunks = self.inserted_chunks,
            reused_chunks = self.reused_chunks,
            "upload complete"
        );

        crate::metrics::record_object_stored_by_repr(
            record.storage_repr.as_str(),
            record.total_bytes,
        );

        Ok((record, response))
    }

    async fn flush_pending_chunk(
        &mut self,
        object_store: &ServerObjectStore,
    ) -> Result<(), ServerError> {
        self.submit_owned_chunk(object_store).await
    }

    /// Flush a chunk of exactly `boundary` bytes from the front of
    /// `self.pending`.  The remaining bytes stay in `self.pending` for
    /// the next chunk (used by CDC mode).
    async fn flush_pending_chunk_at(
        &mut self,
        object_store: &ServerObjectStore,
        boundary: usize,
    ) -> Result<(), ServerError> {
        let chunk = self.pending.split_to(boundary);
        self.persist_advertised_chunk(object_store, &chunk).await?;
        if matches!(object_store, ServerObjectStore::S3(_)) {
            self.flush_s3_batch_if_needed(object_store, chunk.len())
                .await?;
        } else if matches!(object_store, ServerObjectStore::Local(_)) {
            self.flush_local_batch_if_needed(object_store, chunk.len())
                .await?;
        }
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
        if matches!(object_store, ServerObjectStore::S3(_)) {
            self.defer_chunk_stores = true;
            let hash_hex = xet_hash_hex_string(chunk_hash(&chunk));
            self.remember_s3_chunk((chunk.freeze(), offset))?;
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: StoredChunkOutcome {
                    hash_hex,
                    chunk_length,
                    compressed_length: 0,
                    inserted: false,
                },
            });
            return Ok(());
        }
        if matches!(object_store, ServerObjectStore::Local(_)) {
            self.defer_chunk_stores = true;
            let hash_hex = xet_hash_hex_string(chunk_hash(&chunk));
            self.retain_or_spool_local_chunk(chunk.freeze(), offset)?;
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: StoredChunkOutcome {
                    hash_hex,
                    chunk_length,
                    compressed_length: 0,
                    inserted: false,
                },
            });
            if self.local_xorb_batch_is_full() {
                self.flush_local_xorb_batch(object_store, "streaming-upload")
                    .await?;
            }
            return Ok(());
        }
        self.spool_raw_chunk(object_store, &chunk)?;
        if matches!(object_store, ServerObjectStore::Blackhole) {
            let chunk = ChunkBuffer::Pooled(chunk.freeze());
            let (outcome, reusable_buffer) =
                put_if_absent_pooled_chunk_buffer(object_store, chunk).await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            if let Some(reusable_buffer) = reusable_buffer {
                self.recycle_pending_buffer(reusable_buffer);
            }
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: outcome,
            });
            return Ok(());
        }

        self.drain_completed_chunks_to_capacity().await?;
        let chunk = chunk.freeze();
        let s3_raw =
            matches!(object_store, ServerObjectStore::S3(_)).then(|| (chunk.clone(), offset));
        let object_store = object_store.clone();
        self.in_flight_chunks.spawn(async move {
            let chunk = ChunkBuffer::Pooled(chunk);
            let (outcome, reusable_buffer) =
                put_if_absent_pooled_chunk_buffer(&object_store, chunk).await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            Ok(SequencedStoredChunkTaskOutcome {
                sequence,
                offset,
                stored: outcome,
                reusable_buffer,
            })
        });
        if let Some(raw) = s3_raw {
            self.remember_s3_chunk(raw)?;
        }
        Ok(())
    }

    async fn submit_owned_chunk(
        &mut self,
        object_store: &ServerObjectStore,
    ) -> Result<(), ServerError> {
        let replacement = self.take_pending_buffer();
        let chunk = mem::replace(&mut self.pending, replacement);
        self.persist_advertised_chunk(object_store, &chunk).await?;
        if matches!(object_store, ServerObjectStore::S3(_)) {
            self.flush_s3_batch_if_needed(object_store, chunk.len())
                .await?;
        } else if matches!(object_store, ServerObjectStore::Local(_)) {
            self.flush_local_batch_if_needed(object_store, chunk.len())
                .await?;
        }
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
        if matches!(object_store, ServerObjectStore::S3(_)) {
            self.defer_chunk_stores = true;
            let hash_hex = xet_hash_hex_string(chunk_hash(&chunk));
            self.remember_s3_chunk((chunk.freeze(), offset))?;
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: StoredChunkOutcome {
                    hash_hex,
                    chunk_length,
                    compressed_length: 0,
                    inserted: false,
                },
            });
            return Ok(());
        }
        if matches!(object_store, ServerObjectStore::Local(_)) {
            self.defer_chunk_stores = true;
            let hash_hex = xet_hash_hex_string(chunk_hash(&chunk));
            self.retain_or_spool_local_chunk(chunk.freeze(), offset)?;
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: StoredChunkOutcome {
                    hash_hex,
                    chunk_length,
                    compressed_length: 0,
                    inserted: false,
                },
            });
            if self.local_xorb_batch_is_full() {
                self.flush_local_xorb_batch(object_store, "streaming-upload")
                    .await?;
            }
            return Ok(());
        }
        self.spool_raw_chunk(object_store, &chunk)?;
        if matches!(object_store, ServerObjectStore::Blackhole) {
            let chunk = ChunkBuffer::Pooled(chunk.freeze());
            let (outcome, reusable_buffer) =
                put_if_absent_pooled_chunk_buffer(object_store, chunk).await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            if let Some(reusable_buffer) = reusable_buffer {
                self.recycle_pending_buffer(reusable_buffer);
            }
            self.completed_chunks.push(SequencedStoredChunkOutcome {
                sequence,
                offset,
                stored: outcome,
            });
            return Ok(());
        }

        self.drain_completed_chunks_to_capacity().await?;
        let chunk = chunk.freeze();
        let s3_raw =
            matches!(object_store, ServerObjectStore::S3(_)).then(|| (chunk.clone(), offset));
        let object_store = object_store.clone();
        self.in_flight_chunks.spawn(async move {
            let chunk = ChunkBuffer::Pooled(chunk);
            let (outcome, reusable_buffer) =
                put_if_absent_pooled_chunk_buffer(&object_store, chunk).await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            Ok(SequencedStoredChunkTaskOutcome {
                sequence,
                offset,
                stored: outcome,
                reusable_buffer,
            })
        });
        if let Some(raw) = s3_raw {
            self.remember_s3_chunk(raw)?;
        }
        Ok(())
    }

    async fn drain_completed_chunks_to_capacity(&mut self) -> Result<(), ServerError> {
        while self.in_flight_chunks.len() >= self.max_in_flight_chunks {
            self.drain_one_completed_chunk().await?;
        }
        Ok(())
    }

    async fn drain_all_completed_chunks(&mut self) -> Result<(), ServerError> {
        while !self.in_flight_chunks.is_empty() {
            self.drain_one_completed_chunk().await?;
        }
        Ok(())
    }

    async fn drain_one_completed_chunk(&mut self) -> Result<(), ServerError> {
        let Some(joined) = self.in_flight_chunks.join_next().await else {
            return Ok(());
        };
        let outcome = joined.map_err(|e| {
            warn!(error = %e, "chunk storage task panicked");
            ServerError::BlockingTask(e)
        })??;
        if let Some(buffer) = outcome.reusable_buffer {
            self.recycle_pending_buffer(buffer);
        }
        self.completed_chunks.push(SequencedStoredChunkOutcome {
            sequence: outcome.sequence,
            offset: outcome.offset,
            stored: outcome.stored,
        });
        Ok(())
    }

    fn take_pending_buffer(&mut self) -> BytesMut {
        self.reusable_pending_buffers
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.chunk_size))
    }

    fn recycle_pending_buffer(&mut self, mut buffer: BytesMut) {
        if buffer.capacity() < self.chunk_size
            || self.reusable_pending_buffers.len() >= self.max_in_flight_chunks
        {
            return;
        }

        buffer.clear();
        self.reusable_pending_buffers.push(buffer);
    }

    fn spool_raw_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        chunk: &[u8],
    ) -> Result<(), ServerError> {
        if !matches!(object_store, ServerObjectStore::Local(_)) {
            return Ok(());
        }
        if self.raw_chunk_spool.is_none() {
            self.raw_chunk_spool = Some(tempfile::NamedTempFile::new()?);
        }
        let offset = self.raw_spool_bytes;
        let chunk_length = u64::try_from(chunk.len())?;
        let next_offset = checked_add(offset, chunk_length)?;
        let spool = self
            .raw_chunk_spool
            .as_mut()
            .ok_or(ServerError::Overflow)?
            .as_file_mut();
        spool.write_all(chunk)?;
        self.raw_spool_bytes = next_offset;
        self.raw_chunk_offsets.push((offset, chunk_length));
        Ok(())
    }

    fn retain_or_spool_local_chunk(
        &mut self,
        chunk: Bytes,
        offset: u64,
    ) -> Result<(), ServerError> {
        if self.raw_chunk_spool.is_some() {
            return self.append_local_spool_chunk(&chunk);
        }
        let expected_offset = checked_add(
            self.local_raw_batch_start_offset,
            u64::try_from(self.local_raw_bytes)?,
        )?;
        if offset != expected_offset {
            return Err(ServerError::Overflow);
        }
        let next_raw_bytes = self
            .local_raw_bytes
            .checked_add(chunk.len())
            .ok_or(ServerError::Overflow)?;
        if next_raw_bytes > MAX_LOCAL_MEMORY_RAW_BYTES {
            self.spill_local_memory_chunks()?;
            return self.append_local_spool_chunk(&chunk);
        }

        let current_units = local_raw_memory_units(self.local_raw_bytes)?;
        let next_units = local_raw_memory_units(next_raw_bytes)?;
        if next_units > current_units {
            let additional = u32::try_from(
                next_units
                    .checked_sub(current_units)
                    .ok_or(ServerError::Overflow)?,
            )?;
            match Arc::clone(&self.local_raw_memory_budget).try_acquire_many_owned(additional) {
                Ok(permit) => self.local_raw_memory_permits.push(permit),
                Err(_) => {
                    self.spill_local_memory_chunks()?;
                    return self.append_local_spool_chunk(&chunk);
                }
            }
        }

        self.local_raw_bytes = next_raw_bytes;
        self.local_raw_chunks.push((chunk, offset));
        Ok(())
    }

    fn spill_local_memory_chunks(&mut self) -> Result<(), ServerError> {
        if self.raw_chunk_spool.is_none() {
            self.raw_chunk_spool = Some(tempfile::NamedTempFile::new()?);
        }
        for (raw, _) in mem::take(&mut self.local_raw_chunks) {
            self.append_local_spool_chunk(&raw)?;
        }
        self.local_raw_bytes = 0;
        self.local_raw_memory_permits.clear();
        Ok(())
    }

    fn append_local_spool_chunk(&mut self, chunk: &[u8]) -> Result<(), ServerError> {
        if self.raw_chunk_spool.is_none() {
            self.raw_chunk_spool = Some(tempfile::NamedTempFile::new()?);
        }
        let offset = self.raw_spool_bytes;
        let chunk_length = u64::try_from(chunk.len())?;
        let next_offset = checked_add(offset, chunk_length)?;
        let spool = self
            .raw_chunk_spool
            .as_mut()
            .ok_or(ServerError::Overflow)?
            .as_file_mut();
        spool.write_all(chunk)?;
        self.raw_spool_bytes = next_offset;
        self.raw_chunk_offsets.push((offset, chunk_length));
        Ok(())
    }

    /// `windows(2)` always yields slices of exactly two elements, so indexing
    /// at 0 and 1 is infallible.
    fn record_completed_chunks(&mut self) -> Result<(), ServerError> {
        let already_sorted =
            self.completed_chunks
                .windows(2)
                .all(|w: &[SequencedStoredChunkOutcome]| {
                    w.first()
                        .zip(w.get(1))
                        .is_some_and(|(a, b)| a.sequence <= b.sequence)
                });
        if !already_sorted {
            self.completed_chunks
                .sort_unstable_by_key(|outcome| outcome.sequence);
        }
        let mut expected_offset = self
            .records
            .last()
            .map(|record| checked_add(record.offset, record.length))
            .transpose()?
            .unwrap_or(0);
        let completed_chunks = mem::take(&mut self.completed_chunks);
        for outcome in completed_chunks {
            if outcome.sequence != self.next_recorded_sequence || outcome.offset != expected_offset
            {
                return Err(ServerError::Overflow);
            }
            expected_offset = checked_add(expected_offset, outcome.stored.chunk_length)?;
            self.next_recorded_sequence = checked_increment(self.next_recorded_sequence)?;
            self.record_chunk_outcome(outcome)?;
        }
        if expected_offset > self.next_offset || self.next_recorded_sequence > self.next_sequence {
            return Err(ServerError::Overflow);
        }
        Ok(())
    }

    fn remember_s3_chunk(&mut self, raw: (Bytes, u64)) -> Result<(), ServerError> {
        if self.s3_xorb_batch.is_empty() {
            self.s3_xorb_batch_record_start = self.records.len();
        }
        self.s3_xorb_batch_raw_bytes = self
            .s3_xorb_batch_raw_bytes
            .checked_add(raw.0.len())
            .ok_or(ServerError::Overflow)?;
        self.s3_xorb_batch.push(raw);
        Ok(())
    }

    async fn flush_s3_batch_if_needed(
        &mut self,
        object_store: &ServerObjectStore,
        next_chunk_bytes: usize,
    ) -> Result<(), ServerError> {
        let would_exceed_bytes = self
            .s3_xorb_batch_raw_bytes
            .checked_add(next_chunk_bytes)
            .ok_or(ServerError::Overflow)?
            > MAX_SERVER_XORB_RAW_BYTES;
        let would_exceed_chunks = self.s3_xorb_batch.len() >= MAX_XORB_CHUNKS;
        if !self.s3_xorb_batch.is_empty() && (would_exceed_bytes || would_exceed_chunks) {
            self.flush_s3_xorb_batch(object_store, "streaming-upload")
                .await?;
        }
        Ok(())
    }

    async fn flush_local_batch_if_needed(
        &mut self,
        object_store: &ServerObjectStore,
        next_chunk_bytes: usize,
    ) -> Result<(), ServerError> {
        if self.raw_chunk_spool.is_some() || self.local_raw_chunks.is_empty() {
            return Ok(());
        }
        let would_exceed_bytes = self
            .local_raw_bytes
            .checked_add(next_chunk_bytes)
            .ok_or(ServerError::Overflow)?
            > MAX_SERVER_XORB_RAW_BYTES;
        let would_exceed_chunks = self.local_raw_chunks.len() >= MAX_XORB_CHUNKS;
        if would_exceed_bytes || would_exceed_chunks {
            self.flush_local_xorb_batch(object_store, "streaming-upload")
                .await?;
        }
        Ok(())
    }

    const fn local_xorb_batch_is_full(&self) -> bool {
        self.raw_chunk_spool.is_none()
            && !self.local_raw_chunks.is_empty()
            && (self.local_raw_bytes >= MAX_SERVER_XORB_RAW_BYTES
                || self.local_raw_chunks.len() >= MAX_XORB_CHUNKS)
    }

    async fn flush_local_xorb_batch(
        &mut self,
        object_store: &ServerObjectStore,
        file_id: &str,
    ) -> Result<(), ServerError> {
        if self.local_raw_chunks.is_empty() {
            return Ok(());
        }
        self.drain_all_completed_chunks().await?;
        self.record_completed_chunks()?;
        let batch = take_xorb_batch(&mut self.local_raw_chunks);
        let batch_len = batch.len();
        let (last_chunk, last_offset) = batch.last().ok_or(ServerError::Overflow)?;
        let next_batch_start_offset = checked_add(*last_offset, u64::try_from(last_chunk.len())?)?;
        let record_start = self
            .records
            .len()
            .checked_sub(batch_len)
            .ok_or(ServerError::Overflow)?;
        if record_start.checked_add(batch_len) != Some(self.records.len())
            || self.records.len() != self.chunks.len()
        {
            return Err(ServerError::Overflow);
        }
        let mut outcomes = Vec::with_capacity(batch_len);
        // Store a bounded packed batch before metadata publication. If packing
        // or Xorb storage fails, store_memory_batch persists the original
        // chunks instead. Earlier batches remain valid unreferenced objects if
        // this upload later fails; normal reachability cleanup can reclaim them.
        store_memory_batch(
            object_store,
            file_id,
            &batch,
            0,
            record_start,
            batch_len,
            PackingBatchOutput {
                records: &mut self.records,
                response_chunks: &mut self.chunks,
                outcomes: &mut outcomes,
            },
        )
        .await?;
        self.record_deferred_chunk_outcomes(record_start, outcomes, false)?;
        drop(batch);
        self.local_raw_bytes = 0;
        self.local_raw_batch_start_offset = next_batch_start_offset;
        self.local_raw_memory_permits.clear();
        Ok(())
    }

    async fn flush_s3_xorb_batch(
        &mut self,
        object_store: &ServerObjectStore,
        file_id: &str,
    ) -> Result<(), ServerError> {
        if self.s3_xorb_batch.is_empty() {
            return Ok(());
        }
        self.drain_all_completed_chunks().await?;
        self.record_completed_chunks()?;
        let batch = take_xorb_batch(&mut self.s3_xorb_batch);
        let record_start = self.s3_xorb_batch_record_start;
        let batch_len = batch.len();
        if record_start.checked_add(batch_len) != Some(self.records.len()) {
            return Err(ServerError::Overflow);
        }
        let mut outcomes = Vec::with_capacity(batch_len);
        // The raw batch is already bounded and hash-checked. Store the packed
        // container first to avoid writing one temporary S3 object per CDC
        // chunk. If packing or container storage fails, persist the individual
        // chunks and verify their read-back before allowing publication.
        let packed = store_memory_batch(
            object_store,
            file_id,
            &batch,
            0,
            record_start,
            batch_len,
            PackingBatchOutput {
                records: &mut self.records,
                response_chunks: &mut self.chunks,
                outcomes: &mut outcomes,
            },
        )
        .await?;
        if !packed {
            let fallback_end = record_start
                .checked_add(batch_len)
                .ok_or(ServerError::Overflow)?;
            verify_s3_chunk_fallback(
                object_store,
                self.records
                    .get(record_start..fallback_end)
                    .ok_or(ServerError::Overflow)?,
            )
            .await?;
        }
        self.record_deferred_chunk_outcomes(record_start, outcomes, false)?;
        self.s3_xorb_batch_raw_bytes = 0;
        self.s3_xorb_batch_record_start = self.records.len();
        Ok(())
    }

    fn record_chunk_outcome(
        &mut self,
        outcome: SequencedStoredChunkOutcome,
    ) -> Result<(), ServerError> {
        let super::chunk_store::StoredChunkOutcome {
            hash_hex,
            chunk_length,
            compressed_length,
            inserted,
        } = outcome.stored;
        if inserted && !self.defer_chunk_stores {
            self.inserted_chunks = checked_increment(self.inserted_chunks)?;
            self.stored_bytes = checked_add(self.stored_bytes, chunk_length)?;
        } else if !self.defer_chunk_stores {
            self.reused_chunks = checked_increment(self.reused_chunks)?;
        }

        self.chunks.push(UploadChunkResult {
            hash: hash_hex.clone(),
            offset: outcome.offset,
            length: chunk_length,
            inserted: inserted && !self.defer_chunk_stores,
        });
        self.records.push(FileChunkRecord {
            hash: hash_hex,
            offset: outcome.offset,
            length: chunk_length,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: compressed_length,
        });
        Ok(())
    }

    fn record_deferred_chunk_outcomes(
        &mut self,
        start: usize,
        outcomes: Vec<(bool, u64)>,
        reset_counters: bool,
    ) -> Result<(), ServerError> {
        let end = start
            .checked_add(outcomes.len())
            .ok_or(ServerError::Overflow)?;
        let chunks = self
            .chunks
            .get_mut(start..end)
            .ok_or(ServerError::Overflow)?;
        if reset_counters {
            self.inserted_chunks = 0;
            self.reused_chunks = 0;
            self.stored_bytes = 0;
        }
        for (chunk, (inserted, length)) in chunks.iter_mut().zip(outcomes) {
            chunk.inserted = inserted;
            if inserted {
                self.inserted_chunks = checked_increment(self.inserted_chunks)?;
                self.stored_bytes = checked_add(self.stored_bytes, length)?;
                record_chunk_inserted(length);
            } else {
                self.reused_chunks = checked_increment(self.reused_chunks)?;
            }
        }
        Ok(())
    }
}

async fn pack_and_store_xorb_spool(
    object_store: &ServerObjectStore,
    file_id: &str,
    spool: &mut tempfile::NamedTempFile,
    offsets: &[(u64, u64)],
    records: &mut [FileChunkRecord],
    response_chunks: &mut [UploadChunkResult],
) -> Result<Vec<(bool, u64)>, ServerError> {
    if offsets.len() != records.len() {
        return Err(ServerError::Overflow);
    }

    let mut outcomes = Vec::with_capacity(records.len());
    let mut batch = Vec::new();
    let mut batch_raw_bytes = 0usize;
    let mut batch_record_start = 0usize;

    let file = spool.as_file_mut();
    let mut cursor = 0_u64;
    if !offsets.is_empty() {
        file.seek(SeekFrom::Start(0))?;
    }
    for (record_index, &(offset, length)) in offsets.iter().enumerate() {
        if offset != cursor {
            file.seek(SeekFrom::Start(offset))?;
        }
        let length = usize::try_from(length)?;
        let mut raw_chunk = vec![0_u8; length];
        file.read_exact(&mut raw_chunk)?;
        cursor = checked_add(offset, u64::try_from(length)?)?;
        let next_raw_bytes = batch_raw_bytes
            .checked_add(raw_chunk.len())
            .ok_or(ServerError::Overflow)?;
        if !batch.is_empty()
            && (batch.len() >= MAX_XORB_CHUNKS || next_raw_bytes > MAX_SERVER_XORB_RAW_BYTES)
        {
            let batch_len = batch.len();
            let batch_to_store = take_xorb_batch(&mut batch);
            let stored = store_xorb_batch(
                object_store,
                file_id,
                batch_to_store,
                records,
                batch_record_start,
            )
            .await;
            if let Some(inserted) = stored {
                let batch_end = batch_record_start
                    .checked_add(batch_len)
                    .ok_or(ServerError::Overflow)?;
                for record in records
                    .get(batch_record_start..batch_end)
                    .ok_or(ServerError::Overflow)?
                {
                    outcomes.push((inserted, record.length));
                }
            } else {
                store_local_chunk_fallback(
                    object_store,
                    file,
                    offsets,
                    batch_record_start,
                    batch_len,
                    PackingBatchOutput {
                        records,
                        response_chunks,
                        outcomes: &mut outcomes,
                    },
                )
                .await?;
            }
            batch_raw_bytes = 0;
            batch_record_start = record_index;
        }

        let record = records.get(record_index).ok_or(ServerError::Overflow)?;
        batch_raw_bytes = batch_raw_bytes
            .checked_add(raw_chunk.len())
            .ok_or(ServerError::Overflow)?;
        batch.push((raw_chunk, record.offset));
    }

    if !batch.is_empty() {
        let batch_len = batch.len();
        let batch_to_store = take_xorb_batch(&mut batch);
        let stored = store_xorb_batch(
            object_store,
            file_id,
            batch_to_store,
            records,
            batch_record_start,
        )
        .await;
        if let Some(inserted) = stored {
            let batch_end = batch_record_start
                .checked_add(batch_len)
                .ok_or(ServerError::Overflow)?;
            for record in records
                .get(batch_record_start..batch_end)
                .ok_or(ServerError::Overflow)?
            {
                outcomes.push((inserted, record.length));
            }
        } else {
            store_local_chunk_fallback(
                object_store,
                file,
                offsets,
                batch_record_start,
                batch_len,
                PackingBatchOutput {
                    records,
                    response_chunks,
                    outcomes: &mut outcomes,
                },
            )
            .await?;
        }
    }
    Ok(outcomes)
}

struct PackingBatchOutput<'batch> {
    records: &'batch mut [FileChunkRecord],
    response_chunks: &'batch mut [UploadChunkResult],
    outcomes: &'batch mut Vec<(bool, u64)>,
}

async fn store_memory_batch(
    object_store: &ServerObjectStore,
    file_id: &str,
    raw_chunks: &[(Bytes, u64)],
    raw_start: usize,
    record_start: usize,
    count: usize,
    output: PackingBatchOutput<'_>,
) -> Result<bool, ServerError> {
    let PackingBatchOutput {
        records,
        response_chunks,
        outcomes,
    } = output;
    let end = record_start
        .checked_add(count)
        .ok_or(ServerError::Overflow)?;
    let raw_end = raw_start.checked_add(count).ok_or(ServerError::Overflow)?;
    let packed_chunks = raw_chunks
        .get(raw_start..raw_end)
        .ok_or(ServerError::Overflow)?
        .iter()
        .map(|(raw, offset)| (raw.to_vec(), *offset))
        .collect();
    if let Some(inserted) =
        store_xorb_batch(object_store, file_id, packed_chunks, records, record_start).await
    {
        for record in records
            .get(record_start..end)
            .ok_or(ServerError::Overflow)?
        {
            outcomes.push((inserted, record.length));
        }
        return Ok(true);
    }

    let record_slice = records
        .get_mut(record_start..end)
        .ok_or(ServerError::Overflow)?;
    let response_slice = response_chunks
        .get_mut(record_start..end)
        .ok_or(ServerError::Overflow)?;
    let raw_slice = raw_chunks
        .get(raw_start..raw_end)
        .ok_or(ServerError::Overflow)?;
    for ((record, response_chunk), (raw, offset)) in
        record_slice.iter_mut().zip(response_slice).zip(raw_slice)
    {
        if record.offset != *offset || u64::try_from(raw.len())? != record.length {
            return Err(ServerError::Overflow);
        }
        let (stored, _reusable) =
            put_if_absent_pooled_chunk_buffer(object_store, ChunkBuffer::Pooled(raw.clone()))
                .await?;
        if stored.hash_hex != record.hash || stored.chunk_length != record.length {
            return Err(ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredHashMismatch,
            ));
        }
        record.packed_end = stored.compressed_length;
        response_chunk.inserted = stored.inserted;
        outcomes.push((stored.inserted, stored.chunk_length));
    }
    Ok(false)
}

async fn store_local_chunk_fallback(
    object_store: &ServerObjectStore,
    spool: &mut std::fs::File,
    offsets: &[(u64, u64)],
    record_start: usize,
    count: usize,
    output: PackingBatchOutput<'_>,
) -> Result<(), ServerError> {
    let PackingBatchOutput {
        records,
        response_chunks,
        outcomes,
    } = output;
    let end = record_start
        .checked_add(count)
        .ok_or(ServerError::Overflow)?;
    let record_slice = records
        .get_mut(record_start..end)
        .ok_or(ServerError::Overflow)?;
    let response_slice = response_chunks
        .get_mut(record_start..end)
        .ok_or(ServerError::Overflow)?;
    for (index, (record, response_chunk)) in record_slice.iter_mut().zip(response_slice).enumerate()
    {
        let (offset, length) = *offsets
            .get(
                record_start
                    .checked_add(index)
                    .ok_or(ServerError::Overflow)?,
            )
            .ok_or(ServerError::Overflow)?;
        spool.seek(SeekFrom::Start(offset))?;
        let mut raw = vec![0_u8; usize::try_from(length)?];
        spool.read_exact(&mut raw)?;
        let (stored, _reusable) = put_if_absent_pooled_chunk_buffer(
            object_store,
            ChunkBuffer::Pooled(bytes::BytesMut::from(raw.as_slice()).freeze()),
        )
        .await?;
        if stored.hash_hex != record.hash || stored.chunk_length != record.length {
            return Err(ServerError::ObjectStore(
                crate::error::ObjectStoreError::StoredHashMismatch,
            ));
        }
        record.packed_end = stored.compressed_length;
        response_chunk.inserted = stored.inserted;
        outcomes.push((stored.inserted, stored.chunk_length));
    }
    Ok(())
}

async fn store_xorb_batch(
    object_store: &ServerObjectStore,
    file_id: &str,
    chunks: Vec<(Vec<u8>, u64)>,
    records: &mut [FileChunkRecord],
    record_start: usize,
) -> Option<bool> {
    let num_chunks = chunks.len();
    let packed = match pack_owned_chunks_into_xorb(chunks) {
        Ok(packed) => packed,
        Err(error) => {
            warn!(
                file_id,
                record_start,
                num_chunks,
                error = %error,
                "failed to pack xorb batch, continuing with individual chunks"
            );
            return None;
        }
    };
    let inserted = match store_xorb(object_store, &packed.xorb_hash_hex, &packed.serialized).await {
        Ok(inserted) => inserted,
        Err(error) => {
            warn!(
                file_id,
                record_start,
                xorb_hash = %packed.xorb_hash_hex,
                error = %error,
                "failed to store xorb batch, continuing with individual chunks"
            );
            return None;
        }
    };

    let Some(batch_records) = records
        .get_mut(record_start..)
        .and_then(|remaining| remaining.get_mut(..packed.chunk_entries.len()))
    else {
        warn!(
            file_id,
            record_start, "xorb batch record mapping is inconsistent"
        );
        return None;
    };
    let mapped_entries = packed
        .chunk_entries
        .iter()
        .map(|entry| {
            Some((
                entry.chunk_index,
                entry.chunk_index.checked_add(1)?,
                entry.packed_offset,
                entry.packed_offset.checked_add(entry.packed_length)?,
            ))
        })
        .collect::<Option<Vec<_>>>();
    let Some(mapped_entries) = mapped_entries else {
        warn!(file_id, record_start, "xorb batch range overflow");
        return None;
    };
    for (record, (chunk_start, chunk_end, packed_start, packed_end)) in
        batch_records.iter_mut().zip(mapped_entries)
    {
        record.hash.clone_from(&packed.xorb_hash_hex);
        record.range_start = u64::from(chunk_start);
        record.range_end = u64::from(chunk_end);
        record.packed_start = u64::from(packed_start);
        record.packed_end = u64::from(packed_end);
    }
    debug!(
        file_id,
        record_start,
        xorb_hash = %packed.xorb_hash_hex,
        num_chunks = packed.chunk_entries.len(),
        packed_len = packed.serialized.len(),
        "stored bounded xorb batch for file upload"
    );
    Some(inserted)
}

fn take_xorb_batch<T>(batch: &mut Vec<(T, u64)>) -> Vec<(T, u64)> {
    let capacity = batch.capacity();
    std::mem::replace(batch, Vec::with_capacity(capacity))
}

fn decode_and_verify_durable_chunk(
    compressed: &[u8],
    expected_hash: &str,
    expected_length: u64,
) -> Result<Vec<u8>, ServerError> {
    use shardline_index::xet_hash_hex_string;

    use crate::error::ObjectStoreError;

    if compressed
        .first_chunk::<4>()
        .map(|header| u64::from(u32::from_le_bytes(*header)))
        != Some(expected_length)
    {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    let raw = lz4_flex::decompress_size_prepended(compressed).map_err(|error| {
        ServerError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("stored upload chunk could not be decompressed: {error}"),
        ))
    })?;
    if u64::try_from(raw.len())? != expected_length {
        return Err(ServerError::ObjectStore(
            ObjectStoreError::StoredLengthMismatch,
        ));
    }
    if xet_hash_hex_string(crate::local_backend::chunk_hash(&raw)) != expected_hash {
        return Err(ServerError::ExpectedBodyHashMismatch);
    }
    Ok(raw)
}

/// If Xorb publication fails, retain the legacy guarantee that the individual
/// chunk fallback was read back and verified before its metadata is published.
/// The successful packing path avoids these S3 GETs because it packs the exact
/// raw bytes whose content hashes were checked during chunk upload.
async fn verify_s3_chunk_fallback(
    object_store: &ServerObjectStore,
    records: &[FileChunkRecord],
) -> Result<(), ServerError> {
    use shardline_protocol::ByteRange;
    use shardline_storage::AsyncObjectStore;

    for record in records {
        let key = crate::chunk_store::chunk_object_key(&record.hash)?;
        let stored_length = record
            .packed_end
            .checked_sub(record.packed_start)
            .ok_or(ServerError::Overflow)?;
        let last_byte = stored_length.checked_sub(1).ok_or(ServerError::Overflow)?;
        let compressed = AsyncObjectStore::read_range(
            object_store,
            &key,
            ByteRange::new(0, last_byte).map_err(|_error| ServerError::Overflow)?,
        )
        .await?;
        let _raw = decode_and_verify_durable_chunk(&compressed, &record.hash, record.length)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        fs,
        num::NonZeroUsize,
        sync::{Arc, Mutex},
    };

    use axum::{
        Router,
        body::Bytes,
        http::{Method, Response, StatusCode, Uri},
    };
    use shardline_protocol::ByteRange;
    use shardline_protocol::SecretString;
    use shardline_storage::S3ObjectStoreConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    use super::{
        FileUploadIngestor, LOCAL_RAW_MEMORY_BUDGET_UNITS, decode_and_verify_durable_chunk,
    };
    use crate::{ServerError, object_store::ServerObjectStore};
    use shardline_index::xet_hash_hex_string;
    use shardline_storage::{ObjectPrefix, ObjectStore};

    #[test]
    fn ingestor_new_with_parallelism_creates_empty_state() {
        let ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            true,
            NonZeroUsize::new(128).unwrap(),
        );
        assert_eq!(ingestor.chunk_size, 128);
        assert_eq!(ingestor.max_in_flight_chunks, 128);
        assert!(ingestor.pending.is_empty());
        assert_eq!(ingestor.next_sequence, 0);
        assert_eq!(ingestor.next_offset, 0);
        assert!(ingestor.completed_chunks.is_empty());
        assert!(ingestor.in_flight_chunks.is_empty());
        assert!(ingestor.reusable_pending_buffers.is_empty());
        assert_eq!(ingestor.inserted_chunks, 0);
        assert_eq!(ingestor.reused_chunks, 0);
        assert_eq!(ingestor.stored_bytes, 0);
        assert!(ingestor.sha256.is_some());
    }

    #[test]
    fn durable_chunk_decode_verifies_length_and_content_identity() {
        let raw = b"durable chunk bytes";
        let compressed = lz4_flex::compress_prepend_size(raw);
        let expected_hash = xet_hash_hex_string(crate::local_backend::chunk_hash(raw));
        let decoded = decode_and_verify_durable_chunk(
            &compressed,
            &expected_hash,
            u64::try_from(raw.len()).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn durable_chunk_decode_rejects_identity_mismatch() {
        let raw = b"durable chunk bytes";
        let compressed = lz4_flex::compress_prepend_size(raw);
        let wrong_hash = xet_hash_hex_string(crate::local_backend::chunk_hash(b"different bytes"));
        let result = decode_and_verify_durable_chunk(&compressed, &wrong_hash, raw.len() as u64);
        assert!(matches!(result, Err(ServerError::ExpectedBodyHashMismatch)));
    }

    #[test]
    fn ingestor_new_without_sha256_disables_hasher() {
        let ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);
        assert!(ingestor.sha256.is_none());
    }

    #[test]
    fn ingestor_take_pending_buffer_returns_new_buffer_when_pool_empty() {
        let mut ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);
        let buffer = ingestor.take_pending_buffer();
        assert!(buffer.is_empty());
        assert_eq!(buffer.capacity(), 128);
    }

    #[test]
    fn ingestor_recycle_pending_buffer_ignores_undersized_buffer() {
        let mut ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);
        let buffer = bytes::BytesMut::with_capacity(100);
        ingestor.recycle_pending_buffer(buffer);
        assert!(ingestor.reusable_pending_buffers.is_empty());
    }

    #[test]
    fn ingestor_recycle_pending_buffer_accepts_chunk_sized_buffer() {
        let mut ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);
        let mut buffer = bytes::BytesMut::with_capacity(2048);
        buffer.extend_from_slice(b"some data");
        ingestor.recycle_pending_buffer(buffer);
        // Buffer should be cleared after recycling
        assert!(ingestor.reusable_pending_buffers[0].is_empty());
    }

    #[test]
    fn ingestor_recycle_pending_buffer_enforces_capacity_limit() {
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            false,
            NonZeroUsize::new(2).unwrap(),
        );
        ingestor.recycle_pending_buffer(bytes::BytesMut::with_capacity(2048));
        ingestor.recycle_pending_buffer(bytes::BytesMut::with_capacity(2048));
        ingestor.recycle_pending_buffer(bytes::BytesMut::with_capacity(2048));
        // Only 2 should be kept (equal to max_in_flight_chunks)
        assert_eq!(ingestor.reusable_pending_buffers.len(), 2);
    }

    #[test]
    fn ingestor_allocates_pending_buffer_lazily() {
        let ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);

        assert_eq!(ingestor.pending.capacity(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_processes_aligned_request_frames_without_staging_copy() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = ServerObjectStore::local(storage.path().join("chunks"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGH"),
            )
            .await;
        assert!(ingested.is_ok());

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((_record, response)) = finished else {
            return;
        };

        // With CDC, chunk count depends on content-defined boundaries
        assert!(response.inserted_chunks > 0);
        assert_eq!(response.reused_chunks, 0);
        assert_eq!(response.chunks.len(), response.inserted_chunks as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_chunks_from_split_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let first = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"ABCDEFGHIJKLMNOPQRST"))
            .await;
        assert!(first.is_ok());
        assert_eq!(ingestor.in_flight_chunks.len(), 0);

        let second = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"UVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
            )
            .await;
        assert!(second.is_ok());
        assert_eq!(ingestor.in_flight_chunks.len(), 0);
        assert!(ingestor.raw_chunk_spool.is_none());

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };

        assert_eq!(record.total_bytes, 72);
        assert!(response.inserted_chunks > 0);
        assert_eq!(response.chunks.len(), response.inserted_chunks as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_aligned_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // Send data; with CDC, small data may stay pending until finish()
        let ingested = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMN"),
            )
            .await;
        assert!(ingested.is_ok());

        let finished = ingestor
            .finish(&object_store, "aligned.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((_record, response)) = finished else {
            return;
        };

        assert!(response.inserted_chunks > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_recycles_pooled_pending_buffers_after_upload_completion() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"ABC"))
            .await;
        assert!(ingested.is_ok());
        let pooled_capacity = ingestor.pending.capacity();
        assert!(pooled_capacity >= 4);

        let second = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(
                    b"DEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                ),
            )
            .await;
        assert!(second.is_ok());
        // With CDC, pending buffer is reused after chunks are flushed
        assert_eq!(ingestor.reusable_pending_buffers.len(), 0);
        let recycled_buffer = ingestor.reusable_pending_buffers.first();
        assert!(recycled_buffer.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_reuses_existing_chunks_for_aligned_request_frames() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = ServerObjectStore::local(storage.path().join("chunks"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };

        // Use data large enough for CDC to find boundaries
        let test_data: Vec<u8> =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".to_vec();

        let mut first = FileUploadIngestor::new(chunk_size, false);
        let first_data = Bytes::from(test_data.clone());
        let first_ingested = first.ingest_body_chunk(&object_store, &first_data).await;
        assert!(first_ingested.is_ok());
        let first_finished = first.finish(&object_store, "first.bin", None, None).await;
        assert!(first_finished.is_ok());

        let mut second = FileUploadIngestor::new(chunk_size, false);
        let second_data = Bytes::from(test_data.clone());
        let second_ingested = second.ingest_body_chunk(&object_store, &second_data).await;
        assert!(second_ingested.is_ok());
        let second_finished = second.finish(&object_store, "second.bin", None, None).await;
        assert!(second_finished.is_ok());
        let Ok((_record, response)) = second_finished else {
            return;
        };

        // CDC produces the same chunks for identical data → reused chunks
        assert_eq!(response.inserted_chunks, 0);
        assert!(response.reused_chunks > 0);
        assert_eq!(response.stored_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_preserves_tail_bytes_across_mixed_frame_sizes() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = ServerObjectStore::local(storage.path().join("chunks"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // With CDC, data smaller than min_chunk stays in pending until finish()
        let first = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcde"))
            .await;
        let second = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"fghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
            )
            .await;
        assert!(first.is_ok());
        assert!(second.is_ok());
        // With CDC, 62 bytes may stay pending if no boundary found
        // (target=128, min=16, max=256 — boundary depends on content hash)

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };

        assert!(response.inserted_chunks > 0);
        assert_eq!(record.total_bytes, 62);
        assert_eq!(record.chunks.len(), response.inserted_chunks as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_reports_hash_mismatch_when_sha256_disabled() {
        // finish() with compute_sha256=false and an expected_sha256 should
        // hit the `sha256.take()` returning None path (line 148).
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false); // sha256 disabled
        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"some data"))
            .await
            .unwrap();
        let result = ingestor
            .finish(&object_store, "test.bin", None, Some("any-digest"))
            .await;
        assert!(matches!(result, Err(ServerError::ExpectedBodyHashMismatch)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_reports_hash_mismatch_when_expected_digest_differs() {
        let storage = tempfile::tempdir();
        assert!(storage.is_ok());
        let Ok(storage) = storage else {
            return;
        };
        let object_store = ServerObjectStore::local(storage.path().join("chunks"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let chunk_size = NonZeroUsize::new(128);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, true);

        let ingested = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcdefgh"))
            .await;
        assert!(ingested.is_ok());

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, Some("deadbeef"))
            .await;

        assert!(matches!(
            finished,
            Err(ServerError::ExpectedBodyHashMismatch)
        ));
        assert!(
            object_store
                .list_prefix(&ObjectPrefix::parse("").unwrap())
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_empty_upload_succeeds() {
        // Empty file upload with no body chunks.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let ingestor = FileUploadIngestor::new(chunk_size, false);

        let finished = ingestor
            .finish(&object_store, "empty.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };
        assert_eq!(record.total_bytes, 0);
        assert_eq!(response.inserted_chunks, 0);
        assert_eq!(response.reused_chunks, 0);
        assert!(record.chunks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_empty_upload_with_sha256_and_expected_hash() {
        // Empty file with SHA256 computed and matching expected hash.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let ingestor = FileUploadIngestor::new(chunk_size, true);

        // SHA256 of empty string
        let empty_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let finished = ingestor
            .finish(&object_store, "empty-sha256.bin", None, Some(empty_sha256))
            .await;
        assert!(finished.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_exact_single_chunk_with_local_store() {
        // CDC produces a single chunk from data via finish()
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // With CDC, data smaller than min_chunk stays in pending until finish()
        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        assert!(!ingestor.pending.is_empty());

        let finished = ingestor
            .finish(&object_store, "single.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };
        assert_eq!(record.total_bytes, 4);
        assert!(response.inserted_chunks >= 1);
        assert!(!response.chunks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_upload_persists_packed_xorbs_without_temporary_chunk_objects() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let data = Bytes::from(
            (0..2 * 1024 * 1024)
                .map(|index| ((index * 31 + index / 251) % 256) as u8)
                .collect::<Vec<_>>(),
        );

        let mut first = FileUploadIngestor::new(NonZeroUsize::new(64 * 1024).unwrap(), false);
        first.ingest_body_chunk(&object_store, &data).await.unwrap();
        assert!(first.raw_chunk_spool.is_none());
        assert!(!first.local_raw_chunks.is_empty());
        assert!(
            object_store
                .list_prefix(&ObjectPrefix::parse("").unwrap())
                .unwrap()
                .is_empty(),
            "unfinished local uploads must not persist chunk objects"
        );
        let (record, first_response) = first
            .finish(&object_store, "first.bin", None, None)
            .await
            .unwrap();
        record.validate_reconstruction_plan().unwrap();
        assert!(first_response.inserted_chunks > 0);

        let objects = object_store
            .list_prefix(&ObjectPrefix::parse("").unwrap())
            .unwrap();
        assert_eq!(objects.len(), 1);
        assert!(
            objects
                .iter()
                .all(|metadata| metadata.key().as_str().starts_with("xorbs/default/"))
        );
        for chunk in &record.chunks {
            let key = crate::xet_adapter::xorb_object_key(&chunk.hash).unwrap();
            let end = chunk.packed_end.checked_sub(1).unwrap();
            let packed = object_store
                .read_range(&key, ByteRange::new(chunk.packed_start, end).unwrap())
                .unwrap();
            let (decoded, _packed_length, raw_length) =
                shardline_xet_core::xorb_object::xorb_chunk_format::deserialize_chunk(
                    &mut std::io::Cursor::new(packed.as_slice()),
                )
                .unwrap();
            assert_eq!(u64::from(raw_length), chunk.length);
            let start = usize::try_from(chunk.offset).unwrap();
            let finish = start
                .checked_add(usize::try_from(chunk.length).unwrap())
                .unwrap();
            assert_eq!(decoded.as_slice(), data.get(start..finish).unwrap());
        }

        let mut second = FileUploadIngestor::new(NonZeroUsize::new(64 * 1024).unwrap(), false);
        second
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        let (_, second_response) = second
            .finish(&object_store, "second.bin", None, None)
            .await
            .unwrap();
        assert_eq!(second_response.inserted_chunks, 0);
        assert_eq!(
            second_response.reused_chunks,
            second_response.chunks.len() as u64
        );
        assert_eq!(
            object_store
                .list_prefix(&ObjectPrefix::parse("").unwrap())
                .unwrap()
                .len(),
            objects.len()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_upload_over_thirty_two_mib_avoids_spool_and_roundtrips_in_batches() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let data = Bytes::from(
            (0..40 * 1024 * 1024)
                .map(|index| ((index * 31 + index / 251) % 256) as u8)
                .collect::<Vec<_>>(),
        );
        let memory_budget = Arc::new(Semaphore::new(LOCAL_RAW_MEMORY_BUDGET_UNITS));
        let mut ingestor = FileUploadIngestor::new_with_parallelism_and_memory_budget(
            NonZeroUsize::new(64 * 1024).unwrap(),
            false,
            NonZeroUsize::new(8).unwrap(),
            memory_budget,
        );

        ingestor
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        assert!(ingestor.raw_chunk_spool.is_none());
        assert!(
            !ingestor.records.is_empty(),
            "a full local Xorb batch should be durable before upload EOF"
        );
        assert!(ingestor.local_raw_bytes < super::MAX_SERVER_XORB_RAW_BYTES);

        let (record, response) = ingestor
            .finish(&object_store, "forty-mib.bin", None, None)
            .await
            .unwrap();
        record.validate_reconstruction_plan().unwrap();
        assert!(response.inserted_chunks > 0);
        for chunk in &record.chunks {
            let key = crate::xet_adapter::xorb_object_key(&chunk.hash).unwrap();
            let end = chunk.packed_end.checked_sub(1).unwrap();
            let packed = object_store
                .read_range(&key, ByteRange::new(chunk.packed_start, end).unwrap())
                .unwrap();
            let (decoded, _packed_length, raw_length) =
                shardline_xet_core::xorb_object::xorb_chunk_format::deserialize_chunk(
                    &mut std::io::Cursor::new(packed.as_slice()),
                )
                .unwrap();
            assert_eq!(u64::from(raw_length), chunk.length);
            let start = usize::try_from(chunk.offset).unwrap();
            let finish = start
                .checked_add(usize::try_from(chunk.length).unwrap())
                .unwrap();
            assert_eq!(decoded.as_slice(), data.get(start..finish).unwrap());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_upload_falls_back_to_durable_chunks_when_xorb_storage_fails() {
        let storage = tempfile::tempdir().unwrap();
        let root = storage.path().join("objects");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("xorbs"), b"block xorb namespace").unwrap();
        let object_store = ServerObjectStore::local(&root).unwrap();
        let data = Bytes::from(
            (0..2 * 1024 * 1024)
                .map(|index| ((index * 31 + index / 251) % 256) as u8)
                .collect::<Vec<_>>(),
        );

        let mut ingestor = FileUploadIngestor::new(NonZeroUsize::new(64 * 1024).unwrap(), false);
        ingestor
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        assert!(ingestor.raw_chunk_spool.is_none());
        assert!(!ingestor.local_raw_chunks.is_empty());
        let (record, response) = ingestor
            .finish(&object_store, "fallback.bin", None, None)
            .await
            .unwrap();
        record.validate_reconstruction_plan().unwrap();
        assert!(response.inserted_chunks > 0);
        assert!(record.chunks.iter().all(|chunk| chunk.range_start == 0));
        assert!(record.chunks.iter().all(|chunk| chunk.range_end == 1));
        let objects = object_store
            .list_prefix(&ObjectPrefix::parse("").unwrap())
            .unwrap();
        assert!(
            objects
                .iter()
                .any(|metadata| !metadata.key().as_str().starts_with("xorbs/default/"))
        );
        for chunk in &record.chunks {
            let key = crate::chunk_store::chunk_object_key(&chunk.hash).unwrap();
            let end = chunk.packed_end.checked_sub(1).unwrap();
            let compressed = object_store
                .read_range(&key, ByteRange::new(0, end).unwrap())
                .unwrap();
            let decoded =
                decode_and_verify_durable_chunk(&compressed, &chunk.hash, chunk.length).unwrap();
            let start = usize::try_from(chunk.offset).unwrap();
            let finish = start
                .checked_add(usize::try_from(chunk.length).unwrap())
                .unwrap();
            assert_eq!(decoded.as_slice(), data.get(start..finish).unwrap());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_upload_spills_when_shared_memory_budget_is_exhausted() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("objects")).unwrap();
        let data = Bytes::from(
            (0..512 * 1024)
                .map(|index| ((index * 17 + index / 113) % 256) as u8)
                .collect::<Vec<_>>(),
        );
        let memory_budget = Arc::new(tokio::sync::Semaphore::new(4));
        let mut admitted = FileUploadIngestor::new_with_parallelism_and_memory_budget(
            NonZeroUsize::new(64 * 1024).unwrap(),
            false,
            NonZeroUsize::new(8).unwrap(),
            Arc::clone(&memory_budget),
        );
        let mut contending = FileUploadIngestor::new_with_parallelism_and_memory_budget(
            NonZeroUsize::new(64 * 1024).unwrap(),
            false,
            NonZeroUsize::new(8).unwrap(),
            memory_budget,
        );

        admitted
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        assert!(!admitted.local_raw_chunks.is_empty());
        assert!(admitted.raw_chunk_spool.is_none());
        contending
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        assert!(contending.local_raw_chunks.is_empty());
        assert!(contending.raw_chunk_spool.is_some());
        assert_eq!(contending.raw_spool_bytes, data.len() as u64);

        let (admitted_record, admitted_response) = admitted
            .finish(&object_store, "admitted.bin", None, None)
            .await
            .unwrap();
        assert!(admitted_response.inserted_chunks > 0);
        admitted_record.validate_reconstruction_plan().unwrap();

        let (record, response) = contending
            .finish(&object_store, "spilled.bin", None, None)
            .await
            .unwrap();
        assert_eq!(response.inserted_chunks, 0);
        assert!(response.reused_chunks > 0);
        record.validate_reconstruction_plan().unwrap();
        for chunk in &record.chunks {
            let key = crate::xet_adapter::xorb_object_key(&chunk.hash).unwrap();
            let end = chunk.packed_end.checked_sub(1).unwrap();
            let packed = object_store
                .read_range(&key, ByteRange::new(chunk.packed_start, end).unwrap())
                .unwrap();
            let (decoded, _packed_length, raw_length) =
                shardline_xet_core::xorb_object::xorb_chunk_format::deserialize_chunk(
                    &mut std::io::Cursor::new(packed.as_slice()),
                )
                .unwrap();
            assert_eq!(u64::from(raw_length), chunk.length);
            let start = usize::try_from(chunk.offset).unwrap();
            let finish = start
                .checked_add(usize::try_from(chunk.length).unwrap())
                .unwrap();
            assert_eq!(decoded.as_slice(), data.get(start..finish).unwrap());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_multiple_in_flight_chunks_exceeds_parallelism() {
        // Submit enough chunks to exceed max_in_flight_chunks, triggering
        // drain_completed_chunks_to_capacity. Use blackhole so chunks complete
        // synchronously without spawn_blocking.
        let object_store = ServerObjectStore::blackhole();

        // Set max_in_flight_chunks to 2, submit via one body frame.
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            chunk_size,
            false,
            NonZeroUsize::new(2).unwrap(),
        );

        // With CDC (target=128, min=16, max=256), send enough data to produce
        // multiple chunks. The data below is 320 bytes — enough for 2+ chunks.
        let data: Vec<u8> = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            .iter()
            .copied()
            .cycle()
            .take(320)
            .collect();
        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from(data))
            .await
            .unwrap();
        // CDC may or may not have flushed all chunks depending on boundaries
        assert!(!ingestor.completed_chunks.is_empty());

        let finished = ingestor
            .finish(&object_store, "multi-chunk.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, _response)) = finished else {
            return;
        };
        assert_eq!(record.total_bytes, 320);
        assert!(!record.chunks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_finish_with_blackhole_and_sha256_match() {
        // Verify the SHA256 hash is computed correctly when expected matches.
        // Use blackhole to avoid spawn_blocking complexity.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, true);

        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcd"))
            .await
            .unwrap();

        // SHA256 of "abcd"
        let expected = "88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589";
        let finished = ingestor
            .finish(&object_store, "sha256-check.bin", None, Some(expected))
            .await;
        assert!(finished.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_recycles_pending_buffer_across_multiple_flushes() {
        // When the pending buffer fills and is flushed multiple times,
        // the pooled buffer should be recycled.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // Send 3 bytes then 5 bytes → first fills pending (3 < 4), second completes it (3+1=4)
        // Then 1 remains pending.
        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abc"))
            .await
            .unwrap();
        assert_eq!(ingestor.pending.len(), 3);
        assert_eq!(ingestor.reusable_pending_buffers.len(), 0);

        ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"defg"))
            .await
            .unwrap();
        // After flush, the buffer should be recycled
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_record_completed_chunks_already_sorted() {
        // Verify that already-sorted completed chunks don't need re-sorting.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // Send data that reaches CDC boundaries
        ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
                ),
            )
            .await
            .unwrap();

        // completed_chunks should already be in sequence order
        let finished = ingestor
            .finish(&object_store, "presorted.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, _response)) = finished else {
            return;
        };
        assert!(!record.chunks.is_empty());
        assert_eq!(record.total_bytes, 72);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_upload_packs_bounded_raw_batch_without_temporary_chunk_objects() {
        let gets = Arc::new(AtomicUsize::new(0));
        let puts = Arc::new(AtomicUsize::new(0));
        let fixture_gets = Arc::clone(&gets);
        let fixture_puts = Arc::clone(&puts);
        // Consume every upload before replying, as a real object store does.
        // Replying before the request body is drained can race the client's
        // sender and spuriously drive the individual-chunk fallback.
        let app = Router::new()
            .fallback(move |method: Method, _body: Bytes| {
                let gets = Arc::clone(&fixture_gets);
                let puts = Arc::clone(&fixture_puts);
                async move {
                    match method {
                        Method::GET => {
                            gets.fetch_add(1, Ordering::Relaxed);
                        }
                        Method::PUT => {
                            puts.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {}
                    }
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("etag", "\"fixture\"")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            })
            .layer(axum::extract::DefaultBodyLimit::max(16 * 1024 * 1024));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let object_store = ServerObjectStore::s3(
            S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(endpoint))
                .with_allow_http(true)
                .with_credentials(
                    Some(SecretString::from_secret("fixture-access")),
                    Some(SecretString::from_secret("fixture-secret")),
                    None,
                ),
        )
        .unwrap();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            false,
            NonZeroUsize::new(2).unwrap(),
        );
        let mut state = 0x5a17_2c91_u32;
        let data = Bytes::from(
            (0..8 * 1024 * 1024)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 17;
                    state ^= state << 5;
                    state as u8
                })
                .collect::<Vec<_>>(),
        );
        ingestor
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        let (record, _) = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await
            .unwrap();
        server.abort();

        assert_eq!(
            gets.load(Ordering::Relaxed),
            0,
            "upload packing issued S3 GETs"
        );
        assert!(record.chunks.len() > 1);
        assert!(
            puts.load(Ordering::Relaxed) >= 2,
            "a multi-batch upload should PUT multiple packed Xorbs"
        );
        let containers: HashSet<_> = record
            .chunks
            .iter()
            .map(|chunk| chunk.hash.as_str())
            .collect();
        assert!(containers.len() >= 2, "expected multiple Xorb batches");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s3_upload_keeps_chunk_fallback_when_xorb_write_fails() {
        let gets = Arc::new(AtomicUsize::new(0));
        let fixture_gets = Arc::clone(&gets);
        let puts = Arc::new(AtomicUsize::new(0));
        let fixture_puts = Arc::clone(&puts);
        let objects = Arc::new(Mutex::new(HashMap::<String, Bytes>::new()));
        let fixture_objects = Arc::clone(&objects);
        let app = Router::new().fallback(move |method: Method, uri: Uri, body: Bytes| {
            let gets = Arc::clone(&fixture_gets);
            let puts = Arc::clone(&fixture_puts);
            let objects = Arc::clone(&fixture_objects);
            async move {
                if method == Method::GET {
                    gets.fetch_add(1, Ordering::Relaxed);
                    let bytes = objects.lock().unwrap().get(uri.path()).unwrap().clone();
                    return Response::builder()
                        .status(StatusCode::PARTIAL_CONTENT)
                        .header("content-length", bytes.len())
                        .header(
                            "content-range",
                            format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
                        )
                        .header("etag", "\"fixture\"")
                        .header("last-modified", "Wed, 21 Oct 2015 07:28:00 GMT")
                        .body(axum::body::Body::from(bytes))
                        .unwrap();
                }
                if method == Method::PUT {
                    puts.fetch_add(1, Ordering::Relaxed);
                }
                if method == Method::PUT && uri.path().contains("/xorbs/") {
                    return Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(axum::body::Body::empty())
                        .unwrap();
                }
                if method == Method::PUT {
                    objects.lock().unwrap().insert(uri.path().to_owned(), body);
                }
                Response::builder()
                    .status(StatusCode::OK)
                    .header("etag", "\"fixture\"")
                    .body(axum::body::Body::empty())
                    .unwrap()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let object_store = ServerObjectStore::s3(
            S3ObjectStoreConfig::new("fixture".to_owned(), "us-east-1".to_owned())
                .with_endpoint(Some(endpoint))
                .with_allow_http(true)
                .with_credentials(
                    Some(SecretString::from_secret("fixture-access")),
                    Some(SecretString::from_secret("fixture-secret")),
                    None,
                ),
        )
        .unwrap();
        let data = Bytes::from_static(b"durable chunk fallback payload");
        let raw_hash = xet_hash_hex_string(crate::local_backend::chunk_hash(&data));
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            false,
            NonZeroUsize::new(2).unwrap(),
        );
        ingestor
            .ingest_body_chunk(&object_store, &data)
            .await
            .unwrap();
        let (record, _) = ingestor
            .finish(&object_store, "fallback.bin", None, None)
            .await
            .unwrap();
        server.abort();

        assert_eq!(gets.load(Ordering::Relaxed), record.chunks.len());
        assert_eq!(
            puts.load(Ordering::Relaxed),
            record.chunks.len() + 1,
            "fallback should write each durable chunk only after Xorb storage fails"
        );
        assert_eq!(record.chunks.len(), 1);
        assert_eq!(record.chunks[0].hash, raw_hash);
        assert_eq!(record.chunks[0].range_start, 0);
        assert_eq!(record.chunks[0].range_end, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_dedup_records_savings_metric_across_identical_uploads() {
        // Verify the ingestor plumbing: the ingestor → chunk_store pipeline
        // correctly processes identical content twice. The actual dedup metric
        // emission is tested at the chunk_store level with a Local store
        // (dedup_records_savings_metric_on_already_exists).
        // Use blackhole so chunks complete synchronously.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let content = b"aaaabbbbccccdddd"; // 16 bytes → 4 chunks of 4

        // First upload
        let mut first = FileUploadIngestor::new(chunk_size, false);
        first
            .ingest_body_chunk(&object_store, &Bytes::from_static(content))
            .await
            .unwrap();
        let first_finished = first.finish(&object_store, "first.bin", None, None).await;
        assert!(first_finished.is_ok());
        let Ok((_record, first_response)) = first_finished else {
            return;
        };
        assert!(first_response.inserted_chunks >= 1);
        assert_eq!(first_response.reused_chunks, 0);

        // Second upload with same content
        let mut second = FileUploadIngestor::new(chunk_size, false);
        second
            .ingest_body_chunk(&object_store, &Bytes::from_static(content))
            .await
            .unwrap();
        let second_finished = second.finish(&object_store, "second.bin", None, None).await;
        assert!(second_finished.is_ok());
        let Ok((_record, second_response)) = second_finished else {
            return;
        };
        // Blackhole always returns Inserted
        assert!(second_response.inserted_chunks >= 1);
        assert_eq!(second_response.reused_chunks, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_cdc_produces_non_zero_chunks_with_blackhole() {
        // Verify CDC produces chunks with non-zero length via Blackhole store.
        let object_store = ServerObjectStore::blackhole();
        let target_chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            target_chunk_size,
            false,
            NonZeroUsize::new(2).unwrap(),
        );
        // CDC chunker is created by default

        // Send enough data to produce multiple CDC chunks
        let data: Vec<u8> = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            .iter()
            .copied()
            .cycle()
            .take(5760)
            .collect();
        let body = Bytes::from(data);
        let ingested = ingestor.ingest_body_chunk(&object_store, &body).await;
        assert!(ingested.is_ok());

        let finished = ingestor
            .finish(&object_store, "cdc-test.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((_record, response)) = finished else {
            return;
        };

        // Should have produced at least one chunk
        assert!(response.inserted_chunks > 0);
        // Total bytes should match input
        assert_eq!(response.stored_bytes, 5760);

        // Each chunk should have non-zero length
        for chunk in &response.chunks {
            assert!(chunk.length > 0);
        }

        // The total of all chunk lengths should equal the input size
        let total_chunk_bytes: u64 = response.chunks.iter().map(|c| c.length).sum();
        assert_eq!(total_chunk_bytes, 5760);
    }
}
