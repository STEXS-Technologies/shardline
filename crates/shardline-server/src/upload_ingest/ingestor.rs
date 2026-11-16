use std::mem;
use std::num::NonZeroUsize;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use shardline_index::{FileChunkRecord, FileRecord};
use shardline_protocol::RepositoryScope;
use tokio::task::JoinSet;
use tracing::{debug, instrument, warn};

use super::body_reader::ChunkBuffer;
use super::cdc::CdcChunker;
use super::chunk_store::{CompletedChunk, put_chunk_if_absent, put_if_absent_with_verified_cache};
use super::durable_chunks::{DurableUploadChunks, ReadCache, SharedReadCache};
use super::xorb_packer::{pack_owned_chunks_into_xorb, store_xorb};

#[cfg(test)]
use super::durable_chunks::decode_and_verify_durable_chunk;

#[cfg(test)]
use crate::config::default_upload_max_in_flight_chunks;
use crate::{
    ServerError,
    local_backend::content_hash,
    metrics::record_chunk_inserted,
    model::{UploadChunkResult, UploadFileResponse},
    object_store::ServerObjectStore,
    overflow::{checked_add, checked_increment},
};

// Native Xet permits up to 64 MiB of raw data per xorb. Server-side ingest uses
// half that ceiling so several independently validated range fetches can be in
// flight without approaching whole-file memory use.
pub(super) const MAX_SERVER_XORB_RAW_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_XORB_CHUNKS: usize = 8 * 1024;

/// Incremental file upload assembler.
pub(crate) struct FileUploadIngestor {
    pub(super) chunk_size: usize,
    pub(super) max_in_flight_chunks: usize,
    pub(super) pending: BytesMut,
    pub(super) submitted_chunks: u64,
    pub(super) next_offset: u64,
    pub(super) completed_chunks: Vec<CompletedChunk>,
    pub(super) in_flight_chunks: JoinSet<Result<CompletedChunk, ServerError>>,
    pub(super) inserted_chunks: u64,
    pub(super) reused_chunks: u64,
    pub(super) stored_bytes: u64,
    pub(super) chunks: Vec<UploadChunkResult>,
    pub(super) records: Vec<FileChunkRecord>,
    pub(super) sha256: Option<Sha256>,
    pub(super) cdc_chunker: Box<CdcChunker>,
    packing_work_pool: crate::admission::BoundedPool,
    local_packing: Option<super::packed_protocol::PackedProtocolUpload>,
    verified_chunks: SharedReadCache,
    packed_protocol: Option<super::packed_protocol::PackedProtocolUpload>,
    reuse_domain: Option<shardline_index::DeduplicationDomain>,
}

impl FileUploadIngestor {
    /// Creates a new file upload ingestor.
    #[cfg(test)]
    pub(crate) fn new(chunk_size: NonZeroUsize, compute_sha256: bool) -> Self {
        Self::new_with_parallelism(
            chunk_size,
            compute_sha256,
            default_upload_max_in_flight_chunks(),
            &crate::admission::ExecutionPools::default_sizes().hashing,
        )
    }

    /// Creates a new file upload ingestor with bounded chunk-level parallelism.
    pub(crate) fn new_with_parallelism(
        chunk_size: NonZeroUsize,
        compute_sha256: bool,
        max_in_flight_chunks: NonZeroUsize,
        packing_work_pool: &crate::admission::BoundedPool,
    ) -> Self {
        let chunk_size = chunk_size.get();
        Self {
            chunk_size,
            max_in_flight_chunks: max_in_flight_chunks.get(),
            pending: BytesMut::new(),
            submitted_chunks: 0,
            next_offset: 0,
            completed_chunks: Vec::new(),
            in_flight_chunks: JoinSet::new(),
            inserted_chunks: 0,
            reused_chunks: 0,
            stored_bytes: 0,
            chunks: Vec::new(),
            records: Vec::new(),
            sha256: compute_sha256.then(Sha256::new),
            cdc_chunker: Box::new(CdcChunker::new(chunk_size)),
            packing_work_pool: packing_work_pool.clone(),
            local_packing: None,
            verified_chunks: SharedReadCache::default(),
            packed_protocol: None,
            reuse_domain: None,
        }
    }

    /// Only internal protocol preparation may omit individual chunk objects.
    /// Public chunk upload responses retain their existing storage behavior.
    pub(crate) fn with_packed_protocol_storage(
        mut self,
        store: &ServerObjectStore,
        pool: &crate::admission::BoundedPool,
    ) -> Self {
        if !matches!(store, ServerObjectStore::Blackhole) {
            self.packed_protocol = Some(super::packed_protocol::PackedProtocolUpload::new(
                store.clone(),
                pool.clone(),
            ));
        }
        self
    }

    pub(crate) fn with_protocol_chunk_reuse(
        mut self,
        backend: crate::ServerBackend,
        scope: Option<RepositoryScope>,
    ) -> Self {
        self.reuse_domain = Some(shardline_index::DeduplicationDomain::from_scope(
            scope.as_ref(),
        ));
        if let Some(packed) = &mut self.packed_protocol {
            packed.reuse = Some(super::protocol_reuse::ProtocolChunkReuse::new(
                backend, scope,
            ));
        }
        self
    }

    /// Consume a checked request stream without publishing file metadata.
    /// Keep backend fencing and final publication outside this shared loop.
    pub(crate) async fn ingest_body(
        &mut self,
        object_store: &ServerObjectStore,
        body: &mut super::RequestBodyReader,
    ) -> Result<(), ServerError> {
        while let Some(bytes) = body.next_bytes().await? {
            self.ingest_body_chunk(object_store, &bytes).await?;
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

        self.pending.extend_from_slice(bytes);
        while self.pending.len() >= self.cdc_chunker.min_chunk()
            && let Some(boundary) = self.cdc_chunker.find_boundary(&self.pending)
        {
            self.flush_pending_chunk_at(object_store, boundary).await?;
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
        if self.packed_protocol.is_none() {
            self.record_completed_chunks()?;
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

        // Pack CDC chunks into bounded xorb containers and store them alongside
        // individual chunks. A file can be arbitrarily larger than one xorb;
        // keeping each container within the native 64 MiB / 8192-chunk envelope
        // also bounds integrity validation before a requested range is exposed.
        // Individual chunks remain authoritative fallback for any batch that
        // cannot be packed or stored.
        if let Some(packed) = self.packed_protocol.take() {
            let result = packed.finish().await?;
            if result.records.len() != usize::try_from(self.submitted_chunks)? {
                return Err(ServerError::Overflow);
            }
            let mut offset = 0;
            for record in &result.records {
                if record.offset != offset {
                    return Err(ServerError::Overflow);
                }
                offset = checked_add(offset, record.length)?;
            }
            if offset != self.next_offset {
                return Err(ServerError::Overflow);
            }
            self.records = result.records;
            self.chunks = result.chunks;
            self.inserted_chunks = result.inserted;
            self.reused_chunks = result.reused;
            self.stored_bytes = result.stored_bytes;
        } else if matches!(object_store, ServerObjectStore::S3(_)) {
            let verified_chunks = {
                let mut cache = self
                    .verified_chunks
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                mem::take(&mut *cache)
            };
            pack_and_store_xorb_from_durable_chunks(
                object_store,
                file_id,
                &mut self.records,
                self.max_in_flight_chunks,
                verified_chunks,
            )
            .await?;
        } else if let Some(packed) = self.local_packing.take() {
            let packed = packed.finish().await?;
            if packed.records.len() != self.records.len()
                || packed
                    .records
                    .iter()
                    .zip(&self.records)
                    .any(|(packed, raw)| packed.offset != raw.offset || packed.length != raw.length)
            {
                return Err(ServerError::Overflow);
            }
            self.records = packed.records;
        }

        let content_hash = content_hash(total_bytes, chunk_size, &self.records);
        let record = FileRecord {
            deduplication_domain: self.reuse_domain,
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
        let chunk = mem::take(&mut self.pending);
        self.submit_chunk(object_store, chunk).await
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
        self.submit_chunk(object_store, chunk).await
    }

    async fn submit_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        chunk: BytesMut,
    ) -> Result<(), ServerError> {
        self.pack_local_chunk(object_store, &chunk).await?;
        self.submitted_chunks = checked_increment(self.submitted_chunks)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
        if let Some(packed) = &mut self.packed_protocol {
            packed.push(&chunk, offset).await?;
            return Ok(());
        }
        if matches!(object_store, ServerObjectStore::Blackhole) {
            let chunk = ChunkBuffer(chunk.freeze());
            let outcome = put_chunk_if_absent(object_store, chunk).await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            self.completed_chunks.push(CompletedChunk {
                offset,
                stored: outcome,
            });
            return Ok(());
        }

        self.drain_completed_chunks_to_capacity().await?;
        let object_store = object_store.clone();
        let verified_chunks = self.verified_chunks.clone();
        self.in_flight_chunks.spawn(async move {
            let chunk = ChunkBuffer(chunk.freeze());
            let outcome =
                put_if_absent_with_verified_cache(&object_store, chunk, Some(&verified_chunks))
                    .await?;
            if outcome.inserted {
                record_chunk_inserted(outcome.chunk_length);
            }
            Ok(CompletedChunk {
                offset,
                stored: outcome,
            })
        });
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
        self.completed_chunks.push(outcome);
        Ok(())
    }

    async fn pack_local_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        chunk: &[u8],
    ) -> Result<(), ServerError> {
        if self.packed_protocol.is_some() || !matches!(object_store, ServerObjectStore::Local(_)) {
            return Ok(());
        }
        let packed = self.local_packing.get_or_insert_with(|| {
            super::packed_protocol::PackedProtocolUpload::for_individual_chunks(
                object_store.clone(),
                self.packing_work_pool.clone(),
            )
        });
        packed.push(chunk, self.next_offset).await
    }

    fn record_completed_chunks(&mut self) -> Result<(), ServerError> {
        let expected_chunks = usize::try_from(self.submitted_chunks)?;
        if self.completed_chunks.len() != expected_chunks {
            return Err(ServerError::Overflow);
        }

        if !self
            .completed_chunks
            .is_sorted_by_key(|outcome| outcome.offset)
        {
            self.completed_chunks
                .sort_unstable_by_key(|outcome| outcome.offset);
        }
        let mut expected_offset = 0_u64;
        let completed_chunks = mem::take(&mut self.completed_chunks);
        for outcome in completed_chunks {
            if outcome.offset != expected_offset {
                return Err(ServerError::Overflow);
            }
            expected_offset = checked_add(expected_offset, outcome.stored.chunk_length)?;
            self.record_chunk_outcome(outcome)?;
        }
        if expected_offset != self.next_offset {
            return Err(ServerError::Overflow);
        }
        Ok(())
    }

    fn record_chunk_outcome(&mut self, outcome: CompletedChunk) -> Result<(), ServerError> {
        let super::chunk_store::StoredChunkOutcome {
            hash_hex,
            chunk_length,
            compressed_length,
            inserted,
        } = outcome.stored;
        if inserted {
            self.inserted_chunks = checked_increment(self.inserted_chunks)?;
            self.stored_bytes = checked_add(self.stored_bytes, chunk_length)?;
        } else {
            self.reused_chunks = checked_increment(self.reused_chunks)?;
        }

        self.chunks.push(UploadChunkResult {
            hash: hash_hex.clone(),
            offset: outcome.offset,
            length: chunk_length,
            inserted,
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
}

/// Packs S3-backed chunks without creating a pod-local raw upload spool.
/// Individual chunks are already durable and content-addressed by the time
/// finalization runs. Bytes already verified against S3 can be reused from the
/// bounded upload-local cache; remaining chunks are read back in bounded Xorb batches. Local
/// deployments retain the filesystem path below because their object store is
/// itself filesystem-backed.
async fn pack_and_store_xorb_from_durable_chunks(
    object_store: &ServerObjectStore,
    file_id: &str,
    records: &mut [FileChunkRecord],
    parallelism: usize,
    cache: ReadCache,
) -> Result<(), ServerError> {
    let mut batch = Vec::new();
    let mut batch_raw_bytes = 0usize;
    let mut batch_record_start = 0usize;
    let mut reader = DurableUploadChunks::with_cache(object_store.clone(), parallelism, cache);

    for record_index in 0..records.len() {
        let (raw, chunk_offset) = reader.next(records).await?.ok_or(ServerError::Overflow)?;

        let next_raw_bytes = batch_raw_bytes
            .checked_add(raw.len())
            .ok_or(ServerError::Overflow)?;
        if !batch.is_empty()
            && (batch.len() >= MAX_XORB_CHUNKS || next_raw_bytes > MAX_SERVER_XORB_RAW_BYTES)
        {
            let batch_to_store = take_xorb_batch(&mut batch);
            store_xorb_batch(
                object_store,
                file_id,
                batch_to_store,
                records,
                batch_record_start,
            )
            .await;
            batch_raw_bytes = 0;
            batch_record_start = record_index;
        }
        batch_raw_bytes = batch_raw_bytes
            .checked_add(raw.len())
            .ok_or(ServerError::Overflow)?;
        batch.push((raw, chunk_offset));
    }

    if !batch.is_empty() {
        let batch_to_store = take_xorb_batch(&mut batch);
        store_xorb_batch(
            object_store,
            file_id,
            batch_to_store,
            records,
            batch_record_start,
        )
        .await;
    }
    Ok(())
}

async fn store_xorb_batch(
    object_store: &ServerObjectStore,
    file_id: &str,
    chunks: Vec<(Vec<u8>, u64)>,
    records: &mut [FileChunkRecord],
    record_start: usize,
) {
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
            return;
        }
    };
    if let Err(error) = store_xorb(object_store, &packed.xorb_hash_hex, &packed.serialized).await {
        warn!(
            file_id,
            record_start,
            xorb_hash = %packed.xorb_hash_hex,
            error = %error,
            "failed to store xorb batch, continuing with individual chunks"
        );
        return;
    }

    let Some(batch_records) = records
        .get_mut(record_start..)
        .and_then(|remaining| remaining.get_mut(..packed.chunk_entries.len()))
    else {
        warn!(
            file_id,
            record_start, "xorb batch record mapping is inconsistent"
        );
        return;
    };
    for (record, entry) in batch_records.iter_mut().zip(&packed.chunk_entries) {
        let Some(next_index) = entry.chunk_index.checked_add(1) else {
            warn!(file_id, record_start, "xorb batch chunk index overflow");
            return;
        };
        let Some(packed_end) = entry.packed_offset.checked_add(entry.packed_length) else {
            warn!(file_id, record_start, "xorb batch packed range overflow");
            return;
        };
        record.hash.clone_from(&packed.xorb_hash_hex);
        record.range_start = u64::from(entry.chunk_index);
        record.range_end = u64::from(next_index);
        record.packed_start = u64::from(entry.packed_offset);
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
}

fn take_xorb_batch(batch: &mut Vec<(Vec<u8>, u64)>) -> Vec<(Vec<u8>, u64)> {
    let capacity = batch.capacity();
    std::mem::replace(batch, Vec::with_capacity(capacity))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::body::Bytes;

    use super::{FileUploadIngestor, decode_and_verify_durable_chunk};
    use crate::{ServerError, object_store::ServerObjectStore};
    use shardline_index::xet_hash_hex_string;

    #[test]
    fn ingestor_new_with_parallelism_creates_empty_state() {
        let ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            true,
            NonZeroUsize::new(128).unwrap(),
            &crate::admission::ExecutionPools::default_sizes().hashing,
        );
        assert_eq!(ingestor.chunk_size, 128);
        assert_eq!(ingestor.max_in_flight_chunks, 128);
        assert!(ingestor.pending.is_empty());
        assert_eq!(ingestor.submitted_chunks, 0);
        assert_eq!(ingestor.next_offset, 0);
        assert!(ingestor.completed_chunks.is_empty());
        assert!(ingestor.in_flight_chunks.is_empty());
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
    fn ingestor_allocates_pending_buffer_lazily() {
        let ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);

        assert_eq!(ingestor.pending.capacity(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_processes_aligned_request_frames_without_staging_copy() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGH"),
            )
            .await;
        assert!(ingested.is_ok());

        let (_record, response) = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await
            .unwrap();

        // With CDC, chunk count depends on content-defined boundaries
        assert!(response.inserted_chunks > 0);
        assert_eq!(response.reused_chunks, 0);
        assert_eq!(response.chunks.len(), response.inserted_chunks as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_chunks_from_split_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
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

        let (record, response) = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await
            .unwrap();

        assert_eq!(record.total_bytes, 72);
        assert!(response.inserted_chunks > 0);
        assert_eq!(response.chunks.len(), response.inserted_chunks as usize);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_aligned_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        // Send data; with CDC, small data may stay pending until finish()
        let ingested = ingestor
            .ingest_body_chunk(
                &object_store,
                &Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMN"),
            )
            .await;
        assert!(ingested.is_ok());

        let (_record, response) = ingestor
            .finish(&object_store, "aligned.bin", None, None)
            .await
            .unwrap();

        assert!(response.inserted_chunks > 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_reuses_existing_chunks_for_aligned_request_frames() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let chunk_size = NonZeroUsize::new(128).unwrap();

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
        let (_record, response) = second
            .finish(&object_store, "second.bin", None, None)
            .await
            .unwrap();

        // CDC produces the same chunks for identical data → reused chunks
        assert_eq!(response.inserted_chunks, 0);
        assert!(response.reused_chunks > 0);
        assert_eq!(response.stored_bytes, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_preserves_tail_bytes_across_mixed_frame_sizes() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let chunk_size = NonZeroUsize::new(128).unwrap();
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

        let (record, response) = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await
            .unwrap();

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
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let chunk_size = NonZeroUsize::new(128).unwrap();
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
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_empty_upload_succeeds() {
        // Empty file upload with no body chunks.
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(128).unwrap();
        let ingestor = FileUploadIngestor::new(chunk_size, false);

        let (record, response) = ingestor
            .finish(&object_store, "empty.bin", None, None)
            .await
            .unwrap();
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

        let (record, response) = ingestor
            .finish(&object_store, "single.bin", None, None)
            .await
            .unwrap();
        assert_eq!(record.total_bytes, 4);
        assert!(response.inserted_chunks >= 1);
        assert!(!response.chunks.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunk_submission_drains_tasks_at_the_in_flight_limit() {
        let storage = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(storage.path().join("chunks")).unwrap();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            NonZeroUsize::new(128).unwrap(),
            false,
            NonZeroUsize::new(2).unwrap(),
            &crate::admission::ExecutionPools::default_sizes().hashing,
        );
        for byte in 0..3 {
            ingestor
                .submit_chunk(&object_store, bytes::BytesMut::from(&[byte; 128][..]))
                .await
                .unwrap();
            assert!(ingestor.in_flight_chunks.len() <= 2);
        }
        assert_eq!(ingestor.completed_chunks.len(), 1);
        assert_eq!(ingestor.in_flight_chunks.len(), 2);

        ingestor.drain_all_completed_chunks().await.unwrap();
        ingestor.record_completed_chunks().unwrap();
        assert_eq!(ingestor.records.len(), 3);
        assert_eq!(ingestor.stored_bytes, 384);
        assert!(ingestor.in_flight_chunks.is_empty());
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

    #[test]
    fn completed_chunks_restore_order_and_reject_missing_or_discontinuous_results() {
        use super::super::chunk_store::{CompletedChunk, StoredChunkOutcome};

        for (order, submitted, valid) in [
            (vec![], 0, true),
            (vec![0, 1, 2], 3, true),
            (vec![2, 0, 1], 3, true),
            (vec![0, 0, 2], 3, false),
            (vec![0, 2, 3], 3, false),
            (vec![0, 1], 3, false),
        ] {
            let mut ingestor = FileUploadIngestor::new(NonZeroUsize::new(128).unwrap(), false);
            ingestor.submitted_chunks = submitted;
            ingestor.next_offset = ingestor.submitted_chunks * 4;
            ingestor.completed_chunks = order
                .into_iter()
                .map(|index| CompletedChunk {
                    offset: index * 4,
                    stored: StoredChunkOutcome {
                        hash_hex: index.to_string(),
                        chunk_length: 4,
                        compressed_length: 4,
                        inserted: true,
                    },
                })
                .collect();

            let result = ingestor.record_completed_chunks();
            if !valid {
                assert!(matches!(result, Err(ServerError::Overflow)));
                continue;
            }
            result.unwrap();

            let actual: Vec<_> = ingestor
                .records
                .iter()
                .map(|record| (record.hash.clone(), record.offset))
                .collect();
            let expected: Vec<_> = (0..ingestor.submitted_chunks)
                .map(|index| (index.to_string(), index * 4))
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(ingestor.stored_bytes, ingestor.next_offset);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn blackhole_uploads_preserve_cdc_accounting_without_reporting_reuse() {
        let object_store = ServerObjectStore::blackhole();
        let data: Vec<u8> = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            .iter()
            .copied()
            .cycle()
            .take(5760)
            .collect();
        for body in [Bytes::from_static(b"aaaabbbbccccdddd"), Bytes::from(data)] {
            // The blackhole always reports insertion, including repeat uploads.
            for file_id in ["first.bin", "second.bin"] {
                let mut ingestor = FileUploadIngestor::new_with_parallelism(
                    NonZeroUsize::new(128).unwrap(),
                    false,
                    NonZeroUsize::new(2).unwrap(),
                    &crate::admission::ExecutionPools::default_sizes().hashing,
                );
                ingestor
                    .ingest_body_chunk(&object_store, &body)
                    .await
                    .unwrap();
                let (_record, response) = ingestor
                    .finish(&object_store, file_id, None, None)
                    .await
                    .unwrap();
                assert!(response.inserted_chunks > 0);
                assert_eq!(response.reused_chunks, 0);
                assert_eq!(response.stored_bytes, body.len() as u64);
                assert!(response.chunks.iter().all(|chunk| chunk.length > 0));
                assert_eq!(
                    response
                        .chunks
                        .iter()
                        .map(|chunk| chunk.length)
                        .sum::<u64>(),
                    body.len() as u64,
                );
            }
        }
    }
}
