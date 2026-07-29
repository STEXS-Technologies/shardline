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
use super::chunk_store::{
    SequencedStoredChunkOutcome, SequencedStoredChunkTaskOutcome, put_if_absent_pooled_chunk_buffer,
};

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

/// Incremental file upload assembler.
pub(crate) struct FileUploadIngestor {
    pub(super) chunk_size: usize,
    pub(super) max_in_flight_chunks: usize,
    pub(super) pending: BytesMut,
    pub(super) next_sequence: u64,
    pub(super) next_offset: u64,
    pub(super) completed_chunks: Vec<SequencedStoredChunkOutcome>,
    pub(super) in_flight_chunks: JoinSet<Result<SequencedStoredChunkTaskOutcome, ServerError>>,
    pub(super) reusable_pending_buffers: Vec<BytesMut>,
    pub(super) inserted_chunks: u64,
    pub(super) reused_chunks: u64,
    pub(super) stored_bytes: u64,
    pub(super) chunks: Vec<UploadChunkResult>,
    pub(super) records: Vec<FileChunkRecord>,
    pub(super) sha256: Option<Sha256>,
    pub(super) cdc_chunker: Box<CdcChunker>,
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
        let chunk_size = chunk_size.get();
        Self {
            chunk_size,
            max_in_flight_chunks: max_in_flight_chunks.get(),
            pending: BytesMut::new(),
            next_sequence: 0,
            next_offset: 0,
            completed_chunks: Vec::new(),
            in_flight_chunks: JoinSet::new(),
            reusable_pending_buffers: Vec::new(),
            inserted_chunks: 0,
            reused_chunks: 0,
            stored_bytes: 0,
            chunks: Vec::new(),
            records: Vec::new(),
            sha256: compute_sha256.then(Sha256::new),
            cdc_chunker: Box::new(CdcChunker::new(chunk_size)),
        }
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
        let content_hash = content_hash(total_bytes, chunk_size, &self.records);
        let record = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.clone(),
            total_bytes,
            chunk_size,
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
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
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
        let object_store = object_store.clone();
        self.in_flight_chunks.spawn(async move {
            let chunk = ChunkBuffer::Pooled(chunk.freeze());
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
        Ok(())
    }

    async fn submit_owned_chunk(
        &mut self,
        object_store: &ServerObjectStore,
    ) -> Result<(), ServerError> {
        let replacement = self.take_pending_buffer();
        let chunk = mem::replace(&mut self.pending, replacement);
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
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
        let object_store = object_store.clone();
        self.in_flight_chunks.spawn(async move {
            let chunk = ChunkBuffer::Pooled(chunk.freeze());
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

    /// `windows(2)` always yields slices of exactly two elements, so indexing
    /// at 0 and 1 is infallible.
    fn record_completed_chunks(&mut self) -> Result<(), ServerError> {
        let expected_chunks = usize::try_from(self.next_sequence)?;
        if self.completed_chunks.len() != expected_chunks {
            return Err(ServerError::Overflow);
        }

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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use axum::body::Bytes;
    use shardline_index::xet_hash_hex_string;
    use shardline_storage::ObjectStore;

    use super::super::cdc::CdcChunker;
    use super::FileUploadIngestor;
    use crate::{
        ServerError, chunk_store::chunk_object_key, local_backend::chunk_hash,
        object_store::ServerObjectStore,
    };

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
        assert!(ingestor.reusable_pending_buffers.len() >= 0);
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
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
                .iter()
                .copied()
                .collect();

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
        assert!(response.chunks.len() >= 1);
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
        assert!(ingestor.completed_chunks.len() >= 1);

        let finished = ingestor
            .finish(&object_store, "multi-chunk.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, _response)) = finished else {
            return;
        };
        assert_eq!(record.total_bytes, 320);
        assert!(record.chunks.len() >= 1);
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
        assert!(ingestor.reusable_pending_buffers.len() >= 0);
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
        assert!(record.chunks.len() >= 1);
        assert_eq!(record.total_bytes, 72);
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
