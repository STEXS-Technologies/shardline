use std::num::NonZeroUsize;

use bytes::BytesMut;
use sha2::{Digest, Sha256};
use shardline_index::{FileChunkRecord, FileRecord};
use shardline_protocol::RepositoryScope;
use tokio::task::JoinSet;

use super::body_reader::ChunkBuffer;
use super::chunk_store::{
    SequencedStoredChunkOutcome, SequencedStoredChunkTaskOutcome, put_if_absent_chunk_buffer,
    put_if_absent_pooled_chunk_buffer,
};

#[cfg(test)]
use crate::config::default_upload_max_in_flight_chunks;
use crate::{
    ServerError,
    local_backend::content_hash,
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
        }
    }

    /// Ingests one body chunk and persists complete content chunks.
    pub(crate) async fn ingest_body_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        bytes: &axum::body::Bytes,
    ) -> Result<(), ServerError> {
        if let Some(sha256) = &mut self.sha256 {
            sha256.update(bytes);
        }

        let mut frame_offset = 0_usize;
        while frame_offset < bytes.len() {
            let remaining = bytes
                .len()
                .checked_sub(frame_offset)
                .ok_or(ServerError::Overflow)?;
            if self.pending.is_empty() && remaining >= self.chunk_size {
                let chunk_end = frame_offset
                    .checked_add(self.chunk_size)
                    .ok_or(ServerError::Overflow)?;
                let chunk = bytes.slice_ref(
                    bytes
                        .get(frame_offset..chunk_end)
                        .ok_or(ServerError::RequestBodyFrameOutOfBounds)?,
                );
                self.submit_shared_chunk(object_store, chunk).await?;
                frame_offset = chunk_end;
                continue;
            }

            let available = self
                .chunk_size
                .checked_sub(self.pending.len())
                .ok_or(ServerError::Overflow)?;
            let take = available.min(remaining);
            let chunk_end = frame_offset
                .checked_add(take)
                .ok_or(ServerError::Overflow)?;
            self.pending.reserve(take);
            self.pending.extend_from_slice(
                bytes
                    .get(frame_offset..chunk_end)
                    .ok_or(ServerError::RequestBodyFrameOutOfBounds)?,
            );
            frame_offset = chunk_end;
            if self.pending.len() == self.chunk_size {
                self.flush_pending_chunk(object_store).await?;
            }
        }

        Ok(())
    }

    /// Finalizes the upload after the stream reaches EOF.
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

        Ok((record, response))
    }

    async fn flush_pending_chunk(
        &mut self,
        object_store: &ServerObjectStore,
    ) -> Result<(), ServerError> {
        self.submit_owned_chunk(object_store).await
    }

    async fn submit_shared_chunk(
        &mut self,
        object_store: &ServerObjectStore,
        chunk: axum::body::Bytes,
    ) -> Result<(), ServerError> {
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
        if matches!(object_store, ServerObjectStore::Blackhole) {
            let chunk = ChunkBuffer::Shared(chunk);
            let outcome = put_if_absent_chunk_buffer(object_store, chunk).await?;
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
            let chunk = ChunkBuffer::Shared(chunk);
            let outcome = put_if_absent_chunk_buffer(&object_store, chunk).await?;
            Ok(SequencedStoredChunkTaskOutcome {
                sequence,
                offset,
                stored: outcome,
                reusable_buffer: None,
            })
        });
        Ok(())
    }

    async fn submit_owned_chunk(
        &mut self,
        object_store: &ServerObjectStore,
    ) -> Result<(), ServerError> {
        let replacement = self.take_pending_buffer();
        let chunk = std::mem::replace(&mut self.pending, replacement);
        let sequence = self.next_sequence;
        self.next_sequence = checked_increment(self.next_sequence)?;
        let offset = self.next_offset;
        let chunk_length = u64::try_from(chunk.len())?;
        self.next_offset = checked_add(self.next_offset, chunk_length)?;
        if matches!(object_store, ServerObjectStore::Blackhole) {
            let chunk = ChunkBuffer::Pooled(chunk.freeze());
            let (outcome, reusable_buffer) =
                put_if_absent_pooled_chunk_buffer(object_store, chunk).await?;
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
        let outcome = joined.map_err(ServerError::BlockingTask)??;
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
    #[allow(clippy::indexing_slicing)]
    fn record_completed_chunks(&mut self) -> Result<(), ServerError> {
        let expected_chunks = usize::try_from(self.next_sequence)?;
        if self.completed_chunks.len() != expected_chunks {
            return Err(ServerError::Overflow);
        }

        let already_sorted = self
            .completed_chunks
            .windows(2)
            .all(|w| w[0].sequence <= w[1].sequence);
        if !already_sorted {
            self.completed_chunks
                .sort_unstable_by_key(|outcome| outcome.sequence);
        }
        let mut expected_offset = 0_u64;
        let completed_chunks = std::mem::take(&mut self.completed_chunks);
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
            packed_end: chunk_length,
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

    use super::FileUploadIngestor;
    use crate::{
        ServerError, chunk_store::chunk_object_key, local_backend::chunk_hash,
        object_store::ServerObjectStore,
    };

    #[test]
    fn ingestor_allocates_pending_buffer_lazily() {
        let ingestor = FileUploadIngestor::new(NonZeroUsize::MAX, false);

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
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcdefgh"))
            .await;
        assert!(ingested.is_ok());
        assert!(ingestor.pending.is_empty());

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((_record, response)) = finished else {
            return;
        };

        assert_eq!(response.inserted_chunks, 2);
        assert_eq!(response.reused_chunks, 0);
        assert_eq!(response.stored_bytes, 8);
        assert_eq!(response.chunks.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_chunks_from_split_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let first = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abc"))
            .await;
        assert!(first.is_ok());
        assert_eq!(ingestor.in_flight_chunks.len(), 0);

        let second = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"defgh"))
            .await;
        assert!(second.is_ok());
        assert!(ingestor.pending.is_empty());
        assert_eq!(ingestor.in_flight_chunks.len(), 0);
        assert_eq!(ingestor.completed_chunks.len(), 2);

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };

        assert_eq!(record.total_bytes, 8);
        assert_eq!(response.inserted_chunks, 2);
        assert_eq!(response.chunks.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_blackhole_does_not_queue_completed_aligned_request_frames() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcdefgh"))
            .await;
        assert!(ingested.is_ok());
        assert!(ingestor.pending.is_empty());
        assert_eq!(ingestor.in_flight_chunks.len(), 0);
        assert_eq!(ingestor.completed_chunks.len(), 2);

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((_record, response)) = finished else {
            return;
        };

        assert_eq!(response.inserted_chunks, 2);
        assert_eq!(response.chunks.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ingestor_recycles_pooled_pending_buffers_after_upload_completion() {
        let object_store = ServerObjectStore::blackhole();
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let ingested = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abc"))
            .await;
        assert!(ingested.is_ok());
        let pooled_capacity = ingestor.pending.capacity();
        assert!(pooled_capacity >= 4);

        let second = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"defgh"))
            .await;
        assert!(second.is_ok());
        assert!(ingestor.pending.is_empty());
        assert_eq!(ingestor.reusable_pending_buffers.len(), 1);
        let recycled_buffer = ingestor.reusable_pending_buffers.first();
        assert!(recycled_buffer.is_some());
        let Some(recycled_buffer) = recycled_buffer else {
            return;
        };
        assert!(
            recycled_buffer.capacity() >= 4,
            "recycled pooled buffer lost chunk-sized capacity"
        );
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
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };

        let mut first = FileUploadIngestor::new(chunk_size, false);
        let first_ingested = first
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcdefgh"))
            .await;
        assert!(first_ingested.is_ok());
        let first_finished = first.finish(&object_store, "first.bin", None, None).await;
        assert!(first_finished.is_ok());

        let mut second = FileUploadIngestor::new(chunk_size, false);
        let second_ingested = second
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcdefgh"))
            .await;
        assert!(second_ingested.is_ok());
        let second_finished = second.finish(&object_store, "second.bin", None, None).await;
        assert!(second_finished.is_ok());
        let Ok((_record, response)) = second_finished else {
            return;
        };

        assert_eq!(response.inserted_chunks, 0);
        assert_eq!(response.reused_chunks, 2);
        assert_eq!(response.stored_bytes, 0);
        let first_chunk = xet_hash_hex_string(chunk_hash(b"abcd"));
        let second_chunk = xet_hash_hex_string(chunk_hash(b"efgh"));
        let first_key = chunk_object_key(&first_chunk);
        let second_key = chunk_object_key(&second_chunk);
        assert!(first_key.is_ok());
        assert!(second_key.is_ok());
        let Ok(first_key) = first_key else {
            return;
        };
        let Ok(second_key) = second_key else {
            return;
        };
        let first_metadata = object_store.metadata(&first_key);
        let second_metadata = object_store.metadata(&second_key);
        assert!(first_metadata.is_ok());
        assert!(second_metadata.is_ok());
        assert!(matches!(first_metadata, Ok(Some(metadata)) if metadata.length() == 4));
        assert!(matches!(second_metadata, Ok(Some(metadata)) if metadata.length() == 4));
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
        let chunk_size = NonZeroUsize::new(4);
        assert!(chunk_size.is_some());
        let Some(chunk_size) = chunk_size else {
            return;
        };
        let mut ingestor = FileUploadIngestor::new(chunk_size, false);

        let first = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"abcde"))
            .await;
        let second = ingestor
            .ingest_body_chunk(&object_store, &Bytes::from_static(b"fghi"))
            .await;
        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(ingestor.pending, b"i".to_vec());

        let finished = ingestor
            .finish(&object_store, "asset.bin", None, None)
            .await;
        assert!(finished.is_ok());
        let Ok((record, response)) = finished else {
            return;
        };

        assert_eq!(response.inserted_chunks, 3);
        assert_eq!(record.total_bytes, 9);
        assert_eq!(record.chunks.len(), 3);
        let first_chunk = record.chunks.first();
        let second_chunk = record.chunks.get(1);
        let third_chunk = record.chunks.get(2);
        assert!(first_chunk.is_some());
        assert!(second_chunk.is_some());
        assert!(third_chunk.is_some());
        if let Some(first_chunk) = first_chunk {
            assert_eq!(first_chunk.length, 4);
        }
        if let Some(second_chunk) = second_chunk {
            assert_eq!(second_chunk.length, 4);
        }
        if let Some(third_chunk) = third_chunk {
            assert_eq!(third_chunk.length, 1);
        }
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
        let chunk_size = NonZeroUsize::new(4);
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
}
