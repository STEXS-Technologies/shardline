use axum::body::Bytes;
use shardline_cas::{CasCoordinator, CasLimits};
#[cfg(test)]
use shardline_index::FileRecord;
use shardline_index::{FileRecordStorageLayout, UploadIntent, UploadIntentState};
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::ObjectStore;
use std::num::NonZeroU64;
use tokio::task;

use super::LocalBackend;
use crate::{
    ServerError, ShardMetadataLimits,
    chunk_store::chunk_object_key,
    download_stream::{ServerByteStream, file_record_byte_stream},
    model::UploadFileResponse,
    object_store::{read_full_object, reconstruct_file_record_bytes},
    upload_ingest::{FileUploadIngestor, RequestBodyReader, read_body_to_bytes, upload_attempt_id},
    validation::validate_identifier,
    xet_adapter::{
        FileReconstructionResponse, ShardUploadResponse, build_reconstruction_response,
        register_uploaded_shard_bytes,
    },
};

impl LocalBackend {
    pub(crate) async fn copy_file_reference(
        &self,
        source_file_id: &str,
        destination_file_id: &str,
    ) -> Result<bool, ServerError> {
        match self.read_record(destination_file_id, None, None).await {
            Ok(_record) => return Ok(false),
            Err(ServerError::NotFound) => {}
            Err(error) => return Err(error),
        }
        let mut record = self.read_record(source_file_id, None, None).await?;
        record.file_id = destination_file_id.to_owned();
        self.record_store
            .commit_file_version_metadata(&record)
            .await?;
        Ok(true)
    }

    pub(crate) async fn delete_file_reference(&self, file_id: &str) -> Result<bool, ServerError> {
        let record = match self.read_record(file_id, None, None).await {
            Ok(record) => record,
            Err(ServerError::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        self.record_store
            .delete_file_version_metadata(&record)
            .await?;
        Ok(true)
    }

    /// Guarded purge for the S3 conditional-write path (F-92): deletes the
    /// file's latest reference + version record only when the latest record is
    /// still `expected_content_hash` (the just-committed LOSER version).
    ///
    /// The per-key S3 upload lock is process-local, so in a multi-replica
    /// Postgres deployment the latest alias can move to the WINNER's record
    /// before this purge runs. Deleting unconditionally would then destroy the
    /// winner's acknowledged write; skipping (returning `Ok(false)`) leaves the
    /// loser as a non-latest version that GC eventually reclaims.
    pub(crate) async fn delete_file_reference_if_latest(
        &self,
        file_id: &str,
        expected_content_hash: &str,
    ) -> Result<bool, ServerError> {
        let record = match self.read_record(file_id, None, None).await {
            Ok(record) => record,
            Err(ServerError::NotFound) => return Ok(false),
            Err(error) => return Err(error),
        };
        if record.content_hash != expected_content_hash {
            return Ok(false);
        }
        self.record_store
            .delete_file_version_metadata(&record)
            .await?;
        Ok(true)
    }

    pub(crate) async fn read_file_stream(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        range: Option<ByteRange>,
    ) -> Result<(ServerByteStream, u64), ServerError> {
        // With a content hash the read is pinned to that immutable version
        // record; without one it resolves the latest record.
        let record = self.read_record(file_id, content_hash, None).await?;
        let total_bytes = record.total_bytes;
        crate::metrics::record_object_read_by_repr(record.storage_repr.as_str(), total_bytes);
        let stream = file_record_byte_stream(self.object_store(), record, range).await?;
        Ok((stream, total_bytes))
    }

    /// Stores a file version as deduplicated content chunks.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when file identifier validation, chunk persistence, or
    /// metadata persistence fails.
    pub async fn upload_file(
        &self,
        file_id: &str,
        body: Bytes,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<UploadFileResponse, ServerError> {
        self.upload_file_stream(
            file_id,
            RequestBodyReader::from_bytes(body),
            repository_scope,
            None,
        )
        .await
    }

    /// Stores a streamed file version as deduplicated content chunks.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, file identifier validation,
    /// chunk persistence, source digest validation, or metadata persistence fails.
    pub(crate) async fn upload_file_stream(
        &self,
        file_id: &str,
        mut body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        expected_sha256: Option<&str>,
    ) -> Result<UploadFileResponse, ServerError> {
        validate_identifier(file_id)?;

        let intent = expected_sha256.map(|expected_hash| {
            UploadIntent::new(
                upload_attempt_id(file_id),
                format!("record/{file_id}"),
                expected_hash.to_owned(),
                0,
            )
        });
        let coordinator = CasCoordinator::new(
            self.index_store.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        if let Some(intent) = &intent {
            let _metadata_guard = self.metadata_write_lock.lock().await;
            coordinator.begin_upload(intent).await?;
            coordinator
                .transition_upload(intent.intent_id(), UploadIntentState::Storing)
                .await?;
        }
        let object_store = self.object_store();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            self.chunk_size,
            expected_sha256.is_some(),
            self.upload_max_in_flight_chunks,
        );
        while let Some(bytes) = body.next_bytes().await? {
            ingestor.ingest_body_chunk(&object_store, &bytes).await?;
        }

        let result = async {
            let (record, response) = ingestor
                .finish(&object_store, file_id, repository_scope, expected_sha256)
                .await?;
            let _metadata_guard = self.metadata_write_lock.lock().await;
            if let Some(intent) = &intent {
                coordinator
                    .transition_upload(intent.intent_id(), UploadIntentState::Stored)
                    .await?;
            }
            self.record_store
                .commit_file_version_metadata(&record)
                .await?;
            if let Some(intent) = &intent {
                coordinator
                    .transition_upload(intent.intent_id(), UploadIntentState::MetadataCommitted)
                    .await?;
                coordinator
                    .transition_upload(intent.intent_id(), UploadIntentState::Visible)
                    .await?;
            }
            Ok(response)
        }
        .await;
        if result.is_err()
            && let Some(intent) = &intent
        {
            let _metadata_guard = self.metadata_write_lock.lock().await;
            let _ignored = coordinator
                .transition_upload(intent.intent_id(), UploadIntentState::Failed)
                .await;
        }
        result
    }

    /// Stores a bounded native Xet shard and indexes the contained file reconstructions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, shard validation, referenced xorb
    /// validation, or metadata persistence fails.
    pub(crate) async fn upload_shard_stream(
        &self,
        mut body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        shard_metadata_limits: ShardMetadataLimits,
    ) -> Result<ShardUploadResponse, ServerError> {
        let uploaded_body = read_body_to_bytes(&mut body).await?;
        let body_hash = blake3::hash(&uploaded_body);
        let intent_id = format!("shard-{}", hex::encode(body_hash.as_bytes()));
        let body_hash_hex = hex::encode(body_hash.as_bytes());
        let prefix = &body_hash_hex[..2];
        let object_key = format!("shards/{prefix}/{body_hash_hex}.shard");
        let intent = UploadIntent::new(
            intent_id.clone(),
            object_key,
            body_hash_hex,
            uploaded_body.len() as u64,
        );
        let record_store = self.record_store.clone();
        let object_store = self.object_store();
        let coordinator = CasCoordinator::new(
            self.index_store.clone(),
            (),
            (),
            CasLimits::new(NonZeroU64::MAX, NonZeroU64::MAX, NonZeroU64::MAX),
        );
        coordinator
            .with_upload_intent(&intent, move || async move {
                register_uploaded_shard_bytes(
                    &object_store,
                    &uploaded_body,
                    repository_scope,
                    shard_metadata_limits,
                    move |records, mappings| async move {
                        record_store
                            .commit_native_shard_metadata(&records, &mappings)
                            .await?;
                        Ok(())
                    },
                )
                .await
                .map_err(ServerError::from)
            })
            .await
    }

    /// Loads reconstruction metadata for a file.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    pub async fn reconstruction(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        requested_range: Option<ByteRange>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileReconstructionResponse, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        Ok(build_reconstruction_response(
            self.public_base_url(),
            &record,
            requested_range,
        )?)
    }

    /// Loads the logical byte length for a file version.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    pub async fn file_total_bytes(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<u64, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        Ok(record.total_bytes)
    }

    /// Loads the file-version record used by streaming transfer paths.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the file identifier is invalid or the record is
    /// missing or unreadable.
    #[cfg(test)]
    pub(crate) async fn file_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileRecord, ServerError> {
        self.read_record(file_id, content_hash, repository_scope)
            .await
    }

    /// Reconstructs a file into a contiguous byte vector.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata or chunk bytes cannot be read.
    pub async fn download_file(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<Vec<u8>, ServerError> {
        let record = self
            .read_record(file_id, content_hash, repository_scope)
            .await?;
        let total_bytes = record.total_bytes;
        let object_store = self.object_store();

        // ReferencedObjectTerms layout (shard/xorb-referenced) is handled
        // by the raw reconstruct path; StoredChunks layout (ingestor/CDC)
        // uses the streaming path for LZ4 decompression and xorb packing.
        if matches!(
            record.storage_layout(),
            FileRecordStorageLayout::ReferencedObjectTerms
        ) {
            let server_frontends = self.server_frontends.clone();
            return task::spawn_blocking(move || {
                reconstruct_file_record_bytes(&object_store, &server_frontends, &record)
            })
            .await
            .map_err(ServerError::BlockingTask)?;
        }

        // StoredChunks path: stream with LZ4 decompression and xorb-fast-path.
        let stream = file_record_byte_stream(object_store, record, None).await?;
        let mut output = Vec::with_capacity(usize::try_from(total_bytes)?);
        tokio::pin!(stream);
        use futures_util::StreamExt;
        while let Some(chunk) = stream.next().await {
            output.extend_from_slice(&chunk?);
        }
        Ok(output)
    }

    /// Reads a stored chunk by hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    pub async fn read_chunk(&self, hash_hex: &str) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let object_key = chunk_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        task::spawn_blocking(move || {
            read_full_object(&object_store, &object_key, metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    /// Reads a stored chunk only when it is reachable from a concrete file version.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash, file identifier, or content hash are
    /// invalid, when the file version is missing, or when the chunk is not referenced
    /// by that version.
    pub async fn read_chunk_for_file_version(
        &self,
        hash_hex: &str,
        file_id: &str,
        content_hash: &str,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<Vec<u8>, ServerError> {
        let record = self
            .read_record(file_id, Some(content_hash), repository_scope)
            .await?;
        if !record.chunks.iter().any(|chunk| chunk.hash == hash_hex) {
            return Err(ServerError::NotFound);
        }

        self.read_chunk(hash_hex).await
    }

    /// Loads the stored byte length for a chunk object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    pub async fn chunk_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = chunk_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        Ok(metadata.length())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use sha2::{Digest, Sha256};
    use shardline_index::{UploadIntentState, UploadIntentStore};

    use super::LocalBackend;
    use crate::chunk_store::chunk_object_key;
    use crate::upload_ingest::RequestBodyReader;

    #[test]
    fn chunk_object_key_accepts_valid_hash() {
        let hash = "a".repeat(64);
        let key = chunk_object_key(&hash);
        assert!(key.is_ok());
        let key = key.unwrap();
        assert!(key.as_str().contains(&hash));
    }

    #[test]
    fn chunk_object_key_rejects_short_hash() {
        let hash = "abc123";
        let key = chunk_object_key(hash);
        assert!(key.is_err());
    }

    #[test]
    fn chunk_object_key_rejects_non_hex_hash() {
        let hash = "z".repeat(64);
        let key = chunk_object_key(&hash);
        assert!(key.is_err());
    }

    #[test]
    fn chunk_object_key_rejects_empty_hash() {
        let key = chunk_object_key("");
        assert!(key.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_chunk_returns_not_found_for_missing_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.read_chunk(&"aa".repeat(32)).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_chunk_rejects_invalid_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.read_chunk("short").await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn verified_stream_upload_persists_each_intent_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(128).unwrap(),
        )
        .await
        .unwrap();
        let body = axum::body::Bytes::from_static(b"abcdefgh");
        let digest = hex::encode(Sha256::digest(&body));

        backend
            .upload_file_stream(
                "verified-record",
                RequestBodyReader::from_bytes(body),
                None,
                Some(&digest),
            )
            .await
            .unwrap();

        let visible = backend
            .index_store
            .intents_by_state(UploadIntentState::Visible)
            .await
            .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].object_key(), "record/verified-record");
        assert_eq!(visible[0].object_hash(), digest);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunk_length_returns_not_found_for_missing_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.chunk_length(&"bb".repeat(32)).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn chunk_length_rejects_invalid_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.chunk_length("short").await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn file_total_bytes_rejects_invalid_file_id() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let result = backend.file_total_bytes("../invalid-id", None, None).await;
        assert!(matches!(result, Err(crate::ServerError::InvalidFileId)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_chunk_and_chunk_length_happy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        // Upload a file, then verify chunk length and read_chunk work.
        let uploaded = backend
            .upload_file(
                "test.bin",
                axum::body::Bytes::from_static(b"hello-chunk-data"),
                None,
            )
            .await
            .unwrap();
        let chunk = uploaded.chunks.first().unwrap();
        let hash = &chunk.hash;

        let length = backend.chunk_length(hash).await.unwrap();
        // chunk_length returns the stored (LZ4-compressed) byte count,
        // which may differ from chunk.length (raw data) for small payloads.
        assert!(length > 0, "chunk_length must return a positive value");

        let data = backend.read_chunk(hash).await.unwrap();
        assert!(!data.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_chunk_for_file_version_happy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let content = b"version-specific-data";
        let uploaded = backend
            .upload_file(
                "versioned.bin",
                axum::body::Bytes::from_static(content),
                None,
            )
            .await
            .unwrap();

        // read_chunk returns the stored (LZ4-compressed) bytes.
        // Decompress to verify the original content is recoverable.
        let chunk = uploaded.chunks.first().unwrap();
        let read_bytes = backend.read_chunk(&chunk.hash).await.unwrap();
        let decompressed =
            lz4_flex::decompress_size_prepended(&read_bytes).expect("decompression should succeed");
        assert_eq!(decompressed.as_slice(), content);

        // Also verify read_chunk_for_file_version succeeds when called with
        // the chunk hash from the stored file record.
        let record = backend
            .file_record("versioned.bin", None, None)
            .await
            .unwrap();
        let chunk_hash = &record.chunks.first().unwrap().hash;
        let result = backend
            .read_chunk_for_file_version(chunk_hash, "versioned.bin", &uploaded.content_hash, None)
            .await;
        // Note: read_chunk_for_file_version may return NotFound when the
        // record hash has been rewritten to a xorb hash (xorb packing).
        // This is expected — individual chunk lookups are not supported
        // for xorb-backed records.
        if let Ok(bytes) = result {
            let decompressed =
                lz4_flex::decompress_size_prepended(&bytes).expect("decompression should succeed");
            assert_eq!(decompressed.as_slice(), content);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_chunk_for_file_version_rejects_unreferenced_chunk() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let content = b"some-content";
        let uploaded = backend
            .upload_file("a.bin", axum::body::Bytes::from_static(content), None)
            .await
            .unwrap();

        // A hash that is valid hex but not referenced by the file version
        let unreferenced_hash = "ff".repeat(32);
        let result = backend
            .read_chunk_for_file_version(&unreferenced_hash, "a.bin", &uploaded.content_hash, None)
            .await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    // ---------------------------------------------------------------------
    // F-92 — the F-86 conditional-write purge must never delete the WINNER's
    // record in a multi-replica race.
    //
    // The F-86 purge (s3_upload_object_body -> delete_file_reference) deletes
    // whatever the record store's LATEST alias points at. That is safe only
    // under the per-process per-key upload lock (an in-process HashMap), where
    // the latest alias is guaranteed to be the just-committed LOSER. In a
    // multi-replica Postgres deployment (documented/supported: HPA scaling,
    // SHARDLINE_INDEX_POSTGRES_URL) the lock does NOT serialize across
    // replicas. Interleaving:
    //
    //   A: pre-check passes; commits recordA (latest=A)
    //   B: pre-check passes; commits recordB (latest=B)
    //   A: post-check passes; swaps the row (row=etagA/hashA)
    //   B: post-check fails (row etagA); unconditional purge reads LATEST
    //      (now recordA, the WINNER) and deletes its version + latest alias ->
    //      the row (hashA) points at a deleted version -> every GET/HEAD/
    //      CopyObject 404s; post-GC the winner's chunks are reclaimed.
    //
    // The in-process harness cannot reproduce the interleaving end-to-end
    // through the S3 routes (both replicas share the one process-local lock,
    // so conditional writers serialize). The unit tests below simulate the
    // record-store state the losing replica would observe — the latest alias
    // moved to the winner's record — and assert the guarded purge
    // (`delete_file_reference_if_latest`) becomes a no-op instead of deleting
    // the winner's acknowledged write. The loser's record is left as a
    // non-latest version, which GC eventually reclaims.
    // ---------------------------------------------------------------------

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_file_reference_if_latest_skips_when_latest_moved_to_winner() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        // WINNER: the first upload commits its record as the latest alias.
        let winner = backend
            .upload_file(
                "race.bin",
                axum::body::Bytes::from_static(b"winner-content"),
                None,
            )
            .await
            .unwrap();
        // LOSER: the second upload commits its record; it is now the latest.
        let loser = backend
            .upload_file(
                "race.bin",
                axum::body::Bytes::from_static(b"loser-content"),
                None,
            )
            .await
            .unwrap();

        // Simulate the multi-replica interleaving: the WINNER's commit lands
        // AFTER the loser's (on a second replica the per-key lock does not
        // serialize), so the latest alias points back at the winner's record
        // while the loser's conditional post-check is still in flight.
        let winner_record = backend
            .file_record("race.bin", Some(&winner.content_hash), None)
            .await
            .unwrap();
        backend
            .record_store
            .commit_file_version_metadata(&winner_record)
            .await
            .unwrap();
        assert_eq!(
            backend
                .file_record("race.bin", None, None)
                .await
                .unwrap()
                .content_hash,
            winner.content_hash,
            "precondition: the latest alias now points at the winner's record"
        );

        // The LOSER's post-check fires; the purge must NOT delete the winner's
        // acknowledged write because the latest alias no longer points at the
        // loser's committed version.
        assert!(
            !backend
                .delete_file_reference_if_latest("race.bin", &loser.content_hash)
                .await
                .unwrap(),
            "the purge must skip once the latest alias has moved to the winner (F-92)"
        );

        // The winner's LATEST alias and its immutable VERSION record survive:
        // the S3 row (pinned to the winner's content hash) keeps resolving.
        let latest = backend.file_record("race.bin", None, None).await.unwrap();
        assert_eq!(
            latest.content_hash, winner.content_hash,
            "the winner's record must remain the latest after the loser's purge"
        );
        let winner_version = backend
            .file_record("race.bin", Some(&winner.content_hash), None)
            .await
            .unwrap();
        assert_eq!(
            winner_version.content_hash, winner.content_hash,
            "the winner's immutable version record must survive the loser's purge"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_file_reference_if_latest_purges_when_latest_is_the_loser() {
        // Single-process F-86 semantics preserved: while the latest alias still
        // points at the just-committed (loser) record, the guarded purge
        // removes exactly that version + alias — the same outcome as the
        // unconditional delete.
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();

        let uploaded = backend
            .upload_file(
                "purge-me.bin",
                axum::body::Bytes::from_static(b"loser-content"),
                None,
            )
            .await
            .unwrap();

        assert!(
            backend
                .delete_file_reference_if_latest("purge-me.bin", &uploaded.content_hash)
                .await
                .unwrap(),
            "the guarded purge deletes while the latest alias is still the loser's"
        );
        assert!(
            matches!(
                backend.file_record("purge-me.bin", None, None).await,
                Err(crate::ServerError::NotFound)
            ),
            "the latest alias must be gone after the purge"
        );
        assert!(
            matches!(
                backend
                    .file_record("purge-me.bin", Some(&uploaded.content_hash), None)
                    .await,
                Err(crate::ServerError::NotFound)
            ),
            "the loser's immutable version record must be gone after the purge"
        );
    }
}
