use axum::body::Bytes;
#[cfg(test)]
use shardline_index::FileRecord;
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::ObjectStore;
use tokio::task;

use super::LocalBackend;
use crate::{
    ServerError, ShardMetadataLimits,
    chunk_store::chunk_object_key,
    model::UploadFileResponse,
    object_store::{read_full_object, reconstruct_file_record_bytes},
    upload_ingest::{FileUploadIngestor, RequestBodyReader, read_body_to_bytes},
    validation::validate_identifier,
    xet_adapter::{
        FileReconstructionResponse, ShardUploadResponse, build_reconstruction_response,
        register_uploaded_shard_bytes,
    },
};

impl LocalBackend {
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

        let object_store = self.object_store();
        let mut ingestor = FileUploadIngestor::new_with_parallelism(
            self.chunk_size,
            expected_sha256.is_some(),
            self.upload_max_in_flight_chunks,
        );
        while let Some(bytes) = body.next_bytes().await? {
            ingestor.ingest_body_chunk(&object_store, &bytes).await?;
        }

        let (record, response) = ingestor
            .finish(&object_store, file_id, repository_scope, expected_sha256)
            .await?;
        self.record_store
            .commit_file_version_metadata(&record)
            .await?;

        Ok(response)
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
        let record_store = self.record_store.clone();
        let object_store = self.object_store();
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
        let object_store = self.object_store();
        let server_frontends = self.server_frontends.clone();
        task::spawn_blocking(move || {
            reconstruct_file_record_bytes(&object_store, &server_frontends, &record)
        })
        .await
        .map_err(ServerError::BlockingTask)?
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

    use super::LocalBackend;
    use crate::chunk_store::chunk_object_key;

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
        assert_eq!(length, chunk.length);

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
        let chunk = uploaded.chunks.first().unwrap();
        let hash = &chunk.hash;

        let result = backend
            .read_chunk_for_file_version(hash, "versioned.bin", &uploaded.content_hash, None)
            .await;
        assert!(result.is_ok());
        let bytes = result.unwrap();
        assert_eq!(bytes.as_slice(), content);
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
}
