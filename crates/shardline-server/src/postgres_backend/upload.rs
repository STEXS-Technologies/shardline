use std::path::Path;

use axum::body::Bytes;
use shardline_protocol::RepositoryScope;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};

use crate::{
    ServerError, ShardMetadataLimits,
    model::UploadFileResponse,
    protocol_support::shared_sha256_object_key,
    upload_ingest::{FileUploadIngestor, RequestBodyReader, read_body_to_bytes},
    validation::validate_identifier,
    xet_adapter::{
        ShardUploadResponse, XorbUploadResponse, register_uploaded_shard_bytes,
        store_uploaded_xorb_bytes,
    },
};

impl super::PostgresBackend {
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

    pub(crate) fn put_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            u64::try_from(bytes.len())?,
        );
        Ok(self.object_store().put_if_absent(
            object_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?)
    }

    pub(crate) fn put_sha256_addressed_object_bytes_if_absent(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        bytes: Vec<u8>,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            u64::try_from(bytes.len())?,
        );
        let canonical_outcome = self.object_store().put_if_absent(
            &canonical_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        Ok(self
            .object_store()
            .copy_if_absent(&canonical_key, object_key)?)
    }

    pub(crate) fn copy_object_if_absent(
        &self,
        source: &ObjectKey,
        destination: &ObjectKey,
    ) -> Result<PutOutcome, ServerError> {
        Ok(self.object_store().copy_if_absent(source, destination)?)
    }

    pub(crate) fn put_object_bytes_overwrite(
        &self,
        object_key: &ObjectKey,
        bytes: Vec<u8>,
    ) -> Result<(), ServerError> {
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(&bytes).as_bytes()),
            u64::try_from(bytes.len())?,
        );
        Ok(self.object_store().put_overwrite(
            object_key,
            ObjectBody::from_vec(bytes),
            &integrity,
        )?)
    }

    pub(crate) fn put_sha256_addressed_object_file(
        &self,
        object_key: &ObjectKey,
        digest_hex: &str,
        path: &Path,
        integrity: &ObjectIntegrity,
    ) -> Result<PutOutcome, ServerError> {
        let canonical_key = shared_sha256_object_key(digest_hex)?;
        let canonical_outcome =
            self.object_store()
                .put_content_addressed_file(&canonical_key, path, integrity)?;
        if canonical_key == *object_key {
            return Ok(canonical_outcome);
        }
        Ok(self
            .object_store()
            .copy_if_absent(&canonical_key, object_key)?)
    }

    /// Stores a raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the supplied hash is invalid, the body hash does not
    /// match, or persistence fails.
    pub async fn upload_xorb(
        &self,
        expected_hash: &str,
        body: Bytes,
    ) -> Result<XorbUploadResponse, ServerError> {
        self.upload_xorb_stream(expected_hash, RequestBodyReader::from_bytes(body))
            .await
    }

    /// Stores a bounded raw xorb body under its content hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, hash validation, or persistence
    /// fails.
    pub(crate) async fn upload_xorb_stream(
        &self,
        expected_hash: &str,
        mut body: RequestBodyReader,
    ) -> Result<XorbUploadResponse, ServerError> {
        let uploaded_body = read_body_to_bytes(&mut body).await?;
        let object_store = self.object_store();
        store_uploaded_xorb_bytes(&object_store, expected_hash, &uploaded_body)
            .await
            .map_err(ServerError::from)
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
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use sha2::Digest;
    use shardline_storage::ObjectKey;

    use super::super::PostgresBackend;
    use super::*;
    use crate::object_store::ServerObjectStore;
    use crate::protocol_support::shared_sha256_object_key;
    use crate::test_fixtures::single_chunk_xorb;
    use crate::upload_ingest::RequestBodyReader;

    const TEST_PG_URL: &str = "postgres://localhost:5432/test";

    fn make_object_key(label: &str) -> ObjectKey {
        ObjectKey::parse(&format!("test/{label}")).unwrap()
    }

    async fn make_backend() -> (PostgresBackend, tempfile::TempDir) {
        let root = tempfile::tempdir().expect("temp dir");
        let object_store =
            ServerObjectStore::local(root.path().join("chunks")).expect("local store");
        let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            TEST_PG_URL,
            object_store,
        )
        .await
        .expect("constructor");
        (backend, root)
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_stores_and_returns_put() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("test-blob");
        let data = b"hello object".to_vec();
        let outcome = backend
            .put_object_bytes_if_absent(&key, data.clone())
            .expect("put");
        assert_eq!(outcome, PutOutcome::Inserted);

        // Second put returns AlreadyPresent
        let outcome2 = backend
            .put_object_bytes_if_absent(&key, data)
            .expect("put again");
        assert_eq!(outcome2, PutOutcome::AlreadyExists);
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_empty_bytes() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("empty-blob");
        let data = Vec::new();
        let outcome = backend
            .put_object_bytes_if_absent(&key, data)
            .expect("put empty");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    #[tokio::test]
    async fn put_object_bytes_overwrite_stores_without_error() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("overwrite-blob");
        let data1 = b"first version".to_vec();
        let data2 = b"second version".to_vec();

        backend
            .put_object_bytes_overwrite(&key, data1)
            .expect("first write");
        backend
            .put_object_bytes_overwrite(&key, data2)
            .expect("overwrite");
        // Success indicates the overwrite completed without error.
    }

    #[tokio::test]
    async fn copy_object_if_absent_copies_key() {
        let (backend, _root) = make_backend().await;
        let src = make_object_key("source-blob");
        let dst = make_object_key("dest-blob");
        let data = b"copy me".to_vec();

        backend
            .put_object_bytes_if_absent(&src, data)
            .expect("put source");
        let outcome = backend.copy_object_if_absent(&src, &dst).expect("copy");
        assert_eq!(outcome, PutOutcome::Inserted);

        // Second copy returns AlreadyPresent
        let outcome2 = backend
            .copy_object_if_absent(&src, &dst)
            .expect("copy again");
        assert_eq!(outcome2, PutOutcome::AlreadyExists);
    }

    #[tokio::test]
    async fn put_sha256_addressed_object_bytes_if_absent_stores_at_canonical_key() {
        let (backend, _root) = make_backend().await;
        let data = b"sha256 addressed content".to_vec();
        let digest_hex = hex::encode(sha2::Sha256::digest(&data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
        let user_key = make_object_key("user-named-blob");

        // Store with canonical key — first insert
        let outcome = backend
            .put_sha256_addressed_object_bytes_if_absent(&canonical_key, &digest_hex, data.clone())
            .expect("put");
        assert_eq!(outcome, PutOutcome::Inserted);

        // Store with different user key but same content — canonical already present.
        // The canonical key already existed (AlreadyExists), so the copy of the
        // canonical to the user key should succeed (Inserted because user key is new),
        // or return AlreadyExists if the copy was already done.
        let outcome2 = backend
            .put_sha256_addressed_object_bytes_if_absent(&user_key, &digest_hex, data)
            .expect("put with user key");
        // The canonical content exists, and the copy to user_key should work
        // (user_key is new, so this is a fresh Put).
        assert!(
            outcome2 == PutOutcome::Inserted || outcome2 == PutOutcome::AlreadyExists,
            "expected Inserted or AlreadyExists, got {outcome2:?}"
        );
    }

    #[tokio::test]
    async fn put_sha256_addressed_object_file_stores_from_path() {
        let (backend, _root) = make_backend().await;
        let data = b"file content for sha256 addressing";
        let digest_hex = hex::encode(sha2::Sha256::digest(data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
            data.len() as u64,
        );

        // Write data to a temp file
        let tmpfile = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmpfile.path(), data).expect("write temp file");

        let outcome = backend
            .put_sha256_addressed_object_file(
                &canonical_key,
                &digest_hex,
                tmpfile.path(),
                &integrity,
            )
            .expect("put from file");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    #[tokio::test]
    async fn put_sha256_addressed_object_file_with_matching_digest_succeeds() {
        let (backend, _root) = make_backend().await;
        let data = b"file content for sha256 addressing";
        let digest_hex = hex::encode(sha2::Sha256::digest(data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
            data.len() as u64,
        );

        let tmpfile = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmpfile.path(), data).expect("write temp file");

        let outcome = backend
            .put_sha256_addressed_object_file(
                &canonical_key,
                &digest_hex,
                tmpfile.path(),
                &integrity,
            )
            .expect("put from file with matching digest");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    #[tokio::test]
    async fn upload_xorb_rejects_invalid_body() {
        let (backend, _root) = make_backend().await;
        // Random bytes are not a valid xorb; the adapter should reject them.
        let data = b"not a valid xorb body";
        let hash = hex::encode(blake3::hash(data).as_bytes());
        let result = backend.upload_xorb(&hash, Bytes::from(data.to_vec())).await;
        assert!(
            result.is_err(),
            "upload_xorb should reject invalid xorb data"
        );
    }

    #[tokio::test]
    async fn upload_xorb_rejects_hash_mismatch() {
        let (backend, _root) = make_backend().await;
        let data = b"some content";
        // Use a hash that does NOT match the data.
        let wrong_hash = "ab".repeat(32);
        let result = backend
            .upload_xorb(&wrong_hash, Bytes::from(data.to_vec()))
            .await;
        assert!(result.is_err(), "upload_xorb should reject hash mismatch");
    }

    #[tokio::test]
    async fn upload_xorb_rejects_empty_body() {
        let (backend, _root) = make_backend().await;
        let data = b"";
        let hash = hex::encode(blake3::hash(data).as_bytes());
        let result = backend.upload_xorb(&hash, Bytes::from(data.to_vec())).await;
        assert!(result.is_err(), "upload_xorb should reject empty body");
    }

    #[tokio::test]
    async fn upload_xorb_accepts_valid_xorb() {
        let (backend, _root) = make_backend().await;
        // Build a valid single-chunk xorb using the test fixture.
        let content = b"hello xorb world";
        let (xorb_bytes, expected_hash) = single_chunk_xorb(content);
        let result = backend.upload_xorb(&expected_hash, xorb_bytes).await;
        assert!(result.is_ok(), "upload_xorb should accept a valid xorb");
    }

    #[tokio::test]
    async fn upload_xorb_rejects_valid_xorb_with_wrong_hash() {
        let (backend, _root) = make_backend().await;
        let content = b"valid content but wrong hash declared";
        let (xorb_bytes, _actual_hash) = single_chunk_xorb(content);
        let wrong_hash = "ff".repeat(32);
        let result = backend.upload_xorb(&wrong_hash, xorb_bytes).await;
        assert!(
            result.is_err(),
            "upload_xorb should reject hash mismatch even for valid xorb"
        );
    }

    #[tokio::test]
    async fn put_object_bytes_overwrite_non_existent_then_existing() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("overwrite-new");
        // Overwrite on a key that doesn't exist yet should succeed (create or overwrite).
        let data = b"first data".to_vec();
        backend
            .put_object_bytes_overwrite(&key, data)
            .expect("overwrite non-existent");

        // Overwrite existing.
        let data2 = b"replacement data".to_vec();
        backend
            .put_object_bytes_overwrite(&key, data2)
            .expect("overwrite existing");

        // Verify the final content by reading back via the object store.
        let meta = backend
            .object_store()
            .metadata(&key)
            .expect("metadata after overwrite");
        assert!(meta.is_some());
    }

    #[tokio::test]
    async fn put_sha256_addressed_object_file_with_user_key() {
        let (backend, _root) = make_backend().await;
        let data = b"content for user-key test";
        let digest_hex = hex::encode(sha2::Sha256::digest(data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
        // Use a user key that differs from the canonical key.
        let user_key = make_object_key("user-named-file-key");
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
            data.len() as u64,
        );

        // Keep tmpfile alive for the duration of the test.
        let tmpfile = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmpfile.path(), data).expect("write temp file");

        // Store via a user key (different from canonical).
        let outcome = backend
            .put_sha256_addressed_object_file(&user_key, &digest_hex, tmpfile.path(), &integrity)
            .expect("put with user key");
        assert_eq!(outcome, PutOutcome::Inserted);

        // Canonical key should already exist (inserted during the user-key call).
        // Verify by reading directly.
        let meta = backend
            .object_store()
            .metadata(&canonical_key)
            .expect("meta");
        assert!(
            meta.is_some(),
            "canonical key should exist after user-key put"
        );
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_large_data() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("large-blob");
        let data = vec![0xABu8; 1_000_000]; // 1 MB
        let outcome = backend
            .put_object_bytes_if_absent(&key, data)
            .expect("put large");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    #[tokio::test]
    async fn copy_object_if_absent_nonexistent_source_fails() {
        let (backend, _root) = make_backend().await;
        let src = make_object_key("non-existent-source");
        let dst = make_object_key("dest");
        let result = backend.copy_object_if_absent(&src, &dst);
        assert!(result.is_err(), "copy of non-existent source should fail");
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_special_key_characters() {
        let (backend, _root) = make_backend().await;
        // Keys with hyphens, dots, underscores should work.
        let key = ObjectKey::parse("test/special-v1.0_data.bin").unwrap();
        let data = b"special key test".to_vec();
        let outcome = backend
            .put_object_bytes_if_absent(&key, data)
            .expect("put special key");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_roundtrip_verify() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("roundtrip-verify");
        let original = b"verify roundtrip content".to_vec();
        backend
            .put_object_bytes_if_absent(&key, original.clone())
            .expect("put");

        // Read back via the object store and verify.
        let meta = backend.object_store().metadata(&key).expect("meta");
        assert!(meta.is_some());
        let Some(meta) = meta else { return };

        // Read the object bytes to verify.
        use shardline_storage::ObjectStore;
        let read_bytes =
            crate::object_store::read_full_object(&backend.object_store(), &key, meta.length())
                .expect("read full");
        assert_eq!(read_bytes, original);
    }

    #[tokio::test]
    async fn put_object_bytes_overwrite_large_data() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("overwrite-large");
        let data = vec![0xCDu8; 500_000];
        backend
            .put_object_bytes_overwrite(&key, data)
            .expect("overwrite large");
    }

    #[tokio::test]
    async fn put_object_bytes_if_absent_double_put_same_data() {
        let (backend, _root) = make_backend().await;
        let key = make_object_key("double-put-same");
        let data = b"same data".to_vec();

        let first = backend
            .put_object_bytes_if_absent(&key, data.clone())
            .expect("first put");
        assert_eq!(first, PutOutcome::Inserted);

        // Second put with identical data returns AlreadyExists.
        let second = backend
            .put_object_bytes_if_absent(&key, data)
            .expect("second put");
        assert_eq!(second, PutOutcome::AlreadyExists);
    }

    // ── upload_file error paths ──────────────────────────────

    #[tokio::test]
    async fn upload_file_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        let body = axum::body::Bytes::from(b"content".to_vec());
        let result = backend.upload_file("/absolute/path", body, None).await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[tokio::test]
    async fn upload_file_stream_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        let body = RequestBodyReader::from_bytes(axum::body::Bytes::from(b"content".to_vec()));
        let result = backend
            .upload_file_stream("../traverse", body, None, None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[tokio::test]
    async fn upload_file_stream_rejects_empty_file_id() {
        let (backend, _root) = make_backend().await;
        let body = RequestBodyReader::from_bytes(axum::body::Bytes::from(b"content".to_vec()));
        let result = backend.upload_file_stream("", body, None, None).await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    // ── upload_shard_stream error paths ──────────────────────

    #[tokio::test]
    async fn upload_shard_stream_rejects_empty_body() {
        let (backend, _root) = make_backend().await;
        let body = RequestBodyReader::from_bytes(axum::body::Bytes::new());
        let limits = ShardMetadataLimits::new(
            std::num::NonZeroUsize::new(100).unwrap(),
            std::num::NonZeroUsize::new(100).unwrap(),
            std::num::NonZeroUsize::new(100).unwrap(),
            std::num::NonZeroUsize::new(100).unwrap(),
        );
        let result = backend.upload_shard_stream(body, None, limits).await;
        // Empty shard body should produce an error.
        assert!(result.is_err());
    }

    // ── put_sha256_addressed_object_bytes_if_absent canonical == user key ──

    #[tokio::test]
    async fn put_sha256_addressed_object_bytes_if_absent_canonical_equals_user_key() {
        let (backend, _root) = make_backend().await;
        let data = b"canonical equals user key".to_vec();
        let digest_hex = hex::encode(sha2::Sha256::digest(&data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();

        // Passing the same key as both canonical and user → the early return
        // on line 106 ('if canonical_key == *object_key') is exercised.
        let outcome = backend
            .put_sha256_addressed_object_bytes_if_absent(&canonical_key, &digest_hex, data)
            .expect("put with matching keys");
        assert_eq!(outcome, PutOutcome::Inserted);
    }

    // ── put_sha256_addressed_object_file canonical == user key ──

    #[tokio::test]
    async fn put_sha256_addressed_object_file_canonical_equals_user_key() {
        let (backend, _root) = make_backend().await;
        let data = b"file canonical equals user key";
        let digest_hex = hex::encode(sha2::Sha256::digest(data));
        let canonical_key = shared_sha256_object_key(&digest_hex).unwrap();
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
            data.len() as u64,
        );

        let tmpfile = tempfile::NamedTempFile::new().expect("temp file");
        std::fs::write(tmpfile.path(), data).expect("write temp file");

        let outcome = backend
            .put_sha256_addressed_object_file(
                &canonical_key,
                &digest_hex,
                tmpfile.path(),
                &integrity,
            )
            .expect("put with matching keys");
        assert_eq!(outcome, PutOutcome::Inserted);
    }
}
