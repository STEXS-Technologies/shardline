use axum::body::Bytes;
use shardline_protocol::RepositoryScope;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, PutOutcome};

use crate::{
    ServerError, ShardMetadataLimits,
    model::UploadFileResponse,
    protocol_support::shared_sha256_object_key,
    upload_ingest::{FileUploadIngestor, RequestBodyReader},
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
        path: &std::path::Path,
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
        let uploaded_body = crate::upload_ingest::read_body_to_bytes(&mut body).await?;
        let object_store = self.object_store();
        store_uploaded_xorb_bytes(&object_store, expected_hash, &uploaded_body)
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
        let uploaded_body = crate::upload_ingest::read_body_to_bytes(&mut body).await?;
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
