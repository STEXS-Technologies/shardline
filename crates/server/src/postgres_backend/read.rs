use shardline_index::{
    FileRecord, PostgresMetadataStoreError, RecordStore, RepositoryRecordScope,
};
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::{DeleteOutcome, ObjectKey, ObjectMetadata, ObjectPrefix};
use sqlx::query_scalar;
use tokio::task;

use crate::{
    ServerError,
    chunk_store::chunk_object_key,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    object_store::{read_full_object, reconstruct_file_record_bytes},
    record_store::parse_stored_file_record_bytes,
    validation::{ensure_directory, validate_content_hash, validate_identifier},
    xet_adapter::{
        FileReconstructionResponse, build_reconstruction_response, resolve_dedupe_shard_object,
        xorb_object_key,
    },
};

const REQUIRED_METADATA_TABLES: [&str; 6] = [
    "shardline_file_records",
    "shardline_file_reconstructions",
    "shardline_stored_objects",
    "shardline_dedupe_shards",
    "shardline_quarantine_candidates",
    "shardline_retention_holds",
];

impl super::PostgresBackend {
    /// Verifies that the local object store and required Postgres metadata tables are
    /// reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when local chunk storage is unreadable, the Postgres
    /// pool cannot execute queries, or required metadata tables are missing.
    pub async fn ready(&self) -> Result<(), ServerError> {
        let object_store = self.object_store();
        if let Some(local_root) = object_store.local_root() {
            ensure_directory(local_root).await?;
        } else {
            let probe_key = ObjectKey::parse("health/probe")
                .map_err(|_error| ServerError::InvalidContentHash)?;
            let _object_store_reachable = object_store.metadata(&probe_key)?;
        }
        let _probe = query_scalar::<_, i32>("SELECT 1")
            .fetch_one(self.record_store.pool())
            .await
            .map_err(PostgresMetadataStoreError::from)?;

        for table_name in REQUIRED_METADATA_TABLES {
            let registered_name = query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(table_name)
                .fetch_one(self.record_store.pool())
                .await
                .map_err(PostgresMetadataStoreError::from)?;
            if registered_name.is_none() {
                return Err(ServerError::MissingRequiredMetadataTable(
                    table_name.to_owned(),
                ));
            }
        }

        Ok(())
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

    pub(crate) async fn object_length(&self, object_key: &ObjectKey) -> Result<u64, ServerError> {
        let metadata = self.object_store().metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        Ok(metadata.length())
    }

    pub(crate) async fn read_object(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let metadata = object_store.metadata(object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        let object_key = object_key.clone();
        task::spawn_blocking(move || {
            read_full_object(&object_store, &object_key, metadata.length())
        })
        .await
        .map_err(ServerError::BlockingTask)?
    }

    pub(crate) async fn read_object_stream(
        &self,
        object_key: &ObjectKey,
        total_length: u64,
        range: Option<ByteRange>,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        if let Some(range) = range {
            return object_byte_range_stream(object_store, object_key.clone(), total_length, range)
                .await;
        }

        object_byte_stream(object_store, object_key.clone(), total_length).await
    }

    pub(crate) fn visit_object_prefix<Visitor>(
        &self,
        prefix: &ObjectPrefix,
        visitor: Visitor,
    ) -> Result<(), ServerError>
    where
        Visitor: FnMut(ObjectMetadata) -> Result<(), ServerError>,
    {
        crate::object_store::visit_object_prefix(&self.object_store(), prefix, visitor)
    }

    pub(crate) fn list_object_flat_namespace_page(
        &self,
        prefix: &ObjectPrefix,
        start_after: Option<&ObjectKey>,
        limit: usize,
    ) -> Result<Vec<ObjectMetadata>, ServerError> {
        Ok(self
            .object_store()
            .list_flat_namespace_page(prefix, start_after, limit)?)
    }

    pub(crate) async fn delete_object_if_present(
        &self,
        object_key: &ObjectKey,
    ) -> Result<DeleteOutcome, ServerError> {
        Ok(self.object_store().delete_if_present(object_key)?)
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

    pub(crate) async fn read_dedupe_shard_stream(
        &self,
        hash_hex: &str,
    ) -> Result<(ServerByteStream, u64), ServerError> {
        let object_store = self.object_store();
        let (object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;
        let byte_stream = object_byte_stream(object_store, object_key, total_length).await?;

        Ok((byte_stream, total_length))
    }

    pub(crate) async fn dedupe_shard_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let (_object_key, total_length) =
            resolve_dedupe_shard_object(&self.index_store, &object_store, hash_hex).await?;

        Ok(total_length)
    }

    /// Streams a stored xorb byte range by hash.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid, the xorb is missing, or the
    /// requested byte range cannot be served.
    pub(crate) async fn read_xorb_range_stream(
        &self,
        hash_hex: &str,
        total_length: u64,
        range: ByteRange,
    ) -> Result<ServerByteStream, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;

        object_byte_range_stream(object_store, object_key, total_length, range).await
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

    /// Loads the stored byte length for a serialized xorb object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the xorb is missing.
    pub async fn xorb_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = xorb_object_key(hash_hex)?;
        let metadata = object_store.metadata(&object_key)?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };

        Ok(metadata.length())
    }

    pub(crate) async fn repository_references_xorb(
        &self,
        hash_hex: &str,
        repository_scope: &RepositoryScope,
    ) -> Result<bool, ServerError> {
        repository_references_hash_in_scope(&self.record_store, hash_hex, repository_scope).await
    }

    async fn read_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&RepositoryScope>,
    ) -> Result<FileRecord, ServerError> {
        validate_identifier(file_id)?;
        let probe = FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.unwrap_or_default().to_owned(),
            total_bytes: 0,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = if let Some(content_hash) = content_hash {
            validate_content_hash(content_hash)?;
            self.record_store.version_record_locator(&probe)
        } else {
            self.record_store.latest_record_locator(&probe)
        };
        let bytes = RecordStore::read_record_bytes(&self.record_store, &locator)
            .await
            .map_err(map_record_store_error)?;
        parse_stored_file_record_bytes(&bytes)
    }
}

pub(crate) async fn repository_references_hash_in_scope<RecordAdapter>(
    record_store: &RecordAdapter,
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError>
where
    RecordAdapter: RecordStore + Sync,
    ServerError: From<RecordAdapter::Error>,
{
    let repository = RepositoryRecordScope::from_repository_scope(repository_scope);
    let mut found = false;
    RecordStore::visit_repository_latest_records(record_store, &repository, |entry| {
        if found {
            return Ok::<(), ServerError>(());
        }
        if stored_record_references_hash(&entry.bytes, hash_hex, repository_scope)? {
            found = true;
        }
        Ok(())
    })
    .await?;

    if found {
        return Ok(true);
    }

    RecordStore::visit_repository_version_records(record_store, &repository, |entry| {
        if found {
            return Ok::<(), ServerError>(());
        }
        if stored_record_references_hash(&entry.bytes, hash_hex, repository_scope)? {
            found = true;
        }
        Ok(())
    })
    .await?;

    Ok(found)
}

fn stored_record_references_hash(
    bytes: &[u8],
    hash_hex: &str,
    repository_scope: &RepositoryScope,
) -> Result<bool, ServerError> {
    let record = parse_stored_file_record_bytes(bytes)?;
    if record.repository_scope.as_ref() != Some(repository_scope) {
        return Ok(false);
    }

    Ok(record.chunks.iter().any(|chunk| chunk.hash == hash_hex))
}

pub(crate) fn connect_postgres_metadata_pool(
    index_postgres_url: &str,
    max_connections: u32,
) -> Result<sqlx::PgPool, ServerError> {
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .connect_lazy(index_postgres_url)
        .map_err(PostgresMetadataStoreError::from)
        .map_err(ServerError::from)
}

fn map_record_store_error(error: PostgresMetadataStoreError) -> ServerError {
    match error {
        PostgresMetadataStoreError::RecordNotFound => ServerError::NotFound,
        PostgresMetadataStoreError::Sqlx(_)
        | PostgresMetadataStoreError::Json(_)
        | PostgresMetadataStoreError::HashParse(_)
        | PostgresMetadataStoreError::ObjectKey(_)
        | PostgresMetadataStoreError::Range(_)
        | PostgresMetadataStoreError::RetentionHold(_)
        | PostgresMetadataStoreError::QuarantineCandidate(_)
        | PostgresMetadataStoreError::WebhookDelivery(_)
        | PostgresMetadataStoreError::IntegerOutOfRange
        | PostgresMetadataStoreError::InvalidRecordKind => ServerError::PostgresMetadata(error),
    }
}
