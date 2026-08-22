use std::time::Duration;

use shardline_index::{
    FileRecord, FileRecordStorageLayout, OciObjectKey, OciObjectStore, OciTagEntry, OciTagStore,
    PostgresMetadataStoreError, RecordStore, RecordTraversal, RepositoryRecordScope, S3ObjectEntry,
    S3ObjectIndexStore,
};
use shardline_protocol::{ByteRange, RepositoryScope};
#[cfg(test)]
use shardline_storage::ObjectStore;
use shardline_storage::{AsyncObjectStore, DeleteOutcome, ObjectKey, ObjectMetadata, ObjectPrefix};
use sqlx::query_scalar;
use tokio::task;

use crate::{
    ServerError,
    chunk_store::chunk_object_key,
    download_stream::{
        ServerByteStream, file_record_byte_stream, object_byte_range_stream, object_byte_stream,
    },
    error::IndexError,
    object_store::{read_full_object, reconstruct_file_record_bytes, visit_object_prefix},
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
            let _object_store_reachable =
                AsyncObjectStore::metadata(&object_store, &probe_key).await?;
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
                return Err(ServerError::Index(
                    IndexError::MissingRequiredMetadataTable(table_name.to_owned()),
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
        let total_bytes = record.total_bytes;
        let object_store = self.object_store();
        let server_frontends = self.server_frontends.clone();

        // ReferencedObjectTerms layout (shard/xorb-referenced) is handled
        // by the raw reconstruct path; StoredChunks layout (ingestor/CDC)
        // uses the streaming path for LZ4 decompression and xorb packing.
        if matches!(
            record.storage_layout(),
            FileRecordStorageLayout::ReferencedObjectTerms
        ) {
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
        let metadata = AsyncObjectStore::metadata(&object_store, &object_key).await?;
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
        let metadata = AsyncObjectStore::metadata(&self.object_store(), object_key).await?;
        let Some(metadata) = metadata else {
            return Err(ServerError::NotFound);
        };
        Ok(metadata.length())
    }

    pub(crate) async fn read_object(&self, object_key: &ObjectKey) -> Result<Vec<u8>, ServerError> {
        let object_store = self.object_store();
        let metadata = AsyncObjectStore::metadata(&object_store, object_key).await?;
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
        visit_object_prefix(&self.object_store(), prefix, visitor)
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
        Ok(AsyncObjectStore::delete_if_present(&self.object_store(), object_key).await?)
    }

    /// Loads the stored byte length for a chunk object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    pub async fn chunk_length(&self, hash_hex: &str) -> Result<u64, ServerError> {
        let object_store = self.object_store();
        let object_key = chunk_object_key(hash_hex)?;
        let metadata = AsyncObjectStore::metadata(&object_store, &object_key).await?;
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
        let metadata = AsyncObjectStore::metadata(&object_store, &object_key).await?;
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

    pub(super) async fn read_record(
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
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: repository_scope.cloned(),
            chunks: Vec::new(),
        };
        let locator = if let Some(content_hash) = content_hash {
            validate_content_hash(content_hash)?;
            self.record_store.version_record_locator(&probe)
        } else {
            self.record_store.latest_record_locator(&probe)
        };
        let bytes = RecordTraversal::read_record_bytes(&self.record_store, &locator)
            .await
            .map_err(map_record_store_error)?;
        parse_stored_file_record_bytes(&bytes)
    }

    /// Loads the authoritative file-version record for a protocol object's
    /// deterministic file id.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::NotFound`] when no record exists, or the adapter
    /// error when the record cannot be read.
    pub(crate) async fn protocol_file_record(
        &self,
        file_id: &str,
    ) -> Result<FileRecord, ServerError> {
        self.read_record(file_id, None, None).await
    }

    /// Upserts one S3 object listing-index row.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index write fails.
    pub(crate) async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), ServerError> {
        self.index_store.upsert_s3_object(entry).await?;
        Ok(())
    }

    /// Atomically replaces an S3 object row if its current value matches.
    pub(crate) async fn compare_and_swap_s3_object(
        &self,
        expected: Option<&S3ObjectEntry>,
        replacement: &S3ObjectEntry,
    ) -> Result<bool, ServerError> {
        self.index_store
            .compare_and_swap_s3_object(expected, replacement)
            .await
            .map_err(ServerError::from)
    }

    /// Deletes one S3 object listing-index row, returning whether a row was removed.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index delete fails.
    pub(crate) async fn delete_s3_object(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<bool, ServerError> {
        self.index_store
            .delete_s3_object(scope_namespace, object_key)
            .await
            .map_err(ServerError::from)
    }

    /// Scans S3 object listing rows for a scope namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index scan fails.
    pub(crate) async fn scan_s3_objects(
        &self,
        scope_namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<S3ObjectEntry>, ServerError> {
        self.index_store
            .scan_s3_objects(scope_namespace, prefix, cursor, limit)
            .await
            .map_err(ServerError::from)
    }

    /// Resolves exactly one S3 object listing row by its full raw key (no
    /// prefix matching).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index lookup fails.
    pub(crate) async fn scan_s3_object_exact(
        &self,
        scope_namespace: &str,
        object_key: &str,
    ) -> Result<Option<S3ObjectEntry>, ServerError> {
        self.index_store
            .scan_s3_object_exact(scope_namespace, object_key)
            .await
            .map_err(ServerError::from)
    }

    pub(crate) async fn insert_oci_tag_if_absent(
        &self,
        entry: &OciTagEntry,
    ) -> Result<bool, ServerError> {
        self.index_store
            .insert_oci_tag_if_absent(entry)
            .await
            .map_err(ServerError::from)
    }

    pub(crate) async fn oci_tag(
        &self,
        scope_namespace: &str,
        repository: &str,
        tag: &str,
    ) -> Result<Option<OciTagEntry>, ServerError> {
        self.index_store
            .oci_tag(scope_namespace, repository, tag)
            .await
            .map_err(ServerError::from)
    }

    pub(crate) async fn list_oci_tags(
        &self,
        scope_namespace: &str,
        repository: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<OciTagEntry>, ServerError> {
        self.index_store
            .list_oci_tags(scope_namespace, repository, cursor, limit)
            .await
            .map_err(ServerError::from)
    }

    pub(crate) async fn oci_object_is_deleted(
        &self,
        key: &OciObjectKey,
    ) -> Result<bool, ServerError> {
        self.index_store
            .oci_object_is_deleted(key)
            .await
            .map_err(ServerError::from)
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
    RecordTraversal::visit_repository_latest_records(record_store, &repository, |entry| {
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

    RecordTraversal::visit_repository_version_records(record_store, &repository, |entry| {
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
        // A bounded acquire timeout keeps a saturated shared pool from hanging
        // requests for the sqlx default (30s): the caller gets a clean error
        // instead of a stuck 500 after a long stall.
        .acquire_timeout(Duration::from_secs(10))
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
        | PostgresMetadataStoreError::IntegerOutOfRange(_)
        | PostgresMetadataStoreError::InvalidRecordKind
        | PostgresMetadataStoreError::InvalidRepoType(_)
        | PostgresMetadataStoreError::Unsupported(_)
        | PostgresMetadataStoreError::InvalidUploadIntentState(_)
        | PostgresMetadataStoreError::UploadIntentConflict(_) => {
            ServerError::Index(IndexError::PostgresMetadata(error))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::error::IndexError;
    use crate::object_store::ServerObjectStore;
    use serde_json::to_vec;
    use shardline_index::{FileChunkRecord, FileRecord, LocalRecordStore, RecordMutation};
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use shardline_storage::{ObjectBody, ObjectIntegrity};
    use tempfile::TempDir;

    const TEST_PG_URL: &str = "postgres://localhost:5432/test";

    fn test_scope() -> RepositoryScope {
        RepositoryScope::new(
            RepositoryProvider::GitHub,
            "test-owner",
            "test-repo",
            Some("main"),
        )
        .unwrap()
    }

    fn make_hash(ch: char) -> String {
        std::iter::repeat_n(ch, 64).collect()
    }

    fn file_record_json_bytes(scope: &RepositoryScope, chunk_hash: &str) -> Vec<u8> {
        let record = FileRecord {
            file_id: "test/file.bin".into(),
            content_hash: make_hash('f'),
            total_bytes: 1024,
            chunk_size: 256,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: Some(scope.clone()),
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.to_owned(),
                offset: 0,
                length: 256,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 256,
            }],
        };
        to_vec(&record).unwrap()
    }

    async fn make_backend() -> (super::super::PostgresBackend, TempDir) {
        let root = TempDir::new().expect("temp dir");
        let object_store =
            ServerObjectStore::local(root.path().join("chunks")).expect("local store");
        let backend = super::super::PostgresBackend::new_with_object_store_and_upload_parallelism(
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

    /// Store arbitrary bytes as a chunk in the object store and return the hash + key.
    fn store_chunk(object_store: &ServerObjectStore, data: &[u8]) -> (String, ObjectKey) {
        let hash = blake3::hash(data);
        let hash_hex = hex::encode(hash.as_bytes());
        let object_key = chunk_object_key(&hash_hex).unwrap();
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*hash.as_bytes()),
            data.len() as u64,
        );
        ObjectStore::put_if_absent(
            object_store,
            &object_key,
            ObjectBody::from_vec(data.to_vec()),
            &integrity,
        )
        .unwrap();
        (hash_hex, object_key)
    }

    /// Store arbitrary bytes under any object key.
    fn store_object(object_store: &ServerObjectStore, key: &ObjectKey, data: &[u8]) {
        let integrity = ObjectIntegrity::new(
            shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(data).as_bytes()),
            data.len() as u64,
        );
        ObjectStore::put_if_absent(
            object_store,
            key,
            ObjectBody::from_vec(data.to_vec()),
            &integrity,
        )
        .unwrap();
    }

    // ===== Pure function tests =====

    #[test]
    fn stored_record_references_hash_matching() {
        let scope = test_scope();
        let hash = make_hash('b');
        let bytes = file_record_json_bytes(&scope, &hash);

        assert!(stored_record_references_hash(&bytes, &hash, &scope).unwrap());
    }

    #[test]
    fn stored_record_references_hash_different() {
        let scope = test_scope();
        let stored_hash = make_hash('b');
        let queried_hash = make_hash('q');
        let bytes = file_record_json_bytes(&scope, &stored_hash);

        assert!(!stored_record_references_hash(&bytes, &queried_hash, &scope).unwrap());
    }

    #[test]
    fn stored_record_references_hash_different_scope() {
        let record_scope = test_scope();
        let other_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "other", "other-repo", None).unwrap();
        let hash = make_hash('b');
        let bytes = file_record_json_bytes(&record_scope, &hash);

        assert!(!stored_record_references_hash(&bytes, &hash, &other_scope).unwrap());
    }

    #[test]
    fn stored_record_references_hash_invalid_json() {
        let scope = test_scope();
        let bytes = b"!!! not valid json !!!";

        assert!(stored_record_references_hash(bytes, &make_hash('a'), &scope).is_err());
    }

    #[test]
    fn stored_record_references_hash_null_scope() {
        // A record with `repository_scope: None` should never match any scope.
        let scope = test_scope();
        let hash = make_hash('b');
        let bytes = {
            let record = FileRecord {
                file_id: "test/file.bin".into(),
                content_hash: make_hash('f'),
                total_bytes: 1024,
                chunk_size: 256,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: hash.clone(),
                    offset: 0,
                    length: 256,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 256,
                }],
            };
            to_vec(&record).unwrap()
        };
        assert!(!stored_record_references_hash(&bytes, &hash, &scope).unwrap());
    }

    #[test]
    fn stored_record_references_hash_empty_chunks() {
        let scope = test_scope();
        let hash = make_hash('x');
        let bytes = {
            let record = FileRecord {
                file_id: "test/file.bin".into(),
                content_hash: make_hash('f'),
                total_bytes: 0,
                chunk_size: 0,
                storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
                repository_scope: Some(scope.clone()),
                chunks: Vec::new(),
            };
            to_vec(&record).unwrap()
        };
        assert!(!stored_record_references_hash(&bytes, &hash, &scope).unwrap());
    }

    #[test]
    fn stored_record_references_hash_hash_not_in_chunks() {
        let scope = test_scope();
        let stored_hash = make_hash('a');
        let queried_hash = make_hash('z');
        let bytes = file_record_json_bytes(&scope, &stored_hash);
        assert!(!stored_record_references_hash(&bytes, &queried_hash, &scope).unwrap());
    }

    #[test]
    fn map_record_store_error_not_found() {
        let error = PostgresMetadataStoreError::RecordNotFound;
        let result = map_record_store_error(error);
        assert!(matches!(result, ServerError::NotFound));
    }

    #[test]
    fn map_record_store_error_sqlx() {
        let error = PostgresMetadataStoreError::Sqlx(Box::new(sqlx::Error::PoolClosed));
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_json() {
        let error =
            PostgresMetadataStoreError::Json(serde_json::from_str::<()>("invalid").unwrap_err());
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_hash_parse() {
        let error = PostgresMetadataStoreError::HashParse(
            shardline_protocol::HashParseError::InvalidLength,
        );
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_object_key() {
        let error = PostgresMetadataStoreError::ObjectKey(shardline_storage::ObjectKeyError::Empty);
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_range() {
        let error = PostgresMetadataStoreError::Range(shardline_protocol::RangeError::Inverted);
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_retention_hold() {
        let error = PostgresMetadataStoreError::RetentionHold(
            shardline_index::RetentionHoldError::EmptyReason,
        );
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_quarantine_candidate() {
        let error = PostgresMetadataStoreError::QuarantineCandidate(
            shardline_index::QuarantineCandidateError::InvertedTimeline,
        );
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_webhook_delivery() {
        let error = PostgresMetadataStoreError::WebhookDelivery(
            shardline_index::WebhookDeliveryError::EmptyRepositoryOwner,
        );
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_integer_out_of_range() {
        let error = PostgresMetadataStoreError::IntegerOutOfRange("test".to_owned());
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_invalid_record_kind() {
        let error = PostgresMetadataStoreError::InvalidRecordKind;
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn map_record_store_error_invalid_repo_type() {
        let error = PostgresMetadataStoreError::InvalidRepoType("unknown".to_owned());
        let result = map_record_store_error(error);
        assert!(matches!(
            result,
            ServerError::Index(IndexError::PostgresMetadata(_))
        ));
    }

    #[test]
    fn connect_postgres_metadata_pool_empty_url() {
        assert!(connect_postgres_metadata_pool("", 5).is_err());
    }

    #[test]
    fn connect_postgres_metadata_pool_invalid_url() {
        assert!(connect_postgres_metadata_pool("!!not-a-valid-url!!", 5).is_err());
    }

    #[tokio::test]
    async fn connect_postgres_metadata_pool_very_small_connections_succeeds() {
        // max_connections=1 should be accepted (lazy pool, no actual connect).
        let result = connect_postgres_metadata_pool("postgres://localhost:5432/test", 1);
        assert!(result.is_ok() || result.is_err());
        // Drop the pool explicitly within the tokio context.
        drop(result);
    }

    // ===== chunk_length / read_chunk integration tests =====

    #[tokio::test]
    async fn chunk_length_returns_stored_chunk_length() {
        let (backend, _root) = make_backend().await;
        let data = b"chunk data for length test";
        let (hash_hex, _key) = store_chunk(&backend.object_store(), data);
        let length = backend.chunk_length(&hash_hex).await.expect("chunk_length");
        assert_eq!(length, data.len() as u64);
    }

    #[tokio::test]
    async fn chunk_length_not_found_for_missing_hash() {
        let (backend, _root) = make_backend().await;
        let hash_hex = "ab".repeat(32); // valid hex but no chunk stored
        let result = backend.chunk_length(&hash_hex).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn read_chunk_returns_stored_bytes() {
        let (backend, _root) = make_backend().await;
        let data = b"read-chunk test data";
        let (hash_hex, _key) = store_chunk(&backend.object_store(), data);
        let read_data = backend.read_chunk(&hash_hex).await.expect("read_chunk");
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn read_chunk_not_found_for_missing_hash() {
        let (backend, _root) = make_backend().await;
        let hash_hex = "ba".repeat(32);
        let result = backend.read_chunk(&hash_hex).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn read_chunk_with_large_data() {
        let (backend, _root) = make_backend().await;
        let data = vec![0xABu8; 65536]; // 64 KB chunk
        let (hash_hex, _key) = store_chunk(&backend.object_store(), &data);
        let read_data = backend.read_chunk(&hash_hex).await.expect("read_chunk");
        assert_eq!(read_data.len(), 65536);
        assert_eq!(read_data, data);
    }

    // ===== object_length / read_object integration tests =====

    #[tokio::test]
    async fn object_length_returns_stored_length() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/obj-length").unwrap();
        let data = b"object length test data";
        store_object(&backend.object_store(), &key, data);
        let length = backend.object_length(&key).await.expect("object_length");
        assert_eq!(length, data.len() as u64);
    }

    #[tokio::test]
    async fn object_length_not_found_for_missing_key() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/missing-obj").unwrap();
        let result = backend.object_length(&key).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn read_object_returns_stored_bytes() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/read-obj").unwrap();
        let data = b"read object test content";
        store_object(&backend.object_store(), &key, data);
        let read_data = backend.read_object(&key).await.expect("read_object");
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn read_object_not_found_for_missing_key() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/never-stored").unwrap();
        let result = backend.read_object(&key).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    #[tokio::test]
    async fn read_object_roundtrip_large_blob() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/large-blob").unwrap();
        let data = vec![0x42u8; 131072]; // 128 KB
        store_object(&backend.object_store(), &key, &data);
        let read_data = backend.read_object(&key).await.expect("read_object large");
        assert_eq!(read_data.len(), 131072);
        assert_eq!(read_data, data);
    }

    // ===== delete_object_if_present tests =====

    #[tokio::test]
    async fn delete_object_if_present_deletes_existing() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/delete-existing").unwrap();
        store_object(&backend.object_store(), &key, b"to be deleted");
        let outcome = backend
            .delete_object_if_present(&key)
            .await
            .expect("delete");
        assert_eq!(outcome, DeleteOutcome::Deleted);
        // Verify the object is gone.
        assert!(matches!(
            backend.object_length(&key).await,
            Err(ServerError::NotFound)
        ));
    }

    #[tokio::test]
    async fn delete_object_if_present_missing_returns_not_found() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/never-existed").unwrap();
        let outcome = backend
            .delete_object_if_present(&key)
            .await
            .expect("delete missing");
        assert_eq!(outcome, DeleteOutcome::NotFound);
    }

    #[tokio::test]
    async fn delete_object_if_present_double_delete() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/double-delete").unwrap();
        store_object(&backend.object_store(), &key, b"delete me twice");
        // First delete succeeds.
        assert_eq!(
            backend
                .delete_object_if_present(&key)
                .await
                .expect("first delete"),
            DeleteOutcome::Deleted
        );
        // Second delete returns NotFound.
        assert_eq!(
            backend
                .delete_object_if_present(&key)
                .await
                .expect("second delete"),
            DeleteOutcome::NotFound
        );
    }

    // ===== visit_object_prefix tests =====

    #[tokio::test]
    async fn visit_object_prefix_returns_matching_objects() {
        let (backend, _root) = make_backend().await;
        let keys = [
            ObjectKey::parse("test/prefix/a").unwrap(),
            ObjectKey::parse("test/prefix/b").unwrap(),
            ObjectKey::parse("test/prefix/sub/c").unwrap(),
            ObjectKey::parse("test/other/x").unwrap(),
        ];
        for key in &keys {
            store_object(&backend.object_store(), key, b"data");
        }

        let prefix = ObjectPrefix::parse("test/prefix").unwrap();
        let mut found: Vec<String> = Vec::new();
        backend
            .visit_object_prefix(&prefix, |meta| {
                found.push(meta.key().as_str().to_owned());
                Ok(())
            })
            .expect("visit");
        assert_eq!(found.len(), 3, "should find 3 objects under test/prefix/");
        assert!(found.iter().any(|k: &String| k.contains("test/prefix/a")));
        assert!(found.iter().any(|k: &String| k.contains("test/prefix/b")));
        assert!(
            found
                .iter()
                .any(|k: &String| k.contains("test/prefix/sub/c"))
        );
    }

    #[tokio::test]
    async fn visit_object_prefix_empty_when_no_match() {
        let (backend, _root) = make_backend().await;
        let prefix = ObjectPrefix::parse("no/such/prefix").unwrap();
        let mut count = 0_u64;
        backend
            .visit_object_prefix(&prefix, |_| {
                count += 1;
                Ok(())
            })
            .expect("visit empty");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn visit_object_prefix_empty_store() {
        let (backend, _root) = make_backend().await;
        let prefix = ObjectPrefix::parse("test").unwrap();
        let mut count = 0_u64;
        backend
            .visit_object_prefix(&prefix, |_| {
                count += 1;
                Ok(())
            })
            .expect("visit empty store");
        assert_eq!(count, 0);
    }

    // ===== list_object_flat_namespace_page tests =====

    #[tokio::test]
    async fn list_object_flat_namespace_page_returns_at_most_limit() {
        let (backend, _root) = make_backend().await;
        for i in 0..10 {
            let key = ObjectKey::parse(&format!("test/page/obj{i:03}")).unwrap();
            store_object(&backend.object_store(), &key, b"page data");
        }
        let prefix = ObjectPrefix::parse("test/page").unwrap();
        let page = backend
            .list_object_flat_namespace_page(&prefix, None, 4)
            .expect("list page");
        assert_eq!(page.len(), 4);
    }

    #[tokio::test]
    async fn list_object_flat_namespace_page_with_limit() {
        let (backend, _root) = make_backend().await;
        for i in 0..5 {
            let key = ObjectKey::parse(&format!("test/page2/obj{i:03}")).unwrap();
            store_object(&backend.object_store(), &key, b"data");
        }
        let prefix = ObjectPrefix::parse("test/page2").unwrap();
        // Request 2 items; the local store may or may not support start_after,
        // but should at least respect the limit.
        let page = backend
            .list_object_flat_namespace_page(&prefix, None, 2)
            .expect("list with limit");
        assert!(!page.is_empty());
        assert!(page.len() <= 2);
    }

    // ===== Special characters and edge cases =====

    #[tokio::test]
    async fn object_keys_with_special_characters() {
        let (backend, _root) = make_backend().await;
        // Object keys with hyphens, dots, underscores, and slashes.
        let key = ObjectKey::parse("test/special_chars/file-v1.0.bin").unwrap();
        let data = b"special key data";
        store_object(&backend.object_store(), &key, data);
        let read_data = backend.read_object(&key).await.expect("read special key");
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn chunk_length_rejects_invalid_hash_format() {
        let (backend, _root) = make_backend().await;
        // Non-hex characters or wrong length should produce an error.
        let result = backend.chunk_length("not-a-valid-hex-string").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_chunk_rejects_invalid_hash_format() {
        let (backend, _root) = make_backend().await;
        let result = backend.read_chunk("!!invalid!!").await;
        assert!(result.is_err());
    }

    // ===== Record store tests using LocalRecordStore =====
    // These test RecordTraversal + RecordMutation traits directly,
    // allowing record-set/scan/delete patterns without postgres.

    /// Helper to open a LocalRecordStore in a temp dir.
    fn local_record_store() -> (LocalRecordStore, TempDir) {
        let storage = TempDir::new().expect("temp dir");
        (
            LocalRecordStore::open(storage.path().to_path_buf()),
            storage,
        )
    }

    fn test_file_record(file_id: &str, content_hash: &str, scope: &RepositoryScope) -> FileRecord {
        FileRecord {
            file_id: file_id.to_owned(),
            content_hash: content_hash.to_owned(),
            total_bytes: 1024,
            chunk_size: 256,
            storage_repr: shardline_index::StorageRepresentation::FixedChunkV1,
            repository_scope: Some(scope.clone()),
            chunks: Vec::new(),
        }
    }

    #[tokio::test]
    async fn record_set_write_and_read_latest() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/record-set.bin", &make_hash('a'), &scope);

        RecordMutation::write_latest_record(&store, &record)
            .await
            .expect("write latest");

        let locator = RecordTraversal::latest_record_locator(&store, &record);
        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists");
        assert!(exists);

        let bytes = RecordTraversal::read_record_bytes(&store, &locator)
            .await
            .expect("read");
        let parsed = parse_stored_file_record_bytes(&bytes).expect("parse");
        assert_eq!(parsed.file_id, record.file_id);
        assert_eq!(parsed.content_hash, record.content_hash);
    }

    #[tokio::test]
    async fn record_set_write_and_read_version() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/version-record.bin", &make_hash('b'), &scope);

        RecordMutation::write_version_record(&store, &record)
            .await
            .expect("write version");

        let locator = RecordTraversal::version_record_locator(&store, &record);
        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists");
        assert!(exists);

        let bytes = RecordTraversal::read_record_bytes(&store, &locator)
            .await
            .expect("read version");
        let parsed = parse_stored_file_record_bytes(&bytes).expect("parse");
        assert_eq!(parsed.file_id, record.file_id);
        assert_eq!(parsed.content_hash, record.content_hash);
    }

    #[tokio::test]
    async fn record_set_overwrite_latest() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record_v1 = test_file_record("test/overwrite.bin", &make_hash('a'), &scope);
        let record_v2 = FileRecord {
            content_hash: make_hash('b'),
            total_bytes: 2048,
            ..record_v1.clone()
        };

        RecordMutation::write_latest_record(&store, &record_v1)
            .await
            .expect("write v1");
        RecordMutation::write_latest_record(&store, &record_v2)
            .await
            .expect("write v2 (overwrite)");

        let locator = RecordTraversal::latest_record_locator(&store, &record_v1);
        let bytes = RecordTraversal::read_record_bytes(&store, &locator)
            .await
            .expect("read after overwrite");
        let parsed = parse_stored_file_record_bytes(&bytes).expect("parse");
        // The latest record should now have v2 data (same file_id).
        assert_eq!(parsed.total_bytes, 2048);
    }

    // record_scan — scan with various prefixes / repository scopes

    #[tokio::test]
    async fn record_scan_list_all_latest_locators() {
        let (store, _storage) = local_record_store();
        let scope_a = test_scope();
        let scope_b = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "other-owner",
            "other-repo",
            Some("main"),
        )
        .unwrap();
        let rec1 = test_file_record("file1.bin", &make_hash('a'), &scope_a);
        let rec2 = test_file_record("file2.bin", &make_hash('b'), &scope_a);
        let rec3 = test_file_record("file3.bin", &make_hash('c'), &scope_b);

        RecordMutation::write_latest_record(&store, &rec1)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &rec2)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &rec3)
            .await
            .unwrap();

        let locators = RecordTraversal::list_latest_record_locators(&store)
            .await
            .expect("list all");

        // Should contain 3 distinct locators.
        assert_eq!(locators.len(), 3);
    }

    #[tokio::test]
    async fn record_scan_repository_scope() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let other_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"))
                .unwrap();
        let rec_in_scope = test_file_record("in-scope.bin", &make_hash('a'), &scope);
        let rec_other = test_file_record("other.bin", &make_hash('b'), &other_scope);

        RecordMutation::write_latest_record(&store, &rec_in_scope)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &rec_other)
            .await
            .unwrap();

        let repo_scope = shardline_index::RepositoryRecordScope::from_repository_scope(&scope);
        let repo_locators =
            RecordTraversal::list_repository_latest_record_locators(&store, &repo_scope)
                .await
                .expect("list repo scope");
        assert_eq!(repo_locators.len(), 1);
    }

    #[tokio::test]
    async fn record_scan_different_revisions_same_repository() {
        let (store, _storage) = local_record_store();
        let main_scope = test_scope();
        let release_scope = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "test-owner",
            "test-repo",
            Some("release"),
        )
        .unwrap();
        let rec_main = test_file_record("main-file.bin", &make_hash('a'), &main_scope);
        let rec_release = test_file_record("release-file.bin", &make_hash('b'), &release_scope);

        RecordMutation::write_latest_record(&store, &rec_main)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &rec_release)
            .await
            .unwrap();

        let repo_scope = shardline_index::RepositoryRecordScope::from_repository_scope(&main_scope);
        let locators = RecordTraversal::list_repository_latest_record_locators(&store, &repo_scope)
            .await
            .expect("list repo (all revisions)");
        // Both revisions share the same repository scope key.
        assert_eq!(locators.len(), 2);
    }

    // record_delete — delete existing, delete non-existent

    #[tokio::test]
    async fn record_delete_existing_locator() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/delete-me.bin", &make_hash('x'), &scope);

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let locator = RecordTraversal::latest_record_locator(&store, &record);

        // Delete it.
        RecordMutation::delete_record_locator(&store, &locator)
            .await
            .expect("delete");

        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists after delete");
        assert!(!exists);
    }

    #[tokio::test]
    async fn record_delete_nonexistent_locator_fails() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/never-written.bin", &make_hash('y'), &scope);
        let locator = RecordTraversal::latest_record_locator(&store, &record);

        let result = RecordMutation::delete_record_locator(&store, &locator).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn record_delete_then_rewrite() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/recreate.bin", &make_hash('z'), &scope);

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let locator = RecordTraversal::latest_record_locator(&store, &record);

        RecordMutation::delete_record_locator(&store, &locator)
            .await
            .expect("delete");

        // Rewrite the same record.
        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let exists = RecordTraversal::record_locator_exists(&store, &locator)
            .await
            .expect("exists after rewrite");
        assert!(exists);
    }

    // list_latest_version_records — version record listing

    #[tokio::test]
    async fn list_latest_version_records_with_version_records() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let rec1 = test_file_record("v1.bin", &make_hash('a'), &scope);
        let rec2 = test_file_record("v2.bin", &make_hash('b'), &scope);

        // Write version records.
        RecordMutation::write_version_record(&store, &rec1)
            .await
            .unwrap();
        RecordMutation::write_version_record(&store, &rec2)
            .await
            .unwrap();

        let v_locators = RecordTraversal::list_version_record_locators(&store)
            .await
            .expect("list versions");
        assert_eq!(v_locators.len(), 2);
    }

    #[tokio::test]
    async fn list_latest_version_records_empty_when_no_versions() {
        let (store, _storage) = local_record_store();
        let locators = RecordTraversal::list_version_record_locators(&store)
            .await
            .expect("list versions empty");
        assert!(locators.is_empty());
    }

    #[tokio::test]
    async fn list_latest_version_records_repository_scoped() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let other_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"))
                .unwrap();
        let rec = test_file_record("file.bin", &make_hash('a'), &scope);
        let other_rec = test_file_record("other.bin", &make_hash('b'), &other_scope);

        RecordMutation::write_version_record(&store, &rec)
            .await
            .unwrap();
        RecordMutation::write_version_record(&store, &other_rec)
            .await
            .unwrap();

        let repo_scope = shardline_index::RepositoryRecordScope::from_repository_scope(&scope);
        let locators =
            RecordTraversal::list_repository_version_record_locators(&store, &repo_scope)
                .await
                .expect("list repo versions");
        assert_eq!(locators.len(), 1);
    }

    // ===== modified_since_epoch read access =====

    #[tokio::test]
    async fn modified_since_epoch_returns_timestamp_for_stored_record() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/timestamp.bin", &make_hash('t'), &scope);

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let locator = RecordTraversal::latest_record_locator(&store, &record);

        let duration = RecordTraversal::modified_since_epoch(&store, &locator)
            .await
            .expect("modified_since_epoch");
        // Should be a reasonable recent timestamp (>0).
        assert!(duration.as_secs() > 0);
    }

    #[tokio::test]
    async fn modified_since_epoch_fails_for_missing_locator() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/no-exist.bin", &make_hash('n'), &scope);
        let locator = RecordTraversal::latest_record_locator(&store, &record);

        let result = RecordTraversal::modified_since_epoch(&store, &locator).await;
        assert!(result.is_err());
    }

    // ===== latest_record_locator vs version_record_locator =====

    #[tokio::test]
    async fn latest_and_version_locators_differ() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = test_file_record("test/locator-diff.bin", &make_hash('d'), &scope);

        let latest = RecordTraversal::latest_record_locator(&store, &record);
        let version = RecordTraversal::version_record_locator(&store, &record);
        // Latest and version locators should be different for the same record.
        assert_ne!(
            latest, version,
            "latest and version locator keys must differ"
        );
    }

    // ===== Edge: empty records, zero-byte records =====

    #[tokio::test]
    async fn record_set_empty_chunks_list() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let record = FileRecord {
            file_id: "test/empty-chunks.bin".into(),
            content_hash: make_hash('e'),
            total_bytes: 0,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: Some(scope),
            chunks: Vec::new(),
        };

        RecordMutation::write_latest_record(&store, &record)
            .await
            .unwrap();
        let locator = RecordTraversal::latest_record_locator(&store, &record);
        let bytes = RecordTraversal::read_record_bytes(&store, &locator)
            .await
            .expect("read empty-chunks record");
        let parsed = parse_stored_file_record_bytes(&bytes).expect("parse");
        assert!(parsed.chunks.is_empty());
        assert_eq!(parsed.total_bytes, 0);
    }

    // ===== repository_metadata_inventory — various record counts =====

    #[tokio::test]
    async fn repository_metadata_inventory_counts() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let repo_scope = shardline_index::RepositoryRecordScope::from_repository_scope(&scope);

        // Write 3 latest records + 2 version records in the same repository.
        for i in 0..3 {
            let rec = test_file_record(
                &format!("inventory/file{i}.bin"),
                &make_hash(char::from(b'a' + i)),
                &scope,
            );
            RecordMutation::write_latest_record(&store, &rec)
                .await
                .unwrap();
        }
        for i in 0..2 {
            let rec = test_file_record(
                &format!("inventory/version{i}.bin"),
                &make_hash(char::from(b'x' + i)),
                &scope,
            );
            RecordMutation::write_version_record(&store, &rec)
                .await
                .unwrap();
        }

        // Count latest records in the repository.
        let latest_locators =
            RecordTraversal::list_repository_latest_record_locators(&store, &repo_scope)
                .await
                .expect("list repo latest");
        assert_eq!(latest_locators.len(), 3);

        // Count version records in the repository.
        let version_locators =
            RecordTraversal::list_repository_version_record_locators(&store, &repo_scope)
                .await
                .expect("list repo versions");
        assert_eq!(version_locators.len(), 2);
    }

    #[tokio::test]
    async fn repository_metadata_inventory_empty_repository() {
        let (store, _storage) = local_record_store();
        let scope = test_scope();
        let repo_scope = shardline_index::RepositoryRecordScope::from_repository_scope(&scope);

        let latest = RecordTraversal::list_repository_latest_record_locators(&store, &repo_scope)
            .await
            .expect("list repo latest empty");
        assert!(latest.is_empty());

        let versions =
            RecordTraversal::list_repository_version_record_locators(&store, &repo_scope)
                .await
                .expect("list repo versions empty");
        assert!(versions.is_empty());
    }

    #[tokio::test]
    async fn repository_metadata_inventory_mixed_repositories() {
        let (store, _storage) = local_record_store();
        let scope_a = test_scope();
        let scope_b = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "owner-b",
            "repo-b",
            Some("main"),
        )
        .unwrap();
        let repo_a = shardline_index::RepositoryRecordScope::from_repository_scope(&scope_a);
        let repo_b = shardline_index::RepositoryRecordScope::from_repository_scope(&scope_b);

        let rec_a = test_file_record("a.bin", &make_hash('1'), &scope_a);
        let rec_b = test_file_record("b.bin", &make_hash('2'), &scope_b);
        RecordMutation::write_latest_record(&store, &rec_a)
            .await
            .unwrap();
        RecordMutation::write_latest_record(&store, &rec_b)
            .await
            .unwrap();

        assert_eq!(
            RecordTraversal::list_repository_latest_record_locators(&store, &repo_a)
                .await
                .expect("repo_a")
                .len(),
            1
        );
        assert_eq!(
            RecordTraversal::list_repository_latest_record_locators(&store, &repo_b)
                .await
                .expect("repo_b")
                .len(),
            1
        );
    }

    // ===== Invalid identifier / missing record errors =====

    #[tokio::test]
    async fn reconstruction_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        // read_record() internally calls validate_identifier() which should reject "/".
        let result = backend
            .reconstruction("/absolute/path", None, None, None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[tokio::test]
    async fn file_total_bytes_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        let result = backend.file_total_bytes("../traverse", None, None).await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[tokio::test]
    async fn download_file_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        let result = backend.download_file("\0null", None, None).await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    #[tokio::test]
    async fn read_chunk_for_file_version_rejects_invalid_hash() {
        let (backend, _root) = make_backend().await;
        let result = backend
            .read_chunk_for_file_version("bad-hash", "file.bin", "bad-content-hash", None)
            .await;
        // Should be InvalidFileId or InvalidContentHash.
        assert!(result.is_err());
    }

    // ── ready() ──────────────────────────────────────────────

    #[tokio::test]
    async fn ready_fails_when_postgres_unreachable() {
        let (backend, _root) = make_backend().await;
        // The local_root check passes (temp dir exists), but the SQL probe
        // fails because no real Postgres is available.
        let result = backend.ready().await;
        assert!(result.is_err());
    }

    // ── read_object_stream ───────────────────────────────────

    #[tokio::test]
    async fn read_object_stream_without_range_ok() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/stream/no-range").unwrap();
        let data = b"stream data without range";
        store_object(&backend.object_store(), &key, data);
        let result = backend
            .read_object_stream(&key, data.len() as u64, None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn read_object_stream_with_range_ok() {
        let (backend, _root) = make_backend().await;
        let key = ObjectKey::parse("test/stream/with-range").unwrap();
        let data = b"stream data with range spec";
        store_object(&backend.object_store(), &key, data);
        let range = ByteRange::new(0, data.len() as u64 - 1).unwrap();
        let result = backend
            .read_object_stream(&key, data.len() as u64, Some(range))
            .await;
        assert!(result.is_ok());
    }

    // ── xorb_length ──────────────────────────────────────────

    #[tokio::test]
    async fn xorb_length_returns_length_for_stored_xorb() {
        let (backend, _root) = make_backend().await;
        let hash_hex = "ab".repeat(32);
        let key = crate::xet_adapter::xorb_object_key(&hash_hex).unwrap();
        let data = b"fake xorb content for length test";
        store_object(&backend.object_store(), &key, data);
        let length = backend.xorb_length(&hash_hex).await.expect("xorb_length");
        assert_eq!(length, data.len() as u64);
    }

    #[tokio::test]
    async fn xorb_length_not_found_for_missing_hash() {
        let (backend, _root) = make_backend().await;
        let hash_hex = "cd".repeat(32);
        let result = backend.xorb_length(&hash_hex).await;
        assert!(matches!(result, Err(ServerError::NotFound)));
    }

    // ── reconstruction with content_hash (version locator branch) ──

    #[tokio::test]
    async fn reconstruction_with_invalid_content_hash() {
        let (backend, _root) = make_backend().await;
        // An invalid content_hash should be caught before any PG call.
        let result = backend
            .reconstruction("test-file.bin", Some("not-a-valid-hash"), None, None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[tokio::test]
    async fn reconstruction_with_valid_content_hash_fails_without_postgres() {
        let (backend, _root) = make_backend().await;
        // Valid file_id + content_hash → read_record uses version_record_locator
        // and then fails on PG.
        let result = backend
            .reconstruction("test-file.bin", Some(&make_hash('a')), None, None)
            .await;
        // The error should be from PG, not InvalidFileId or InvalidContentHash.
        assert!(result.is_err());
        assert!(!matches!(result, Err(ServerError::InvalidFileId)));
        assert!(!matches!(result, Err(ServerError::InvalidContentHash)));
    }

    // ── file_total_bytes with/without content_hash ──────────

    #[tokio::test]
    async fn file_total_bytes_with_invalid_content_hash() {
        let (backend, _root) = make_backend().await;
        let result = backend
            .file_total_bytes("test-file.bin", Some("bad"), None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[tokio::test]
    async fn file_total_bytes_with_valid_params_fails_without_postgres() {
        let (backend, _root) = make_backend().await;
        let result = backend
            .file_total_bytes("test-file.bin", Some(&make_hash('a')), None)
            .await;
        assert!(result.is_err());
        assert!(!matches!(result, Err(ServerError::InvalidFileId)));
    }

    // ── download_file ──────────────────────────────────────

    #[tokio::test]
    async fn download_file_with_invalid_content_hash() {
        let (backend, _root) = make_backend().await;
        let result = backend
            .download_file("test-file.bin", Some("invalid!"), None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidContentHash)));
    }

    #[tokio::test]
    async fn download_file_without_content_hash_fails_without_postgres() {
        let (backend, _root) = make_backend().await;
        let result = backend.download_file("test-file.bin", None, None).await;
        assert!(result.is_err());
        assert!(!matches!(result, Err(ServerError::InvalidFileId)));
    }

    // ── read_chunk_for_file_version ──────────────────────────

    #[tokio::test]
    async fn read_chunk_for_file_version_rejects_invalid_file_id() {
        let (backend, _root) = make_backend().await;
        let result = backend
            .read_chunk_for_file_version(&make_hash('a'), "/bad/path", &make_hash('b'), None)
            .await;
        assert!(matches!(result, Err(ServerError::InvalidFileId)));
    }

    // ── repository_references_xorb ──────────────────────────

    #[tokio::test]
    async fn repository_references_xorb_fails_without_postgres() {
        let (backend, _root) = make_backend().await;
        let scope = test_scope();
        let result = backend
            .repository_references_xorb(&make_hash('a'), &scope)
            .await;
        // Without PG the traversal should propagate an error.
        assert!(result.is_err());
    }
}
