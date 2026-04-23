use std::{num::NonZeroUsize, path::PathBuf};

use axum::body::Bytes;
use shardline_index::{
    FileRecord, PostgresIndexStore, PostgresMetadataStoreError, PostgresRecordStore, RecordStore,
    RepositoryRecordScope,
};
use shardline_protocol::{ByteRange, RepositoryScope};
use shardline_storage::{ObjectKey, ObjectPrefix};
use sqlx::{PgPool, postgres::PgPoolOptions, query_scalar};
use tokio::task;

use crate::{
    ServerError, ShardMetadataLimits,
    chunk_store::{chunk_hash_from_chunk_object_key_if_present, chunk_object_key},
    config::default_upload_max_in_flight_chunks,
    download_stream::{ServerByteStream, object_byte_range_stream, object_byte_stream},
    model::{
        FileReconstructionResponse, ServerStatsResponse, ShardUploadResponse, UploadFileResponse,
        XorbUploadResponse,
    },
    object_store::{ServerObjectStore, read_full_object, reconstruct_file_record_bytes},
    overflow::{checked_add, checked_increment},
    record_store::parse_stored_file_record_bytes,
    upload_ingest::{FileUploadIngestor, RequestBodyReader},
    validation::{ensure_directory, validate_content_hash, validate_identifier},
    xet_adapter::{
        build_reconstruction_response, register_uploaded_shard_stream, resolve_dedupe_shard_object,
        store_uploaded_xorb_stream, xorb_object_key,
    },
};

/// Server backend that keeps file metadata in Postgres and object bytes in the selected store.
#[derive(Debug, Clone)]
pub struct PostgresBackend {
    public_base_url: String,
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    index_store: PostgresIndexStore,
    record_store: PostgresRecordStore,
    object_store: ServerObjectStore,
}

impl PostgresBackend {
    /// Creates a Postgres-backed metadata backend.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local chunk store cannot initialize or the
    /// Postgres pool configuration is invalid.
    pub async fn new(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        index_postgres_url: &str,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store(
            root,
            public_base_url,
            chunk_size,
            index_postgres_url,
            object_store,
        )
        .await
    }

    pub(crate) async fn new_with_object_store(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        index_postgres_url: &str,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism(
            root,
            public_base_url,
            chunk_size,
            default_upload_max_in_flight_chunks(),
            index_postgres_url,
            object_store,
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism(
        _root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        index_postgres_url: &str,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 10)?;

        Ok(Self {
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            index_store: PostgresIndexStore::new(pool.clone()),
            record_store: PostgresRecordStore::new(pool),
            object_store,
        })
    }

    /// Returns the public base URL used in generated download links.
    #[must_use]
    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
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
        body: RequestBodyReader,
    ) -> Result<XorbUploadResponse, ServerError> {
        let object_store = self.object_store();
        store_uploaded_xorb_stream(&object_store, expected_hash, body).await
    }

    /// Stores a bounded native Xet shard and indexes the contained file reconstructions.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when request streaming, shard validation, referenced xorb
    /// validation, or metadata persistence fails.
    pub(crate) async fn upload_shard_stream(
        &self,
        body: RequestBodyReader,
        repository_scope: Option<&RepositoryScope>,
        shard_metadata_limits: ShardMetadataLimits,
    ) -> Result<ShardUploadResponse, ServerError> {
        let record_store = self.record_store.clone();
        let object_store = self.object_store();
        register_uploaded_shard_stream(
            &object_store,
            body,
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
        build_reconstruction_response(self.public_base_url(), &record, requested_range)
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
        task::spawn_blocking(move || reconstruct_file_record_bytes(&object_store, &record))
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

    /// Loads the stored byte length for a chunk object.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the hash is invalid or the chunk is missing.
    #[cfg(test)]
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

    /// Returns backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when metadata inventory cannot be loaded.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        let object_store = self.object_store();
        let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        object_store.visit_prefix(&prefix, |metadata| {
            let is_chunk = chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let mut files = 0_u64;
        RecordStore::visit_latest_record_locators(&self.record_store, |_locator| {
            files = checked_increment(files)?;
            Ok::<(), ServerError>(())
        })
        .await?;

        Ok(ServerStatsResponse {
            chunks,
            chunk_bytes,
            files,
        })
    }

    fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
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

async fn repository_references_hash_in_scope<RecordAdapter>(
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
) -> Result<PgPool, ServerError> {
    PgPoolOptions::new()
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

const REQUIRED_METADATA_TABLES: [&str; 6] = [
    "shardline_file_records",
    "shardline_file_reconstructions",
    "shardline_stored_objects",
    "shardline_dedupe_shards",
    "shardline_quarantine_candidates",
    "shardline_retention_holds",
];

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        env::var as env_var,
        error::Error,
        future::ready,
        num::NonZeroUsize,
        process,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use serde_json::to_vec;
    use shardline_index::{
        FileChunkRecord, FileRecord, RecordStore, RecordStoreFuture, RepositoryRecordScope,
    };
    use shardline_protocol::{RepositoryProvider, RepositoryScope};
    use sqlx::{PgPool, postgres::PgPoolOptions, query};
    use thiserror::Error;
    use url::Url;

    use super::{PostgresBackend, repository_references_hash_in_scope};
    use crate::{
        InvalidReconstructionResponseError, ServerError, apply_database_migrations,
        object_store::ServerObjectStore,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn postgres_backend_ready_accepts_live_postgres_metadata_store() {
        let result = exercise_postgres_backend_ready_accepts_live_postgres_metadata_store().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "postgres backend readiness flow failed: {error:?}"
        );
    }

    async fn exercise_postgres_backend_ready_accepts_live_postgres_metadata_store()
    -> Result<(), Box<dyn Error>> {
        let Some(base_url) = env_var("DATABASE_URL").ok() else {
            return Ok(());
        };

        let database_name = format!("shardline_postgres_backend_{}", process::id());
        let admin_url = database_url_for(&base_url, "postgres")?;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        recreate_database(&admin_pool, &database_name).await?;

        let database_url = database_url_for(&base_url, &database_name)?;
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;
        apply_database_migrations(&pool).await?;

        let root = tempfile::tempdir()?;
        let object_store = ServerObjectStore::local(root.path().join("chunks"))?;
        let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65_536).unwrap_or(NonZeroUsize::MIN),
            NonZeroUsize::new(64).unwrap_or(NonZeroUsize::MIN),
            &database_url,
            object_store,
        )
        .await?;

        backend.ready().await?;

        Ok(())
    }

    async fn recreate_database(pool: &PgPool, database_name: &str) -> Result<(), Box<dyn Error>> {
        query(&format!("DROP DATABASE IF EXISTS {database_name}"))
            .execute(pool)
            .await?;
        query(&format!("CREATE DATABASE {database_name}"))
            .execute(pool)
            .await?;
        Ok(())
    }
    fn database_url_for(base_url: &str, database_name: &str) -> Result<String, Box<dyn Error>> {
        let mut url = Url::parse(base_url)?;
        url.set_path(database_name);
        Ok(url.to_string())
    }

    #[test]
    fn local_object_store_root_stays_beneath_temp_root() {
        let root = tempfile::tempdir();
        assert!(root.is_ok());
        let Ok(root) = root else {
            return;
        };
        let object_store = ServerObjectStore::local(root.path().join("chunks"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };
        let local_root = object_store.local_root();
        assert!(local_root.is_some());
        let Some(local_root) = local_root else {
            return;
        };
        assert!(local_root.starts_with(root.path()));
    }

    #[tokio::test]
    async fn repository_reference_lookup_stays_repository_scoped_and_avoids_global_latest_walks() {
        let scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main"));
        assert!(scope.is_ok());
        let Ok(scope) = scope else {
            return;
        };
        let other_scope =
            RepositoryScope::new(RepositoryProvider::GitHub, "team", "other", Some("main"));
        assert!(other_scope.is_ok());
        let Ok(other_scope) = other_scope else {
            return;
        };

        let matching_version = file_record(&scope, "wanted-hash");
        let unrelated_latest = file_record(&other_scope, "wanted-hash");
        let store = GuardedScopeRecordStore::new(
            vec![("latest/unrelated".to_owned(), unrelated_latest)],
            vec![("version/matching".to_owned(), matching_version)],
        );

        let found = repository_references_hash_in_scope(&store, "wanted-hash", &scope).await;

        assert!(matches!(found, Ok(true)));
        assert!(
            !store.global_latest_walk_attempted(),
            "scoped repository lookup fell back to a global latest-record walk"
        );
    }

    #[derive(Debug, Error)]
    enum GuardedScopeRecordStoreError {
        #[error("global latest-record walk attempted")]
        GlobalLatestWalkAttempted,
        #[error("record not found")]
        RecordNotFound,
    }

    impl From<GuardedScopeRecordStoreError> for ServerError {
        fn from(value: GuardedScopeRecordStoreError) -> Self {
            match value {
                GuardedScopeRecordStoreError::GlobalLatestWalkAttempted => {
                    InvalidReconstructionResponseError::RecordStoreGlobalLatestWalkAttempted
                }
                GuardedScopeRecordStoreError::RecordNotFound => {
                    InvalidReconstructionResponseError::RecordStoreRecordNotFound
                }
            }
            .into()
        }
    }

    #[derive(Debug)]
    struct GuardedScopeRecordStore {
        latest_records: BTreeMap<String, Vec<u8>>,
        version_records: BTreeMap<String, Vec<u8>>,
        global_latest_walk_attempted: AtomicBool,
    }

    impl GuardedScopeRecordStore {
        fn new(
            latest_records: Vec<(String, FileRecord)>,
            version_records: Vec<(String, FileRecord)>,
        ) -> Self {
            let latest_records = latest_records
                .into_iter()
                .map(|(locator, record)| (locator, serialize_record(&record)))
                .collect();
            let version_records = version_records
                .into_iter()
                .map(|(locator, record)| (locator, serialize_record(&record)))
                .collect();
            Self {
                latest_records,
                version_records,
                global_latest_walk_attempted: AtomicBool::new(false),
            }
        }

        fn global_latest_walk_attempted(&self) -> bool {
            self.global_latest_walk_attempted.load(Ordering::Relaxed)
        }
    }

    impl RecordStore for GuardedScopeRecordStore {
        type Error = GuardedScopeRecordStoreError;
        type Locator = String;

        fn list_latest_record_locators(
            &self,
        ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
            self.global_latest_walk_attempted
                .store(true, Ordering::Relaxed);
            Box::pin(ready(Err(
                GuardedScopeRecordStoreError::GlobalLatestWalkAttempted,
            )))
        }

        fn list_repository_latest_record_locators<'operation>(
            &'operation self,
            _repository: &'operation RepositoryRecordScope,
        ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
            Box::pin(ready(Ok(self.latest_records.keys().cloned().collect())))
        }

        fn list_version_record_locators(
            &self,
        ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
            Box::pin(ready(Ok(self.version_records.keys().cloned().collect())))
        }

        fn list_repository_version_record_locators<'operation>(
            &'operation self,
            _repository: &'operation RepositoryRecordScope,
        ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
            Box::pin(ready(Ok(self.version_records.keys().cloned().collect())))
        }

        fn read_record_bytes<'operation>(
            &'operation self,
            locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
            let bytes = self
                .latest_records
                .get(locator)
                .or_else(|| self.version_records.get(locator))
                .cloned()
                .ok_or(GuardedScopeRecordStoreError::RecordNotFound);
            Box::pin(ready(bytes))
        }

        fn read_latest_record_bytes<'operation>(
            &'operation self,
            _record: &'operation FileRecord,
        ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
            Box::pin(ready(Ok(None)))
        }

        fn write_version_record<'operation>(
            &'operation self,
            _record: &'operation FileRecord,
        ) -> RecordStoreFuture<'operation, (), Self::Error> {
            Box::pin(ready(Ok(())))
        }

        fn write_latest_record<'operation>(
            &'operation self,
            _record: &'operation FileRecord,
        ) -> RecordStoreFuture<'operation, (), Self::Error> {
            Box::pin(ready(Ok(())))
        }

        fn delete_record_locator<'operation>(
            &'operation self,
            _locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, (), Self::Error> {
            Box::pin(ready(Ok(())))
        }

        fn record_locator_exists<'operation>(
            &'operation self,
            _locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, bool, Self::Error> {
            Box::pin(ready(Ok(false)))
        }

        fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
            Box::pin(ready(Ok(())))
        }

        fn modified_since_epoch<'operation>(
            &'operation self,
            _locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
            Box::pin(ready(Ok(Duration::ZERO)))
        }

        fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
            record.file_id.clone()
        }

        fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
            record.content_hash.clone()
        }
    }

    fn file_record(scope: &RepositoryScope, chunk_hash: &str) -> FileRecord {
        FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope: Some(scope.clone()),
            chunks: vec![FileChunkRecord {
                hash: chunk_hash.to_owned(),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        }
    }

    fn serialize_record(record: &FileRecord) -> Vec<u8> {
        let bytes = to_vec(record);
        assert!(bytes.is_ok());
        let Ok(bytes) = bytes else {
            return Vec::new();
        };
        bytes
    }
}
