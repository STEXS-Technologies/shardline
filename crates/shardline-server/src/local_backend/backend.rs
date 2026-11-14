use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use shardline_index::{
    LocalIndexStore, LocalRecordStore, OciObjectKey, OciObjectStore, OciTagEntry,
    ReconstructionStore, RecordTraversal,
};
use shardline_storage::ObjectStore;

use crate::{
    ServerError, ServerFrontend, config::default_upload_max_in_flight_chunks,
    local_path::ensure_directory_path_components_are_not_symlinked, model::ServerStatsResponse,
    object_store::ServerObjectStore,
};

use super::records::read_record;

/// Local filesystem backend for file chunk storage and reconstruction metadata.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub(crate) metadata_root: PathBuf,
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: LocalIndexStore,
    pub(super) record_store: LocalRecordStore,
    pub(super) object_store: ServerObjectStore,
    pub(crate) stream_work_pool: crate::admission::BoundedPool,
    pub(crate) packing_work_pool: crate::admission::BoundedPool,
    pub(super) metadata_write_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) protocol_upload_lock: Arc<tokio::sync::Mutex<()>>,
}

impl LocalBackend {
    pub(crate) const fn index_store(&self) -> &LocalIndexStore {
        &self.index_store
    }

    pub(crate) async fn lock_metadata_writes(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.metadata_write_lock.lock().await
    }

    pub(crate) const fn record_store(&self) -> &LocalRecordStore {
        &self.record_store
    }

    pub(crate) fn xet_file_identity(
        &self,
        repository_scope: &shardline_protocol::RepositoryScope,
        sha256: &str,
    ) -> Result<Option<shardline_index::XetFileIdentity>, ServerError> {
        self.record_store
            .xet_file_identity(repository_scope, sha256)
            .map_err(ServerError::from)
    }

    /// Creates a local backend and initializes its directory structure.
    ///
    /// Initialization prepares the existing GC lock file when permissions allow.
    /// Public uploads acquire this barrier automatically before writing. An
    /// existing read-only deployment can still initialize for reads; uploads
    /// require the barrier to be available.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store(root, public_base_url, chunk_size, object_store).await
    }

    /// Creates a local backend with explicit upload chunk parallelism.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local directories cannot be created.
    pub async fn new_with_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
    ) -> Result<Self, ServerError> {
        let object_store = ServerObjectStore::local(root.join("chunks"))?;
        Self::new_with_object_store_and_upload_parallelism(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
        )
        .await
    }

    pub(crate) async fn new_with_object_store(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            default_upload_max_in_flight_chunks(),
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism_with_frontends(
        root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        object_store: ServerObjectStore,
        server_frontends: &[ServerFrontend],
    ) -> Result<Self, ServerError> {
        ensure_directory_path_components_are_not_symlinked(&root)?;
        let index_store = LocalIndexStore::open(root.clone());
        index_store.check_schema_compatibility()?;
        let backend = Self {
            // Initialize and migrate SQLite before the server begins accepting
            // concurrent protocol requests. Lazy first-use initialization lets
            // simultaneous uploads race while creating the schema/import marker.
            index_store: LocalIndexStore::new_with_persistent_connection(root.clone())?,
            metadata_root: std::path::absolute(&root)?,
            record_store: LocalRecordStore::open(root),
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
            object_store,
            stream_work_pool: crate::admission::ExecutionPools::default_sizes().blocking_io,
            packing_work_pool: crate::admission::ExecutionPools::default_sizes().hashing,
            metadata_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            protocol_upload_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        if let Err(error) =
            crate::maintenance_barrier::prepare_local_barrier(&backend.metadata_root).await
        {
            // Preserve read-only initialization. Uploads still acquire the
            // barrier before writing and cannot bypass an unavailable lock.
            tracing::debug!("local upload barrier preparation unavailable: {error}");
        }
        Ok(backend)
    }

    /// Returns the public base URL used in generated download links.
    #[must_use]
    pub fn public_base_url(&self) -> &str {
        &self.public_base_url
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
    }

    /// Verifies that local storage paths remain reachable.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the local object store or metadata roots cannot be
    /// traversed.
    pub async fn ready(&self) -> Result<(), ServerError> {
        let object_store = self.object_store();
        if let Some(local_root) = object_store.local_root() {
            tokio::fs::create_dir_all(local_root).await?;
        } else {
            let probe_key = shardline_storage::ObjectKey::parse("health/probe")
                .map_err(|_error| ServerError::InvalidContentHash)?;
            let _object_store_reachable = object_store.metadata(&probe_key)?;
        }
        let _latest = RecordTraversal::list_latest_record_locators(&self.record_store).await?;
        let _reconstructions =
            ReconstructionStore::list_reconstruction_file_ids(&self.index_store)?;
        Ok(())
    }

    /// Returns local backend storage stats.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when local metadata cannot be traversed.
    pub async fn stats(&self) -> Result<ServerStatsResponse, ServerError> {
        crate::storage_stats::storage_stats(&self.object_store(), &self.record_store).await
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
    }

    /// Probes the SQLite metadata store for connectivity.
    pub(crate) fn probe_metadata(&self) -> Result<(), String> {
        self.index_store.probe()
    }

    pub(crate) async fn reconcile_stuck_upload_intents(&self) -> Result<(), ServerError> {
        self.reconcile_stuck_upload_intents_older_than(std::time::Duration::from_secs(30))
            .await
    }

    async fn reconcile_stuck_upload_intents_older_than(
        &self,
        minimum_age: std::time::Duration,
    ) -> Result<(), ServerError> {
        self.reconcile_upload_intents(
            minimum_age,
            &crate::upload_intent_recovery::startup::RecoveryPass::Complete,
        )
        .await
        .map(|_pending| ())
    }

    pub(crate) async fn reconcile_upload_intents(
        &self,
        minimum_age: std::time::Duration,
        pass: &crate::upload_intent_recovery::startup::RecoveryPass,
    ) -> Result<bool, ServerError> {
        crate::upload_intent_recovery::reconcile(
            &self.index_store,
            self.object_store(),
            self.stream_work_pool.clone(),
            minimum_age,
            pass,
            |file_id, scope| async move { self.read_record(&file_id, None, scope.as_ref()).await },
        )
        .await
    }

    pub(crate) async fn read_record(
        &self,
        file_id: &str,
        content_hash: Option<&str>,
        repository_scope: Option<&shardline_protocol::RepositoryScope>,
    ) -> Result<shardline_index::FileRecord, ServerError> {
        read_record(&self.record_store, file_id, content_hash, repository_scope).await
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
    ) -> Result<shardline_index::FileRecord, ServerError> {
        self.read_record(file_id, None, None).await
    }

    pub(crate) async fn publish_oci_object(
        &self,
        key: &OciObjectKey,
        tags: &[OciTagEntry],
    ) -> Result<(), ServerError> {
        self.index_store.publish_oci_object(key, tags).await?;
        Ok(())
    }

    pub(crate) async fn delete_oci_object(&self, key: &OciObjectKey) -> Result<(), ServerError> {
        self.index_store.delete_oci_object(key).await?;
        Ok(())
    }
}

/// Computes the BLAKE3 content hash used to address a chunk.
///
/// This is the same hash the local backend uses to store and retrieve chunk
/// objects, so it is safe to use for content-addressing before upload.
///
/// # Examples
///
/// ```
/// use shardline_server::chunk_hash;
///
/// let hash = chunk_hash(b"hello world");
/// assert_eq!(hash.hex_string().len(), 64);
/// assert_eq!(hash, chunk_hash(b"hello world"));
/// ```
pub use shardline_server_core::chunk_hash;
pub(crate) use shardline_server_core::content_hash;

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
    use shardline_protocol::ShardlineHash;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

    use super::LocalBackend;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_new_creates_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await;
        assert!(backend.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normalized_shard_recovery_preserves_retry_after_metadata_failure() {
        let root = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            root.path().to_owned(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(4096).unwrap(),
        )
        .await
        .unwrap();
        let payload = b"normalized local shard retry";
        let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload);
        backend.upload_xorb(&hash, xorb).await.unwrap();
        let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(payload, &hash)]);
        let (intent_id, _repository) =
            crate::upload_identity::native_shard(blake3::hash(&shard).to_hex().as_ref(), None)
                .unwrap();
        let database = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        database.execute_batch("CREATE TRIGGER deny_native_metadata BEFORE INSERT ON shardline_file_records BEGIN SELECT RAISE(ABORT, 'fixture denies native publication'); END;").unwrap();
        assert!(
            backend
                .upload_shard_stream(
                    crate::upload_ingest::RequestBodyReader::from_bytes(shard.clone()),
                    None,
                    crate::ShardMetadataLimits::default()
                )
                .await
                .is_err()
        );
        assert_eq!(
            backend
                .index_store
                .intent_by_id(&intent_id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
        backend
            .reconcile_stuck_upload_intents_older_than(std::time::Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            backend
                .index_store
                .intent_by_id(&intent_id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Storing
        );
        assert!(matches!(
            backend.read_record(&file_hash, None, None).await,
            Err(crate::ServerError::NotFound)
        ));
        database
            .execute_batch("DROP TRIGGER deny_native_metadata")
            .unwrap();
        backend
            .upload_shard_stream(
                crate::upload_ingest::RequestBodyReader::from_bytes(shard),
                None,
                crate::ShardMetadataLimits::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            backend
                .index_store
                .intent_by_id(&intent_id)
                .await
                .unwrap()
                .unwrap()
                .state(),
            UploadIntentState::Visible
        );
        assert!(backend.read_record(&file_hash, None, None).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconciliation_promotes_visible_storage_across_crash_boundaries() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65_536).unwrap(),
        )
        .await
        .unwrap();
        let key = ObjectKey::parse("xorbs/reconcile-test").unwrap();
        let bytes = b"reconciled";
        let integrity = ObjectIntegrity::new(
            ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
            bytes.len() as u64,
        );
        backend
            .object_store
            .put_if_absent(&key, ObjectBody::from_slice(bytes), &integrity)
            .unwrap();

        let intent = UploadIntent::new(
            "committed-reconcile".to_owned(),
            key.as_str().to_owned(),
            "hash".to_owned(),
            bytes.len() as u64,
        );
        backend.index_store.create_intent(&intent).await.unwrap();
        for state in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ] {
            assert!(
                backend
                    .index_store
                    .transition_intent(intent.intent_id(), state)
                    .await
                    .unwrap()
            );
        }
        let missing = UploadIntent::new(
            "missing-reconcile".to_owned(),
            "xorbs/missing-reconcile-test".to_owned(),
            "hash".to_owned(),
            1,
        );
        backend.index_store.create_intent(&missing).await.unwrap();
        for state in [
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ] {
            assert!(
                backend
                    .index_store
                    .transition_intent(missing.intent_id(), state)
                    .await
                    .unwrap()
            );
        }
        let ambiguous = UploadIntent::new(
            "ambiguous-reconcile".to_owned(),
            key.as_str().to_owned(),
            "hash".to_owned(),
            bytes.len() as u64,
        );
        backend.index_store.create_intent(&ambiguous).await.unwrap();
        assert!(
            backend
                .index_store
                .transition_intent(ambiguous.intent_id(), UploadIntentState::Storing)
                .await
                .unwrap()
        );
        backend
            .upload_file(
                "stored-record-reconcile",
                axum::body::Bytes::from_static(b"visible record"),
                None,
            )
            .await
            .unwrap();
        let stored_record = UploadIntent::new(
            "stored-record-intent".to_owned(),
            "record/stored-record-reconcile".to_owned(),
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(b"visible record")),
            14,
        );
        backend
            .index_store
            .create_intent(&stored_record)
            .await
            .unwrap();
        for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
            assert!(
                backend
                    .index_store
                    .transition_intent(stored_record.intent_id(), state)
                    .await
                    .unwrap()
            );
        }

        let checksum_failed = UploadIntent::new(
            "failed-checksum-record-intent".to_owned(),
            "record/stored-record-reconcile".to_owned(),
            "00".repeat(32),
            0,
        );
        backend
            .index_store
            .create_intent(&checksum_failed)
            .await
            .unwrap();
        assert!(
            backend
                .index_store
                .transition_intent(checksum_failed.intent_id(), UploadIntentState::Storing,)
                .await
                .unwrap()
        );
        let unrelated_stored = UploadIntent::new(
            "unrelated-stored-record-intent".to_owned(),
            "record/stored-record-reconcile".to_owned(),
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(b"unrelated version")),
            0,
        );
        backend
            .index_store
            .create_intent(&unrelated_stored)
            .await
            .unwrap();
        for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
            assert!(
                backend
                    .index_store
                    .transition_intent(unrelated_stored.intent_id(), state)
                    .await
                    .unwrap()
            );
        }

        backend
            .reconcile_stuck_upload_intents_older_than(std::time::Duration::ZERO)
            .await
            .unwrap();
        let reconciled = backend
            .index_store
            .intent_by_id(intent.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reconciled.state(), UploadIntentState::Visible);
        let missing = backend
            .index_store
            .intent_by_id(missing.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(missing.state(), UploadIntentState::Failed);
        let ambiguous = backend
            .index_store
            .intent_by_id(ambiguous.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ambiguous.state(), UploadIntentState::Storing);
        let stored_record = backend
            .index_store
            .intent_by_id(stored_record.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_record.state(), UploadIntentState::Visible);
        for (intent, expected) in [
            (&checksum_failed, UploadIntentState::Storing),
            (&unrelated_stored, UploadIntentState::Stored),
        ] {
            assert_eq!(
                backend
                    .index_store
                    .intent_by_id(intent.intent_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .state(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn scoped_file_recovery_preserves_absent_and_literal_dash_revisions() {
        use shardline_index::{UploadIntent, UploadIntentState, UploadIntentStore};
        use shardline_protocol::{RepositoryProvider, RepositoryScope};
        let root = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            root.path().to_path_buf(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(128).unwrap(),
        )
        .await
        .unwrap();
        for (id, revision, bytes) in [
            ("absent-revision", None, b"absent".as_slice()),
            ("literal-dash", Some("-"), b"dash".as_slice()),
        ] {
            let scope =
                RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", revision)
                    .unwrap();
            let digest = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(bytes));
            backend
                .upload_file_stream(
                    "scoped-file",
                    crate::upload_ingest::RequestBodyReader::from_bytes(
                        axum::body::Bytes::copy_from_slice(bytes),
                    ),
                    Some(&scope),
                    Some(&digest),
                )
                .await
                .unwrap();
            let intent =
                UploadIntent::new(id.to_owned(), "record/scoped-file".to_owned(), digest, 0);
            let repository =
                crate::upload_intent_recovery::file_intent_repository_scope(Some(&scope)).unwrap();
            backend
                .index_store
                .create_intent_scoped(&intent, "shardline", &repository)
                .await
                .unwrap();
            for state in [UploadIntentState::Storing, UploadIntentState::Stored] {
                assert!(
                    backend
                        .index_store
                        .transition_intent(id, state)
                        .await
                        .unwrap()
                );
            }
            backend
                .reconcile_stuck_upload_intents_older_than(std::time::Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(
                backend
                    .index_store
                    .intent_by_id(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .state(),
                UploadIntentState::Visible
            );
            let events = backend.index_store.reliability_events(id).await.unwrap();
            assert_eq!(events.len(), 5);
            assert!(
                matches!(crate::upload_intent_recovery::file_recovery_scope(&events),
                crate::upload_intent_recovery::FileRecoveryScope::Known(Some(actual)) if actual == scope)
            );
            assert_eq!(
                backend
                    .read_record("scoped-file", None, Some(&scope))
                    .await
                    .unwrap()
                    .total_bytes,
                u64::try_from(bytes.len()).unwrap()
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_public_base_url_returns_configured_url() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://localhost:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        assert_eq!(backend.public_base_url(), "http://localhost:8080");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_object_backend_name_returns_local() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        assert_eq!(backend.object_backend_name(), "local");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_ready_succeeds_with_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.ready().await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_stats_returns_zero_counts_for_empty_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let stats = backend.stats().await.unwrap();
        assert_eq!(stats.objects, 0);
        assert_eq!(stats.object_bytes, 0);
        assert_eq!(stats.chunks, 0);
        assert_eq!(stats.chunk_bytes, 0);
        assert_eq!(stats.files, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_object_store_returns_usable_store() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let store = backend.object_store();
        let key = ObjectKey::parse("test/probe").unwrap();
        let _meta = store.metadata(&key);
        // Should not panic
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_new_with_upload_parallelism_creates_backend() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new_with_upload_parallelism(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
            NonZeroUsize::new(4).unwrap_or(NonZeroUsize::MIN),
        )
        .await;
        let backend = backend.unwrap();
        assert_eq!(backend.public_base_url(), "http://127.0.0.1:8080");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_read_record_returns_not_found_for_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.read_record("nonexistent.txt", None, None).await;
        assert!(matches!(result, Err(crate::ServerError::NotFound)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_read_record_rejects_invalid_file_id() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = LocalBackend::new(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
        )
        .await
        .unwrap();
        let result = backend.read_record("../bad", None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_backend_ready_with_blackhole_store_hits_non_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let blackhole = crate::object_store::ServerObjectStore::blackhole();
        // The blackhole store has no local_root, so ready() goes through the
        // else branch (ObjectKey::parse + metadata probe).
        // Blackhole returns Ok(None) for metadata, not an error, so ready() continues
        // to the record store checks and eventually succeeds.
        let backend = LocalBackend::new_with_object_store(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap_or(NonZeroUsize::MIN),
            blackhole,
        )
        .await
        .unwrap();
        let result = backend.ready().await;
        assert!(
            result.is_ok(),
            "ready() with blackhole store should succeed since metadata returns Ok(None), got: {:?}",
            result.err()
        );
    }
}
