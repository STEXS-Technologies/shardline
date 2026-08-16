use std::{num::NonZeroUsize, path::PathBuf, sync::Arc};

use shardline_index::{
    FileChunkRecord, LocalIndexStore, LocalRecordStore, ReconstructionStore, RecordTraversal,
    RepoKey, RevisionRecord, S3ObjectEntry, S3ObjectIndexStore, TreeEntry, TreeKey, TreeStore,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_storage::{ObjectPrefix, ObjectStore};

use crate::{
    ServerError, ServerFrontend,
    chunk_store::chunk_hash_from_chunk_object_key_if_present,
    config::default_upload_max_in_flight_chunks,
    local_path::ensure_directory_path_components_are_not_symlinked,
    model::ServerStatsResponse,
    object_store::{ServerObjectStore, visit_object_prefix},
    overflow::{checked_add, checked_increment},
    validation::validate_content_hash,
};

use super::records::read_record;

/// Local filesystem backend for file chunk storage and reconstruction metadata.
#[derive(Debug, Clone)]
pub struct LocalBackend {
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: LocalIndexStore,
    pub(super) record_store: LocalRecordStore,
    pub(super) object_store: ServerObjectStore,
    pub(super) metadata_write_lock: Arc<tokio::sync::Mutex<()>>,
    pub(crate) protocol_upload_lock: Arc<tokio::sync::Mutex<()>>,
}

impl LocalBackend {
    /// Creates a local backend and initializes its directory structure.
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
        let backend = Self {
            // Initialize and migrate SQLite before the server begins accepting
            // concurrent protocol requests. Lazy first-use initialization lets
            // simultaneous uploads race while creating the schema/import marker.
            index_store: LocalIndexStore::new(root.clone())?,
            record_store: LocalRecordStore::open(root),
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
            object_store,
            metadata_write_lock: Arc::new(tokio::sync::Mutex::new(())),
            protocol_upload_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
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
        let object_store = self.object_store();
        let prefix = ObjectPrefix::parse("").map_err(|_error| ServerError::InvalidContentHash)?;
        let mut chunks = 0_u64;
        let mut chunk_bytes = 0_u64;
        visit_object_prefix(&object_store, &prefix, |metadata| {
            let is_chunk = chunk_hash_from_chunk_object_key_if_present(metadata.key())?.is_some();
            if is_chunk {
                chunks = checked_increment(chunks)?;
                chunk_bytes = checked_add(chunk_bytes, metadata.length())?;
            }

            Ok(())
        })?;
        let files = u64::try_from(
            RecordTraversal::list_latest_record_locators(&self.record_store)
                .await?
                .len(),
        )?;

        Ok(ServerStatsResponse {
            chunks,
            chunk_bytes,
            files,
        })
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
        use shardline_index::{UploadIntentState, UploadIntentStore};
        use shardline_storage::{AsyncObjectStore, ObjectKey};
        use std::time::{Duration, SystemTime, UNIX_EPOCH};

        let mut reconciled = 0u64;
        for state in &[
            UploadIntentState::Created,
            UploadIntentState::Storing,
            UploadIntentState::Stored,
            UploadIntentState::MetadataCommitted,
        ] {
            let intents = match self.index_store.intents_by_state(*state).await {
                Ok(intents) => intents,
                Err(e) => {
                    // If the intents table does not exist yet (no migration has created it),
                    // there is nothing to reconcile. Log and continue.
                    tracing::debug!("intent query skipped (table may not exist): {e}");
                    continue;
                }
            };
            for intent in &intents {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO);
                let age = now.saturating_sub(intent.created_at());
                if age < minimum_age {
                    continue;
                }
                tracing::warn!(
                    intent_id = %intent.intent_id(),
                    state = ?intent.state(),
                    age_secs = age.as_secs(),
                    "reconciling stuck intent"
                );
                let visible_file_record =
                    if let Some(file_id) = intent.object_key().strip_prefix("record/") {
                        match self.read_record(file_id, None, None).await {
                            Ok(_record) => true,
                            Err(ServerError::NotFound) => false,
                            Err(error) => return Err(error),
                        }
                    } else {
                        false
                    };
                if visible_file_record {
                    // The atomic file-record commit is the visibility boundary.
                    // Walk every durable boundary so recovery remains valid even
                    // when the process stopped while the intent was still Stored.
                    for recovery_state in [
                        UploadIntentState::Storing,
                        UploadIntentState::Stored,
                        UploadIntentState::MetadataCommitted,
                        UploadIntentState::Visible,
                    ] {
                        if let Err(error) = self
                            .index_store
                            .transition_intent(intent.intent_id(), recovery_state)
                            .await
                        {
                            tracing::warn!(
                                intent_id = %intent.intent_id(),
                                %error,
                                "intent recovery transition failed"
                            );
                            return Err(error.into());
                        }
                    }
                    reconciled = reconciled.saturating_add(1);
                    continue;
                }
                let target_state = if intent.state() == UploadIntentState::MetadataCommitted {
                    match ObjectKey::parse(intent.object_key()) {
                        Ok(key)
                            if AsyncObjectStore::metadata(&self.object_store, &key)
                                .await?
                                .is_some() =>
                        {
                            UploadIntentState::Visible
                        }
                        Ok(_) | Err(_) => UploadIntentState::Failed,
                    }
                } else {
                    UploadIntentState::Failed
                };
                if let Err(e) = self
                    .index_store
                    .transition_intent(intent.intent_id(), target_state)
                    .await
                {
                    tracing::warn!(intent_id = %intent.intent_id(), error = %e, "intent transition failed, skipping");
                } else {
                    reconciled = reconciled.saturating_add(1);
                }
            }
        }
        if reconciled > 0 {
            tracing::info!(count = reconciled, "intent reconciliation complete");
        }
        Ok(())
    }

    pub(super) async fn read_record(
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

    /// Upserts one S3 object listing-index row.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index write fails.
    pub(crate) async fn upsert_s3_object(&self, entry: &S3ObjectEntry) -> Result<(), ServerError> {
        self.index_store.upsert_s3_object(entry).await?;
        Ok(())
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

    /// Resolves a single canonical path to its tree entry, if any.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index lookup fails.
    pub(crate) async fn resolve_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
    ) -> Result<Option<TreeEntry>, ServerError> {
        Ok(self.index_store.tree_entry(key, path).await?)
    }

    /// Scans raw tree rows for a revision under `prefix`, resuming after `cursor`,
    /// returning at most `limit` raw rows ordered by path.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index scan fails.
    pub(crate) async fn scan_tree_raw(
        &self,
        key: &TreeKey,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<TreeEntry>, ServerError> {
        Ok(self
            .index_store
            .scan_tree(key, prefix, cursor, limit)
            .await?)
    }

    /// Validates the referenced file record and upserts a path mapping, wrapped in
    /// the metadata write lock so concurrent registrations cannot race.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::UnregisteredFile`] when no record exists in the revision
    /// scope, [`ServerError::InvalidContentHash`] for a malformed `file_id`, or the
    /// adapter error when persistence fails.
    pub(crate) async fn register_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
        file_id: &str,
        repository_scope: Option<&shardline_protocol::RepositoryScope>,
    ) -> Result<super::RegisterPathOutcome, ServerError> {
        let _guard = self.metadata_write_lock.lock().await;
        validate_content_hash(file_id)?;
        let record = match self.read_record(file_id, None, repository_scope).await {
            Ok(record) => record,
            Err(ServerError::NotFound) => {
                return Err(ServerError::UnregisteredFile(key.revision.clone()));
            }
            Err(error) => return Err(error),
        };
        let now = unix_now_seconds_lossy();
        let revision_record = RevisionRecord {
            provider: key.provider.clone(),
            owner: key.owner.clone(),
            repo: key.repo.clone(),
            revision: key.revision.clone(),
            created_at_unix_seconds: now,
            updated_at_unix_seconds: now,
        };
        // Auto-create the revision registry row when it does not yet exist.
        let _created = self.index_store.upsert_revision(&revision_record).await?;
        let entry = TreeEntry {
            provider: key.provider.clone(),
            owner: key.owner.clone(),
            repo: key.repo.clone(),
            revision: key.revision.clone(),
            path: path.to_owned(),
            file_id: file_id.to_owned(),
            size_bytes: record.total_bytes,
            updated_at_unix_seconds: now,
        };
        let outcome = self.index_store.upsert_tree_entry(&entry).await?;
        Ok(super::RegisterPathOutcome {
            entry,
            created: outcome.created,
        })
    }

    /// Deletes one path mapping (or the path and every descendant when `recursive`).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index deletion fails.
    pub(crate) async fn delete_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
        recursive: bool,
    ) -> Result<u64, ServerError> {
        Ok(self
            .index_store
            .delete_tree_entries(key, path, recursive)
            .await?)
    }

    /// Lists the revision registry for a repository.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index lookup fails.
    pub(crate) async fn list_revisions(
        &self,
        key: &RepoKey,
    ) -> Result<Vec<RevisionRecord>, ServerError> {
        Ok(self.index_store.list_revisions(key).await?)
    }

    /// Creates a revision registry row, returning whether it was newly created.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index upsert fails.
    pub(crate) async fn create_revision(&self, rev: &RevisionRecord) -> Result<bool, ServerError> {
        Ok(self.index_store.upsert_revision(rev).await?)
    }

    /// Deletes a revision and all of its tree entries, returning whether the revision
    /// previously existed.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index deletion fails.
    pub(crate) async fn delete_revision(
        &self,
        key: &RepoKey,
        rev: &str,
    ) -> Result<bool, ServerError> {
        let removed = self.index_store.delete_revision(key, rev).await?;
        Ok(removed > 0)
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
#[must_use]
pub fn chunk_hash(bytes: &[u8]) -> shardline_protocol::ShardlineHash {
    let digest = blake3::hash(bytes);
    shardline_protocol::ShardlineHash::from_bytes(*digest.as_bytes())
}

pub(crate) fn content_hash(
    total_bytes: u64,
    chunk_size: u64,
    chunks: &[FileChunkRecord],
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&total_bytes.to_le_bytes());
    hasher.update(&chunk_size.to_le_bytes());
    for chunk in chunks {
        hasher.update(chunk.hash.as_bytes());
        hasher.update(&chunk.offset.to_le_bytes());
        hasher.update(&chunk.length.to_le_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use shardline_index::{FileChunkRecord, UploadIntent, UploadIntentState, UploadIntentStore};
    use shardline_protocol::ShardlineHash;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

    use super::LocalBackend;
    use super::{chunk_hash, content_hash};

    #[test]
    fn chunk_hash_is_deterministic() {
        let data = b"hello world";
        let hash1 = chunk_hash(data);
        let hash2 = chunk_hash(data);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn chunk_hash_differs_for_different_inputs() {
        let hash1 = chunk_hash(b"hello");
        let hash2 = chunk_hash(b"world");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn chunk_hash_returns_fixed_length() {
        let hash = chunk_hash(b"test data");
        assert_eq!(hash.as_bytes().len(), 32);
    }

    #[test]
    fn chunk_hash_empty_input() {
        let hash = chunk_hash(b"");
        // Should still produce a valid hash
        assert_eq!(hash.as_bytes().len(), 32);
    }

    #[test]
    fn content_hash_is_deterministic() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let hash1 = content_hash(10, 10, &chunks);
        let hash2 = content_hash(10, 10, &chunks);
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn content_hash_differs_for_different_chunks() {
        let chunks_a = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let chunks_b = [FileChunkRecord {
            hash: "def".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        assert_ne!(
            content_hash(10, 10, &chunks_a),
            content_hash(10, 10, &chunks_b)
        );
    }

    #[test]
    fn content_hash_empty_chunks() {
        let hash = content_hash(0, 0, &[]);
        assert!(!hash.is_empty());
        assert_eq!(hash.len(), 64); // blake3 hex is 64 chars
    }

    #[test]
    fn content_hash_includes_total_bytes_and_chunk_size() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        // Different total_bytes should produce different hashes
        assert_ne!(content_hash(10, 10, &chunks), content_hash(20, 10, &chunks));
    }

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
            "sha256".to_owned(),
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
        let stored_record = backend
            .index_store
            .intent_by_id(stored_record.intent_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored_record.state(), UploadIntentState::Visible);
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
        assert!(backend.is_ok());
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

    #[test]
    fn content_hash_with_single_chunk() {
        let chunks = [FileChunkRecord {
            hash: "abc".to_owned(),
            offset: 0,
            length: 10,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 10,
        }];
        let hash = content_hash(10, 10, &chunks);
        assert_eq!(hash.len(), 64); // blake3 hex
    }

    #[test]
    fn content_hash_with_multiple_chunks() {
        let chunks = [
            FileChunkRecord {
                hash: "abc".to_owned(),
                offset: 0,
                length: 10,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 10,
            },
            FileChunkRecord {
                hash: "def".to_owned(),
                offset: 10,
                length: 20,
                range_start: 1,
                range_end: 2,
                packed_start: 10,
                packed_end: 30,
            },
        ];
        let hash = content_hash(30, 10, &chunks);
        assert_eq!(hash.len(), 64);
        assert!(!hash.is_empty());
    }
}
