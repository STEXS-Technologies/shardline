use std::{num::NonZeroUsize, path::PathBuf};

use shardline_index::{
    PostgresIndexStore, PostgresRecordStore, RepoKey, RevisionRecord, TreeEntry, TreeKey, TreeStore,
};
use shardline_protocol::unix_now_seconds_lossy;

use super::connect_postgres_metadata_pool;
use crate::{
    ServerError, ServerFrontend, config::default_upload_max_in_flight_chunks,
    object_store::ServerObjectStore, validation::validate_content_hash,
};

/// Server backend that keeps file metadata in Postgres and object bytes in the selected store.
#[derive(Debug, Clone)]
pub struct PostgresBackend {
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: PostgresIndexStore,
    pub(super) record_store: PostgresRecordStore,
    pub(super) object_store: ServerObjectStore,
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
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            root,
            public_base_url,
            chunk_size,
            default_upload_max_in_flight_chunks(),
            index_postgres_url,
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn new_with_object_store_and_upload_parallelism(
        _root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        index_postgres_url: &str,
        object_store: ServerObjectStore,
    ) -> Result<Self, ServerError> {
        Self::new_with_object_store_and_upload_parallelism_with_frontends(
            _root,
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            index_postgres_url,
            object_store,
            &[ServerFrontend::Xet],
        )
        .await
    }

    pub(crate) async fn new_with_object_store_and_upload_parallelism_with_frontends(
        _root: PathBuf,
        public_base_url: String,
        chunk_size: NonZeroUsize,
        upload_max_in_flight_chunks: NonZeroUsize,
        index_postgres_url: &str,
        object_store: ServerObjectStore,
        server_frontends: &[ServerFrontend],
    ) -> Result<Self, ServerError> {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 10)?;

        Ok(Self {
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
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

    /// Returns the Postgres metadata index store (shares the backend's pool).
    #[must_use]
    pub(crate) const fn index_store(&self) -> &PostgresIndexStore {
        &self.index_store
    }

    /// Returns the Postgres record store (shares the backend's pool).
    #[must_use]
    pub(crate) const fn record_store(&self) -> &PostgresRecordStore {
        &self.record_store
    }

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
    }

    /// Probes the Postgres metadata store for connectivity.
    pub(crate) async fn probe_metadata(&self) -> Result<(), String> {
        self.index_store.probe().await
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
                let target_state = match ObjectKey::parse(intent.object_key()) {
                    Ok(key) => match AsyncObjectStore::metadata(&self.object_store, &key).await? {
                        Some(metadata) if metadata.length() != intent.object_length() => {
                            Some(UploadIntentState::Failed)
                        }
                        Some(_) if intent.state() == UploadIntentState::MetadataCommitted => {
                            Some(UploadIntentState::Visible)
                        }
                        Some(_) => {
                            // The immutable write may have committed before its
                            // response was lost. Earlier states do not prove that
                            // protocol metadata is visible (notably for shards),
                            // so preserve the retryable state instead of guessing.
                            tracing::warn!(
                                intent_id = %intent.intent_id(),
                                state = ?intent.state(),
                                "durable object found for in-flight intent; preserving for retry"
                            );
                            None
                        }
                        None => Some(UploadIntentState::Failed),
                    },
                    Err(_) => Some(UploadIntentState::Failed),
                };
                let Some(target_state) = target_state else {
                    continue;
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

    /// Validates the referenced file record and upserts a path mapping.
    ///
    /// Auto-creates the revision registry row for `key` when it does not yet
    /// exist, enforcing the same per-repo revision cap as `create_revision`
    /// (F-89): a genuinely new revision is rejected with
    /// [`ServerError::TooManyRevisions`] once the repo holds
    /// `max_revisions_per_repo` rows, while a refresh of an existing revision
    /// stays allowed at capacity. The count/exists-then-insert pair is not
    /// serialized here, so concurrent `register_tree_path` calls can overshoot
    /// the cap by the number of concurrent writers at the boundary (the cap is
    /// a bound on growth, not a hard invariant).
    ///
    /// Also enforces the per-repo tree-entry cap (F-103/F-108): once the repo
    /// holds `max_tree_entries_per_repo` tree-entry rows, the registration is
    /// rejected with [`ServerError::TooManyRevisions`] regardless of whether
    /// the path already exists, mirroring `create_revision`'s count-before-insert
    /// gate (a refresh at capacity is rejected too, so both paths agree at cap).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::UnregisteredFile`] when no record exists in the revision
    /// scope, [`ServerError::InvalidContentHash`] for a malformed `file_id`,
    /// [`ServerError::TooManyRevisions`] at the per-repo revision or tree-entry cap,
    /// or the adapter error when persistence fails.
    pub(crate) async fn register_tree_path(
        &self,
        key: &TreeKey,
        path: &str,
        file_id: &str,
        repository_scope: Option<&shardline_protocol::RepositoryScope>,
        max_revisions_per_repo: NonZeroUsize,
        max_tree_entries_per_repo: NonZeroUsize,
    ) -> Result<crate::backend::RegisterPathOutcome, ServerError> {
        validate_content_hash(file_id)?;
        let record = match self.read_record(file_id, None, repository_scope).await {
            Ok(record) => record,
            Err(ServerError::NotFound) => {
                return Err(ServerError::UnregisteredFile(key.revision.clone()));
            }
            Err(error) => return Err(error),
        };
        // F-89: register_path auto-creates the revision registry row, so the
        // F-75 per-repo cap must be enforced here too — not only in
        // create_revision. Only a genuinely NEW revision counts against the
        // cap; a refresh of an existing revision (upsert 'created' == false)
        // is still allowed at capacity.
        let repo_key = RepoKey::new(&key.provider, &key.owner, &key.repo);
        let cap = u64::try_from(max_revisions_per_repo.get()).unwrap_or(u64::MAX);
        if self.count_revisions(&repo_key).await? >= cap
            && self
                .index_store
                .revision(&repo_key, &key.revision)
                .await?
                .is_none()
        {
            return Err(ServerError::TooManyRevisions);
        }
        // F-103/F-108: per-repo tree-entry cap. Unlike the F-89 revision cap
        // (which exempts a refresh of an existing revision), the tree-entry
        // gate rejects at capacity regardless of whether the path already
        // exists — a refresh at cap would otherwise bypass the bound the same
        // way create_revision's same-name upsert cannot, so both paths stay
        // consistent under one gate.
        let tree_cap = u64::try_from(max_tree_entries_per_repo.get()).unwrap_or(u64::MAX);
        if self.index_store.count_tree_entries(&repo_key).await? >= tree_cap {
            return Err(ServerError::TooManyRevisions);
        }
        let now = unix_now_seconds_lossy();
        let revision_record = RevisionRecord {
            provider: key.provider.clone(),
            owner: key.owner.clone(),
            repo: key.repo.clone(),
            revision: key.revision.clone(),
            created_at_unix_seconds: now,
            updated_at_unix_seconds: now,
        };
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
        Ok(crate::backend::RegisterPathOutcome {
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

    /// Lists the revision registry for a repository, resuming after `cursor`
    /// and returning at most `limit` rows (bounded in the index backend).
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index lookup fails.
    pub(crate) async fn list_revisions(
        &self,
        key: &RepoKey,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RevisionRecord>, ServerError> {
        Ok(self.index_store.list_revisions(key, cursor, limit).await?)
    }

    /// Creates a revision registry row, returning whether it was newly created.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index upsert fails.
    pub(crate) async fn create_revision(&self, rev: &RevisionRecord) -> Result<bool, ServerError> {
        Ok(self.index_store.upsert_revision(rev).await?)
    }

    /// Counts the revision registry rows for a repository.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the index count fails.
    pub(crate) async fn count_revisions(&self, key: &RepoKey) -> Result<u64, ServerError> {
        Ok(self.index_store.count_revisions(key).await?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::ServerObjectStore;
    use std::num::NonZeroUsize;
    use tempfile::tempdir;

    const TEST_POSTGRES_URL: &str = "postgres://localhost:5432/test";

    async fn make_backend() -> PostgresBackend {
        let root = tempdir().unwrap();
        let object_store = ServerObjectStore::local(root.path().join("chunks")).unwrap();
        PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            TEST_POSTGRES_URL,
            object_store,
        )
        .await
        .expect("constructor should succeed with lazy pool")
    }

    #[tokio::test]
    async fn constructor_succeeds() {
        let root = tempdir().unwrap();
        let object_store = ServerObjectStore::local(root.path().join("chunks")).unwrap();
        let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            TEST_POSTGRES_URL,
            object_store,
        )
        .await;
        assert!(backend.is_ok(), "constructor should succeed with lazy pool");
    }

    #[tokio::test]
    async fn constructor_fails_empty_url() {
        let root = tempdir().unwrap();
        let object_store = ServerObjectStore::local(root.path().join("chunks")).unwrap();
        let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            "",
            object_store,
        )
        .await;
        assert!(
            backend.is_err(),
            "constructor should fail with empty postgres URL"
        );
    }

    #[tokio::test]
    async fn public_base_url() {
        let backend = make_backend().await;
        assert_eq!(backend.public_base_url(), "http://127.0.0.1:8080");
    }

    #[tokio::test]
    async fn object_backend_name() {
        let backend = make_backend().await;
        assert_eq!(backend.object_backend_name(), "local");
    }

    #[tokio::test]
    async fn object_store() {
        let root = tempdir().unwrap();
        let object_store = ServerObjectStore::local(root.path().join("chunks")).unwrap();
        let backend = PostgresBackend::new_with_object_store_and_upload_parallelism(
            root.path().to_path_buf(),
            "http://127.0.0.1:8080".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            NonZeroUsize::new(64).unwrap(),
            TEST_POSTGRES_URL,
            object_store.clone(),
        )
        .await
        .unwrap();
        let retrieved = backend.object_store();
        // ServerObjectStore implements Clone but not PartialEq, so compare backend names
        assert_eq!(retrieved.backend_name(), object_store.backend_name());
    }
}
