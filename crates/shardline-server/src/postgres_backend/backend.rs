use std::{num::NonZeroUsize, path::PathBuf};

use shardline_index::{PostgresIndexStore, PostgresRecordStore};

use super::connect_postgres_metadata_pool;
use crate::{
    ServerError, ServerFrontend, config::default_upload_max_in_flight_chunks,
    object_store::ServerObjectStore,
};

/// Server backend that keeps file metadata in Postgres and object bytes in the selected store.
#[derive(Debug, Clone)]
pub struct PostgresBackend {
    pub(super) public_base_url: String,
    pub(super) chunk_size: NonZeroUsize,
    pub(super) upload_max_in_flight_chunks: NonZeroUsize,
    pub(super) server_frontends: Vec<ServerFrontend>,
    pub(super) index_store: PostgresIndexStore,
    pub(crate) gc_barrier_pool: sqlx::PgPool,
    pub(crate) resource_lock_pool: sqlx::PgPool,
    pub(crate) request_session_identity: std::sync::Arc<()>,
    pub(crate) request_session_slots: std::sync::Arc<tokio::sync::Semaphore>,
    pub(super) record_store: PostgresRecordStore,
    pub(super) object_store: ServerObjectStore,
    pub(crate) stream_work_pool: crate::admission::BoundedPool,
    pub(crate) packing_work_pool: crate::admission::BoundedPool,
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
        // Reuse the former 10 metadata + 4 request connections. Admission
        // leaves four connections available for metadata even under write load.
        let pool = connect_postgres_metadata_pool(index_postgres_url, 14)?;

        Ok(Self {
            public_base_url,
            chunk_size,
            upload_max_in_flight_chunks,
            server_frontends: server_frontends.to_vec(),
            index_store: PostgresIndexStore::new(pool.clone()),
            record_store: PostgresRecordStore::new(pool.clone()),
            gc_barrier_pool: pool,
            resource_lock_pool: crate::maintenance_barrier::postgres_coordination_pool(
                index_postgres_url,
            )?,
            request_session_identity: std::sync::Arc::new(()),
            request_session_slots: std::sync::Arc::new(tokio::sync::Semaphore::new(10)),
            object_store,
            stream_work_pool: crate::admission::ExecutionPools::default_sizes().blocking_io,
            packing_work_pool: crate::admission::ExecutionPools::default_sizes().hashing,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_single_connection_pools(mut self, url: &str) -> Result<Self, ServerError> {
        let pool = connect_postgres_metadata_pool(url, 1)?;
        self.index_store = PostgresIndexStore::new(pool.clone());
        self.record_store = PostgresRecordStore::new(pool);
        self.gc_barrier_pool = connect_postgres_metadata_pool(url, 1)?;
        self.resource_lock_pool = connect_postgres_metadata_pool(url, 1)?;
        Ok(self)
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

    pub(crate) async fn xet_file_identity(
        &self,
        repository_scope: &shardline_protocol::RepositoryScope,
        sha256: &str,
    ) -> Result<Option<shardline_index::XetFileIdentity>, crate::ServerError> {
        self.record_store
            .xet_file_identity(repository_scope, sha256)
            .await
            .map_err(crate::ServerError::from)
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
