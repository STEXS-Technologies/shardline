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

    pub(crate) const fn object_backend_name(&self) -> &'static str {
        self.object_store.backend_name()
    }

    pub(crate) fn object_store(&self) -> ServerObjectStore {
        self.object_store.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;
    use tempfile::tempdir;
    use crate::object_store::ServerObjectStore;

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
        assert!(
            backend.is_ok(),
            "constructor should succeed with lazy pool"
        );
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
