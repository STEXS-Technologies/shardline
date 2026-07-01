use std::{num::NonZeroUsize, path::PathBuf};

use shardline_index::{PostgresIndexStore, PostgresRecordStore};

use crate::{
    ServerError, ServerFrontend, config::default_upload_max_in_flight_chunks,
    object_store::ServerObjectStore,
};

mod read;
mod stats;
mod upload;

pub(super) use read::*;

/// Server backend that keeps file metadata in Postgres and object bytes in the selected store.
#[derive(Debug, Clone)]
pub struct PostgresBackend {
    public_base_url: String,
    chunk_size: NonZeroUsize,
    upload_max_in_flight_chunks: NonZeroUsize,
    server_frontends: Vec<ServerFrontend>,
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
        FileChunkRecord, FileRecord, RecordMutation, RecordStoreFuture, RecordTraversal,
        RepositoryRecordScope,
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

    impl RecordTraversal for GuardedScopeRecordStore {
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

        fn record_locator_exists<'operation>(
            &'operation self,
            _locator: &'operation Self::Locator,
        ) -> RecordStoreFuture<'operation, bool, Self::Error> {
            Box::pin(ready(Ok(false)))
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

    impl RecordMutation for GuardedScopeRecordStore {
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

        fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
            Box::pin(ready(Ok(())))
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
