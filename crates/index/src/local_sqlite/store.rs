use std::path::{Path, PathBuf};

use rusqlite::{Connection, OptionalExtension, params};
use shardline_protocol::unix_now_seconds_lossy;

use crate::{
    DedupeShardMapping, FileId, FileReconstruction, FileRecord, RepositoryRecordScope,
    StoredObjectId, XorbId,
    record_key::repository_record_scope_key as shared_repository_record_scope_key,
    xet_hash_hex_string,
};

use super::{LOCAL_METADATA_DATABASE_FILE_NAME, LocalIndexStoreError, helpers};

/// Local SQLite implementation of [`IndexStore`](crate::IndexStore).
#[derive(Debug, Clone)]
pub struct LocalIndexStore {
    root: PathBuf,
}

impl LocalIndexStore {
    /// Opens a local metadata store rooted at `root` without eagerly mutating the filesystem.
    #[must_use]
    pub fn open(root: PathBuf) -> Self {
        Self {
            root: helpers::normalize_local_root(root),
        }
    }

    /// Creates and initializes a local metadata store rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the local database cannot be created,
    /// migrated, or imported from the legacy filesystem metadata layout.
    pub fn new(root: PathBuf) -> Result<Self, LocalIndexStoreError> {
        let store = Self::open(root);
        let _connection = store.open_connection()?;
        Ok(store)
    }

    /// Returns the store root directory.
    #[must_use]
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn open_connection(&self) -> Result<Connection, LocalIndexStoreError> {
        helpers::initialize_local_metadata_root(&self.root)?;
        let database_path = self.database_path();
        helpers::ensure_sqlite_database_path_is_safe(&database_path)?;
        let mut connection =
            Connection::open_with_flags(database_path, helpers::sqlite_open_flags())?;
        helpers::prepare_connection(&mut connection)?;
        helpers::ensure_local_schema_migrations_table(&connection)?;
        helpers::apply_pending_local_migrations(&mut connection)?;
        helpers::ensure_legacy_import_state(&mut connection, &self.root)?;
        Ok(connection)
    }

    fn database_path(&self) -> PathBuf {
        self.root.join(LOCAL_METADATA_DATABASE_FILE_NAME)
    }

    /// Persists a file reconstruction.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the reconstruction cannot be serialized or written.
    pub fn insert_reconstruction(
        &self,
        file_id: &FileId,
        reconstruction: &FileReconstruction,
    ) -> Result<(), LocalIndexStoreError> {
        let connection = self.open_connection()?;
        helpers::upsert_reconstruction_row(
            &connection,
            file_id,
            reconstruction,
            unix_now_seconds_lossy(),
        )?;
        Ok(())
    }

    /// Persists stored-object presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the object marker cannot be written.
    pub fn insert_object(&self, object_id: &StoredObjectId) -> Result<(), LocalIndexStoreError> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO shardline_stored_objects (object_hash, registered_at_unix_seconds)
             VALUES (?1, ?2)
             ON CONFLICT (object_hash) DO NOTHING",
            params![
                xet_hash_hex_string(object_id.hash()),
                helpers::u64_to_i64(unix_now_seconds_lossy())?
            ],
        )?;
        Ok(())
    }

    /// Persists Xet xorb presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the xorb marker cannot be written.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), LocalIndexStoreError> {
        self.insert_object(xorb_id)
    }

    /// Persists a chunk-hash to retained-shard mapping.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the mapping cannot be written.
    pub fn upsert_dedupe_shard_mapping(
        &self,
        mapping: &DedupeShardMapping,
    ) -> Result<(), LocalIndexStoreError> {
        let connection = self.open_connection()?;
        helpers::upsert_dedupe_mapping_row(&connection, mapping, unix_now_seconds_lossy())?;
        Ok(())
    }
}

/// Opaque local SQLite file-record locator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LocalRecordLocator {
    pub(crate) record_key: String,
    pub(crate) kind: LocalRecordKind,
    pub(crate) scope_key: String,
    pub(crate) file_id: String,
    pub(crate) content_hash: Option<String>,
}

impl LocalRecordLocator {
    /// Returns the stable record key for this locator.
    #[must_use]
    pub fn record_key(&self) -> &str {
        &self.record_key
    }

    /// Returns the file identifier associated with this locator.
    #[must_use]
    pub fn file_id(&self) -> &str {
        &self.file_id
    }

    /// Returns the immutable content hash when this locator points at a version record.
    #[must_use]
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }
}

/// Local SQLite implementation of the record-store contract.
#[derive(Debug, Clone)]
pub struct LocalRecordStore {
    root: PathBuf,
}

impl LocalRecordStore {
    /// Opens a local record store rooted at `root`.
    #[must_use]
    pub fn open(root: PathBuf) -> Self {
        Self {
            root: helpers::normalize_local_root(root),
        }
    }

    /// Creates and initializes a local record store rooted at `root`.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the local database cannot be created,
    /// migrated, or imported.
    pub fn new(root: PathBuf) -> Result<Self, LocalIndexStoreError> {
        let store = Self::open(root);
        let _connection = store.open_connection()?;
        Ok(store)
    }

    pub(crate) fn open_connection(&self) -> Result<Connection, LocalIndexStoreError> {
        LocalIndexStore::open(self.root.clone()).open_connection()
    }

    /// Atomically commits one file-version record and its latest-file alias.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when either row cannot be written.
    pub async fn commit_file_version_metadata(
        &self,
        record: &FileRecord,
    ) -> Result<(), LocalIndexStoreError> {
        let store = self.clone();
        let record = record.clone();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
            let now = unix_now_seconds_lossy();
            let version_locator = store.version_record_locator(&record);
            helpers::upsert_file_record_row(&transaction, &version_locator, &record, now)?;
            let latest_locator = store.latest_record_locator(&record);
            helpers::upsert_file_record_row(&transaction, &latest_locator, &record, now)?;
            transaction.commit()?;
            Ok::<_, LocalIndexStoreError>(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    /// Atomically commits native shard metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when any row in the metadata set cannot be written.
    pub async fn commit_native_shard_metadata(
        &self,
        records: &[FileRecord],
        dedupe_mappings: &[DedupeShardMapping],
    ) -> Result<(), LocalIndexStoreError> {
        let store = self.clone();
        let records = records.to_vec();
        let dedupe_mappings = dedupe_mappings.to_vec();
        tokio::task::spawn_blocking(move || {
            let connection = store.open_connection()?;
            let transaction = connection.unchecked_transaction()?;
            let now = unix_now_seconds_lossy();
            for record in &records {
                let version_locator = store.version_record_locator(record);
                helpers::upsert_file_record_row(&transaction, &version_locator, record, now)?;
            }
            for mapping in &dedupe_mappings {
                helpers::upsert_dedupe_mapping_row(&transaction, mapping, now)?;
            }
            for record in &records {
                let latest_locator = store.latest_record_locator(record);
                helpers::upsert_file_record_row(&transaction, &latest_locator, record, now)?;
            }
            transaction.commit()?;
            Ok::<_, LocalIndexStoreError>(())
        })
        .await
        .map_err(|e| LocalIndexStoreError::BlockingTask(e.to_string()))?
    }

    fn version_record_locator(&self, record: &FileRecord) -> LocalRecordLocator {
        helpers::local_record_locator(
            LocalRecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
    }

    fn latest_record_locator(&self, record: &FileRecord) -> LocalRecordLocator {
        helpers::local_record_locator(LocalRecordKind::Latest, record, None)
    }

    pub(crate) fn list_record_locators(
        &self,
        kind: LocalRecordKind,
    ) -> Result<Vec<LocalRecordLocator>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = ?1
             ORDER BY record_key",
        )?;
        let rows = statement.query_map(
            params![kind.as_str()],
            helpers::local_record_locator_from_row,
        )?;
        helpers::collect_rows(rows)
    }

    fn escape_like(value: &str) -> String {
        value
            .replace('\\', "\\\\")
            .replace('_', "\\_")
            .replace('%', "\\%")
    }

    pub(crate) fn list_repository_record_locators(
        &self,
        kind: LocalRecordKind,
        repository: &RepositoryRecordScope,
    ) -> Result<Vec<LocalRecordLocator>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        let scope_key = shared_repository_record_scope_key(repository);
        let scope_prefix = format!("{}%", Self::escape_like(&scope_key));
        let mut statement = connection.prepare(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = ?1
               AND (scope_key = ?2 OR scope_key LIKE ?3 ESCAPE '\\')
             ORDER BY record_key",
        )?;
        let rows = statement.query_map(
            params![kind.as_str(), scope_key, scope_prefix],
            helpers::local_record_locator_from_row,
        )?;
        helpers::collect_rows(rows)
    }

    pub(crate) fn read_record_bytes_raw(
        &self,
        locator: &LocalRecordLocator,
    ) -> Result<Option<Vec<u8>>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT record
                 FROM shardline_file_records
                 WHERE record_key = ?1",
                params![locator.record_key()],
                |row| helpers::read_sqlite_record_bytes(row.get_ref(0)?),
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum LocalRecordKind {
    Latest,
    Version,
}

impl LocalRecordKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, LocalIndexStoreError> {
        match value {
            "latest" => Ok(Self::Latest),
            "version" => Ok(Self::Version),
            _other => Err(LocalIndexStoreError::InvalidRecordKind),
        }
    }
}
