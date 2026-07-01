use std::{
    ffi::OsStr,
    io::{Error as IoError, ErrorKind},
    ops::Deref,
    path::{Path, PathBuf},
};

use rusqlite::{
    Connection, Error as SqliteError, MappedRows, OptionalExtension, Params,
    Result as SqliteResult, Row, Transaction,
    params,
};
use serde::{Deserialize, Serialize};
use serde_json::Error as JsonError;
use shardline_protocol::{ChunkRange, HashParseError, RangeError, unix_now_seconds_lossy};
use shardline_storage::{ObjectKey, ObjectKeyError, local_path::resolve_platform_symlinks};
use thiserror::Error;

use crate::{
    DedupeShardMapping, FileId, FileReconstruction, FileRecord, QuarantineCandidateError,
    ReconstructionTerm, RecordTraversal, RepositoryRecordScope, RetentionHoldError, StoredObjectId,
    WebhookDeliveryError, XorbId,
    parse_xet_hash_hex,
    record_key::repository_record_scope_key as shared_repository_record_scope_key,
    xet_hash_hex_string,
};

mod async_index_store;
mod helpers;
mod index_store;
mod record_store;
#[cfg(test)]
mod tests;

pub(crate) const LOCAL_METADATA_DATABASE_FILE_NAME: &str = "metadata.sqlite3";
const LOCAL_SCHEMA_MIGRATIONS_TABLE: &str = "shardline_local_schema_migrations";
const LEGACY_IMPORT_COMPLETED_KEY: &str = "legacy_filesystem_import_completed";
const MAX_CONTROL_PLANE_METADATA_BYTES: u64 = 1_048_576;
const MAX_RECONSTRUCTION_METADATA_BYTES: u64 = 1_073_741_824;
const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

pub(super) trait SqliteExecutor {
    fn execute_sql<P>(&self, sql: &str, params: P) -> SqliteResult<usize>
    where
        P: Params;
}

impl SqliteExecutor for Connection {
    fn execute_sql<P>(&self, sql: &str, params: P) -> SqliteResult<usize>
    where
        P: Params,
    {
        Connection::execute(self, sql, params)
    }
}

impl SqliteExecutor for Transaction<'_> {
    fn execute_sql<P>(&self, sql: &str, params: P) -> SqliteResult<usize>
    where
        P: Params,
    {
        Deref::deref(self).execute(sql, params)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalSqliteMigration {
    version: &'static str,
    name: &'static str,
    up_sql: &'static str,
    down_sql: &'static str,
}

const LOCAL_SQLITE_MIGRATIONS: [LocalSqliteMigration; 7] = [
    LocalSqliteMigration {
        version: "20260417000000",
        name: "metadata_store",
        up_sql: include_str!("../../migrations/20260417000000_metadata_store.up.sql"),
        down_sql: include_str!("../../migrations/20260417000000_metadata_store.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260417010000",
        name: "retention_holds",
        up_sql: include_str!("../../migrations/20260417010000_retention_holds.up.sql"),
        down_sql: include_str!("../../migrations/20260417010000_retention_holds.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418000000",
        name: "dedupe_shards",
        up_sql: include_str!("../../migrations/20260418000000_dedupe_shards.up.sql"),
        down_sql: include_str!("../../migrations/20260418000000_dedupe_shards.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418010000",
        name: "webhook_deliveries",
        up_sql: include_str!("../../migrations/20260418010000_webhook_deliveries.up.sql"),
        down_sql: include_str!("../../migrations/20260418010000_webhook_deliveries.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418020000",
        name: "provider_repository_states",
        up_sql: include_str!(
            "../../migrations/20260418020000_provider_repository_states.up.sql"
        ),
        down_sql: include_str!(
            "../../migrations/20260418020000_provider_repository_states.down.sql"
        ),
    },
    LocalSqliteMigration {
        version: "20260418110000",
        name: "provider_repository_reconciliation",
        up_sql: include_str!(
            "../../migrations/20260418110000_provider_repository_reconciliation.up.sql"
        ),
        down_sql: include_str!(
            "../../migrations/20260418110000_provider_repository_reconciliation.down.sql"
        ),
    },
    LocalSqliteMigration {
        version: "20260629000000",
        name: "hub_api",
        up_sql: include_str!("../../migrations/20260629000000_hub_api.up.sql"),
        down_sql: include_str!("../../migrations/20260629000000_hub_api.down.sql"),
    },
];

/// Local SQLite implementation of [`IndexStore`].
#[derive(Debug, Clone)]
pub struct LocalIndexStore {
    root: PathBuf,
}

impl LocalIndexStore {
    /// Opens a local metadata store rooted at `root` without eagerly mutating the filesystem.
    #[must_use]
    pub fn open(root: PathBuf) -> Self {
        Self {
            root: normalize_local_root(root),
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
                u64_to_i64(unix_now_seconds_lossy())?
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
    record_key: String,
    kind: LocalRecordKind,
    scope_key: String,
    file_id: String,
    content_hash: Option<String>,
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
            root: normalize_local_root(root),
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
        let connection = self.open_connection()?;
        let transaction = connection.unchecked_transaction()?;
        let now = unix_now_seconds_lossy();
        let version_locator = self.version_record_locator(record);
        helpers::upsert_file_record_row(&transaction, &version_locator, record, now)?;
        let latest_locator = self.latest_record_locator(record);
        helpers::upsert_file_record_row(&transaction, &latest_locator, record, now)?;
        transaction.commit()?;
        Ok(())
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
        let connection = self.open_connection()?;
        let transaction = connection.unchecked_transaction()?;
        let now = unix_now_seconds_lossy();
        for record in records {
            let version_locator = self.version_record_locator(record);
            helpers::upsert_file_record_row(&transaction, &version_locator, record, now)?;
        }
        for mapping in dedupe_mappings {
            helpers::upsert_dedupe_mapping_row(&transaction, mapping, now)?;
        }
        for record in records {
            let latest_locator = self.latest_record_locator(record);
            helpers::upsert_file_record_row(&transaction, &latest_locator, record, now)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn list_record_locators(
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
        let rows =
            statement.query_map(params![kind.as_str()], helpers::local_record_locator_from_row)?;
        collect_rows(rows)
    }

    fn list_repository_record_locators(
        &self,
        kind: LocalRecordKind,
        repository: &RepositoryRecordScope,
    ) -> Result<Vec<LocalRecordLocator>, LocalIndexStoreError> {
        let connection = self.open_connection()?;
        let scope_key = shared_repository_record_scope_key(repository);
        let scope_prefix = format!("{scope_key}%");
        let mut statement = connection.prepare(
            "SELECT record_key, record_kind, scope_key, file_id, content_hash
             FROM shardline_file_records
             WHERE record_kind = ?1
               AND (scope_key = ?2 OR scope_key LIKE ?3)
             ORDER BY record_key",
        )?;
        let rows = statement.query_map(
            params![kind.as_str(), scope_key, scope_prefix],
            helpers::local_record_locator_from_row,
        )?;
        collect_rows(rows)
    }

    fn read_record_bytes_raw(
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
enum LocalRecordKind {
    Latest,
    Version,
}

impl LocalRecordKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Version => "version",
        }
    }

    fn parse(value: &str) -> Result<Self, LocalIndexStoreError> {
        match value {
            "latest" => Ok(Self::Latest),
            "version" => Ok(Self::Version),
            _other => Err(LocalIndexStoreError::InvalidRecordKind),
        }
    }
}

/// Local metadata-store failure.
#[derive(Debug, Error)]
pub enum LocalIndexStoreError {
    /// Local filesystem access failed.
    #[error("local metadata operation failed")]
    Io(#[from] IoError),
    /// SQLite access failed.
    #[error("local sqlite metadata operation failed")]
    Sqlite(#[from] SqliteError),
    /// JSON serialization or deserialization failed.
    #[error("local metadata json operation failed")]
    Json(#[from] JsonError),
    /// Stored metadata exceeded the bounded parser ceiling.
    #[error("local metadata exceeded the bounded parser ceiling")]
    MetadataTooLarge {
        /// Number of bytes observed in the stored metadata payload.
        observed_bytes: u64,
        /// Maximum accepted metadata payload size.
        maximum_bytes: u64,
    },
    /// Stored metadata changed during a bounded read.
    #[error("local metadata changed during bounded read")]
    MetadataLengthMismatch {
        /// Number of bytes expected from the initial metadata length check.
        expected_bytes: u64,
        /// Number of bytes observed while reading the metadata payload.
        observed_bytes: u64,
    },
    /// A stored hash value was invalid.
    #[error("stored hash value was invalid")]
    HashParse(#[from] HashParseError),
    /// A stored object key was invalid.
    #[error("stored object key was invalid")]
    ObjectKey(#[from] ObjectKeyError),
    /// A stored chunk range was invalid.
    #[error("stored chunk range was invalid")]
    Range(#[from] RangeError),
    /// A stored retention hold was invalid.
    #[error("stored retention hold was invalid")]
    RetentionHold(#[from] RetentionHoldError),
    /// A stored quarantine candidate was invalid.
    #[error("stored quarantine candidate was invalid")]
    QuarantineCandidate(#[from] QuarantineCandidateError),
    /// A stored webhook delivery was invalid.
    #[error("stored webhook delivery was invalid")]
    WebhookDelivery(#[from] WebhookDeliveryError),
    /// A stored integer exceeded the supported range.
    #[error("stored integer exceeded the supported range")]
    IntegerOutOfRange,
    /// A stored record kind was invalid.
    #[error("stored local record kind was invalid")]
    InvalidRecordKind,
    /// The local metadata database had inconsistent import state.
    #[error("local metadata database had inconsistent legacy import state")]
    InvalidLegacyImportState,
    /// An invalid repository type string was encountered.
    #[error("invalid repository type: {0}")]
    InvalidRepoType(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct FileReconstructionRecord {
    terms: Vec<ReconstructionTermRecord>,
}

impl FileReconstructionRecord {
    fn from_domain(reconstruction: &FileReconstruction) -> Self {
        Self {
            terms: reconstruction
                .terms()
                .iter()
                .map(ReconstructionTermRecord::from_domain)
                .collect(),
        }
    }

    fn into_domain(self) -> Result<FileReconstruction, LocalIndexStoreError> {
        let terms = self
            .terms
            .into_iter()
            .map(ReconstructionTermRecord::into_domain)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(FileReconstruction::new(terms))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReconstructionTermRecord {
    object_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl ReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            object_hash: xet_hash_hex_string(term.object_id().hash()),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, LocalIndexStoreError> {
        let hash = parse_xet_hash_hex(&self.object_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            StoredObjectId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct LegacyQuarantineCandidateRecord {
    pub(super) hash: String,
    pub(super) bytes: u64,
    pub(super) first_seen_unreachable_at_unix_seconds: u64,
    pub(super) delete_after_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct StoredObjectPresenceRecord {
    pub(super) hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct DedupeShardRecord {
    pub(super) chunk_hash: String,
    pub(super) shard_object_key: String,
}

impl DedupeShardRecord {
    pub(super) fn into_domain(self) -> Result<DedupeShardMapping, LocalIndexStoreError> {
        let chunk_hash = parse_xet_hash_hex(&self.chunk_hash)?;
        let shard_object_key = ObjectKey::parse(&self.shard_object_key)?;
        Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
    }
}

fn normalize_local_root(root: PathBuf) -> PathBuf {
    let mut root = root;
    if root.file_name() == Some(OsStr::new("gc")) {
        root = root
            .parent()
            .map_or_else(|| root.clone(), Path::to_path_buf);
    }
    resolve_platform_symlinks(&root)
}

pub(super) fn u64_to_i64(value: u64) -> Result<i64, LocalIndexStoreError> {
    i64::try_from(value).map_err(|_error| LocalIndexStoreError::IntegerOutOfRange)
}

pub(super) fn i64_to_u64(value: i64) -> Result<u64, LocalIndexStoreError> {
    u64::try_from(value).map_err(|_error| LocalIndexStoreError::IntegerOutOfRange)
}

pub(super) fn collect_rows<T>(
    rows: MappedRows<'_, impl FnMut(&Row<'_>) -> Result<T, SqliteError>>,
) -> Result<Vec<T>, LocalIndexStoreError> {
    let mut collected = Vec::new();
    for row in rows {
        collected.push(row?);
    }
    Ok(collected)
}

pub(super) fn record_not_found_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::from(ErrorKind::NotFound))
}

pub(super) fn invalid_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local metadata path must be a regular file and must not be a symlink",
    ))
}

pub(super) fn invalid_record_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local record metadata path must be a regular file and must not be a symlink",
    ))
}
