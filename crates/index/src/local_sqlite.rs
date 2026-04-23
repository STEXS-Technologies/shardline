#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    error::Error as StdError,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    ops::Deref,
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use rusqlite::{
    Connection, Error as SqliteError, MappedRows, OpenFlags, OptionalExtension, Params,
    Result as SqliteResult, Row, Transaction,
    config::DbConfig,
    params,
    types::{Type, ValueRef},
};
use serde::{Deserialize, Serialize};
use serde_json::{Error as JsonError, from_slice, from_str, to_string};
use shardline_protocol::{
    ChunkRange, HashParseError, RangeError, RepositoryProvider, RepositoryScope, ShardlineHash,
    unix_now_seconds_lossy,
};
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};
use thiserror::Error;

use crate::{
    AsyncIndexStore, DedupeShardMapping, FileId, FileReconstruction, FileRecord, IndexStore,
    IndexStoreFuture, ProviderRepositoryState, QuarantineCandidate, QuarantineCandidateError,
    ReconstructionTerm, RecordStore, RecordStoreFuture, RepositoryRecordScope, RetentionHold,
    RetentionHoldError, WebhookDelivery, WebhookDeliveryError, XorbId,
    provider::parse_repository_provider,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_record_scope_key as shared_repository_record_scope_key,
        repository_scope_key as shared_repository_scope_key,
    },
};

const LOCAL_METADATA_DATABASE_FILE_NAME: &str = "metadata.sqlite3";
const LOCAL_SCHEMA_MIGRATIONS_TABLE: &str = "shardline_local_schema_migrations";
const LEGACY_IMPORT_COMPLETED_KEY: &str = "legacy_filesystem_import_completed";
const MAX_CONTROL_PLANE_METADATA_BYTES: u64 = 1_048_576;
const MAX_RECONSTRUCTION_METADATA_BYTES: u64 = 1_073_741_824;
const MAX_LOCAL_RECORD_METADATA_BYTES: u64 = 1_073_741_824;

trait SqliteExecutor {
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

const LOCAL_SQLITE_MIGRATIONS: [LocalSqliteMigration; 6] = [
    LocalSqliteMigration {
        version: "20260417000000",
        name: "metadata_store",
        up_sql: include_str!("../migrations/20260417000000_metadata_store.up.sql"),
        down_sql: include_str!("../migrations/20260417000000_metadata_store.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260417010000",
        name: "retention_holds",
        up_sql: include_str!("../migrations/20260417010000_retention_holds.up.sql"),
        down_sql: include_str!("../migrations/20260417010000_retention_holds.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418000000",
        name: "dedupe_shards",
        up_sql: include_str!("../migrations/20260418000000_dedupe_shards.up.sql"),
        down_sql: include_str!("../migrations/20260418000000_dedupe_shards.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418010000",
        name: "webhook_deliveries",
        up_sql: include_str!("../migrations/20260418010000_webhook_deliveries.up.sql"),
        down_sql: include_str!("../migrations/20260418010000_webhook_deliveries.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418020000",
        name: "provider_repository_states",
        up_sql: include_str!("../migrations/20260418020000_provider_repository_states.up.sql"),
        down_sql: include_str!("../migrations/20260418020000_provider_repository_states.down.sql"),
    },
    LocalSqliteMigration {
        version: "20260418110000",
        name: "provider_repository_reconciliation",
        up_sql: include_str!(
            "../migrations/20260418110000_provider_repository_reconciliation.up.sql"
        ),
        down_sql: include_str!(
            "../migrations/20260418110000_provider_repository_reconciliation.down.sql"
        ),
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

    fn open_connection(&self) -> Result<Connection, LocalIndexStoreError> {
        initialize_local_metadata_root(&self.root)?;
        let database_path = self.database_path();
        ensure_sqlite_database_path_is_safe(&database_path)?;
        let mut connection = Connection::open_with_flags(database_path, sqlite_open_flags())?;
        prepare_connection(&mut connection)?;
        ensure_local_schema_migrations_table(&connection)?;
        apply_pending_local_migrations(&mut connection)?;
        ensure_legacy_import_state(&mut connection, &self.root)?;
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
        upsert_reconstruction_row(
            &connection,
            file_id,
            reconstruction,
            unix_now_seconds_lossy(),
        )?;
        Ok(())
    }

    /// Persists xorb presence metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LocalIndexStoreError`] when the xorb marker cannot be written.
    pub fn insert_xorb(&self, xorb_id: &XorbId) -> Result<(), LocalIndexStoreError> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO shardline_xorbs (xorb_hash, registered_at_unix_seconds)
             VALUES (?1, ?2)
             ON CONFLICT (xorb_hash) DO NOTHING",
            params![
                xorb_id.hash().api_hex_string(),
                u64_to_i64(unix_now_seconds_lossy())?
            ],
        )?;
        Ok(())
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
        upsert_dedupe_mapping_row(&connection, mapping, unix_now_seconds_lossy())?;
        Ok(())
    }
}

impl IndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction(&self, file_id: &FileId) -> Result<Option<FileReconstruction>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT terms
                 FROM shardline_file_reconstructions
                 WHERE file_id = ?1",
                params![file_id.hash().api_hex_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| parse_reconstruction_json(&value))
            .transpose()
    }

    fn list_reconstruction_file_ids(&self) -> Result<Vec<FileId>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT file_id
             FROM shardline_file_reconstructions
             ORDER BY file_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut file_ids = Vec::new();
        for row in rows {
            let hash = ShardlineHash::parse_api_hex(&row?)?;
            file_ids.push(FileId::new(hash));
        }
        Ok(file_ids)
    }

    fn delete_reconstruction(&self, file_id: &FileId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_file_reconstructions WHERE file_id = ?1",
            params![file_id.hash().api_hex_string()],
        )?;
        Ok(changed > 0)
    }

    fn contains_xorb(&self, xorb_id: &XorbId) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let exists = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM shardline_xorbs WHERE xorb_hash = ?1
             )",
            params![xorb_id.hash().api_hex_string()],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists != 0)
    }

    fn dedupe_shard_mapping(
        &self,
        chunk_hash: &ShardlineHash,
    ) -> Result<Option<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT chunk_hash, shard_object_key
                 FROM shardline_dedupe_shards
                 WHERE chunk_hash = ?1",
                params![chunk_hash.api_hex_string()],
                dedupe_shard_mapping_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_dedupe_shard_mappings(&self) -> Result<Vec<DedupeShardMapping>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT chunk_hash, shard_object_key
             FROM shardline_dedupe_shards
             ORDER BY chunk_hash",
        )?;
        let rows = statement.query_map([], dedupe_shard_mapping_from_row)?;
        collect_rows(rows)
    }

    fn visit_dedupe_shard_mappings<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError>,
    {
        for mapping in IndexStore::list_dedupe_shard_mappings(self).map_err(Into::into)? {
            visitor(mapping)?;
        }
        Ok(())
    }

    fn delete_dedupe_shard_mapping(&self, chunk_hash: &ShardlineHash) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_dedupe_shards WHERE chunk_hash = ?1",
            params![chunk_hash.api_hex_string()],
        )?;
        Ok(changed > 0)
    }

    fn quarantine_candidate(
        &self,
        object_key: &ObjectKey,
    ) -> Result<Option<QuarantineCandidate>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT object_key,
                        observed_length,
                        first_seen_unreachable_at_unix_seconds,
                        delete_after_unix_seconds
                 FROM shardline_quarantine_candidates
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                quarantine_candidate_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_quarantine_candidates(&self) -> Result<Vec<QuarantineCandidate>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT object_key,
                    observed_length,
                    first_seen_unreachable_at_unix_seconds,
                    delete_after_unix_seconds
             FROM shardline_quarantine_candidates
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], quarantine_candidate_from_row)?;
        collect_rows(rows)
    }

    fn visit_quarantine_candidates<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError>,
    {
        for candidate in IndexStore::list_quarantine_candidates(self).map_err(Into::into)? {
            visitor(candidate)?;
        }
        Ok(())
    }

    fn upsert_quarantine_candidate(
        &self,
        candidate: &QuarantineCandidate,
    ) -> Result<(), Self::Error> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO shardline_quarantine_candidates (
                object_key,
                observed_length,
                first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                observed_length = excluded.observed_length,
                first_seen_unreachable_at_unix_seconds =
                    excluded.first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds = excluded.delete_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                candidate.object_key().as_str(),
                u64_to_i64(candidate.observed_length())?,
                u64_to_i64(candidate.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(candidate.delete_after_unix_seconds())?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        Ok(())
    }

    fn delete_quarantine_candidate(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_quarantine_candidates WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        Ok(changed > 0)
    }

    fn retention_hold(&self, object_key: &ObjectKey) -> Result<Option<RetentionHold>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT object_key,
                        reason,
                        held_at_unix_seconds,
                        release_after_unix_seconds
                 FROM shardline_retention_holds
                 WHERE object_key = ?1",
                params![object_key.as_str()],
                retention_hold_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_retention_holds(&self) -> Result<Vec<RetentionHold>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT object_key,
                    reason,
                    held_at_unix_seconds,
                    release_after_unix_seconds
             FROM shardline_retention_holds
             ORDER BY object_key",
        )?;
        let rows = statement.query_map([], retention_hold_from_row)?;
        collect_rows(rows)
    }

    fn visit_retention_holds<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError>,
    {
        for hold in IndexStore::list_retention_holds(self).map_err(Into::into)? {
            visitor(hold)?;
        }
        Ok(())
    }

    fn upsert_retention_hold(&self, hold: &RetentionHold) -> Result<(), Self::Error> {
        let connection = self.open_connection()?;
        connection.execute(
            "INSERT INTO shardline_retention_holds (
                object_key,
                reason,
                held_at_unix_seconds,
                release_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                reason = excluded.reason,
                held_at_unix_seconds = excluded.held_at_unix_seconds,
                release_after_unix_seconds = excluded.release_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                hold.object_key().as_str(),
                hold.reason(),
                u64_to_i64(hold.held_at_unix_seconds())?,
                hold.release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        Ok(())
    }

    fn delete_retention_hold(&self, object_key: &ObjectKey) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_retention_holds WHERE object_key = ?1",
            params![object_key.as_str()],
        )?;
        Ok(changed > 0)
    }

    fn record_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "INSERT INTO shardline_webhook_deliveries (
                provider,
                owner,
                repo,
                delivery_id,
                processed_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (provider, owner, repo, delivery_id) DO NOTHING",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
                u64_to_i64(delivery.processed_at_unix_seconds())?,
            ],
        )?;
        Ok(changed > 0)
    }

    fn list_webhook_deliveries(&self) -> Result<Vec<WebhookDelivery>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    delivery_id,
                    processed_at_unix_seconds
             FROM shardline_webhook_deliveries
             ORDER BY provider, owner, repo, delivery_id",
        )?;
        let rows = statement.query_map([], webhook_delivery_from_row)?;
        collect_rows(rows)
    }

    fn visit_webhook_deliveries<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError>,
    {
        for delivery in IndexStore::list_webhook_deliveries(self).map_err(Into::into)? {
            visitor(delivery)?;
        }
        Ok(())
    }

    fn delete_webhook_delivery(&self, delivery: &WebhookDelivery) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_webhook_deliveries
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3 AND delivery_id = ?4",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
            ],
        )?;
        Ok(changed > 0)
    }

    fn provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<Option<ProviderRepositoryState>, Self::Error> {
        let connection = self.open_connection()?;
        connection
            .query_row(
                "SELECT provider,
                        owner,
                        repo,
                        last_access_changed_at_unix_seconds,
                        last_revision_pushed_at_unix_seconds,
                        last_pushed_revision,
                        last_cache_invalidated_at_unix_seconds,
                        last_authorization_rechecked_at_unix_seconds,
                        last_drift_checked_at_unix_seconds
                 FROM shardline_provider_repository_states
                 WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
                params![provider.as_str(), owner, repo],
                provider_repository_state_from_row,
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }

    fn list_provider_repository_states(&self) -> Result<Vec<ProviderRepositoryState>, Self::Error> {
        let connection = self.open_connection()?;
        let mut statement = connection.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
             FROM shardline_provider_repository_states
             ORDER BY provider, owner, repo",
        )?;
        let rows = statement.query_map([], provider_repository_state_from_row)?;
        collect_rows(rows)
    }

    fn visit_provider_repository_states<Visitor, VisitorError>(
        &self,
        mut visitor: Visitor,
    ) -> Result<(), VisitorError>
    where
        Self::Error: Into<VisitorError>,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError>,
    {
        for state in IndexStore::list_provider_repository_states(self).map_err(Into::into)? {
            visitor(state)?;
        }
        Ok(())
    }

    fn upsert_provider_repository_state(
        &self,
        state: &ProviderRepositoryState,
    ) -> Result<(), Self::Error> {
        let connection = self.open_connection()?;
        let now = unix_now_seconds_lossy();
        connection.execute(
            "INSERT INTO shardline_provider_repository_states (
                provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds,
                created_at_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (provider, owner, repo)
             DO UPDATE SET
                last_access_changed_at_unix_seconds =
                    excluded.last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds =
                    excluded.last_revision_pushed_at_unix_seconds,
                last_pushed_revision = excluded.last_pushed_revision,
                last_cache_invalidated_at_unix_seconds =
                    excluded.last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds =
                    excluded.last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds =
                    excluded.last_drift_checked_at_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                state.provider().as_str(),
                state.owner(),
                state.repo(),
                state
                    .last_access_changed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_revision_pushed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state.last_pushed_revision(),
                state
                    .last_cache_invalidated_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_authorization_rechecked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_drift_checked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(now)?,
                u64_to_i64(now)?,
            ],
        )?;
        Ok(())
    }

    fn delete_provider_repository_state(
        &self,
        provider: RepositoryProvider,
        owner: &str,
        repo: &str,
    ) -> Result<bool, Self::Error> {
        let connection = self.open_connection()?;
        let changed = connection.execute(
            "DELETE FROM shardline_provider_repository_states
             WHERE provider = ?1 AND owner = ?2 AND repo = ?3",
            params![provider.as_str(), owner, repo],
        )?;
        Ok(changed > 0)
    }
}

impl AsyncIndexStore for LocalIndexStore {
    type Error = LocalIndexStoreError;

    fn reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, Option<FileReconstruction>, Self::Error> {
        Box::pin(async move { IndexStore::reconstruction(self, file_id) })
    }

    fn insert_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
        reconstruction: &'operation FileReconstruction,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(
            async move { LocalIndexStore::insert_reconstruction(self, file_id, reconstruction) },
        )
    }

    fn list_reconstruction_file_ids(&self) -> IndexStoreFuture<'_, Vec<FileId>, Self::Error> {
        Box::pin(async move { IndexStore::list_reconstruction_file_ids(self) })
    }

    fn delete_reconstruction<'operation>(
        &'operation self,
        file_id: &'operation FileId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_reconstruction(self, file_id) })
    }

    fn contains_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::contains_xorb(self, xorb_id) })
    }

    fn dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, Option<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { IndexStore::dedupe_shard_mapping(self, chunk_hash) })
    }

    fn list_dedupe_shard_mappings(
        &self,
    ) -> IndexStoreFuture<'_, Vec<DedupeShardMapping>, Self::Error> {
        Box::pin(async move { IndexStore::list_dedupe_shard_mappings(self) })
    }

    fn visit_dedupe_shard_mappings<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(DedupeShardMapping) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_dedupe_shard_mappings(self, visitor) })
    }

    fn insert_xorb<'operation>(
        &'operation self,
        xorb_id: &'operation XorbId,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { LocalIndexStore::insert_xorb(self, xorb_id) })
    }

    fn upsert_dedupe_shard_mapping<'operation>(
        &'operation self,
        mapping: &'operation DedupeShardMapping,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { LocalIndexStore::upsert_dedupe_shard_mapping(self, mapping) })
    }

    fn delete_dedupe_shard_mapping<'operation>(
        &'operation self,
        chunk_hash: &'operation ShardlineHash,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_dedupe_shard_mapping(self, chunk_hash) })
    }

    fn quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { IndexStore::quarantine_candidate(self, object_key) })
    }

    fn list_quarantine_candidates(
        &self,
    ) -> IndexStoreFuture<'_, Vec<QuarantineCandidate>, Self::Error> {
        Box::pin(async move { IndexStore::list_quarantine_candidates(self) })
    }

    fn visit_quarantine_candidates<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(QuarantineCandidate) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_quarantine_candidates(self, visitor) })
    }

    fn upsert_quarantine_candidate<'operation>(
        &'operation self,
        candidate: &'operation QuarantineCandidate,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_quarantine_candidate(self, candidate) })
    }

    fn delete_quarantine_candidate<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_quarantine_candidate(self, object_key) })
    }

    fn retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, Option<RetentionHold>, Self::Error> {
        Box::pin(async move { IndexStore::retention_hold(self, object_key) })
    }

    fn list_retention_holds(&self) -> IndexStoreFuture<'_, Vec<RetentionHold>, Self::Error> {
        Box::pin(async move { IndexStore::list_retention_holds(self) })
    }

    fn visit_retention_holds<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(RetentionHold) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_retention_holds(self, visitor) })
    }

    fn upsert_retention_hold<'operation>(
        &'operation self,
        hold: &'operation RetentionHold,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_retention_hold(self, hold) })
    }

    fn delete_retention_hold<'operation>(
        &'operation self,
        object_key: &'operation ObjectKey,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_retention_hold(self, object_key) })
    }

    fn record_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::record_webhook_delivery(self, delivery) })
    }

    fn list_webhook_deliveries(&self) -> IndexStoreFuture<'_, Vec<WebhookDelivery>, Self::Error> {
        Box::pin(async move { IndexStore::list_webhook_deliveries(self) })
    }

    fn visit_webhook_deliveries<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(WebhookDelivery) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_webhook_deliveries(self, visitor) })
    }

    fn delete_webhook_delivery<'operation>(
        &'operation self,
        delivery: &'operation WebhookDelivery,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move { IndexStore::delete_webhook_delivery(self, delivery) })
    }

    fn provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, Option<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { IndexStore::provider_repository_state(self, provider, owner, repo) })
    }

    fn list_provider_repository_states(
        &self,
    ) -> IndexStoreFuture<'_, Vec<ProviderRepositoryState>, Self::Error> {
        Box::pin(async move { IndexStore::list_provider_repository_states(self) })
    }

    fn visit_provider_repository_states<'operation, Visitor, VisitorError>(
        &'operation self,
        visitor: Visitor,
    ) -> IndexStoreFuture<'operation, (), VisitorError>
    where
        Self::Error: Into<VisitorError> + 'operation,
        Visitor: FnMut(ProviderRepositoryState) -> Result<(), VisitorError> + Send + 'operation,
        VisitorError: Send + 'operation,
    {
        Box::pin(async move { IndexStore::visit_provider_repository_states(self, visitor) })
    }

    fn upsert_provider_repository_state<'operation>(
        &'operation self,
        state: &'operation ProviderRepositoryState,
    ) -> IndexStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move { IndexStore::upsert_provider_repository_state(self, state) })
    }

    fn delete_provider_repository_state<'operation>(
        &'operation self,
        provider: RepositoryProvider,
        owner: &'operation str,
        repo: &'operation str,
    ) -> IndexStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            IndexStore::delete_provider_repository_state(self, provider, owner, repo)
        })
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

    fn open_connection(&self) -> Result<Connection, LocalIndexStoreError> {
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
        upsert_file_record_row(&transaction, &version_locator, record, now)?;
        let latest_locator = self.latest_record_locator(record);
        upsert_file_record_row(&transaction, &latest_locator, record, now)?;
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
            upsert_file_record_row(&transaction, &version_locator, record, now)?;
        }
        for mapping in dedupe_mappings {
            upsert_dedupe_mapping_row(&transaction, mapping, now)?;
        }
        for record in records {
            let latest_locator = self.latest_record_locator(record);
            upsert_file_record_row(&transaction, &latest_locator, record, now)?;
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
        let rows = statement.query_map(params![kind.as_str()], local_record_locator_from_row)?;
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
            local_record_locator_from_row,
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
                |row| read_sqlite_record_bytes(row.get_ref(0)?),
            )
            .optional()
            .map_err(LocalIndexStoreError::from)
    }
}

impl RecordStore for LocalRecordStore {
    type Error = LocalIndexStoreError;
    type Locator = LocalRecordLocator;

    fn list_latest_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(LocalRecordKind::Latest) })
    }

    fn list_repository_latest_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(LocalRecordKind::Latest, repository)
        })
    }

    fn list_version_record_locators(
        &self,
    ) -> RecordStoreFuture<'_, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move { self.list_record_locators(LocalRecordKind::Version) })
    }

    fn list_repository_version_record_locators<'operation>(
        &'operation self,
        repository: &'operation RepositoryRecordScope,
    ) -> RecordStoreFuture<'operation, Vec<Self::Locator>, Self::Error> {
        Box::pin(async move {
            self.list_repository_record_locators(LocalRecordKind::Version, repository)
        })
    }

    fn read_record_bytes<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Vec<u8>, Self::Error> {
        Box::pin(async move {
            self.read_record_bytes_raw(locator)?
                .ok_or_else(record_not_found_error)
        })
    }

    fn read_latest_record_bytes<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, Option<Vec<u8>>, Self::Error> {
        Box::pin(async move {
            let locator = self.latest_record_locator(record);
            self.read_record_bytes_raw(&locator)
        })
    }

    fn write_version_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let connection = self.open_connection()?;
            let locator = self.version_record_locator(record);
            upsert_file_record_row(&connection, &locator, record, unix_now_seconds_lossy())?;
            Ok(())
        })
    }

    fn write_latest_record<'operation>(
        &'operation self,
        record: &'operation FileRecord,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let connection = self.open_connection()?;
            let locator = self.latest_record_locator(record);
            upsert_file_record_row(&connection, &locator, record, unix_now_seconds_lossy())?;
            Ok(())
        })
    }

    fn delete_record_locator<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, (), Self::Error> {
        Box::pin(async move {
            let connection = self.open_connection()?;
            let deleted = connection.execute(
                "DELETE FROM shardline_file_records WHERE record_key = ?1",
                params![locator.record_key()],
            )?;
            if deleted == 0 {
                return Err(record_not_found_error());
            }
            Ok(())
        })
    }

    fn record_locator_exists<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, bool, Self::Error> {
        Box::pin(async move {
            let connection = self.open_connection()?;
            let exists = connection.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM shardline_file_records WHERE record_key = ?1
                 )",
                params![locator.record_key()],
                |row| row.get::<_, i64>(0),
            )?;
            Ok(exists != 0)
        })
    }

    fn prune_empty_latest_records(&self) -> RecordStoreFuture<'_, (), Self::Error> {
        Box::pin(async move { Ok(()) })
    }

    fn modified_since_epoch<'operation>(
        &'operation self,
        locator: &'operation Self::Locator,
    ) -> RecordStoreFuture<'operation, Duration, Self::Error> {
        Box::pin(async move {
            let connection = self.open_connection()?;
            let value = connection
                .query_row(
                    "SELECT updated_at_unix_seconds
                     FROM shardline_file_records
                     WHERE record_key = ?1",
                    params![locator.record_key()],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .ok_or_else(record_not_found_error)?;
            Ok(Duration::from_secs(i64_to_u64(value)?))
        })
    }

    fn latest_record_locator(&self, record: &FileRecord) -> Self::Locator {
        local_record_locator(LocalRecordKind::Latest, record, None)
    }

    fn version_record_locator(&self, record: &FileRecord) -> Self::Locator {
        local_record_locator(
            LocalRecordKind::Version,
            record,
            Some(record.content_hash.clone()),
        )
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FileReconstructionRecord {
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
    xorb_hash: String,
    chunk_start: u32,
    chunk_end_exclusive: u32,
    unpacked_length: u64,
}

impl ReconstructionTermRecord {
    fn from_domain(term: &ReconstructionTerm) -> Self {
        Self {
            xorb_hash: term.xorb_id().hash().api_hex_string(),
            chunk_start: term.chunk_range().start(),
            chunk_end_exclusive: term.chunk_range().end_exclusive(),
            unpacked_length: term.unpacked_length(),
        }
    }

    fn into_domain(self) -> Result<ReconstructionTerm, LocalIndexStoreError> {
        let hash = ShardlineHash::parse_api_hex(&self.xorb_hash)?;
        let range = ChunkRange::new(self.chunk_start, self.chunk_end_exclusive)?;
        Ok(ReconstructionTerm::new(
            XorbId::new(hash),
            range,
            self.unpacked_length,
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyQuarantineCandidateRecord {
    hash: String,
    bytes: u64,
    first_seen_unreachable_at_unix_seconds: u64,
    delete_after_unix_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct XorbPresenceRecord {
    hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DedupeShardRecord {
    chunk_hash: String,
    shard_object_key: String,
}

impl DedupeShardRecord {
    fn into_domain(self) -> Result<DedupeShardMapping, LocalIndexStoreError> {
        let chunk_hash = ShardlineHash::parse_api_hex(&self.chunk_hash)?;
        let shard_object_key = ObjectKey::parse(&self.shard_object_key)?;
        Ok(DedupeShardMapping::new(chunk_hash, shard_object_key))
    }
}

fn normalize_local_root(root: PathBuf) -> PathBuf {
    if root.file_name() == Some(OsStr::new("gc")) {
        return root
            .parent()
            .map_or_else(|| root.clone(), Path::to_path_buf);
    }
    root
}

fn initialize_local_metadata_root(root: &Path) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    fs::create_dir_all(root)?;
    Ok(())
}

fn ensure_sqlite_database_path_is_safe(path: &Path) -> Result<(), LocalIndexStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_metadata_path_error()),
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_metadata) => Err(invalid_metadata_path_error()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalIndexStoreError::Io(error)),
    }
}

fn prepare_connection(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    let _enabled = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.pragma_update(None, "cell_size_check", "ON")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(())
}

const fn sqlite_open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        .union(OpenFlags::SQLITE_OPEN_CREATE)
        .union(OpenFlags::SQLITE_OPEN_NO_MUTEX)
        .union(OpenFlags::SQLITE_OPEN_URI)
        .union(OpenFlags::SQLITE_OPEN_NOFOLLOW)
        .union(OpenFlags::SQLITE_OPEN_EXRESCODE)
}

fn ensure_local_schema_migrations_table(
    connection: &Connection,
) -> Result<(), LocalIndexStoreError> {
    connection.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {LOCAL_SCHEMA_MIGRATIONS_TABLE} (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            applied_at_unix_seconds INTEGER NOT NULL
        );"
    ))?;
    Ok(())
}

fn apply_pending_local_migrations(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    let mut applied_versions = Vec::new();
    {
        let mut statement = connection.prepare(&format!(
            "SELECT version
             FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}
             ORDER BY version"
        ))?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            applied_versions.push(row?);
        }
    }

    for migration in LOCAL_SQLITE_MIGRATIONS {
        if applied_versions
            .iter()
            .any(|version| version == migration.version)
        {
            continue;
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(migration.up_sql)?;
        transaction.execute(
            &format!(
                "INSERT INTO {LOCAL_SCHEMA_MIGRATIONS_TABLE} (
                    version,
                    name,
                    applied_at_unix_seconds
                 )
                 VALUES (?1, ?2, ?3)"
            ),
            params![
                migration.version,
                migration.name,
                u64_to_i64(unix_now_seconds_lossy())?,
            ],
        )?;
        transaction.commit()?;
    }

    Ok(())
}

fn ensure_legacy_import_state(
    connection: &mut Connection,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let import_completed = connection
        .query_row(
            "SELECT value
             FROM shardline_local_metadata_meta
             WHERE key = ?1",
            params![LEGACY_IMPORT_COMPLETED_KEY],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    if let Some(import_completed) = import_completed.as_deref() {
        if import_completed == "1" {
            return Ok(());
        }
        return Err(LocalIndexStoreError::InvalidLegacyImportState);
    }

    if local_metadata_has_rows(connection)? {
        return Err(LocalIndexStoreError::InvalidLegacyImportState);
    }

    if !legacy_layout_exists(root) {
        mark_legacy_import_completed(connection)?;
        return Ok(());
    }

    let transaction = connection.transaction()?;
    import_legacy_file_records(&transaction, root, LocalRecordKind::Latest)?;
    import_legacy_file_records(&transaction, root, LocalRecordKind::Version)?;
    import_legacy_reconstructions(&transaction, root)?;
    import_legacy_xorbs(&transaction, root)?;
    import_legacy_dedupe_mappings(&transaction, root)?;
    import_legacy_quarantine_candidates(&transaction, root)?;
    import_legacy_retention_holds(&transaction, root)?;
    import_legacy_webhook_deliveries(&transaction, root)?;
    import_legacy_provider_repository_states(&transaction, root)?;
    mark_legacy_import_completed(&transaction)?;
    transaction.commit()?;
    Ok(())
}

fn local_metadata_has_rows(connection: &Connection) -> Result<bool, LocalIndexStoreError> {
    let tables = [
        "shardline_file_records",
        "shardline_file_reconstructions",
        "shardline_xorbs",
        "shardline_dedupe_shards",
        "shardline_quarantine_candidates",
        "shardline_retention_holds",
        "shardline_webhook_deliveries",
        "shardline_provider_repository_states",
    ];
    for table in tables {
        let exists = connection.query_row(
            &format!("SELECT EXISTS(SELECT 1 FROM {table} LIMIT 1)"),
            [],
            |row| row.get::<_, i64>(0),
        )?;
        if exists != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn mark_legacy_import_completed(
    connection: &impl SqliteExecutor,
) -> Result<(), LocalIndexStoreError> {
    connection.execute_sql(
        "INSERT INTO shardline_local_metadata_meta (key, value)
         VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        params![LEGACY_IMPORT_COMPLETED_KEY, "1"],
    )?;
    Ok(())
}

fn legacy_layout_exists(root: &Path) -> bool {
    root.join("files").exists() || root.join("file_versions").exists() || root.join("gc").exists()
}

fn import_legacy_file_records(
    transaction: &Transaction<'_>,
    root: &Path,
    kind: LocalRecordKind,
) -> Result<(), LocalIndexStoreError> {
    let directory = match kind {
        LocalRecordKind::Latest => root.join("files"),
        LocalRecordKind::Version => root.join("file_versions"),
    };
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_LOCAL_RECORD_METADATA_BYTES,
            invalid_record_metadata_path_error,
        )?;
        let record = parse_file_record_json_bytes(&bytes)?;
        let expected_path = legacy_record_path(root, kind, &record);
        if expected_path != path {
            return Err(LocalIndexStoreError::Io(IoError::new(
                ErrorKind::InvalidData,
                "legacy file-record path did not match record contents",
            )));
        }
        let locator = local_record_locator(
            kind,
            &record,
            (kind == LocalRecordKind::Version).then(|| record.content_hash.clone()),
        );
        upsert_file_record_row(
            transaction,
            &locator,
            &record,
            file_modified_since_epoch(&path)?,
        )?;
    }
    Ok(())
}

fn import_legacy_reconstructions(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("reconstructions");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_RECONSTRUCTION_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let path_hash = path
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(invalid_metadata_path_error)?;
        let file_id = FileId::new(ShardlineHash::parse_api_hex(path_hash)?);
        let reconstruction = parse_reconstruction_json_bytes(&bytes)?;
        upsert_reconstruction_row(
            transaction,
            &file_id,
            &reconstruction,
            file_modified_since_epoch(&path)?,
        )?;
    }
    Ok(())
}

fn import_legacy_xorbs(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("xorbs");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let record = from_slice::<XorbPresenceRecord>(&bytes)?;
        let path_hash = path
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(invalid_metadata_path_error)?;
        if record.hash != path_hash {
            return Err(LocalIndexStoreError::Io(IoError::new(
                ErrorKind::InvalidData,
                "legacy xorb marker path did not match marker hash",
            )));
        }
        transaction.execute(
            "INSERT INTO shardline_xorbs (xorb_hash, registered_at_unix_seconds)
             VALUES (?1, ?2)
             ON CONFLICT (xorb_hash) DO NOTHING",
            params![record.hash, u64_to_i64(file_modified_since_epoch(&path)?)?],
        )?;
    }
    Ok(())
}

fn import_legacy_dedupe_mappings(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("dedupe-shards");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let record = from_slice::<DedupeShardRecord>(&bytes)?;
        let path_hash = path
            .file_stem()
            .and_then(OsStr::to_str)
            .ok_or_else(invalid_metadata_path_error)?;
        if record.chunk_hash != path_hash {
            return Err(LocalIndexStoreError::Io(IoError::new(
                ErrorKind::InvalidData,
                "legacy dedupe mapping path did not match stored chunk hash",
            )));
        }
        let mapping = record.into_domain()?;
        upsert_dedupe_mapping_row(transaction, &mapping, file_modified_since_epoch(&path)?)?;
    }
    Ok(())
}

fn import_legacy_quarantine_candidates(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("quarantine");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let candidate = parse_quarantine_candidate_json_bytes(&bytes)?;
        transaction.execute(
            "INSERT INTO shardline_quarantine_candidates (
                object_key,
                observed_length,
                first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                observed_length = excluded.observed_length,
                first_seen_unreachable_at_unix_seconds =
                    excluded.first_seen_unreachable_at_unix_seconds,
                delete_after_unix_seconds = excluded.delete_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                candidate.object_key().as_str(),
                u64_to_i64(candidate.observed_length())?,
                u64_to_i64(candidate.first_seen_unreachable_at_unix_seconds())?,
                u64_to_i64(candidate.delete_after_unix_seconds())?,
                u64_to_i64(file_modified_since_epoch(&path)?)?,
            ],
        )?;
    }
    Ok(())
}

fn import_legacy_retention_holds(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("retention-holds");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let hold = parse_retention_hold_json_bytes(&bytes)?;
        transaction.execute(
            "INSERT INTO shardline_retention_holds (
                object_key,
                reason,
                held_at_unix_seconds,
                release_after_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (object_key)
             DO UPDATE SET
                reason = excluded.reason,
                held_at_unix_seconds = excluded.held_at_unix_seconds,
                release_after_unix_seconds = excluded.release_after_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                hold.object_key().as_str(),
                hold.reason(),
                u64_to_i64(hold.held_at_unix_seconds())?,
                hold.release_after_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(file_modified_since_epoch(&path)?)?,
            ],
        )?;
    }
    Ok(())
}

fn import_legacy_webhook_deliveries(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("webhook-deliveries");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let delivery = parse_webhook_delivery_json_bytes(&bytes)?;
        transaction.execute(
            "INSERT INTO shardline_webhook_deliveries (
                provider,
                owner,
                repo,
                delivery_id,
                processed_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (provider, owner, repo, delivery_id) DO NOTHING",
            params![
                delivery.provider().as_str(),
                delivery.owner(),
                delivery.repo(),
                delivery.delivery_id(),
                u64_to_i64(delivery.processed_at_unix_seconds())?,
            ],
        )?;
    }
    Ok(())
}

fn import_legacy_provider_repository_states(
    transaction: &Transaction<'_>,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    let directory = root.join("gc").join("provider-repository-states");
    for path in collect_legacy_files(&directory)? {
        let bytes = read_existing_file_bounded(
            &path,
            MAX_CONTROL_PLANE_METADATA_BYTES,
            invalid_metadata_path_error,
        )?;
        let state = parse_provider_repository_state_json_bytes(&bytes)?;
        let modified = file_modified_since_epoch(&path)?;
        transaction.execute(
            "INSERT INTO shardline_provider_repository_states (
                provider,
                owner,
                repo,
                last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds,
                last_pushed_revision,
                last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds,
                created_at_unix_seconds,
                updated_at_unix_seconds
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
             ON CONFLICT (provider, owner, repo)
             DO UPDATE SET
                last_access_changed_at_unix_seconds =
                    excluded.last_access_changed_at_unix_seconds,
                last_revision_pushed_at_unix_seconds =
                    excluded.last_revision_pushed_at_unix_seconds,
                last_pushed_revision = excluded.last_pushed_revision,
                last_cache_invalidated_at_unix_seconds =
                    excluded.last_cache_invalidated_at_unix_seconds,
                last_authorization_rechecked_at_unix_seconds =
                    excluded.last_authorization_rechecked_at_unix_seconds,
                last_drift_checked_at_unix_seconds =
                    excluded.last_drift_checked_at_unix_seconds,
                updated_at_unix_seconds = excluded.updated_at_unix_seconds",
            params![
                state.provider().as_str(),
                state.owner(),
                state.repo(),
                state
                    .last_access_changed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_revision_pushed_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state.last_pushed_revision(),
                state
                    .last_cache_invalidated_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_authorization_rechecked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                state
                    .last_drift_checked_at_unix_seconds()
                    .map(u64_to_i64)
                    .transpose()?,
                u64_to_i64(modified)?,
                u64_to_i64(modified)?,
            ],
        )?;
    }
    Ok(())
}

fn upsert_reconstruction_row(
    connection: &impl SqliteExecutor,
    file_id: &FileId,
    reconstruction: &FileReconstruction,
    updated_at_unix_seconds: u64,
) -> Result<(), LocalIndexStoreError> {
    let json = to_string(&FileReconstructionRecord::from_domain(reconstruction))?;
    ensure_metadata_size_within_limit(
        u64::try_from(json.len()).unwrap_or(u64::MAX),
        MAX_RECONSTRUCTION_METADATA_BYTES,
    )?;
    connection.execute_sql(
        "INSERT INTO shardline_file_reconstructions (
            file_id,
            terms,
            updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3)
         ON CONFLICT (file_id)
         DO UPDATE SET
            terms = excluded.terms,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        params![
            file_id.hash().api_hex_string(),
            json,
            u64_to_i64(updated_at_unix_seconds)?,
        ],
    )?;
    Ok(())
}

fn upsert_dedupe_mapping_row(
    connection: &impl SqliteExecutor,
    mapping: &DedupeShardMapping,
    updated_at_unix_seconds: u64,
) -> Result<(), LocalIndexStoreError> {
    connection.execute_sql(
        "INSERT INTO shardline_dedupe_shards (
            chunk_hash,
            shard_object_key,
            updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3)
         ON CONFLICT (chunk_hash)
         DO UPDATE SET
            shard_object_key = excluded.shard_object_key,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        params![
            mapping.chunk_hash().api_hex_string(),
            mapping.shard_object_key().as_str(),
            u64_to_i64(updated_at_unix_seconds)?,
        ],
    )?;
    Ok(())
}

fn upsert_file_record_row(
    connection: &impl SqliteExecutor,
    locator: &LocalRecordLocator,
    record: &FileRecord,
    updated_at_unix_seconds: u64,
) -> Result<(), LocalIndexStoreError> {
    let json = to_string(record)?;
    ensure_metadata_size_within_limit(
        u64::try_from(json.len()).unwrap_or(u64::MAX),
        MAX_LOCAL_RECORD_METADATA_BYTES,
    )?;
    connection.execute_sql(
        "INSERT INTO shardline_file_records (
            record_key,
            record_kind,
            scope_key,
            file_id,
            content_hash,
            record,
            updated_at_unix_seconds
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT (record_key)
         DO UPDATE SET
            record_kind = excluded.record_kind,
            scope_key = excluded.scope_key,
            file_id = excluded.file_id,
            content_hash = excluded.content_hash,
            record = excluded.record,
            updated_at_unix_seconds = excluded.updated_at_unix_seconds",
        params![
            locator.record_key(),
            locator.kind.as_str(),
            &locator.scope_key,
            locator.file_id(),
            &record.content_hash,
            json,
            u64_to_i64(updated_at_unix_seconds)?,
        ],
    )?;
    Ok(())
}

fn local_record_locator(
    kind: LocalRecordKind,
    record: &FileRecord,
    content_hash: Option<String>,
) -> LocalRecordLocator {
    let scope_key = shared_repository_scope_key(record.repository_scope.as_ref());
    let record_key = shared_record_key(
        kind.as_str(),
        &scope_key,
        &record.file_id,
        content_hash.as_deref(),
    );
    LocalRecordLocator {
        record_key,
        kind,
        scope_key,
        file_id: record.file_id.clone(),
        content_hash,
    }
}

fn local_record_locator_from_row(row: &Row<'_>) -> Result<LocalRecordLocator, SqliteError> {
    let kind = LocalRecordKind::parse(row.get_ref("record_kind")?.as_str()?)
        .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))?;
    let content_hash = match kind {
        LocalRecordKind::Latest => None,
        LocalRecordKind::Version => Some(row.get::<_, String>("content_hash")?),
    };
    Ok(LocalRecordLocator {
        record_key: row.get("record_key")?,
        kind,
        scope_key: row.get("scope_key")?,
        file_id: row.get("file_id")?,
        content_hash,
    })
}

fn quarantine_candidate_from_row(row: &Row<'_>) -> Result<QuarantineCandidate, SqliteError> {
    let object_key =
        ObjectKey::parse(&row.get::<_, String>("object_key")?).map_err(from_sql_error)?;
    QuarantineCandidate::new(
        object_key,
        i64_to_u64(row.get("observed_length")?).map_err(from_sql_error)?,
        i64_to_u64(row.get("first_seen_unreachable_at_unix_seconds")?).map_err(from_sql_error)?,
        i64_to_u64(row.get("delete_after_unix_seconds")?).map_err(from_sql_error)?,
    )
    .map_err(from_sql_error)
}

fn retention_hold_from_row(row: &Row<'_>) -> Result<RetentionHold, SqliteError> {
    let object_key =
        ObjectKey::parse(&row.get::<_, String>("object_key")?).map_err(from_sql_error)?;
    RetentionHold::new(
        object_key,
        row.get("reason")?,
        i64_to_u64(row.get("held_at_unix_seconds")?).map_err(from_sql_error)?,
        row.get::<_, Option<i64>>("release_after_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
    )
    .map_err(from_sql_error)
}

fn webhook_delivery_from_row(row: &Row<'_>) -> Result<WebhookDelivery, SqliteError> {
    let provider_name = row.get::<_, String>("provider")?;
    let provider = parse_repository_provider(&provider_name, || {
        SqliteError::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(WebhookDeliveryError::InvalidProvider),
        )
    })?;
    WebhookDelivery::new(
        provider,
        row.get("owner")?,
        row.get("repo")?,
        row.get("delivery_id")?,
        i64_to_u64(row.get("processed_at_unix_seconds")?).map_err(from_sql_error)?,
    )
    .map_err(from_sql_error)
}

fn provider_repository_state_from_row(
    row: &Row<'_>,
) -> Result<ProviderRepositoryState, SqliteError> {
    let provider_name = row.get::<_, String>("provider")?;
    let provider = parse_repository_provider(&provider_name, || {
        SqliteError::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(WebhookDeliveryError::InvalidProvider),
        )
    })?;
    Ok(ProviderRepositoryState::new(
        provider,
        row.get("owner")?,
        row.get("repo")?,
        row.get::<_, Option<i64>>("last_access_changed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
        row.get::<_, Option<i64>>("last_revision_pushed_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
        row.get("last_pushed_revision")?,
    )
    .with_reconciliation(
        row.get::<_, Option<i64>>("last_cache_invalidated_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
        row.get::<_, Option<i64>>("last_authorization_rechecked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
        row.get::<_, Option<i64>>("last_drift_checked_at_unix_seconds")?
            .map(i64_to_u64)
            .transpose()
            .map_err(from_sql_error)?,
    ))
}

fn dedupe_shard_mapping_from_row(row: &Row<'_>) -> Result<DedupeShardMapping, SqliteError> {
    let chunk_hash = ShardlineHash::parse_api_hex(&row.get::<_, String>("chunk_hash")?)
        .map_err(from_sql_error)?;
    let object_key =
        ObjectKey::parse(&row.get::<_, String>("shard_object_key")?).map_err(from_sql_error)?;
    Ok(DedupeShardMapping::new(chunk_hash, object_key))
}

fn parse_reconstruction_json(value: &str) -> Result<FileReconstruction, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(value.len()).unwrap_or(u64::MAX),
        MAX_RECONSTRUCTION_METADATA_BYTES,
    )?;
    from_str::<FileReconstructionRecord>(value)?.into_domain()
}

fn parse_reconstruction_json_bytes(
    bytes: &[u8],
) -> Result<FileReconstruction, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_RECONSTRUCTION_METADATA_BYTES,
    )?;
    from_slice::<FileReconstructionRecord>(bytes)?.into_domain()
}

fn parse_file_record_json_bytes(bytes: &[u8]) -> Result<FileRecord, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_LOCAL_RECORD_METADATA_BYTES,
    )?;
    Ok(from_slice(bytes)?)
}

fn parse_quarantine_candidate_json_bytes(
    bytes: &[u8],
) -> Result<QuarantineCandidate, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_CONTROL_PLANE_METADATA_BYTES,
    )?;
    #[derive(Deserialize)]
    struct QuarantineCandidateRecord {
        object_key: String,
        observed_length: u64,
        first_seen_unreachable_at_unix_seconds: u64,
        delete_after_unix_seconds: u64,
    }
    if let Ok(record) = from_slice::<QuarantineCandidateRecord>(bytes) {
        return QuarantineCandidate::new(
            ObjectKey::parse(&record.object_key)?,
            record.observed_length,
            record.first_seen_unreachable_at_unix_seconds,
            record.delete_after_unix_seconds,
        )
        .map_err(LocalIndexStoreError::from);
    }
    let legacy = from_slice::<LegacyQuarantineCandidateRecord>(bytes)?;
    QuarantineCandidate::new(
        legacy_quarantine_object_key(&legacy.hash)?,
        legacy.bytes,
        legacy.first_seen_unreachable_at_unix_seconds,
        legacy.delete_after_unix_seconds,
    )
    .map_err(LocalIndexStoreError::from)
}

fn parse_retention_hold_json_bytes(bytes: &[u8]) -> Result<RetentionHold, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_CONTROL_PLANE_METADATA_BYTES,
    )?;
    #[derive(Deserialize)]
    struct RetentionHoldRecord {
        object_key: String,
        reason: String,
        held_at_unix_seconds: u64,
        release_after_unix_seconds: Option<u64>,
    }
    let record = from_slice::<RetentionHoldRecord>(bytes)?;
    RetentionHold::new(
        ObjectKey::parse(&record.object_key)?,
        record.reason,
        record.held_at_unix_seconds,
        record.release_after_unix_seconds,
    )
    .map_err(LocalIndexStoreError::from)
}

fn parse_webhook_delivery_json_bytes(
    bytes: &[u8],
) -> Result<WebhookDelivery, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_CONTROL_PLANE_METADATA_BYTES,
    )?;
    #[derive(Deserialize)]
    struct WebhookDeliveryRecord {
        provider: String,
        owner: String,
        repo: String,
        delivery_id: String,
        processed_at_unix_seconds: u64,
    }
    let record = from_slice::<WebhookDeliveryRecord>(bytes)?;
    let provider = parse_repository_provider(&record.provider, || {
        LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
    })?;
    WebhookDelivery::new(
        provider,
        record.owner,
        record.repo,
        record.delivery_id,
        record.processed_at_unix_seconds,
    )
    .map_err(LocalIndexStoreError::from)
}

fn parse_provider_repository_state_json_bytes(
    bytes: &[u8],
) -> Result<ProviderRepositoryState, LocalIndexStoreError> {
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_CONTROL_PLANE_METADATA_BYTES,
    )?;
    #[derive(Deserialize)]
    struct ProviderRepositoryStateRecord {
        provider: String,
        owner: String,
        repo: String,
        last_access_changed_at_unix_seconds: Option<u64>,
        last_revision_pushed_at_unix_seconds: Option<u64>,
        last_pushed_revision: Option<String>,
        #[serde(default)]
        last_cache_invalidated_at_unix_seconds: Option<u64>,
        #[serde(default)]
        last_authorization_rechecked_at_unix_seconds: Option<u64>,
        #[serde(default)]
        last_drift_checked_at_unix_seconds: Option<u64>,
    }
    let record = from_slice::<ProviderRepositoryStateRecord>(bytes)?;
    let provider = parse_repository_provider(&record.provider, || {
        LocalIndexStoreError::WebhookDelivery(WebhookDeliveryError::InvalidProvider)
    })?;
    Ok(ProviderRepositoryState::new(
        provider,
        record.owner,
        record.repo,
        record.last_access_changed_at_unix_seconds,
        record.last_revision_pushed_at_unix_seconds,
        record.last_pushed_revision,
    )
    .with_reconciliation(
        record.last_cache_invalidated_at_unix_seconds,
        record.last_authorization_rechecked_at_unix_seconds,
        record.last_drift_checked_at_unix_seconds,
    ))
}

fn collect_legacy_files(root: &Path) -> Result<Vec<PathBuf>, LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => return Err(LocalIndexStoreError::Io(error)),
        };
        if metadata.file_type().is_symlink() {
            return Err(invalid_metadata_path_error());
        }
        if metadata.is_file() {
            files.push(path);
            continue;
        }
        if !metadata.is_dir() {
            return Err(invalid_metadata_path_error());
        }
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(invalid_metadata_path_error());
            }
            stack.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

fn read_existing_file_bounded(
    path: &Path,
    maximum_bytes: u64,
    invalid_path_error: fn() -> LocalIndexStoreError,
) -> Result<Vec<u8>, LocalIndexStoreError> {
    ensure_parent_directory_path_components_are_not_symlinked(path, invalid_path_error)?;
    let metadata = fs::symlink_metadata(path)?;
    ensure_regular_metadata_file(&metadata, invalid_path_error)?;
    ensure_metadata_size_within_limit(metadata.len(), maximum_bytes)?;

    let mut file = open_metadata_file(path)?;
    let opened_metadata = file.metadata()?;
    ensure_regular_metadata_file(&opened_metadata, invalid_path_error)?;
    ensure_metadata_size_within_limit(opened_metadata.len(), maximum_bytes)?;
    let mut bytes =
        Vec::with_capacity(usize::try_from(opened_metadata.len()).unwrap_or(usize::MAX));
    let mut limited = Read::by_ref(&mut file).take(opened_metadata.len());
    limited.read_to_end(&mut bytes)?;

    let observed_bytes = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if observed_bytes != opened_metadata.len() {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: opened_metadata.len(),
            observed_bytes,
        });
    }

    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: opened_metadata.len(),
            observed_bytes: opened_metadata.len().saturating_add(1),
        });
    }

    let final_metadata = file.metadata()?;
    if final_metadata.len() != opened_metadata.len() {
        return Err(LocalIndexStoreError::MetadataLengthMismatch {
            expected_bytes: opened_metadata.len(),
            observed_bytes: final_metadata.len(),
        });
    }

    Ok(bytes)
}

#[cfg(unix)]
fn open_metadata_file(path: &Path) -> Result<fs::File, LocalIndexStoreError> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(LocalIndexStoreError::Io)
}

#[cfg(not(unix))]
fn open_metadata_file(path: &Path) -> Result<fs::File, LocalIndexStoreError> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(LocalIndexStoreError::Io)
}

fn file_modified_since_epoch(path: &Path) -> Result<u64, LocalIndexStoreError> {
    let metadata = fs::symlink_metadata(path)?;
    let modified = match metadata.modified() {
        Ok(value) => value,
        Err(_error) => return Ok(0),
    };
    Ok(match modified.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_error) => 0,
    })
}

fn ensure_regular_metadata_file(
    metadata: &fs::Metadata,
    invalid_path_error: fn() -> LocalIndexStoreError,
) -> Result<(), LocalIndexStoreError> {
    if !metadata.file_type().is_file() {
        return Err(invalid_path_error());
    }
    Ok(())
}

fn read_sqlite_record_bytes(value: ValueRef<'_>) -> Result<Vec<u8>, SqliteError> {
    let bytes = match value {
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => bytes.to_vec(),
        ValueRef::Null | ValueRef::Integer(_) | ValueRef::Real(_) => {
            return Err(SqliteError::FromSqlConversionFailure(
                0,
                Type::Text,
                Box::new(IoError::new(
                    ErrorKind::InvalidData,
                    "stored sqlite record metadata must be text or blob",
                )),
            ));
        }
    };
    ensure_metadata_size_within_limit(
        u64::try_from(bytes.len()).unwrap_or(u64::MAX),
        MAX_LOCAL_RECORD_METADATA_BYTES,
    )
    .map_err(|error| match error {
        LocalIndexStoreError::MetadataTooLarge {
            observed_bytes,
            maximum_bytes,
        } => SqliteError::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(LocalIndexStoreError::MetadataTooLarge {
                observed_bytes,
                maximum_bytes,
            }),
        ),
        other @ LocalIndexStoreError::Io(_)
        | other @ LocalIndexStoreError::Sqlite(_)
        | other @ LocalIndexStoreError::Json(_)
        | other @ LocalIndexStoreError::MetadataLengthMismatch { .. }
        | other @ LocalIndexStoreError::HashParse(_)
        | other @ LocalIndexStoreError::ObjectKey(_)
        | other @ LocalIndexStoreError::Range(_)
        | other @ LocalIndexStoreError::RetentionHold(_)
        | other @ LocalIndexStoreError::QuarantineCandidate(_)
        | other @ LocalIndexStoreError::WebhookDelivery(_)
        | other @ LocalIndexStoreError::IntegerOutOfRange
        | other @ LocalIndexStoreError::InvalidRecordKind
        | other @ LocalIndexStoreError::InvalidLegacyImportState => {
            SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(other))
        }
    })?;
    Ok(bytes)
}

const fn ensure_metadata_size_within_limit(
    observed_bytes: u64,
    maximum_bytes: u64,
) -> Result<(), LocalIndexStoreError> {
    if observed_bytes > maximum_bytes {
        return Err(LocalIndexStoreError::MetadataTooLarge {
            observed_bytes,
            maximum_bytes,
        });
    }
    Ok(())
}

fn ensure_parent_directory_path_components_are_not_symlinked(
    path: &Path,
    invalid_path_error: fn() -> LocalIndexStoreError,
) -> Result<(), LocalIndexStoreError> {
    let parent = path.parent().ok_or_else(invalid_path_error)?;
    ensure_directory_path_components_are_not_symlinked_with(parent, invalid_path_error)
}

fn ensure_directory_path_components_are_not_symlinked(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked_with(path, invalid_metadata_path_error)
}

fn ensure_directory_path_components_are_not_symlinked_with(
    path: &Path,
    invalid_path_error: fn() -> LocalIndexStoreError,
) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked_shared(path)
        .map_err(|error| map_directory_path_error(error, invalid_path_error))
}

fn map_directory_path_error(
    error: DirectoryPathError,
    invalid_path_error: fn() -> LocalIndexStoreError,
) -> LocalIndexStoreError {
    match error {
        DirectoryPathError::UnsupportedPrefix
        | DirectoryPathError::SymlinkedComponent(_)
        | DirectoryPathError::NonDirectoryComponent(_) => invalid_path_error(),
        DirectoryPathError::Io(error) => LocalIndexStoreError::Io(error),
    }
}

fn legacy_record_path(root: &Path, kind: LocalRecordKind, record: &FileRecord) -> PathBuf {
    let base = match kind {
        LocalRecordKind::Latest => root.join("files"),
        LocalRecordKind::Version => root.join("file_versions"),
    };
    match (&record.repository_scope, kind) {
        (Some(scope), LocalRecordKind::Latest) => scoped_root(&base, scope).join(&record.file_id),
        (Some(scope), LocalRecordKind::Version) => scoped_root(&base, scope)
            .join(&record.file_id)
            .join(&record.content_hash),
        (None, LocalRecordKind::Latest) => base.join(&record.file_id),
        (None, LocalRecordKind::Version) => base.join(&record.file_id).join(&record.content_hash),
    }
}

fn scoped_root(base: &Path, repository_scope: &RepositoryScope) -> PathBuf {
    let mut path = base
        .to_path_buf()
        .join(repository_scope.provider().as_str())
        .join(hex::encode(repository_scope.owner().as_bytes()))
        .join(hex::encode(repository_scope.name().as_bytes()));
    if let Some(revision) = repository_scope.revision() {
        path = path.join(hex::encode(revision.as_bytes()));
    }
    path
}

fn legacy_quarantine_object_key(hash: &str) -> Result<ObjectKey, LocalIndexStoreError> {
    let prefix = hash.get(..2).ok_or(ObjectKeyError::UnsafePath)?;
    ObjectKey::parse(&format!("{prefix}/{hash}")).map_err(LocalIndexStoreError::from)
}

fn record_not_found_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::from(ErrorKind::NotFound))
}

fn invalid_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local metadata path must be a regular file and must not be a symlink",
    ))
}

fn invalid_record_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local record metadata path must be a regular file and must not be a symlink",
    ))
}

fn u64_to_i64(value: u64) -> Result<i64, LocalIndexStoreError> {
    i64::try_from(value).map_err(|_error| LocalIndexStoreError::IntegerOutOfRange)
}

fn i64_to_u64(value: i64) -> Result<u64, LocalIndexStoreError> {
    u64::try_from(value).map_err(|_error| LocalIndexStoreError::IntegerOutOfRange)
}

fn collect_rows<T>(
    rows: MappedRows<'_, impl FnMut(&Row<'_>) -> Result<T, SqliteError>>,
) -> Result<Vec<T>, LocalIndexStoreError> {
    let mut collected = Vec::new();
    for row in rows {
        collected.push(row?);
    }
    Ok(collected)
}

fn from_sql_error(error: impl StdError + Send + Sync + 'static) -> SqliteError {
    SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs, path::Path, slice::from_ref};

    use rusqlite::{Connection, config::DbConfig, params};
    use serde::Serialize;
    use serde_json::{from_slice, json, to_vec};
    use shardline_protocol::{ChunkRange, RepositoryProvider, RepositoryScope, ShardlineHash};
    use shardline_storage::ObjectKey;

    use super::{
        DedupeShardRecord, FileReconstructionRecord, LEGACY_IMPORT_COMPLETED_KEY,
        LOCAL_METADATA_DATABASE_FILE_NAME, LOCAL_SCHEMA_MIGRATIONS_TABLE, LOCAL_SQLITE_MIGRATIONS,
        LegacyQuarantineCandidateRecord, LocalIndexStore, LocalIndexStoreError, LocalRecordKind,
        LocalRecordStore, XorbPresenceRecord, legacy_record_path,
    };
    use crate::{
        DedupeShardMapping, FileChunkRecord, FileId, FileReconstruction, FileRecord, IndexStore,
        MemoryIndexStore, MemoryRecordStore, ProviderRepositoryState, QuarantineCandidate,
        ReconstructionTerm, RecordStore, RetentionHold, WebhookDelivery, XorbId,
        test_invariant_error::LocalSqliteInvariantError,
    };

    fn sample_repository_scope() -> Result<RepositoryScope, Box<dyn Error>> {
        Ok(RepositoryScope::new(
            RepositoryProvider::GitHub,
            "team",
            "assets",
            Some("main"),
        )?)
    }

    fn sample_record(repository_scope: Option<RepositoryScope>) -> FileRecord {
        FileRecord {
            file_id: "asset.bin".to_owned(),
            content_hash: "a".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            repository_scope,
            chunks: vec![FileChunkRecord {
                hash: "b".repeat(64),
                offset: 0,
                length: 4,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 4,
            }],
        }
    }

    fn open_sqlite_connection(root: &Path) -> Result<Connection, Box<dyn Error>> {
        Ok(Connection::open(
            root.join(LOCAL_METADATA_DATABASE_FILE_NAME),
        )?)
    }

    fn write_json(path: &Path, value: &impl Serialize) -> Result<(), Box<dyn Error>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, to_vec(value)?)?;
        Ok(())
    }

    #[tokio::test]
    async fn local_record_store_commit_file_version_metadata_is_atomic() {
        let result = exercise_local_record_store_commit_file_version_metadata_is_atomic().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite file-version commit atomicity regression: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_record_store_commit_native_shard_metadata_is_atomic() {
        let result = exercise_local_record_store_commit_native_shard_metadata_is_atomic().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite native shard commit atomicity regression: {error:?}"
        );
    }

    #[test]
    fn local_metadata_root_normalizes_gc_suffix_to_parent_database() {
        let result = exercise_local_metadata_root_normalizes_gc_suffix_to_parent_database();
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite root normalization regression: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_sqlite_imports_legacy_filesystem_metadata() {
        let result = exercise_local_sqlite_imports_legacy_filesystem_metadata().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite legacy filesystem import regression: {error:?}"
        );
    }

    #[test]
    fn local_sqlite_connection_enables_defensive_settings() {
        let result = exercise_local_sqlite_connection_enables_defensive_settings();
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite connection hardening regression: {error:?}"
        );
    }

    #[test]
    fn local_sqlite_rejects_invalid_legacy_import_state() {
        let result = exercise_local_sqlite_rejects_invalid_legacy_import_state();
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite invalid legacy import state regression: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_record_store_reads_corrupt_sqlite_bytes_verbatim() {
        let result = exercise_local_record_store_reads_corrupt_sqlite_bytes_verbatim().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite raw record byte preservation regression: {error:?}"
        );
    }

    #[tokio::test]
    async fn local_sqlite_matches_memory_adapters_across_state_machine_operations() {
        let result =
            exercise_local_sqlite_matches_memory_adapters_across_state_machine_operations().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "local sqlite state-machine parity regression: {error:?}"
        );
    }

    async fn exercise_local_record_store_commit_file_version_metadata_is_atomic()
    -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let store = LocalRecordStore::new(storage.path().to_path_buf())?;
        let record = sample_record(Some(sample_repository_scope()?));
        let connection = open_sqlite_connection(storage.path())?;
        connection.execute_batch(
            "CREATE TRIGGER fail_latest_insert
             BEFORE INSERT ON shardline_file_records
             WHEN NEW.record_kind = 'latest'
             BEGIN
                 SELECT RAISE(FAIL, 'fail latest insert');
             END;",
        )?;

        let result = store.commit_file_version_metadata(&record).await;
        if !matches!(result, Err(LocalIndexStoreError::Sqlite(_))) {
            return Err(LocalSqliteInvariantError::new("expected sqlite trigger failure").into());
        }

        let latest_locator = RecordStore::latest_record_locator(&store, &record);
        let version_locator = RecordStore::version_record_locator(&store, &record);
        if RecordStore::record_locator_exists(&store, &latest_locator).await? {
            return Err(LocalSqliteInvariantError::new(
                "latest locator survived failed transaction",
            )
            .into());
        }
        if RecordStore::record_locator_exists(&store, &version_locator).await? {
            return Err(LocalSqliteInvariantError::new(
                "version locator survived failed transaction",
            )
            .into());
        }

        Ok(())
    }

    async fn exercise_local_record_store_commit_native_shard_metadata_is_atomic()
    -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
        let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
        let record = sample_record(None);
        let shard_object_key = ObjectKey::parse("shards/aa/native.shard")?;
        let mapping = DedupeShardMapping::new(ShardlineHash::from_bytes([7; 32]), shard_object_key);
        let connection = open_sqlite_connection(storage.path())?;
        connection.execute_batch(
            "CREATE TRIGGER fail_dedupe_insert
             BEFORE INSERT ON shardline_dedupe_shards
             BEGIN
                 SELECT RAISE(FAIL, 'fail dedupe insert');
             END;",
        )?;

        let result = record_store
            .commit_native_shard_metadata(from_ref(&record), from_ref(&mapping))
            .await;
        if !matches!(result, Err(LocalIndexStoreError::Sqlite(_))) {
            return Err(
                LocalSqliteInvariantError::new("expected sqlite dedupe trigger failure").into(),
            );
        }
        let latest_locator = RecordStore::latest_record_locator(&record_store, &record);
        let version_locator = RecordStore::version_record_locator(&record_store, &record);
        if RecordStore::record_locator_exists(&record_store, &latest_locator).await? {
            return Err(LocalSqliteInvariantError::new(
                "latest locator survived failed shard transaction",
            )
            .into());
        }
        if RecordStore::record_locator_exists(&record_store, &version_locator).await? {
            return Err(LocalSqliteInvariantError::new(
                "version locator survived failed shard transaction",
            )
            .into());
        }
        if IndexStore::dedupe_shard_mapping(&index_store, &mapping.chunk_hash())?.is_some() {
            return Err(LocalSqliteInvariantError::new(
                "dedupe mapping survived failed shard transaction",
            )
            .into());
        }

        Ok(())
    }

    fn exercise_local_metadata_root_normalizes_gc_suffix_to_parent_database()
    -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;

        let _store = LocalIndexStore::new(storage.path().join("gc"))?;

        if !storage
            .path()
            .join(LOCAL_METADATA_DATABASE_FILE_NAME)
            .is_file()
        {
            return Err(LocalSqliteInvariantError::new(
                "normalized root did not create metadata database",
            )
            .into());
        }
        if storage
            .path()
            .join("gc")
            .join(LOCAL_METADATA_DATABASE_FILE_NAME)
            .exists()
        {
            return Err(LocalSqliteInvariantError::new(
                "gc child directory unexpectedly received sqlite database",
            )
            .into());
        }

        Ok(())
    }

    fn exercise_local_sqlite_connection_enables_defensive_settings() -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let store = LocalIndexStore::new(storage.path().to_path_buf())?;
        let connection = store.open_connection()?;

        if !connection.db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE)? {
            return Err(
                LocalSqliteInvariantError::new("sqlite defensive mode was not enabled").into(),
            );
        }

        let trusted_schema =
            connection.pragma_query_value(None, "trusted_schema", |row| row.get::<_, i64>(0))?;
        if trusted_schema != 0 {
            return Err(
                LocalSqliteInvariantError::new("sqlite trusted_schema remained enabled").into(),
            );
        }

        let cell_size_check =
            connection.pragma_query_value(None, "cell_size_check", |row| row.get::<_, i64>(0))?;
        if cell_size_check != 1 {
            return Err(
                LocalSqliteInvariantError::new("sqlite cell_size_check was not enabled").into(),
            );
        }

        Ok(())
    }

    fn exercise_local_sqlite_rejects_invalid_legacy_import_state() -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let _store = LocalIndexStore::new(storage.path().to_path_buf())?;
        let connection = open_sqlite_connection(storage.path())?;
        connection.execute(
            "UPDATE shardline_local_metadata_meta
             SET value = ?1
             WHERE key = ?2",
            params!["not-a-boolean", LEGACY_IMPORT_COMPLETED_KEY],
        )?;

        let reopened = LocalIndexStore::new(storage.path().to_path_buf());
        if !matches!(
            reopened,
            Err(LocalIndexStoreError::InvalidLegacyImportState)
        ) {
            return Err(
                LocalSqliteInvariantError::new("invalid import state did not fail closed").into(),
            );
        }

        Ok(())
    }

    async fn exercise_local_record_store_reads_corrupt_sqlite_bytes_verbatim()
    -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let store = LocalRecordStore::new(storage.path().to_path_buf())?;
        let record = sample_record(Some(sample_repository_scope()?));
        RecordStore::write_version_record(&store, &record).await?;
        RecordStore::write_latest_record(&store, &record).await?;

        let corrupt_bytes = b"{not-json".to_vec();
        let connection = open_sqlite_connection(storage.path())?;
        connection.execute(
            "UPDATE shardline_file_records
             SET record = ?1
             WHERE record_kind = ?2 AND file_id = ?3 AND content_hash = ?4",
            params![
                corrupt_bytes.clone(),
                "version",
                record.file_id,
                record.content_hash,
            ],
        )?;

        let version_locator = RecordStore::version_record_locator(&store, &record);
        let loaded = RecordStore::read_record_bytes(&store, &version_locator).await?;
        if loaded != corrupt_bytes {
            return Err(LocalSqliteInvariantError::new(
                "sqlite record read normalized corrupt bytes",
            )
            .into());
        }

        Ok(())
    }

    async fn exercise_local_sqlite_matches_memory_adapters_across_state_machine_operations()
    -> Result<(), Box<dyn Error>> {
        let storage = tempfile::tempdir()?;
        let local_index_store = LocalIndexStore::new(storage.path().to_path_buf())?;
        let local_record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
        let memory_index_store = MemoryIndexStore::new();
        let memory_record_store = MemoryRecordStore::new();
        let fixtures = StateMachineFixtures::load()?;

        let mut lcg_state = 0x5eed_cafe_d15c_a11e_u64;
        for step in 0..96_usize {
            lcg_state = next_state_machine_value(lcg_state);
            let record = state_machine_item(
                fixtures.records.as_slice(),
                state_machine_index(lcg_state, 0, fixtures.records.len())?,
                "record",
            )?;
            let reconstruction = state_machine_item(
                fixtures.reconstructions.as_slice(),
                state_machine_index(lcg_state, 8, fixtures.reconstructions.len())?,
                "reconstruction",
            )?;
            let mapping = state_machine_item(
                fixtures.dedupe_mappings.as_slice(),
                state_machine_index(lcg_state, 16, fixtures.dedupe_mappings.len())?,
                "dedupe mapping",
            )?;
            let quarantine_candidate = state_machine_item(
                fixtures.quarantine_candidates.as_slice(),
                state_machine_index(lcg_state, 24, fixtures.quarantine_candidates.len())?,
                "quarantine candidate",
            )?;
            let retention_hold = state_machine_item(
                fixtures.retention_holds.as_slice(),
                state_machine_index(lcg_state, 32, fixtures.retention_holds.len())?,
                "retention hold",
            )?;
            let webhook_delivery = state_machine_item(
                fixtures.webhook_deliveries.as_slice(),
                state_machine_index(lcg_state, 40, fixtures.webhook_deliveries.len())?,
                "webhook delivery",
            )?;
            let provider_state = state_machine_item(
                fixtures.provider_states.as_slice(),
                state_machine_index(lcg_state, 48, fixtures.provider_states.len())?,
                "provider repository state",
            )?;
            let xorb_id = state_machine_item(
                fixtures.xorb_ids.as_slice(),
                state_machine_index(lcg_state, 12, fixtures.xorb_ids.len())?,
                "xorb marker",
            )?;

            match step & 15 {
                0 => {
                    RecordStore::write_latest_record(&local_record_store, record).await?;
                    RecordStore::write_latest_record(&memory_record_store, record).await?;
                }
                1 => {
                    RecordStore::write_version_record(&local_record_store, record).await?;
                    RecordStore::write_version_record(&memory_record_store, record).await?;
                }
                2 => {
                    let local_locator =
                        RecordStore::latest_record_locator(&local_record_store, record);
                    let memory_locator =
                        RecordStore::latest_record_locator(&memory_record_store, record);
                    let local_exists =
                        RecordStore::record_locator_exists(&local_record_store, &local_locator)
                            .await?;
                    let memory_exists =
                        RecordStore::record_locator_exists(&memory_record_store, &memory_locator)
                            .await?;
                    if local_exists != memory_exists {
                        return Err(LocalSqliteInvariantError::new(
                            "local and memory latest-record existence diverged",
                        )
                        .into());
                    }
                    if local_exists {
                        RecordStore::delete_record_locator(&local_record_store, &local_locator)
                            .await?;
                        RecordStore::delete_record_locator(&memory_record_store, &memory_locator)
                            .await?;
                    }
                }
                3 => {
                    let local_locator =
                        RecordStore::version_record_locator(&local_record_store, record);
                    let memory_locator =
                        RecordStore::version_record_locator(&memory_record_store, record);
                    let local_exists =
                        RecordStore::record_locator_exists(&local_record_store, &local_locator)
                            .await?;
                    let memory_exists =
                        RecordStore::record_locator_exists(&memory_record_store, &memory_locator)
                            .await?;
                    if local_exists != memory_exists {
                        return Err(LocalSqliteInvariantError::new(
                            "local and memory version-record existence diverged",
                        )
                        .into());
                    }
                    if local_exists {
                        RecordStore::delete_record_locator(&local_record_store, &local_locator)
                            .await?;
                        RecordStore::delete_record_locator(&memory_record_store, &memory_locator)
                            .await?;
                    }
                }
                4 => {
                    local_index_store
                        .insert_reconstruction(&reconstruction.0, &reconstruction.1)?;
                    memory_index_store
                        .insert_reconstruction(&reconstruction.0, &reconstruction.1)?;
                }
                5 => {
                    let local_deleted =
                        IndexStore::delete_reconstruction(&local_index_store, &reconstruction.0)?;
                    let memory_deleted =
                        IndexStore::delete_reconstruction(&memory_index_store, &reconstruction.0)?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "reconstruction delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
                6 => {
                    local_index_store.insert_xorb(xorb_id)?;
                    memory_index_store.insert_xorb(xorb_id)?;
                }
                7 => {
                    local_index_store.upsert_dedupe_shard_mapping(mapping)?;
                    memory_index_store.upsert_dedupe_shard_mapping(mapping)?;
                }
                8 => {
                    let local_deleted = IndexStore::delete_dedupe_shard_mapping(
                        &local_index_store,
                        &mapping.chunk_hash(),
                    )?;
                    let memory_deleted = IndexStore::delete_dedupe_shard_mapping(
                        &memory_index_store,
                        &mapping.chunk_hash(),
                    )?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "dedupe mapping delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
                9 => {
                    local_index_store.upsert_quarantine_candidate(quarantine_candidate)?;
                    memory_index_store.upsert_quarantine_candidate(quarantine_candidate)?;
                }
                10 => {
                    let local_deleted = IndexStore::delete_quarantine_candidate(
                        &local_index_store,
                        quarantine_candidate.object_key(),
                    )?;
                    let memory_deleted = IndexStore::delete_quarantine_candidate(
                        &memory_index_store,
                        quarantine_candidate.object_key(),
                    )?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "quarantine delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
                11 => {
                    local_index_store.upsert_retention_hold(retention_hold)?;
                    memory_index_store.upsert_retention_hold(retention_hold)?;
                }
                12 => {
                    let local_deleted = IndexStore::delete_retention_hold(
                        &local_index_store,
                        retention_hold.object_key(),
                    )?;
                    let memory_deleted = IndexStore::delete_retention_hold(
                        &memory_index_store,
                        retention_hold.object_key(),
                    )?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "retention hold delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
                13 => {
                    let local_recorded =
                        local_index_store.record_webhook_delivery(webhook_delivery)?;
                    let memory_recorded =
                        memory_index_store.record_webhook_delivery(webhook_delivery)?;
                    if local_recorded != memory_recorded {
                        return Err(LocalSqliteInvariantError::new(
                            "webhook delivery insert behavior diverged from memory",
                        )
                        .into());
                    }
                }
                14 => {
                    let local_deleted =
                        IndexStore::delete_webhook_delivery(&local_index_store, webhook_delivery)?;
                    let memory_deleted =
                        IndexStore::delete_webhook_delivery(&memory_index_store, webhook_delivery)?;
                    if local_deleted != memory_deleted {
                        return Err(LocalSqliteInvariantError::new(
                            "webhook delivery delete behavior diverged from memory",
                        )
                        .into());
                    }
                }
                15 => {
                    let upsert_even_step = ((lcg_state >> 20) & 1) == 0;
                    if upsert_even_step {
                        local_index_store.upsert_provider_repository_state(provider_state)?;
                        memory_index_store.upsert_provider_repository_state(provider_state)?;
                    } else {
                        let local_deleted = IndexStore::delete_provider_repository_state(
                            &local_index_store,
                            provider_state.provider(),
                            provider_state.owner(),
                            provider_state.repo(),
                        )?;
                        let memory_deleted = IndexStore::delete_provider_repository_state(
                            &memory_index_store,
                            provider_state.provider(),
                            provider_state.owner(),
                            provider_state.repo(),
                        )?;
                        if local_deleted != memory_deleted {
                            return Err(LocalSqliteInvariantError::new(
                                "provider state delete behavior diverged from memory",
                            )
                            .into());
                        }
                    }
                }
                _other => {
                    return Err(LocalSqliteInvariantError::new(
                        "state-machine operation selection overflowed",
                    )
                    .into());
                }
            }

            assert_local_sqlite_matches_memory_state(
                &local_record_store,
                &memory_record_store,
                &local_index_store,
                &memory_index_store,
                &fixtures,
            )
            .await?;
        }

        Ok(())
    }

    async fn exercise_local_sqlite_imports_legacy_filesystem_metadata() -> Result<(), Box<dyn Error>>
    {
        let storage = tempfile::tempdir()?;
        let scope = sample_repository_scope()?;
        let record = sample_record(Some(scope.clone()));
        write_json(
            &legacy_record_path(storage.path(), LocalRecordKind::Latest, &record),
            &record,
        )?;
        write_json(
            &legacy_record_path(storage.path(), LocalRecordKind::Version, &record),
            &record,
        )?;

        let reconstruction_hash = ShardlineHash::from_bytes([3; 32]);
        let file_id = FileId::new(reconstruction_hash);
        let xorb_id = XorbId::new(reconstruction_hash);
        let reconstruction = FileReconstruction::new(vec![ReconstructionTerm::new(
            xorb_id,
            ChunkRange::new(0, 1)?,
            4,
        )]);
        write_json(
            &storage
                .path()
                .join("gc")
                .join("reconstructions")
                .join(format!("{}.json", file_id.hash().api_hex_string())),
            &FileReconstructionRecord::from_domain(&reconstruction),
        )?;

        let xorb_hash = ShardlineHash::from_bytes([4; 32]).api_hex_string();
        write_json(
            &storage
                .path()
                .join("gc")
                .join("xorbs")
                .join(format!("{xorb_hash}.json")),
            &XorbPresenceRecord {
                hash: xorb_hash.clone(),
            },
        )?;

        let dedupe_chunk_hash = ShardlineHash::from_bytes([5; 32]).api_hex_string();
        let dedupe_mapping = DedupeShardMapping::new(
            ShardlineHash::parse_api_hex(&dedupe_chunk_hash)?,
            ObjectKey::parse("shards/aa/native.shard")?,
        );
        write_json(
            &storage
                .path()
                .join("gc")
                .join("dedupe-shards")
                .join(format!("{dedupe_chunk_hash}.json")),
            &DedupeShardRecord {
                chunk_hash: dedupe_chunk_hash.clone(),
                shard_object_key: dedupe_mapping.shard_object_key().as_str().to_owned(),
            },
        )?;

        let quarantine_hash = ShardlineHash::from_bytes([6; 32]).api_hex_string();
        let quarantine_candidate = QuarantineCandidate::new(
            ObjectKey::parse(&format!("{}/{}", &quarantine_hash[..2], quarantine_hash))?,
            9,
            10,
            20,
        )?;
        write_json(
            &storage
                .path()
                .join("gc")
                .join("quarantine")
                .join("candidate.json"),
            &LegacyQuarantineCandidateRecord {
                hash: quarantine_hash.clone(),
                bytes: quarantine_candidate.observed_length(),
                first_seen_unreachable_at_unix_seconds: quarantine_candidate
                    .first_seen_unreachable_at_unix_seconds(),
                delete_after_unix_seconds: quarantine_candidate.delete_after_unix_seconds(),
            },
        )?;

        let retention_hold = RetentionHold::new(
            ObjectKey::parse("chunks/aa/held-object")?,
            "provider deletion grace".to_owned(),
            30,
            Some(40),
        )?;
        write_json(
            &storage
                .path()
                .join("gc")
                .join("retention-holds")
                .join("hold.json"),
            &json!({
                "object_key": retention_hold.object_key().as_str(),
                "reason": retention_hold.reason(),
                "held_at_unix_seconds": retention_hold.held_at_unix_seconds(),
                "release_after_unix_seconds": retention_hold.release_after_unix_seconds(),
            }),
        )?;

        let webhook_delivery = WebhookDelivery::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            "delivery-1".to_owned(),
            50,
        )?;
        write_json(
            &storage
                .path()
                .join("gc")
                .join("webhook-deliveries")
                .join("delivery.json"),
            &json!({
                "provider": webhook_delivery.provider().as_str(),
                "owner": webhook_delivery.owner(),
                "repo": webhook_delivery.repo(),
                "delivery_id": webhook_delivery.delivery_id(),
                "processed_at_unix_seconds": webhook_delivery.processed_at_unix_seconds(),
            }),
        )?;

        let provider_state = ProviderRepositoryState::new(
            RepositoryProvider::GitHub,
            "team".to_owned(),
            "assets".to_owned(),
            Some(60),
            Some(70),
            Some("refs/heads/main".to_owned()),
        )
        .with_reconciliation(Some(80), Some(81), Some(82));
        write_json(
            &storage
                .path()
                .join("gc")
                .join("provider-repository-states")
                .join("state.json"),
            &json!({
                "provider": provider_state.provider().as_str(),
                "owner": provider_state.owner(),
                "repo": provider_state.repo(),
                "last_access_changed_at_unix_seconds": provider_state.last_access_changed_at_unix_seconds(),
                "last_revision_pushed_at_unix_seconds": provider_state.last_revision_pushed_at_unix_seconds(),
                "last_pushed_revision": provider_state.last_pushed_revision(),
                "last_cache_invalidated_at_unix_seconds": provider_state.last_cache_invalidated_at_unix_seconds(),
                "last_authorization_rechecked_at_unix_seconds": provider_state.last_authorization_rechecked_at_unix_seconds(),
                "last_drift_checked_at_unix_seconds": provider_state.last_drift_checked_at_unix_seconds(),
            }),
        )?;

        let record_store = LocalRecordStore::new(storage.path().to_path_buf())?;
        let index_store = LocalIndexStore::new(storage.path().to_path_buf())?;

        let latest_locator = RecordStore::latest_record_locator(&record_store, &record);
        let version_locator = RecordStore::version_record_locator(&record_store, &record);
        if !RecordStore::record_locator_exists(&record_store, &latest_locator).await? {
            return Err(
                LocalSqliteInvariantError::new("latest legacy record was not imported").into(),
            );
        }
        if !RecordStore::record_locator_exists(&record_store, &version_locator).await? {
            return Err(
                LocalSqliteInvariantError::new("version legacy record was not imported").into(),
            );
        }
        let latest_record = from_slice::<FileRecord>(
            &RecordStore::read_record_bytes(&record_store, &latest_locator).await?,
        )?;
        if latest_record != record {
            return Err(LocalSqliteInvariantError::new(
                "latest legacy record contents changed during import",
            )
            .into());
        }
        if IndexStore::reconstruction(&index_store, &file_id)? != Some(reconstruction) {
            return Err(
                LocalSqliteInvariantError::new("reconstruction row was not imported").into(),
            );
        }
        if !IndexStore::contains_xorb(
            &index_store,
            &XorbId::new(ShardlineHash::parse_api_hex(&xorb_hash)?),
        )? {
            return Err(LocalSqliteInvariantError::new("xorb marker was not imported").into());
        }
        if IndexStore::dedupe_shard_mapping(
            &index_store,
            &ShardlineHash::parse_api_hex(&dedupe_chunk_hash)?,
        )? != Some(dedupe_mapping)
        {
            return Err(LocalSqliteInvariantError::new("dedupe mapping was not imported").into());
        }
        if IndexStore::quarantine_candidate(&index_store, quarantine_candidate.object_key())?
            != Some(quarantine_candidate)
        {
            return Err(
                LocalSqliteInvariantError::new("quarantine candidate was not imported").into(),
            );
        }
        if IndexStore::retention_hold(&index_store, retention_hold.object_key())?
            != Some(retention_hold)
        {
            return Err(LocalSqliteInvariantError::new("retention hold was not imported").into());
        }
        if IndexStore::list_webhook_deliveries(&index_store)? != vec![webhook_delivery] {
            return Err(LocalSqliteInvariantError::new("webhook delivery was not imported").into());
        }
        if IndexStore::provider_repository_state(
            &index_store,
            provider_state.provider(),
            provider_state.owner(),
            provider_state.repo(),
        )? != Some(provider_state)
        {
            return Err(LocalSqliteInvariantError::new(
                "provider repository state was not imported",
            )
            .into());
        }

        let connection = open_sqlite_connection(storage.path())?;
        let applied_versions = connection.query_row(
            &format!(
                "SELECT COUNT(*)
                 FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}"
            ),
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let expected_applied_versions = i64::try_from(LOCAL_SQLITE_MIGRATIONS.len())?;
        if applied_versions != expected_applied_versions {
            return Err(
                LocalSqliteInvariantError::new("sqlite migrations were not fully applied").into(),
            );
        }

        Ok(())
    }

    fn sample_state_machine_records() -> Result<Vec<FileRecord>, Box<dyn Error>> {
        let scope_a = RepositoryScope::new(
            RepositoryProvider::GitHub,
            "team-a",
            "assets-a",
            Some("main"),
        )?;
        let scope_b = RepositoryScope::new(
            RepositoryProvider::Gitea,
            "team-b",
            "assets-b",
            Some("release"),
        )?;
        Ok(vec![
            FileRecord {
                file_id: "alpha.bin".to_owned(),
                content_hash: ShardlineHash::from_bytes([11; 32]).api_hex_string(),
                total_bytes: 4,
                chunk_size: 4,
                repository_scope: None,
                chunks: vec![FileChunkRecord {
                    hash: ShardlineHash::from_bytes([21; 32]).api_hex_string(),
                    offset: 0,
                    length: 4,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 4,
                }],
            },
            FileRecord {
                file_id: "beta.bin".to_owned(),
                content_hash: ShardlineHash::from_bytes([12; 32]).api_hex_string(),
                total_bytes: 8,
                chunk_size: 4,
                repository_scope: Some(scope_a),
                chunks: vec![
                    FileChunkRecord {
                        hash: ShardlineHash::from_bytes([22; 32]).api_hex_string(),
                        offset: 0,
                        length: 4,
                        range_start: 0,
                        range_end: 1,
                        packed_start: 0,
                        packed_end: 4,
                    },
                    FileChunkRecord {
                        hash: ShardlineHash::from_bytes([23; 32]).api_hex_string(),
                        offset: 4,
                        length: 4,
                        range_start: 1,
                        range_end: 2,
                        packed_start: 4,
                        packed_end: 8,
                    },
                ],
            },
            FileRecord {
                file_id: "gamma.bin".to_owned(),
                content_hash: ShardlineHash::from_bytes([13; 32]).api_hex_string(),
                total_bytes: 6,
                chunk_size: 6,
                repository_scope: Some(scope_b),
                chunks: vec![FileChunkRecord {
                    hash: ShardlineHash::from_bytes([24; 32]).api_hex_string(),
                    offset: 0,
                    length: 6,
                    range_start: 0,
                    range_end: 1,
                    packed_start: 0,
                    packed_end: 6,
                }],
            },
        ])
    }

    fn sample_state_machine_reconstructions()
    -> Result<Vec<(FileId, FileReconstruction)>, Box<dyn Error>> {
        Ok(vec![
            (
                FileId::new(ShardlineHash::from_bytes([41; 32])),
                FileReconstruction::new(vec![ReconstructionTerm::new(
                    XorbId::new(ShardlineHash::from_bytes([51; 32])),
                    ChunkRange::new(0, 1)?,
                    4,
                )]),
            ),
            (
                FileId::new(ShardlineHash::from_bytes([42; 32])),
                FileReconstruction::new(vec![
                    ReconstructionTerm::new(
                        XorbId::new(ShardlineHash::from_bytes([52; 32])),
                        ChunkRange::new(0, 1)?,
                        8,
                    ),
                    ReconstructionTerm::new(
                        XorbId::new(ShardlineHash::from_bytes([53; 32])),
                        ChunkRange::new(1, 2)?,
                        8,
                    ),
                ]),
            ),
        ])
    }

    fn sample_state_machine_xorbs() -> Vec<XorbId> {
        vec![
            XorbId::new(ShardlineHash::from_bytes([51; 32])),
            XorbId::new(ShardlineHash::from_bytes([52; 32])),
            XorbId::new(ShardlineHash::from_bytes([53; 32])),
            XorbId::new(ShardlineHash::from_bytes([54; 32])),
        ]
    }

    fn sample_state_machine_dedupe_mappings() -> Result<Vec<DedupeShardMapping>, Box<dyn Error>> {
        Ok(vec![
            DedupeShardMapping::new(
                ShardlineHash::from_bytes([61; 32]),
                ObjectKey::parse("shards/aa/native-a.shard")?,
            ),
            DedupeShardMapping::new(
                ShardlineHash::from_bytes([62; 32]),
                ObjectKey::parse("shards/bb/native-b.shard")?,
            ),
        ])
    }

    fn sample_state_machine_quarantine_candidates()
    -> Result<Vec<QuarantineCandidate>, Box<dyn Error>> {
        Ok(vec![
            QuarantineCandidate::new(ObjectKey::parse("aa/aaaaaaaa")?, 4, 10, 20)?,
            QuarantineCandidate::new(ObjectKey::parse("bb/bbbbbbbb")?, 8, 11, 21)?,
        ])
    }

    fn sample_state_machine_retention_holds() -> Result<Vec<RetentionHold>, Box<dyn Error>> {
        Ok(vec![
            RetentionHold::new(
                ObjectKey::parse("cc/cccccccc")?,
                "provider deletion grace".to_owned(),
                12,
                Some(22),
            )?,
            RetentionHold::new(
                ObjectKey::parse("dd/dddddddd")?,
                "manual retain".to_owned(),
                13,
                None,
            )?,
        ])
    }

    fn sample_state_machine_webhook_deliveries() -> Result<Vec<WebhookDelivery>, Box<dyn Error>> {
        Ok(vec![
            WebhookDelivery::new(
                RepositoryProvider::GitHub,
                "team-a".to_owned(),
                "assets-a".to_owned(),
                "delivery-a".to_owned(),
                30,
            )?,
            WebhookDelivery::new(
                RepositoryProvider::GitLab,
                "team-b".to_owned(),
                "assets-b".to_owned(),
                "delivery-b".to_owned(),
                31,
            )?,
        ])
    }

    fn sample_state_machine_provider_states() -> Vec<ProviderRepositoryState> {
        vec![
            ProviderRepositoryState::new(
                RepositoryProvider::GitHub,
                "team-a".to_owned(),
                "assets-a".to_owned(),
                Some(40),
                Some(41),
                Some("refs/heads/main".to_owned()),
            )
            .with_reconciliation(Some(42), Some(43), Some(44)),
            ProviderRepositoryState::new(
                RepositoryProvider::Gitea,
                "team-b".to_owned(),
                "assets-b".to_owned(),
                Some(45),
                None,
                None,
            )
            .with_reconciliation(None, Some(46), None),
        ]
    }

    type CanonicalChunk = (String, u64, u64, u32, u32, u64, u64);
    type CanonicalRecord = (
        Option<String>,
        String,
        String,
        u64,
        u64,
        Vec<CanonicalChunk>,
    );
    type CanonicalProviderState = (
        String,
        String,
        String,
        Option<u64>,
        Option<u64>,
        Option<String>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
    );

    struct StateMachineFixtures {
        records: Vec<FileRecord>,
        reconstructions: Vec<(FileId, FileReconstruction)>,
        xorb_ids: Vec<XorbId>,
        dedupe_mappings: Vec<DedupeShardMapping>,
        quarantine_candidates: Vec<QuarantineCandidate>,
        retention_holds: Vec<RetentionHold>,
        webhook_deliveries: Vec<WebhookDelivery>,
        provider_states: Vec<ProviderRepositoryState>,
    }

    impl StateMachineFixtures {
        fn load() -> Result<Self, Box<dyn Error>> {
            Ok(Self {
                records: sample_state_machine_records()?,
                reconstructions: sample_state_machine_reconstructions()?,
                xorb_ids: sample_state_machine_xorbs(),
                dedupe_mappings: sample_state_machine_dedupe_mappings()?,
                quarantine_candidates: sample_state_machine_quarantine_candidates()?,
                retention_holds: sample_state_machine_retention_holds()?,
                webhook_deliveries: sample_state_machine_webhook_deliveries()?,
                provider_states: sample_state_machine_provider_states(),
            })
        }
    }

    const fn next_state_machine_value(state: u64) -> u64 {
        state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407)
    }

    fn state_machine_index(state: u64, shift: u32, len: usize) -> Result<usize, Box<dyn Error>> {
        let len_u64 = u64::try_from(len)?;
        if len_u64 == 0 {
            return Err(
                LocalSqliteInvariantError::new("state-machine fixture list was empty").into(),
            );
        }
        let shifted = state.checked_shr(shift).unwrap_or(0);
        let bounded = shifted
            .checked_rem(len_u64)
            .ok_or_else(|| LocalSqliteInvariantError::new("state-machine modulus overflowed"))?;
        Ok(usize::try_from(bounded)?)
    }

    fn state_machine_item<'values, T>(
        values: &'values [T],
        index: usize,
        what: &str,
    ) -> Result<&'values T, Box<dyn Error>> {
        values.get(index).ok_or_else(|| {
            LocalSqliteInvariantError::new(format!("missing state-machine {what} at index {index}"))
                .into()
        })
    }

    async fn assert_local_sqlite_matches_memory_state(
        local_record_store: &LocalRecordStore,
        memory_record_store: &MemoryRecordStore,
        local_index_store: &LocalIndexStore,
        memory_index_store: &MemoryIndexStore,
        fixtures: &StateMachineFixtures,
    ) -> Result<(), Box<dyn Error>> {
        if canonical_record_entries(local_record_store, true).await?
            != canonical_record_entries(memory_record_store, true).await?
        {
            return Err(
                LocalSqliteInvariantError::new("latest-record state diverged from memory").into(),
            );
        }
        if canonical_record_entries(local_record_store, false).await?
            != canonical_record_entries(memory_record_store, false).await?
        {
            return Err(LocalSqliteInvariantError::new(
                "version-record state diverged from memory",
            )
            .into());
        }

        for record in &fixtures.records {
            if RecordStore::read_latest_record_bytes(local_record_store, record).await?
                != RecordStore::read_latest_record_bytes(memory_record_store, record).await?
            {
                return Err(LocalSqliteInvariantError::new(
                    "latest-record lookup diverged from memory",
                )
                .into());
            }
        }

        let reconstruction_file_ids = fixtures
            .reconstructions
            .iter()
            .map(|(file_id, _reconstruction)| *file_id)
            .collect::<Vec<_>>();
        for file_id in reconstruction_file_ids {
            if IndexStore::reconstruction(local_index_store, &file_id)?
                != IndexStore::reconstruction(memory_index_store, &file_id)?
            {
                return Err(LocalSqliteInvariantError::new(
                    "reconstruction state diverged from memory",
                )
                .into());
            }
        }

        for xorb_id in &fixtures.xorb_ids {
            if IndexStore::contains_xorb(local_index_store, xorb_id)?
                != IndexStore::contains_xorb(memory_index_store, xorb_id)?
            {
                return Err(LocalSqliteInvariantError::new(
                    "xorb marker state diverged from memory",
                )
                .into());
            }
        }

        if canonical_dedupe_mappings(IndexStore::list_dedupe_shard_mappings(local_index_store)?)
            != canonical_dedupe_mappings(IndexStore::list_dedupe_shard_mappings(
                memory_index_store,
            )?)
        {
            return Err(LocalSqliteInvariantError::new(
                "dedupe mapping state diverged from memory",
            )
            .into());
        }

        if canonical_quarantine_candidates(IndexStore::list_quarantine_candidates(
            local_index_store,
        )?) != canonical_quarantine_candidates(IndexStore::list_quarantine_candidates(
            memory_index_store,
        )?) {
            return Err(LocalSqliteInvariantError::new(
                "quarantine candidate state diverged from memory",
            )
            .into());
        }

        if canonical_retention_holds(IndexStore::list_retention_holds(local_index_store)?)
            != canonical_retention_holds(IndexStore::list_retention_holds(memory_index_store)?)
        {
            return Err(LocalSqliteInvariantError::new(
                "retention hold state diverged from memory",
            )
            .into());
        }

        if canonical_webhook_deliveries(IndexStore::list_webhook_deliveries(local_index_store)?)
            != canonical_webhook_deliveries(IndexStore::list_webhook_deliveries(
                memory_index_store,
            )?)
        {
            return Err(LocalSqliteInvariantError::new(
                "webhook delivery state diverged from memory",
            )
            .into());
        }

        if canonical_provider_states(IndexStore::list_provider_repository_states(
            local_index_store,
        )?) != canonical_provider_states(IndexStore::list_provider_repository_states(
            memory_index_store,
        )?) {
            return Err(LocalSqliteInvariantError::new(
                "provider repository state diverged from memory",
            )
            .into());
        }

        let mut expected_provider_keys = fixtures
            .provider_states
            .iter()
            .map(|state| {
                (
                    state.provider(),
                    state.owner().to_owned(),
                    state.repo().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        expected_provider_keys.sort_by(|left, right| {
            (left.0.as_str(), left.1.as_str(), left.2.as_str()).cmp(&(
                right.0.as_str(),
                right.1.as_str(),
                right.2.as_str(),
            ))
        });
        expected_provider_keys.dedup();
        for (provider, owner, repo) in expected_provider_keys {
            if IndexStore::provider_repository_state(local_index_store, provider, &owner, &repo)?
                != IndexStore::provider_repository_state(
                    memory_index_store,
                    provider,
                    &owner,
                    &repo,
                )?
            {
                return Err(LocalSqliteInvariantError::new(
                    "provider repository lookup diverged from memory",
                )
                .into());
            }
        }

        Ok(())
    }

    async fn canonical_record_entries<Store>(
        store: &Store,
        latest: bool,
    ) -> Result<Vec<CanonicalRecord>, Box<dyn Error>>
    where
        Store: RecordStore,
        Store::Error: Error + Send + Sync + 'static,
    {
        let locators = if latest {
            RecordStore::list_latest_record_locators(store).await?
        } else {
            RecordStore::list_version_record_locators(store).await?
        };
        let mut entries = Vec::with_capacity(locators.len());
        for locator in locators {
            let bytes = RecordStore::read_record_bytes(store, &locator).await?;
            let record = from_slice::<FileRecord>(&bytes)?;
            entries.push(canonical_file_record(&record));
        }
        entries.sort();
        Ok(entries)
    }

    fn canonical_file_record(record: &FileRecord) -> CanonicalRecord {
        let scope = record.repository_scope.as_ref().map(|repository_scope| {
            format!(
                "{}:{}/{}/{}",
                repository_scope.provider().as_str(),
                repository_scope.owner(),
                repository_scope.name(),
                repository_scope.revision().unwrap_or("")
            )
        });
        let chunks = record
            .chunks
            .iter()
            .map(|chunk| {
                (
                    chunk.hash.clone(),
                    chunk.offset,
                    chunk.length,
                    chunk.range_start,
                    chunk.range_end,
                    chunk.packed_start,
                    chunk.packed_end,
                )
            })
            .collect::<Vec<_>>();
        (
            scope,
            record.file_id.clone(),
            record.content_hash.clone(),
            record.total_bytes,
            record.chunk_size,
            chunks,
        )
    }

    fn canonical_dedupe_mappings(mappings: Vec<DedupeShardMapping>) -> Vec<(String, String)> {
        let mut canonical = mappings
            .into_iter()
            .map(|mapping| {
                (
                    mapping.chunk_hash().api_hex_string(),
                    mapping.shard_object_key().as_str().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        canonical.sort();
        canonical
    }

    fn canonical_quarantine_candidates(
        candidates: Vec<QuarantineCandidate>,
    ) -> Vec<(String, u64, u64, u64)> {
        let mut canonical = candidates
            .into_iter()
            .map(|candidate| {
                (
                    candidate.object_key().as_str().to_owned(),
                    candidate.observed_length(),
                    candidate.first_seen_unreachable_at_unix_seconds(),
                    candidate.delete_after_unix_seconds(),
                )
            })
            .collect::<Vec<_>>();
        canonical.sort();
        canonical
    }

    fn canonical_retention_holds(
        holds: Vec<RetentionHold>,
    ) -> Vec<(String, String, u64, Option<u64>)> {
        let mut canonical = holds
            .into_iter()
            .map(|hold| {
                (
                    hold.object_key().as_str().to_owned(),
                    hold.reason().to_owned(),
                    hold.held_at_unix_seconds(),
                    hold.release_after_unix_seconds(),
                )
            })
            .collect::<Vec<_>>();
        canonical.sort();
        canonical
    }

    fn canonical_webhook_deliveries(
        deliveries: Vec<WebhookDelivery>,
    ) -> Vec<(String, String, String, String, u64)> {
        let mut canonical = deliveries
            .into_iter()
            .map(|delivery| {
                (
                    delivery.provider().as_str().to_owned(),
                    delivery.owner().to_owned(),
                    delivery.repo().to_owned(),
                    delivery.delivery_id().to_owned(),
                    delivery.processed_at_unix_seconds(),
                )
            })
            .collect::<Vec<_>>();
        canonical.sort();
        canonical
    }

    fn canonical_provider_states(
        states: Vec<ProviderRepositoryState>,
    ) -> Vec<CanonicalProviderState> {
        let mut canonical = states
            .into_iter()
            .map(|state| {
                (
                    state.provider().as_str().to_owned(),
                    state.owner().to_owned(),
                    state.repo().to_owned(),
                    state.last_access_changed_at_unix_seconds(),
                    state.last_revision_pushed_at_unix_seconds(),
                    state.last_pushed_revision().map(ToOwned::to_owned),
                    state.last_cache_invalidated_at_unix_seconds(),
                    state.last_authorization_rechecked_at_unix_seconds(),
                    state.last_drift_checked_at_unix_seconds(),
                )
            })
            .collect::<Vec<_>>();
        canonical.sort();
        canonical
    }
}
