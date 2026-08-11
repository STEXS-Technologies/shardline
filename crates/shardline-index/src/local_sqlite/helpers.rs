#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    error::Error as StdError,
    ffi::OsStr,
    str::FromStr,
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
use serde_json::{from_slice, from_str, to_string};
use shardline_protocol::{RepositoryScope, unix_now_seconds_lossy};
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
    resolve_platform_symlinks,
};

use super::{
    DedupeShardRecord, FileReconstructionRecord, LEGACY_IMPORT_COMPLETED_KEY,
    LOCAL_SCHEMA_MIGRATIONS_TABLE, LOCAL_SQLITE_MIGRATIONS, LegacyQuarantineCandidateRecord,
    LocalIndexStoreError, LocalRecordLocator, MAX_CONTROL_PLANE_METADATA_BYTES, RecordKind,
    MAX_LOCAL_RECORD_METADATA_BYTES, MAX_RECONSTRUCTION_METADATA_BYTES, StoredObjectPresenceRecord,
};
use crate::{
    DedupeShardMapping, FileId, FileReconstruction, FileRecord, ProviderRepositoryState,
    QuarantineCandidate, RetentionHold, WebhookDelivery, WebhookDeliveryError, parse_xet_hash_hex,
    provider::parse_repository_provider, record_key::record_key as shared_record_key,
    record_key::repository_scope_key as shared_repository_scope_key, xet_hash_hex_string,
};

pub(crate) trait SqliteExecutor {
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

pub(crate) fn initialize_local_metadata_root(root: &Path) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    fs::create_dir_all(root)?;
    Ok(())
}

pub(crate) fn ensure_sqlite_database_path_is_safe(path: &Path) -> Result<(), LocalIndexStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_metadata_path_error()),
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_metadata) => Err(invalid_metadata_path_error()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalIndexStoreError::Io(error)),
    }
}

pub(crate) fn prepare_connection(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    // Install the busy handler before any PRAGMA that may need a database lock.
    // Concurrent protocol uploads open independent connections, and setting WAL
    // mode can otherwise fail immediately while another connection is writing.
    connection.busy_timeout(Duration::from_secs(5))?;
    let _enabled = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.pragma_update(None, "cell_size_check", "ON")?;
    Ok(())
}

pub(crate) const fn sqlite_open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        .union(OpenFlags::SQLITE_OPEN_CREATE)
        .union(OpenFlags::SQLITE_OPEN_NO_MUTEX)
        .union(OpenFlags::SQLITE_OPEN_URI)
        .union(OpenFlags::SQLITE_OPEN_NOFOLLOW)
        .union(OpenFlags::SQLITE_OPEN_EXRESCODE)
}

pub(crate) fn ensure_local_schema_migrations_table(
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

pub(crate) fn apply_pending_local_migrations(
    connection: &mut Connection,
) -> Result<(), LocalIndexStoreError> {
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
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (version) DO NOTHING"
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

pub(crate) fn ensure_legacy_import_state(
    connection: &mut Connection,
    root: &Path,
) -> Result<(), LocalIndexStoreError> {
    // Acquire a write lock immediately to prevent TOCTOU between the
    // existence check and the import.  Two concurrent callers that both
    // see "not yet imported" will serialize here.
    let transaction =
        connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

    let import_completed = transaction
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

    if local_metadata_has_rows(&transaction)? {
        return Err(LocalIndexStoreError::InvalidLegacyImportState);
    }

    if !legacy_layout_exists(root) {
        mark_legacy_import_completed(&transaction)?;
        transaction.commit()?;
        return Ok(());
    }

    import_legacy_file_records(&transaction, root, RecordKind::Latest)?;
    import_legacy_file_records(&transaction, root, RecordKind::Version)?;
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

fn is_valid_local_table_name(name: &str) -> bool {
    matches!(
        name,
        "shardline_file_records"
            | "shardline_file_reconstructions"
            | "shardline_stored_objects"
            | "shardline_dedupe_shards"
            | "shardline_quarantine_candidates"
            | "shardline_retention_holds"
            | "shardline_webhook_deliveries"
            | "shardline_provider_repository_states"
            | "shardline_tree_entries"
            | "shardline_revisions"
    )
}

fn local_metadata_has_rows(connection: &Connection) -> Result<bool, LocalIndexStoreError> {
    let tables = [
        "shardline_file_records",
        "shardline_file_reconstructions",
        "shardline_stored_objects",
        "shardline_dedupe_shards",
        "shardline_quarantine_candidates",
        "shardline_retention_holds",
        "shardline_webhook_deliveries",
        "shardline_provider_repository_states",
        "shardline_tree_entries",
        "shardline_revisions",
    ];
    for table in tables {
        if !is_valid_local_table_name(table) {
            return Err(LocalIndexStoreError::InvalidTableName);
        }
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
    kind: RecordKind,
) -> Result<(), LocalIndexStoreError> {
    let directory = match kind {
        RecordKind::Latest => root.join("files"),
        RecordKind::Version => root.join("file_versions"),
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
            (kind == RecordKind::Version).then(|| record.content_hash.clone()),
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
        let file_id = FileId::new(parse_xet_hash_hex(path_hash)?);
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
        let record = from_slice::<StoredObjectPresenceRecord>(&bytes)?;
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
            "INSERT INTO shardline_stored_objects (object_hash, registered_at_unix_seconds)
             VALUES (?1, ?2)
             ON CONFLICT (object_hash) DO NOTHING",
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

pub(crate) fn upsert_reconstruction_row(
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
            xet_hash_hex_string(file_id.hash()),
            json,
            u64_to_i64(updated_at_unix_seconds)?,
        ],
    )?;
    Ok(())
}

pub(crate) fn upsert_dedupe_mapping_row(
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
            xet_hash_hex_string(mapping.chunk_hash()),
            mapping.shard_object_key().as_str(),
            u64_to_i64(updated_at_unix_seconds)?,
        ],
    )?;
    Ok(())
}

pub(crate) fn upsert_file_record_row(
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

pub(crate) fn local_record_locator(
    kind: RecordKind,
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

pub(crate) fn local_record_locator_from_row(
    row: &Row<'_>,
) -> Result<LocalRecordLocator, SqliteError> {
    let kind = RecordKind::from_str(row.get_ref("record_kind")?.as_str()?)
        .map_err(|_err| LocalIndexStoreError::InvalidRecordKind)
        .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))?;
    let content_hash = match kind {
        RecordKind::Latest => None,
        RecordKind::Version => Some(row.get::<_, String>("content_hash")?),
    };
    Ok(LocalRecordLocator {
        record_key: row.get("record_key")?,
        kind,
        scope_key: row.get("scope_key")?,
        file_id: row.get("file_id")?,
        content_hash,
    })
}

pub(crate) fn quarantine_candidate_from_row(
    row: &Row<'_>,
) -> Result<QuarantineCandidate, SqliteError> {
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

pub(crate) fn retention_hold_from_row(row: &Row<'_>) -> Result<RetentionHold, SqliteError> {
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

pub(crate) fn webhook_delivery_from_row(row: &Row<'_>) -> Result<WebhookDelivery, SqliteError> {
    let provider_name = row.get::<_, String>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
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

pub(crate) fn provider_repository_state_from_row(
    row: &Row<'_>,
) -> Result<ProviderRepositoryState, SqliteError> {
    let provider_name = row.get::<_, String>("provider")?;
    let provider = parse_repository_provider(&provider_name, |_| {
        SqliteError::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(LocalIndexStoreError::InvalidRepoType(provider_name.clone())),
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

pub(crate) fn dedupe_shard_mapping_from_row(
    row: &Row<'_>,
) -> Result<DedupeShardMapping, SqliteError> {
    let chunk_hash =
        parse_xet_hash_hex(&row.get::<_, String>("chunk_hash")?).map_err(from_sql_error)?;
    let object_key =
        ObjectKey::parse(&row.get::<_, String>("shard_object_key")?).map_err(from_sql_error)?;
    Ok(DedupeShardMapping::new(chunk_hash, object_key))
}

pub(crate) fn parse_reconstruction_json(
    value: &str,
) -> Result<FileReconstruction, LocalIndexStoreError> {
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
    #[derive(serde::Deserialize)]
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
    #[derive(serde::Deserialize)]
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
    #[derive(serde::Deserialize)]
    struct WebhookDeliveryRecord {
        provider: String,
        owner: String,
        repo: String,
        delivery_id: String,
        processed_at_unix_seconds: u64,
    }
    let record = from_slice::<WebhookDeliveryRecord>(bytes)?;
    let provider = parse_repository_provider(&record.provider, |_| {
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
    #[derive(serde::Deserialize)]
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
    let provider = parse_repository_provider(&record.provider, |_| {
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

pub(crate) fn read_sqlite_record_bytes(value: ValueRef<'_>) -> Result<Vec<u8>, SqliteError> {
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
        | other @ LocalIndexStoreError::IntegerOutOfRange(_)
        | other @ LocalIndexStoreError::InvalidRecordKind
        | other @ LocalIndexStoreError::InvalidLegacyImportState
        | other @ LocalIndexStoreError::InvalidRepoType(_)
        | other @ LocalIndexStoreError::BlockingTask(_)
        | other @ LocalIndexStoreError::InvalidTableName => {
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

pub(crate) fn legacy_record_path(
    root: &Path,
    kind: RecordKind,
    record: &FileRecord,
) -> PathBuf {
    let base = match kind {
        RecordKind::Latest => root.join("files"),
        RecordKind::Version => root.join("file_versions"),
    };
    match (&record.repository_scope, kind) {
        (Some(scope), RecordKind::Latest) => scoped_root(&base, scope).join(&record.file_id),
        (Some(scope), RecordKind::Version) => scoped_root(&base, scope)
            .join(&record.file_id)
            .join(&record.content_hash),
        (None, RecordKind::Latest) => base.join(&record.file_id),
        (None, RecordKind::Version) => base.join(&record.file_id).join(&record.content_hash),
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

fn from_sql_error(error: impl StdError + Send + Sync + 'static) -> SqliteError {
    SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
}

// ── Small helpers moved from mod.rs ───────────────────────────────────

pub(crate) fn normalize_local_root(root: PathBuf) -> PathBuf {
    let mut root = root;
    if root.file_name() == Some(OsStr::new("gc")) {
        root = root
            .parent()
            .map_or_else(|| root.clone(), Path::to_path_buf);
    }
    resolve_platform_symlinks(&root)
}

pub(crate) fn u64_to_i64(value: u64) -> Result<i64, LocalIndexStoreError> {
    i64::try_from(value).map_err(|err| LocalIndexStoreError::IntegerOutOfRange(err.to_string()))
}

pub(crate) fn i64_to_u64(value: i64) -> Result<u64, LocalIndexStoreError> {
    u64::try_from(value).map_err(|err| LocalIndexStoreError::IntegerOutOfRange(err.to_string()))
}

pub(crate) fn collect_rows<T>(
    rows: MappedRows<'_, impl FnMut(&Row<'_>) -> Result<T, SqliteError>>,
) -> Result<Vec<T>, LocalIndexStoreError> {
    let mut collected = Vec::new();
    for row in rows {
        collected.push(row?);
    }
    Ok(collected)
}

pub(crate) fn record_not_found_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::from(ErrorKind::NotFound))
}

pub(crate) fn invalid_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local metadata path must be a regular file and must not be a symlink",
    ))
}

pub(crate) fn invalid_record_metadata_path_error() -> LocalIndexStoreError {
    LocalIndexStoreError::Io(IoError::new(
        ErrorKind::InvalidData,
        "local record metadata path must be a regular file and must not be a symlink",
    ))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        clippy::unwrap_in_result,
        clippy::arithmetic_side_effects,
        clippy::option_if_let_else,
        clippy::unreachable,
        clippy::shadow_unrelated,
        clippy::let_underscore_must_use
    )]
    use super::*;
    use rusqlite::Connection;

    use shardline_protocol::{RepositoryProvider, RepositoryScope};

    // ── ensure_metadata_size_within_limit ─────────────────────────────────

    #[test]
    fn metadata_size_within_limit_ok() {
        ensure_metadata_size_within_limit(100, 200).unwrap();
    }

    #[test]
    fn metadata_size_at_limit_ok() {
        ensure_metadata_size_within_limit(200, 200).unwrap();
    }

    #[test]
    fn metadata_size_over_limit_errors() {
        let err = ensure_metadata_size_within_limit(300, 200).unwrap_err();
        assert!(matches!(
            err,
            LocalIndexStoreError::MetadataTooLarge {
                observed_bytes: 300,
                maximum_bytes: 200,
            }
        ));
    }

    // ── u64_to_i64 / i64_to_u64 ──────────────────────────────────────────

    #[test]
    fn u64_to_i64_normal_value() {
        assert_eq!(u64_to_i64(42).unwrap(), 42i64);
    }

    #[test]
    fn u64_to_i64_max_i64_value() {
        assert_eq!(u64_to_i64(i64::MAX as u64).unwrap(), i64::MAX);
    }

    #[test]
    fn u64_to_i64_overflow_errors() {
        let too_big = (i64::MAX as u64).saturating_add(1);
        assert!(matches!(
            u64_to_i64(too_big),
            Err(LocalIndexStoreError::IntegerOutOfRange(_))
        ));
    }

    #[test]
    fn i64_to_u64_normal_value() {
        assert_eq!(i64_to_u64(42).unwrap(), 42u64);
    }

    #[test]
    fn i64_to_u64_zero() {
        assert_eq!(i64_to_u64(0).unwrap(), 0u64);
    }

    #[test]
    fn i64_to_u64_negative_errors() {
        assert!(matches!(
            i64_to_u64(-1),
            Err(LocalIndexStoreError::IntegerOutOfRange(_))
        ));
    }

    // ── record_not_found_error / invalid_metadata_path_error / invalid_record_metadata_path_error ──

    #[test]
    fn record_not_found_error_kind() {
        let err = record_not_found_error();
        assert!(
            matches!(err, LocalIndexStoreError::Io(ref e) if e.kind() == std::io::ErrorKind::NotFound)
        );
    }

    #[test]
    fn invalid_metadata_path_error_kind() {
        let err = invalid_metadata_path_error();
        assert!(
            matches!(err, LocalIndexStoreError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn invalid_record_metadata_path_error_kind() {
        let err = invalid_record_metadata_path_error();
        assert!(
            matches!(err, LocalIndexStoreError::Io(ref e) if e.kind() == std::io::ErrorKind::InvalidData)
        );
    }

    // ── is_valid_local_table_name ─────────────────────────────────────────

    #[test]
    fn is_valid_local_table_name_accepts_known_tables() {
        assert!(is_valid_local_table_name("shardline_file_records"));
        assert!(is_valid_local_table_name("shardline_file_reconstructions"));
        assert!(is_valid_local_table_name("shardline_stored_objects"));
        assert!(is_valid_local_table_name("shardline_dedupe_shards"));
        assert!(is_valid_local_table_name("shardline_quarantine_candidates"));
        assert!(is_valid_local_table_name("shardline_retention_holds"));
        assert!(is_valid_local_table_name("shardline_webhook_deliveries"));
        assert!(is_valid_local_table_name(
            "shardline_provider_repository_states"
        ));
        assert!(is_valid_local_table_name("shardline_tree_entries"));
        assert!(is_valid_local_table_name("shardline_revisions"));
    }

    #[test]
    fn is_valid_local_table_name_rejects_unknown() {
        assert!(!is_valid_local_table_name("shardline_unknown_table"));
        assert!(!is_valid_local_table_name(""));
        assert!(!is_valid_local_table_name("sqlite_master"));
    }

    // ── ensure_sqlite_database_path_is_safe ─────────────────────────────────

    #[test]
    fn ensure_sqlite_database_path_is_safe_when_not_exists() {
        let storage = shardline_test_support::TempStorage::new();
        let path = storage.path().join("nonexistent.sqlite3");
        ensure_sqlite_database_path_is_safe(&path).unwrap();
    }

    #[test]
    fn ensure_sqlite_database_path_is_safe_when_regular_file() {
        let storage = shardline_test_support::TempStorage::new();
        let path = storage.path().join("test.sqlite3");
        std::fs::write(&path, b"content").unwrap();
        ensure_sqlite_database_path_is_safe(&path).unwrap();
    }

    #[test]
    fn ensure_sqlite_database_path_is_safe_rejects_directory() {
        let storage = shardline_test_support::TempStorage::new();
        let path = storage.path().join("adir");
        std::fs::create_dir(&path).unwrap();
        assert!(matches!(
            ensure_sqlite_database_path_is_safe(&path),
            Err(LocalIndexStoreError::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidData
        ));
    }

    #[test]
    fn ensure_sqlite_database_path_is_safe_rejects_symlink() {
        let storage = shardline_test_support::TempStorage::new();
        let target = storage.path().join("real.sqlite3");
        let link = storage.path().join("link.sqlite3");
        std::fs::write(&target, b"data").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(matches!(
            ensure_sqlite_database_path_is_safe(&link),
            Err(LocalIndexStoreError::Io(ref e)) if e.kind() == std::io::ErrorKind::InvalidData
        ));
    }

    // ── sqlite_open_flags ──────────────────────────────────────────────────

    #[test]
    fn sqlite_open_flags_includes_no_follow() {
        let flags = sqlite_open_flags();
        assert!(
            flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_NOFOLLOW),
            "expected SQLITE_OPEN_NOFOLLOW to be set"
        );
        assert!(
            flags.contains(rusqlite::OpenFlags::SQLITE_OPEN_CREATE),
            "expected SQLITE_OPEN_CREATE"
        );
    }

    // ── collect_rows ───────────────────────────────────────────────────────

    #[test]
    fn collect_rows_empty_iterator() {
        let storage = shardline_test_support::TempStorage::new();
        initialize_local_metadata_root(storage.path()).unwrap();
        let db_path = storage.path().join("test.sqlite3");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (v TEXT);
             INSERT INTO t VALUES ('a'), ('b');",
        )
        .unwrap();
        let mut stmt = conn
            .prepare("SELECT v FROM t WHERE v = 'nonexistent'")
            .unwrap();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
        let collected: Vec<String> = collect_rows(rows).unwrap();
        assert!(collected.is_empty());
    }

    #[test]
    fn collect_rows_non_empty_iterator() {
        let storage = shardline_test_support::TempStorage::new();
        initialize_local_metadata_root(storage.path()).unwrap();
        let db_path = storage.path().join("test.sqlite3");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE t (v TEXT);
             INSERT INTO t VALUES ('a'), ('b');",
        )
        .unwrap();
        let mut stmt = conn.prepare("SELECT v FROM t ORDER BY v").unwrap();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
        let collected: Vec<String> = collect_rows(rows).unwrap();
        assert_eq!(collected, vec!["a", "b"]);
    }

    // ── legacy_layout_exists ───────────────────────────────────────────────

    #[test]
    fn legacy_layout_exists_returns_false_for_empty_dir() {
        let storage = shardline_test_support::TempStorage::new();
        assert!(!legacy_layout_exists(storage.path()));
    }

    #[test]
    fn legacy_layout_exists_when_files_dir_present() {
        let storage = shardline_test_support::TempStorage::new();
        std::fs::create_dir(storage.path().join("files")).unwrap();
        assert!(legacy_layout_exists(storage.path()));
    }

    #[test]
    fn legacy_layout_exists_when_file_versions_dir_present() {
        let storage = shardline_test_support::TempStorage::new();
        std::fs::create_dir(storage.path().join("file_versions")).unwrap();
        assert!(legacy_layout_exists(storage.path()));
    }

    #[test]
    fn legacy_layout_exists_when_gc_dir_present() {
        let storage = shardline_test_support::TempStorage::new();
        std::fs::create_dir(storage.path().join("gc")).unwrap();
        assert!(legacy_layout_exists(storage.path()));
    }

    // ── legacy_record_path ─────────────────────────────────────────────────

    fn sample_scope() -> RepositoryScope {
        RepositoryScope::new(RepositoryProvider::GitHub, "team", "assets", Some("main")).unwrap()
    }

    fn sample_record(scope: Option<RepositoryScope>) -> crate::FileRecord {
        crate::FileRecord {
            file_id: "test.bin".into(),
            content_hash: "c".repeat(64),
            total_bytes: 4,
            chunk_size: 4,
            storage_repr: crate::StorageRepresentation::FixedChunkV1,
            repository_scope: scope,
            chunks: vec![],
        }
    }

    #[test]
    fn legacy_record_path_latest_no_scope() {
        let storage = shardline_test_support::TempStorage::new();
        let record = sample_record(None);
        let path = legacy_record_path(storage.path(), RecordKind::Latest, &record);
        assert!(path.ends_with("test.bin"));
        assert!(path.starts_with(storage.path().join("files")));
    }

    #[test]
    fn legacy_record_path_latest_with_scope() {
        let storage = shardline_test_support::TempStorage::new();
        let scope = sample_scope();
        let record = sample_record(Some(scope));
        let path = legacy_record_path(storage.path(), RecordKind::Latest, &record);
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("github"));
        assert!(path_str.contains("test.bin"));
    }

    #[test]
    fn legacy_record_path_version_no_scope() {
        let storage = shardline_test_support::TempStorage::new();
        let record = sample_record(None);
        let path = legacy_record_path(storage.path(), RecordKind::Version, &record);
        assert!(path.starts_with(storage.path().join("file_versions")));
        assert!(path.ends_with(&record.content_hash));
        assert!(path.to_string_lossy().contains("test.bin"));
    }

    #[test]
    fn legacy_record_path_version_with_scope() {
        let storage = shardline_test_support::TempStorage::new();
        let scope = sample_scope();
        let record = sample_record(Some(scope));
        let path = legacy_record_path(storage.path(), RecordKind::Version, &record);
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("file_versions"));
        assert!(path_str.contains("github"));
        assert!(path_str.contains("test.bin"));
        assert!(path_str.contains(&record.content_hash));
    }

    // ── legacy_quarantine_object_key ───────────────────────────────────────

    #[test]
    fn legacy_quarantine_object_key_produces_two_char_prefix() {
        let hash = "aabbccdd";
        let ok = legacy_quarantine_object_key(hash).unwrap();
        assert!(
            ok.as_str().starts_with("aa/"),
            "prefix should be first two chars, got: {}",
            ok.as_str()
        );
        assert!(ok.as_str().contains(hash), "hash should be in key");
    }

    #[test]
    fn legacy_quarantine_object_key_short_hash_uses_first_two_chars() {
        let hash = "abcdef1234567890";
        let ok = legacy_quarantine_object_key(hash).unwrap();
        assert!(ok.as_str().starts_with("ab/"));
    }

    // ── file_modified_since_epoch ──────────────────────────────────────────

    #[test]
    fn file_modified_since_epoch_returns_zero_for_non_existent() {
        let storage = shardline_test_support::TempStorage::new();
        let path = storage.path().join("no-such-file");
        let result = file_modified_since_epoch(&path);
        assert!(result.is_err());
    }

    #[test]
    fn file_modified_since_epoch_returns_value_for_existing_file() {
        let storage = shardline_test_support::TempStorage::new();
        let path = storage.path().join("existing.txt");
        std::fs::write(&path, b"data").unwrap();
        let result = file_modified_since_epoch(&path).unwrap();
        assert!(result > 0, "modified timestamp should be positive");
    }

    // ── normalize_local_root ───────────────────────────────────────────────

    #[test]
    fn normalize_local_root_removes_gc_suffix() {
        let storage = shardline_test_support::TempStorage::new();
        let gc_path = storage.path().join("gc");
        std::fs::create_dir_all(&gc_path).unwrap();
        let normalized = normalize_local_root(gc_path);
        assert_eq!(normalized, storage.path().canonicalize().unwrap());
    }

    #[test]
    fn normalize_local_root_keeps_non_gc_path() {
        let storage = shardline_test_support::TempStorage::new();
        let normalized = normalize_local_root(storage.path().to_path_buf());
        assert_eq!(normalized, storage.path().canonicalize().unwrap());
    }

    // ── local_record_locator ───────────────────────────────────────────────

    #[test]
    fn local_record_locator_latest_has_no_content_hash() {
        let record = sample_record(None);
        let locator = local_record_locator(RecordKind::Latest, &record, None);
        assert_eq!(locator.kind, RecordKind::Latest);
        assert_eq!(locator.file_id, "test.bin");
        assert!(locator.content_hash.is_none());
    }

    #[test]
    fn local_record_locator_version_has_content_hash() {
        let record = sample_record(None);
        let ch = record.content_hash.clone();
        let locator = local_record_locator(RecordKind::Version, &record, Some(ch.clone()));
        assert_eq!(locator.kind, RecordKind::Version);
        assert_eq!(locator.content_hash, Some(ch));
    }

    // ── from_sql_error ─────────────────────────────────────────────────────

    #[test]
    fn from_sql_error_wraps_into_sqlite_error() {
        let io_err = std::io::Error::other("test error");
        let sql_err = from_sql_error(io_err);
        assert!(matches!(
            sql_err,
            rusqlite::Error::FromSqlConversionFailure(0, _, _)
        ));
    }

    // ── initialize_local_metadata_root ─────────────────────────────────────

    #[test]
    fn initialize_local_metadata_root_creates_directory() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().join("nested").join("metadata");
        assert!(!root.exists());
        initialize_local_metadata_root(&root).expect("should create directory");
        assert!(root.is_dir());
    }

    #[test]
    fn initialize_local_metadata_root_creates_nested_directories() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage
            .path()
            .join("a")
            .join("b")
            .join("c")
            .join("metadata");
        assert!(!root.exists());
        initialize_local_metadata_root(&root).expect("should create nested directories");
        assert!(root.is_dir());
    }

    #[test]
    fn initialize_local_metadata_root_is_idempotent() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path().join("metadata");
        initialize_local_metadata_root(&root).expect("first call should succeed");
        initialize_local_metadata_root(&root).expect("second call should succeed");
        assert!(root.is_dir());
    }

    #[test]
    fn ensure_local_schema_migrations_table_creates_table() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path();
        initialize_local_metadata_root(root).unwrap();
        let db_path = root.join("metadata.sqlite3");
        let connection = Connection::open(&db_path).unwrap();

        ensure_local_schema_migrations_table(&connection).expect("should create migrations table");

        let exists: bool = connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='{LOCAL_SCHEMA_MIGRATIONS_TABLE}')"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists, "migrations table should exist");
    }

    #[test]
    fn ensure_local_schema_migrations_table_is_idempotent() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path();
        initialize_local_metadata_root(root).unwrap();
        let db_path = root.join("metadata.sqlite3");
        let connection = Connection::open(&db_path).unwrap();

        ensure_local_schema_migrations_table(&connection).unwrap();
        ensure_local_schema_migrations_table(&connection).unwrap();

        let exists: bool = connection
            .query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='{LOCAL_SCHEMA_MIGRATIONS_TABLE}')"
                ),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            exists,
            "migrations table should still exist after idempotent calls"
        );
    }

    #[test]
    fn apply_pending_local_migrations_is_idempotent() {
        let storage = shardline_test_support::TempStorage::new();
        let root = storage.path();
        initialize_local_metadata_root(root).unwrap();
        let db_path = root.join("metadata.sqlite3");
        let mut connection = Connection::open(&db_path).unwrap();
        prepare_connection(&mut connection).unwrap();
        ensure_local_schema_migrations_table(&connection).unwrap();

        apply_pending_local_migrations(&mut connection).expect("first migration should succeed");
        apply_pending_local_migrations(&mut connection)
            .expect("second migration should succeed (idempotent)");

        let count: i64 = connection
            .query_row(
                &format!("SELECT COUNT(*) FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE}"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(count > 0, "should have applied at least one migration");
    }
}
