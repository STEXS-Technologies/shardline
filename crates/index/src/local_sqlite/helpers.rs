#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    error::Error as StdError,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    path::{Path, PathBuf},
    time::{Duration, UNIX_EPOCH},
};

use rusqlite::{
    Connection, Error as SqliteError, OpenFlags, OptionalExtension, Row, Transaction,
    config::DbConfig,
    params,
    types::{Type, ValueRef},
};
use serde_json::{from_slice, from_str, to_string};
use shardline_protocol::{RepositoryScope, unix_now_seconds_lossy};
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
};

use super::{
    DedupeShardRecord, FileReconstructionRecord, LEGACY_IMPORT_COMPLETED_KEY,
    LegacyQuarantineCandidateRecord, LOCAL_SCHEMA_MIGRATIONS_TABLE, LOCAL_SQLITE_MIGRATIONS,
    LocalIndexStoreError, LocalRecordKind, LocalRecordLocator, MAX_CONTROL_PLANE_METADATA_BYTES,
    MAX_LOCAL_RECORD_METADATA_BYTES, MAX_RECONSTRUCTION_METADATA_BYTES, SqliteExecutor,
    StoredObjectPresenceRecord, i64_to_u64, invalid_metadata_path_error,
    invalid_record_metadata_path_error, u64_to_i64,
};
use crate::{
    DedupeShardMapping, FileId, FileReconstruction, FileRecord, ProviderRepositoryState,
    QuarantineCandidate, RetentionHold, WebhookDelivery,
    WebhookDeliveryError,
    parse_xet_hash_hex,
    provider::parse_repository_provider,
    record_key::record_key as shared_record_key,
    record_key::{
        repository_scope_key as shared_repository_scope_key,
    },
    xet_hash_hex_string,
};

pub(super) fn initialize_local_metadata_root(root: &Path) -> Result<(), LocalIndexStoreError> {
    ensure_directory_path_components_are_not_symlinked(root)?;
    fs::create_dir_all(root)?;
    Ok(())
}

pub(super) fn ensure_sqlite_database_path_is_safe(
    path: &Path,
) -> Result<(), LocalIndexStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(invalid_metadata_path_error()),
        Ok(metadata) if metadata.is_file() => Ok(()),
        Ok(_metadata) => Err(invalid_metadata_path_error()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LocalIndexStoreError::Io(error)),
    }
}

pub(super) fn prepare_connection(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    let _enabled = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.pragma_update(None, "trusted_schema", "OFF")?;
    connection.pragma_update(None, "cell_size_check", "ON")?;
    connection.busy_timeout(Duration::from_secs(5))?;
    Ok(())
}

pub(super) const fn sqlite_open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE
        .union(OpenFlags::SQLITE_OPEN_CREATE)
        .union(OpenFlags::SQLITE_OPEN_NO_MUTEX)
        .union(OpenFlags::SQLITE_OPEN_URI)
        .union(OpenFlags::SQLITE_OPEN_NOFOLLOW)
        .union(OpenFlags::SQLITE_OPEN_EXRESCODE)
}

pub(super) fn ensure_local_schema_migrations_table(
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

pub(super) fn apply_pending_local_migrations(
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

pub(super) fn ensure_legacy_import_state(
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
    ];
    for table in tables {
        assert!(
            is_valid_local_table_name(table),
            "table name must be a valid identifier: {table}"
        );
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

pub(super) fn upsert_reconstruction_row(
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

pub(super) fn upsert_dedupe_mapping_row(
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

pub(super) fn upsert_file_record_row(
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

pub(super) fn local_record_locator(
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

pub(super) fn local_record_locator_from_row(
    row: &Row<'_>,
) -> Result<LocalRecordLocator, SqliteError> {
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

pub(super) fn quarantine_candidate_from_row(
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

pub(super) fn retention_hold_from_row(row: &Row<'_>) -> Result<RetentionHold, SqliteError> {
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

pub(super) fn webhook_delivery_from_row(row: &Row<'_>) -> Result<WebhookDelivery, SqliteError> {
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

pub(super) fn provider_repository_state_from_row(
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

pub(super) fn dedupe_shard_mapping_from_row(
    row: &Row<'_>,
) -> Result<DedupeShardMapping, SqliteError> {
    let chunk_hash =
        parse_xet_hash_hex(&row.get::<_, String>("chunk_hash")?).map_err(from_sql_error)?;
    let object_key =
        ObjectKey::parse(&row.get::<_, String>("shard_object_key")?).map_err(from_sql_error)?;
    Ok(DedupeShardMapping::new(chunk_hash, object_key))
}

pub(super) fn parse_reconstruction_json(
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

pub(super) fn read_sqlite_record_bytes(value: ValueRef<'_>) -> Result<Vec<u8>, SqliteError> {
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
        | other @ LocalIndexStoreError::InvalidLegacyImportState
        | other @ LocalIndexStoreError::InvalidRepoType(_) => {
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

pub(super) fn legacy_record_path(
    root: &Path,
    kind: LocalRecordKind,
    record: &FileRecord,
) -> PathBuf {
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

fn from_sql_error(error: impl StdError + Send + Sync + 'static) -> SqliteError {
    SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
}
