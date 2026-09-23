#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    error::Error as StdError,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Read},
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
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
use shardline_reliability::{
    EvidenceEventMetadata, HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, LifecycleEvent,
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleState, OciObjectSnapshot,
    OciTagEvidenceLog, OciTagLifecycleEvent, OciTagSnapshot, OperationKind, ProviderEvidenceLog,
    ProviderLifecycleEvent, ProviderLifecycleSnapshot, QuarantineEvidenceLog,
    QuarantineLifecycleEvent, QuarantineLifecycleState, QuarantineObjectIdentity,
    QuarantineSnapshot, ResumableLifecycleState, RetentionEvidenceLog, RetentionHoldLifecycleEvent,
    RetentionHoldLifecycleState, RetentionHoldSnapshot, RetentionObjectIdentity,
    S3ObjectEvidenceLog, S3ObjectLifecycleEvent, S3ObjectSnapshot, S3ObjectState, SnapshotEvidence,
    StateTransitionEvent, UploadLifecycleState, WebhookDeliveryEvidenceLog,
    WebhookDeliveryIdentity, WebhookDeliveryLifecycleEvent, WebhookDeliveryLifecycleState,
    WebhookDeliverySnapshot, baseline_resumable_session_events, baseline_upload_lifecycle_events,
    verify_hub_ref_events, verify_oci_tag_events, verify_provider_lifecycle_events,
    verify_resumable_session_events, verify_s3_object_events, verify_upload_lifecycle_events,
};
use shardline_storage::{
    DirectoryPathError, ObjectKey, ObjectKeyError,
    ensure_directory_path_components_are_not_symlinked as ensure_directory_path_components_are_not_symlinked_shared,
    resolve_platform_symlinks,
};

use super::{
    DedupeShardRecord, FileReconstructionRecord, LEGACY_IMPORT_COMPLETED_KEY,
    LOCAL_SCHEMA_MIGRATIONS_TABLE, LOCAL_SQLITE_MIGRATIONS, LegacyQuarantineCandidateRecord,
    LocalIndexStoreError, LocalRecordLocator, MAX_CONTROL_PLANE_METADATA_BYTES,
    MAX_LOCAL_RECORD_METADATA_BYTES, MAX_RECONSTRUCTION_METADATA_BYTES, RecordKind,
    StoredObjectPresenceRecord,
};
use crate::{
    DedupeShardMapping, FileId, FileReconstruction, FileRecord, OciObjectKind,
    ProviderRepositoryState, QuarantineCandidate, RetentionHold, WebhookDelivery,
    WebhookDeliveryError, parse_xet_hash_hex, provider::parse_repository_provider,
    provider_evidence::snapshot_from_state, record_key::record_key as shared_record_key,
    record_key::repository_scope_key as shared_repository_scope_key, xet_hash_hex_string,
};

pub(crate) fn quarantine_evidence_operation_id(object_key: &str) -> String {
    object_key.to_owned()
}

/// Persists one authenticated evidence event using its typed operation key.
///
/// All local durable state machines share this writer so a caller cannot bind
/// an event under a separately supplied operation kind or operation id.
pub(crate) fn persist_reliability_event<T: EvidenceEventMetadata>(
    transaction: &Transaction<'_>,
    event: &T,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event_at(transaction, event, u64_to_i64(unix_now_seconds_lossy())?)
}

/// Persists one event with an explicitly selected timestamp source.
///
/// The timestamp is kept separate from the typed event identity so adapters
/// can preserve their existing clock contract while sharing the journal
/// writer and its key derivation.
pub(crate) fn persist_reliability_event_at<T: EvidenceEventMetadata>(
    transaction: &Transaction<'_>,
    event: &T,
    created_at_unix_seconds: i64,
) -> Result<(), LocalIndexStoreError> {
    transaction.execute(
        "INSERT OR IGNORE INTO shardline_reliability_events
            (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            event.operation_identity().kind.as_str(),
            event.operation_identity().operation_id,
            u64_to_i64(event.sequence_number())?,
            to_string(event)?,
            created_at_unix_seconds,
        ],
    )?;
    Ok(())
}

pub(crate) fn quarantine_snapshot(
    candidate: &QuarantineCandidate,
    state: QuarantineLifecycleState,
) -> Result<QuarantineSnapshot, LocalIndexStoreError> {
    Ok(QuarantineSnapshot::new(
        QuarantineObjectIdentity::new(candidate.object_key().as_str())?,
        candidate.observed_length(),
        candidate.first_seen_unreachable_at_unix_seconds(),
        candidate.delete_after_unix_seconds(),
        state,
    )?)
}

pub(crate) fn load_quarantine_evidence(
    transaction: &Transaction<'_>,
    object_key: &str,
) -> Result<QuarantineEvidenceLog, LocalIndexStoreError> {
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'GarbageCollection' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(
        params![quarantine_evidence_operation_id(object_key)],
        |row| {
            let event_json: String = row.get(0)?;
            from_str::<QuarantineLifecycleEvent>(&event_json).map_err(|error| {
                SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        },
    )?;
    Ok(QuarantineEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn persist_quarantine_evidence(
    transaction: &Transaction<'_>,
    event: &QuarantineLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) fn retention_evidence_operation_id(object_key: &str) -> String {
    object_key.to_owned()
}

pub(crate) fn retention_snapshot(
    hold: &RetentionHold,
    state: RetentionHoldLifecycleState,
) -> Result<RetentionHoldSnapshot, LocalIndexStoreError> {
    Ok(RetentionHoldSnapshot::new(
        RetentionObjectIdentity::new(hold.object_key().as_str())?,
        hold.reason(),
        hold.held_at_unix_seconds(),
        hold.release_after_unix_seconds(),
        state,
    )?)
}

pub(crate) fn load_retention_evidence(
    transaction: &Transaction<'_>,
    object_key: &str,
) -> Result<RetentionEvidenceLog, LocalIndexStoreError> {
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'RetentionHold' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(
        params![retention_evidence_operation_id(object_key)],
        |row| {
            let event_json: String = row.get(0)?;
            from_str::<RetentionHoldLifecycleEvent>(&event_json).map_err(|error| {
                SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        },
    )?;
    Ok(RetentionEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn persist_retention_evidence(
    transaction: &Transaction<'_>,
    event: &RetentionHoldLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) fn webhook_snapshot(
    delivery: &WebhookDelivery,
    state: WebhookDeliveryLifecycleState,
) -> Result<WebhookDeliverySnapshot, LocalIndexStoreError> {
    Ok(WebhookDeliverySnapshot::new(
        WebhookDeliveryIdentity::new(
            delivery.provider().as_str(),
            delivery.owner(),
            delivery.repo(),
            delivery.delivery_id(),
        )?,
        delivery.processed_at_unix_seconds(),
        state,
    ))
}

pub(crate) fn load_webhook_evidence(
    transaction: &Transaction<'_>,
    delivery: &WebhookDelivery,
) -> Result<WebhookDeliveryEvidenceLog, LocalIndexStoreError> {
    let operation = webhook_snapshot(delivery, WebhookDeliveryLifecycleState::Processed)?
        .evidence_operation()
        .map_err(LocalIndexStoreError::from)?;
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'WebhookDelivery' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![operation.operation_id], |row| {
        let event_json: String = row.get(0)?;
        from_str::<WebhookDeliveryLifecycleEvent>(&event_json)
            .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
    })?;
    Ok(WebhookDeliveryEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn persist_webhook_evidence(
    transaction: &Transaction<'_>,
    event: &WebhookDeliveryLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) fn hub_ref_snapshot(
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<HubRefSnapshot, LocalIndexStoreError> {
    Ok(HubRefSnapshot::new(repository, ref_name, head_sha)?)
}

pub(crate) fn load_hub_ref_evidence(
    transaction: &Transaction<'_>,
    repository: &str,
    ref_name: &str,
) -> Result<HubRefEvidenceLog, LocalIndexStoreError> {
    let operation = hub_ref_snapshot(repository, ref_name, None)?.evidence_operation()?;
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'MetadataCommit' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![operation.operation_id], |row| {
        let event_json: String = row.get(0)?;
        from_str::<HubRefLifecycleEvent>(&event_json)
            .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
    })?;
    Ok(HubRefEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn persist_hub_ref_evidence(
    transaction: &Transaction<'_>,
    event: &HubRefLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) fn current_hub_ref_evidence(
    transaction: &Transaction<'_>,
    repository: &str,
    ref_name: &str,
    head_sha: Option<String>,
) -> Result<HubRefEvidenceLog, LocalIndexStoreError> {
    let snapshot = hub_ref_snapshot(repository, ref_name, head_sha)?;
    let evidence = load_hub_ref_evidence(transaction, repository, ref_name)?;
    if evidence.events().is_empty() {
        return Ok(HubRefEvidenceLog::baseline(snapshot)?);
    }
    verify_hub_ref_events(evidence.events(), &snapshot)?;
    Ok(evidence)
}

pub(crate) fn oci_tag_snapshot(
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    digest_hex: Option<String>,
) -> Result<OciTagSnapshot, LocalIndexStoreError> {
    Ok(OciTagSnapshot::new(
        scope_namespace,
        repository,
        tag,
        digest_hex,
    )?)
}

pub(crate) fn load_oci_tag_evidence(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
) -> Result<OciTagEvidenceLog, LocalIndexStoreError> {
    let operation =
        oci_tag_snapshot(scope_namespace, repository, tag, None)?.evidence_operation()?;
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = 'OciTag' AND operation_id = ?1 ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![operation.operation_id], |row| {
        let event_json: String = row.get(0)?;
        from_str::<OciTagLifecycleEvent>(&event_json)
            .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
    })?;
    Ok(OciTagEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn current_oci_tag_evidence(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    repository: &str,
    tag: &str,
    digest_hex: Option<String>,
) -> Result<OciTagEvidenceLog, LocalIndexStoreError> {
    let snapshot = oci_tag_snapshot(scope_namespace, repository, tag, digest_hex)?;
    let evidence = load_oci_tag_evidence(transaction, scope_namespace, repository, tag)?;
    if evidence.events().is_empty() {
        return Ok(OciTagEvidenceLog::baseline(snapshot)?);
    }
    verify_oci_tag_events(evidence.events(), &snapshot)?;
    Ok(evidence)
}

pub(crate) fn persist_oci_tag_evidence(
    transaction: &Transaction<'_>,
    event: &OciTagLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) fn s3_object_snapshot(
    scope_namespace: &str,
    object_key: &str,
    entry: Option<&crate::S3ObjectEntry>,
) -> Result<S3ObjectSnapshot, LocalIndexStoreError> {
    let state = entry.map(|entry| S3ObjectState {
        file_id: entry.file_id.clone(),
        size_bytes: entry.size_bytes,
        content_hash: entry.content_hash.clone(),
        etag: entry.etag.clone(),
        user_metadata: entry.user_metadata.clone(),
        updated_at_unix_seconds: entry.updated_at_unix_seconds,
    });
    Ok(S3ObjectSnapshot::new(scope_namespace, object_key, state)?)
}

pub(crate) fn load_s3_object_evidence(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    object_key: &str,
) -> Result<S3ObjectEvidenceLog, LocalIndexStoreError> {
    let operation = s3_object_snapshot(scope_namespace, object_key, None)?.evidence_operation()?;
    let mut statement = transaction.prepare(
        "SELECT event_json FROM shardline_reliability_events
         WHERE operation_kind = ?1 AND operation_id = ?2 ORDER BY sequence",
    )?;
    let rows = statement.query_map(
        params![OperationKind::S3Object.as_str(), operation.operation_id],
        |row| {
            let event_json: String = row.get(0)?;
            from_str::<S3ObjectLifecycleEvent>(&event_json).map_err(|error| {
                SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        },
    )?;
    Ok(S3ObjectEvidenceLog::from_events(
        rows.collect::<Result<Vec<_>, _>>()?,
    )?)
}

pub(crate) fn current_s3_object_evidence(
    transaction: &Transaction<'_>,
    scope_namespace: &str,
    object_key: &str,
    entry: Option<&crate::S3ObjectEntry>,
) -> Result<S3ObjectEvidenceLog, LocalIndexStoreError> {
    let snapshot = s3_object_snapshot(scope_namespace, object_key, entry)?;
    let evidence = load_s3_object_evidence(transaction, scope_namespace, object_key)?;
    if evidence.events().is_empty() {
        return Ok(S3ObjectEvidenceLog::baseline(snapshot)?);
    }
    verify_s3_object_events(evidence.events(), &snapshot)?;
    Ok(evidence)
}

pub(crate) fn persist_s3_object_evidence(
    transaction: &Transaction<'_>,
    event: &S3ObjectLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
}

pub(crate) trait SqliteExecutor {
    fn execute_sql<P>(&self, sql: &str, params: P) -> SqliteResult<usize>
    where
        P: Params;
}

pub(crate) fn provider_evidence_operation_id(snapshot: &ProviderLifecycleSnapshot) -> String {
    format!("{}:{}:{}", snapshot.provider, snapshot.owner, snapshot.repo)
}

pub(crate) fn load_provider_evidence(
    transaction: &Transaction<'_>,
    snapshot: &ProviderLifecycleSnapshot,
) -> Result<ProviderEvidenceLog, LocalIndexStoreError> {
    let operation_id = provider_evidence_operation_id(snapshot);
    let mut statement = transaction.prepare(
        "SELECT event_json
         FROM shardline_reliability_events
         WHERE operation_kind = 'ProviderEvent' AND operation_id = ?1
         ORDER BY sequence",
    )?;
    let rows = statement.query_map(params![operation_id], |row| {
        let event_json: String = row.get(0)?;
        from_str::<ProviderLifecycleEvent>(&event_json)
            .map_err(|error| SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
    })?;
    let events = rows.collect::<Result<Vec<_>, _>>()?;
    Ok(ProviderEvidenceLog::from_events(events)?)
}

pub(crate) fn persist_provider_evidence(
    transaction: &Transaction<'_>,
    event: &ProviderLifecycleEvent,
) -> Result<(), LocalIndexStoreError> {
    persist_reliability_event(transaction, event)
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
    // Reading the mode is connection-local and does not take the schema lock.
    // Re-applying `journal_mode=WAL` for every request connection turns a
    // harmless setup step into a write-style pragma that can race with active
    // readers under concurrent protocol traffic.
    let journal_mode: String =
        connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    if !journal_mode.eq_ignore_ascii_case("wal") {
        connection.pragma_update(None, "journal_mode", "WAL")?;
    }
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

    backfill_reliability_events(connection)?;

    Ok(())
}

/// Gives pre-journal local metadata a deterministic evidence prefix. Existing
/// operation rows are left untouched when any journal evidence is present.
fn backfill_reliability_events(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    let transaction = connection.transaction()?;
    let mut upload_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT i.intent_id, i.object_key, i.object_hash, i.state
             FROM shardline_upload_intents AS i
             WHERE NOT EXISTS (
                 SELECT 1 FROM shardline_reliability_events AS e
                 WHERE e.operation_kind = 'Upload' AND e.operation_id = i.intent_id
             )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            upload_rows.push(row?);
        }
    }
    for (intent_id, object_key, object_hash, state_text) in upload_rows {
        let state = UploadLifecycleState::parse(&state_text).ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "unknown upload intent state",
            ))
        })?;
        let final_state = UploadLifecycleState::parse(state.as_str()).ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "unmapped upload intent state",
            ))
        })?;
        let events = baseline_upload_lifecycle_events(
            "shardline",
            "default",
            intent_id,
            object_key,
            object_hash,
            final_state,
        )?;
        for event in events {
            persist_reliability_event(&transaction, &event)?;
        }
    }

    let mut session_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT s.session_id, s.scope_namespace, s.target_key, s.state
             FROM shardline_resumable_sessions AS s
             WHERE NOT EXISTS (
                 SELECT 1 FROM shardline_reliability_events AS e
                 WHERE e.operation_kind = 'ResumableSession' AND e.operation_id = s.session_id
             )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            session_rows.push(row?);
        }
    }
    for (session_id, scope_namespace, target_key, state_text) in session_rows {
        let state = ResumableLifecycleState::parse(&state_text).ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "unknown resumable session state",
            ))
        })?;
        let events =
            baseline_resumable_session_events(scope_namespace, session_id, target_key, state)?;
        for event in events {
            persist_reliability_event(&transaction, &event)?;
        }
    }

    let mut provider_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
             FROM shardline_provider_repository_states AS s
             WHERE NOT EXISTS (
                 SELECT 1 FROM shardline_reliability_events AS e
                 WHERE e.operation_kind = 'ProviderEvent'
                   AND e.operation_id = s.provider || ':' || s.owner || ':' || s.repo
             )",
        )?;
        let rows = statement.query_map([], provider_repository_state_from_row)?;
        for row in rows {
            provider_rows.push(row?);
        }
    }
    for state in provider_rows {
        let snapshot = snapshot_from_state(&state)?;
        let events = shardline_reliability::ProviderEvidenceLog::baseline(snapshot)?;
        for event in events.events() {
            persist_reliability_event(&transaction, event)?;
        }
    }

    let mut quarantine_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT object_key, observed_length,
                    first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
             FROM shardline_quarantine_candidates AS q
             WHERE NOT EXISTS (
                 SELECT 1 FROM shardline_reliability_events AS e
                 WHERE e.operation_kind = 'GarbageCollection'
                   AND e.operation_id = q.object_key
             )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            quarantine_rows.push(row?);
        }
    }
    for (object_key, observed_length, first_seen, delete_after) in quarantine_rows {
        let candidate = QuarantineCandidate::new(
            ObjectKey::parse(&object_key)?,
            i64_to_u64(observed_length)?,
            i64_to_u64(first_seen)?,
            i64_to_u64(delete_after)?,
        )?;
        let snapshot = quarantine_snapshot(&candidate, QuarantineLifecycleState::Active)?;
        let evidence = QuarantineEvidenceLog::baseline(snapshot)?;
        for event in evidence.events() {
            persist_reliability_event(&transaction, event)?;
        }
    }

    let mut oci_tombstone_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT scope_namespace, repository, object_kind, digest_hex,
                    deleted_at_unix_seconds
             FROM shardline_oci_object_tombstones AS t
             WHERE NOT EXISTS (
                 SELECT 1 FROM shardline_reliability_events AS e
                 WHERE e.operation_kind = 'Visibility'
                   AND e.operation_id = t.scope_namespace || ':' || t.repository || ':' ||
                       t.object_kind || ':' || t.digest_hex
             )",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            oci_tombstone_rows.push(row?);
        }
    }
    for (scope_namespace, repository, object_kind, digest_hex, deleted_at) in oci_tombstone_rows {
        let _kind: OciObjectKind = object_kind.parse()?;
        let snapshot = OciObjectSnapshot::new(
            OciObjectIdentity::new(scope_namespace, repository, object_kind, digest_hex)?,
            OciObjectLifecycleState::Deleted,
            Some(i64_to_u64(deleted_at)?),
        )?;
        let evidence = OciObjectEvidenceLog::baseline(snapshot)?;
        for event in evidence.events() {
            persist_reliability_event(&transaction, event)?;
        }
    }

    // Rows with no evidence are backfilled above. Existing partial or
    // tampered journals must not be silently carried forward by a successful
    // local-database startup, so verify every authoritative row before the
    // transaction commits.
    let mut upload_verification_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT intent_id, object_key, object_hash, state
             FROM shardline_upload_intents",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            upload_verification_rows.push(row?);
        }
    }
    for (intent_id, object_key, object_hash, state_text) in upload_verification_rows {
        let state = UploadLifecycleState::parse(&state_text).ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "unknown upload intent state during verification",
            ))
        })?;
        let mut statement = transaction.prepare(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = ?1
             ORDER BY sequence",
        )?;
        let rows = statement.query_map(params![intent_id], |row| {
            let event_json: String = row.get(0)?;
            from_str::<LifecycleEvent>(&event_json).map_err(|error| {
                SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })?;
        let events = rows.collect::<Result<Vec<_>, _>>()?;
        verify_upload_lifecycle_events(
            &events,
            "shardline",
            "default",
            &intent_id,
            &object_key,
            &object_hash,
            state,
        )?;
    }

    let mut session_verification_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT session_id, scope_namespace, target_key, state
             FROM shardline_resumable_sessions",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            session_verification_rows.push(row?);
        }
    }
    for (session_id, scope_namespace, target_key, state_text) in session_verification_rows {
        let state = ResumableLifecycleState::parse(&state_text).ok_or_else(|| {
            LocalIndexStoreError::Reliability(shardline_reliability::ReliabilityError::EmptyField(
                "unknown resumable session state during verification",
            ))
        })?;
        let mut statement = transaction.prepare(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'ResumableSession' AND operation_id = ?1
             ORDER BY sequence",
        )?;
        let rows = statement.query_map(params![session_id], |row| {
            let event_json: String = row.get(0)?;
            from_str::<StateTransitionEvent>(&event_json).map_err(|error| {
                SqliteError::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })?;
        let events = rows.collect::<Result<Vec<_>, _>>()?;
        verify_resumable_session_events(
            &events,
            &scope_namespace,
            &session_id,
            &target_key,
            state,
        )?;
    }

    let mut provider_verification_rows = Vec::new();
    {
        let mut statement = transaction.prepare(
            "SELECT provider,
                    owner,
                    repo,
                    last_access_changed_at_unix_seconds,
                    last_revision_pushed_at_unix_seconds,
                    last_pushed_revision,
                    last_cache_invalidated_at_unix_seconds,
                    last_authorization_rechecked_at_unix_seconds,
                    last_drift_checked_at_unix_seconds
             FROM shardline_provider_repository_states",
        )?;
        let rows = statement.query_map([], provider_repository_state_from_row)?;
        for row in rows {
            provider_verification_rows.push(row?);
        }
    }
    for state in provider_verification_rows {
        let snapshot = snapshot_from_state(&state)?;
        let evidence = load_provider_evidence(&transaction, &snapshot)?;
        verify_provider_lifecycle_events(evidence.events(), &snapshot)?;
    }
    transaction.commit()?;
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
        | other @ LocalIndexStoreError::UploadIntentConflict(_)
        | other @ LocalIndexStoreError::IntegerOutOfRange(_)
        | other @ LocalIndexStoreError::ReliabilityEventConflict(_)
        | other @ LocalIndexStoreError::Reliability(_)
        | other @ LocalIndexStoreError::InvalidRecordKind
        | other @ LocalIndexStoreError::InvalidOciObjectKind(_)
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

pub(crate) fn legacy_record_path(root: &Path, kind: RecordKind, record: &FileRecord) -> PathBuf {
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
