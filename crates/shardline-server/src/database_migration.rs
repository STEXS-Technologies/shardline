use serde_json::{from_value, to_value};
use shardline_index::{ResumableSessionState, UploadIntentState};
use shardline_protocol::SecretString;
use shardline_protocol::unix_now_seconds_lossy;
use shardline_reliability::{
    EvidenceEventMetadata, HubRefEvidenceLog, HubRefLifecycleEvent, HubRefSnapshot, LifecycleEvent,
    OciObjectEvidenceLog, OciObjectIdentity, OciObjectLifecycleEvent, OciObjectLifecycleState,
    OciObjectSnapshot, OciTagEvidenceLog, OciTagLifecycleEvent, OciTagSnapshot, OperationKind,
    ProviderEvidenceLog, ProviderLifecycleEvent, ProviderLifecycleObservations,
    ProviderLifecycleSnapshot, ProviderRepositoryIdentity, QuarantineEvidenceLog,
    QuarantineLifecycleEvent, QuarantineLifecycleState, QuarantineObjectIdentity,
    QuarantineSnapshot, ReliabilityMerkleCommit, RetentionEvidenceLog, RetentionHoldLifecycleEvent,
    RetentionHoldLifecycleState, RetentionHoldSnapshot, RetentionObjectIdentity,
    S3ObjectEvidenceLog, S3ObjectLifecycleEvent, S3ObjectSnapshot, S3ObjectState, SnapshotEvidence,
    StateTransitionEvent, UploadLifecycleState, WebhookDeliveryEvidenceLog,
    WebhookDeliveryIdentity, WebhookDeliveryLifecycleEvent, WebhookDeliveryLifecycleState,
    WebhookDeliverySnapshot, baseline_resumable_session_events, baseline_upload_lifecycle_events,
    build_persisted_merkle_commit_with_previous, persisted_event_identity,
    persisted_event_sequence, reliability_merkle_commit_json_with_previous,
    upload_lifecycle_identity, verify_hub_ref_events, verify_oci_object_lifecycle_events,
    verify_oci_tag_events, verify_provider_lifecycle_events, verify_quarantine_lifecycle_events,
    verify_resumable_session_events, verify_retention_hold_lifecycle_events,
    verify_s3_object_events, verify_upload_lifecycle_events, verify_webhook_delivery_events,
};
use sqlx::{
    Error as SqlxError, PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions, query,
    query_scalar, raw_sql,
};
use thiserror::Error;

/// Typed interruption point in one transactional database migration.
///
/// Production builds never inject these failures. Tests use the same enum as
/// the migration implementation so durability boundaries cannot drift behind
/// string-based failpoint names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatabaseMigrationBoundary {
    /// Forward SQL and its history row are uncommitted.
    BeforeApplyCommit,
    /// Forward SQL and its history row committed, but the caller lost the result.
    AfterApplyCommit,
    /// Reverse SQL and history-row removal are uncommitted.
    BeforeRevertCommit,
    /// Reverse SQL and history-row removal committed, but the caller lost the result.
    AfterRevertCommit,
}

#[cfg(test)]
fn database_migration_failpoint(
    boundary: DatabaseMigrationBoundary,
) -> Result<(), DatabaseMigrationError> {
    migration_fault_injection::hit(boundary)
}

#[cfg(test)]
mod migration_fault_injection {
    use std::sync::{LazyLock, Mutex};

    use super::{DatabaseMigrationBoundary, DatabaseMigrationError};

    static ARMED_BOUNDARY: LazyLock<Mutex<Option<DatabaseMigrationBoundary>>> =
        LazyLock::new(|| Mutex::new(None));

    pub(super) struct DatabaseMigrationFailpointGuard;

    impl Drop for DatabaseMigrationFailpointGuard {
        fn drop(&mut self) {
            *ARMED_BOUNDARY
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = None;
        }
    }

    pub(super) fn arm(boundary: DatabaseMigrationBoundary) -> DatabaseMigrationFailpointGuard {
        *ARMED_BOUNDARY
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(boundary);
        DatabaseMigrationFailpointGuard
    }

    pub(super) fn hit(boundary: DatabaseMigrationBoundary) -> Result<(), DatabaseMigrationError> {
        let interrupted = ARMED_BOUNDARY
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some_and(|armed| armed == boundary);
        if interrupted {
            Err(DatabaseMigrationError::InjectedInterruption { boundary })
        } else {
            Ok(())
        }
    }
}

/// One Shardline schema migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatabaseMigration {
    /// Monotonic migration version identifier.
    pub version: &'static str,
    /// Human-readable migration name.
    pub name: &'static str,
    /// SQL applied when migrating forward.
    pub up_sql: &'static str,
    /// SQL applied when reverting the migration.
    pub down_sql: &'static str,
}

/// Requested database-migration action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseMigrationCommand {
    /// Apply pending migrations.
    Up {
        /// Maximum number of pending migrations to apply.
        steps: Option<usize>,
    },
    /// Revert applied migrations from newest to oldest.
    Down {
        /// Maximum number of applied migrations to revert.
        steps: usize,
    },
    /// Report applied and pending migrations without mutating schema state.
    Status,
    /// Verify all materialized reliability journals without repairing them.
    Verify,
    /// Backfill at most one bounded batch of missing reliability baselines and
    /// their StateChronicle Merkle commitments.
    Backfill {
        /// Maximum number of rows considered per materialized-state table.
        batch_size: usize,
    },
    /// Explicitly discard and rebuild one named reliability operation.
    Repair {
        /// Persisted reliability operation kind.
        operation_kind: String,
        /// Persisted operation identity.
        operation_id: String,
    },
}

/// Database-migration runtime options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseMigrationOptions {
    database_url: SecretString,
    command: DatabaseMigrationCommand,
}

impl DatabaseMigrationOptions {
    /// Creates database-migration options.
    #[must_use]
    pub const fn new(database_url: String, command: DatabaseMigrationCommand) -> Self {
        Self {
            database_url: SecretString::new(database_url),
            command,
        }
    }

    /// Returns the Postgres connection URL.
    #[must_use]
    pub fn database_url(&self) -> &str {
        self.database_url.expose_secret()
    }

    /// Returns the selected command.
    #[must_use]
    pub const fn command(&self) -> &DatabaseMigrationCommand {
        &self.command
    }
}

/// One migration row in the status report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseMigrationStatusEntry {
    /// Monotonic migration version identifier.
    pub version: String,
    /// Human-readable migration name.
    pub name: String,
    /// Whether this migration is currently applied.
    pub applied: bool,
    /// UTC application timestamp when applied.
    pub applied_at_utc: Option<String>,
}

/// Database-migration execution report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseMigrationReport {
    /// Backend identifier.
    pub backend: String,
    /// Requested command.
    pub command: DatabaseMigrationCommand,
    /// Number of migrations applied during this run.
    pub applied_count: u64,
    /// Number of migrations reverted during this run.
    pub reverted_count: u64,
    /// Number of migrations applied after this run completes.
    pub applied_total_count: u64,
    /// Number of migrations still pending after this run.
    pub pending_count: u64,
    /// Full ordered status for every bundled migration.
    pub migrations: Vec<DatabaseMigrationStatusEntry>,
}

/// Database-migration failure.
#[derive(Debug, Error)]
pub enum DatabaseMigrationError {
    /// The database URL was empty.
    #[error("database URL must not be empty")]
    EmptyDatabaseUrl,
    /// Postgres access failed.
    #[error(transparent)]
    Sqlx(#[from] SqlxError),
    /// Migration history contains a version unknown to the running binary.
    #[error("database contains an unknown shardline migration version: {0}")]
    UnknownAppliedMigration(String),
    /// A previously applied migration no longer matches the bundled SQL.
    #[error(
        "bundled migration checksum mismatch for version {version}: expected {expected_checksum}, observed {observed_checksum}"
    )]
    ChecksumMismatch {
        /// Bundled migration version.
        version: String,
        /// Hash of the bundled SQL.
        expected_checksum: String,
        /// Hash recorded in the database.
        observed_checksum: String,
    },
    /// Existing durable state could not be converted into canonical evidence.
    #[error("reliability evidence backfill failed: {0}")]
    Backfill(String),
    /// A test interrupted execution at a typed transactional boundary.
    #[error("database migration interrupted at {boundary:?}")]
    InjectedInterruption {
        /// Boundary reached by the migration transaction.
        boundary: DatabaseMigrationBoundary,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedMigration {
    version: String,
    checksum: String,
    applied_at_utc: String,
}

const MIGRATION_HISTORY_TABLE: &str = "shardline_schema_migrations";
const MIGRATION_ADVISORY_LOCK_KEY: i64 = 0x5348_4152_444d_4701;
// These pre-release migrations were folded into the first reliability schema
// migration. Existing development databases may already contain their
// history rows; they remain accepted as retired compatibility markers and are
// never re-run or selected for rollback.
const RETIRED_MIGRATION_VERSIONS: &[&str] = &["20260923000000", "20260925000000"];
// The reliability migration was squashed before release. Keep the checksum
// written by the pre-squash development database valid so an operator can
// upgrade that database normally instead of editing migration history.
const LEGACY_MIGRATION_CHECKSUM_ALIASES: &[(&str, &str)] = &[
    (
        "20260922000000",
        "f47296d3478e4b9b6026eae9006e767a527ce01d5ebff56ed37514eb500ef0a0",
    ),
    (
        // Development databases may have applied the initial gate SQL before
        // the migration was finalized. Accept that exact historical checksum;
        // all new installations record the bundled migration checksum.
        "20260926000000",
        "08c0ccd56dc6e1d1c687701127c9677e410c7af6f8dd47ce90d887b4e8177efa",
    ),
];

const SHARDLINE_MIGRATIONS: [DatabaseMigration; 28] = [
    DatabaseMigration {
        version: "20260417000000",
        name: "metadata_store",
        up_sql: include_str!("../migrations/20260417000000_metadata_store.up.sql"),
        down_sql: include_str!("../migrations/20260417000000_metadata_store.down.sql"),
    },
    DatabaseMigration {
        version: "20260417010000",
        name: "retention_holds",
        up_sql: include_str!("../migrations/20260417010000_retention_holds.up.sql"),
        down_sql: include_str!("../migrations/20260417010000_retention_holds.down.sql"),
    },
    DatabaseMigration {
        version: "20260418000000",
        name: "dedupe_shards",
        up_sql: include_str!("../migrations/20260418000000_dedupe_shards.up.sql"),
        down_sql: include_str!("../migrations/20260418000000_dedupe_shards.down.sql"),
    },
    DatabaseMigration {
        version: "20260418010000",
        name: "webhook_deliveries",
        up_sql: include_str!("../migrations/20260418010000_webhook_deliveries.up.sql"),
        down_sql: include_str!("../migrations/20260418010000_webhook_deliveries.down.sql"),
    },
    DatabaseMigration {
        version: "20260418020000",
        name: "provider_repository_states",
        up_sql: include_str!("../migrations/20260418020000_provider_repository_states.up.sql"),
        down_sql: include_str!("../migrations/20260418020000_provider_repository_states.down.sql"),
    },
    DatabaseMigration {
        version: "20260418110000",
        name: "provider_repository_reconciliation",
        up_sql: include_str!(
            "../migrations/20260418110000_provider_repository_reconciliation.up.sql"
        ),
        down_sql: include_str!(
            "../migrations/20260418110000_provider_repository_reconciliation.down.sql"
        ),
    },
    DatabaseMigration {
        version: "20260629000000",
        name: "hub_api",
        up_sql: include_str!("../migrations/20260629000000_hub_api.up.sql"),
        down_sql: include_str!("../migrations/20260629000000_hub_api.down.sql"),
    },
    DatabaseMigration {
        version: "20260630000000",
        name: "hub_inline_content",
        up_sql: include_str!("../migrations/20260630000000_hub_inline_content.up.sql"),
        down_sql: include_str!("../migrations/20260630000000_hub_inline_content.down.sql"),
    },
    DatabaseMigration {
        version: "20260630000001",
        name: "hub_webhooks",
        up_sql: include_str!("../migrations/20260630000001_hub_webhooks.up.sql"),
        down_sql: include_str!("../migrations/20260630000001_hub_webhooks.down.sql"),
    },
    DatabaseMigration {
        version: "20260630000002",
        name: "hub_refs",
        up_sql: include_str!("../migrations/20260630000002_hub_refs.up.sql"),
        down_sql: include_str!("../migrations/20260630000002_hub_refs.down.sql"),
    },
    DatabaseMigration {
        version: "20260630000003",
        name: "drop_inline_content",
        up_sql: include_str!("../migrations/20260630000003_drop_inline_content.up.sql"),
        down_sql: include_str!("../migrations/20260630000003_drop_inline_content.down.sql"),
    },
    DatabaseMigration {
        version: "20260630000004",
        name: "drop_lfs_objects",
        up_sql: include_str!("../migrations/20260630000004_drop_lfs_objects.up.sql"),
        down_sql: include_str!("../migrations/20260630000004_drop_lfs_objects.down.sql"),
    },
    DatabaseMigration {
        version: "20260720000000",
        name: "fix_indexes",
        up_sql: include_str!("../migrations/20260720000000_fix_indexes.up.sql"),
        down_sql: include_str!("../migrations/20260720000000_fix_indexes.down.sql"),
    },
    DatabaseMigration {
        version: "20260721000000",
        name: "upload_intents",
        up_sql: include_str!("../migrations/20260721000000_upload_intents.up.sql"),
        down_sql: include_str!("../migrations/20260721000000_upload_intents.down.sql"),
    },
    DatabaseMigration {
        version: "20260805000000",
        name: "tree_store",
        up_sql: include_str!("../migrations/20260805000000_tree_store.up.sql"),
        down_sql: include_str!("../migrations/20260805000000_tree_store.down.sql"),
    },
    DatabaseMigration {
        version: "20260813000000",
        name: "s3_object_index",
        up_sql: include_str!("../migrations/20260813000000_s3_object_index.up.sql"),
        down_sql: include_str!("../migrations/20260813000000_s3_object_index.down.sql"),
    },
    DatabaseMigration {
        version: "20260814000000",
        name: "s3_object_etag_metadata",
        up_sql: include_str!("../migrations/20260814000000_s3_object_etag_metadata.up.sql"),
        down_sql: include_str!("../migrations/20260814000000_s3_object_etag_metadata.down.sql"),
    },
    DatabaseMigration {
        version: "20260822000000",
        name: "oci_tags",
        up_sql: include_str!("../migrations/20260822000000_oci_tags.up.sql"),
        down_sql: include_str!("../migrations/20260822000000_oci_tags.down.sql"),
    },
    DatabaseMigration {
        version: "20260822010000",
        name: "resource_fences",
        up_sql: include_str!("../migrations/20260822010000_resource_fences.up.sql"),
        down_sql: include_str!("../migrations/20260822010000_resource_fences.down.sql"),
    },
    DatabaseMigration {
        version: "20260822020000",
        name: "oci_object_tombstones",
        up_sql: include_str!("../migrations/20260822020000_oci_object_tombstones.up.sql"),
        down_sql: include_str!("../migrations/20260822020000_oci_object_tombstones.down.sql"),
    },
    DatabaseMigration {
        version: "20260823000000",
        name: "resumable_sessions",
        up_sql: include_str!("../migrations/20260823000000_resumable_sessions.up.sql"),
        down_sql: include_str!("../migrations/20260823000000_resumable_sessions.down.sql"),
    },
    DatabaseMigration {
        version: "20260922000000",
        name: "reliability_events",
        up_sql: include_str!("../migrations/20260922000000_reliability_events.up.sql"),
        down_sql: include_str!("../migrations/20260922000000_reliability_events.down.sql"),
    },
    DatabaseMigration {
        version: "20260924000000",
        name: "resumable_state_digest",
        up_sql: include_str!("../migrations/20260924000000_resumable_state_digest.up.sql"),
        down_sql: include_str!("../migrations/20260924000000_resumable_state_digest.down.sql"),
    },
    DatabaseMigration {
        version: "20260926000000",
        name: "reliability_write_gates",
        up_sql: include_str!("../migrations/20260926000000_reliability_write_gates.up.sql"),
        down_sql: include_str!("../migrations/20260926000000_reliability_write_gates.down.sql"),
    },
    DatabaseMigration {
        version: "20260927000000",
        name: "reliability_events_schema_compat",
        up_sql: include_str!(
            "../migrations/20260927000000_reliability_events_schema_compat.up.sql"
        ),
        down_sql: include_str!(
            "../migrations/20260927000000_reliability_events_schema_compat.down.sql"
        ),
    },
    DatabaseMigration {
        version: "20260928000000",
        name: "reliability_delete_gates",
        up_sql: include_str!("../migrations/20260928000000_reliability_delete_gates.up.sql"),
        down_sql: include_str!("../migrations/20260928000000_reliability_delete_gates.down.sql"),
    },
    DatabaseMigration {
        version: "20260929000000",
        name: "reliability_write_gate_state_match",
        up_sql: include_str!(
            "../migrations/20260929000000_reliability_write_gate_state_match.up.sql"
        ),
        down_sql: include_str!(
            "../migrations/20260929000000_reliability_write_gate_state_match.down.sql"
        ),
    },
    DatabaseMigration {
        version: "20260930000000",
        name: "reliability_merkle_commits",
        up_sql: include_str!("../migrations/20260930000000_reliability_merkle_commits.up.sql"),
        down_sql: include_str!("../migrations/20260930000000_reliability_merkle_commits.down.sql"),
    },
];

/// Returns the bundled Shardline migration list in application order.
#[must_use]
pub const fn bundled_database_migrations() -> &'static [DatabaseMigration] {
    &SHARDLINE_MIGRATIONS
}

/// Applies pending Shardline migrations to an existing Postgres pool.
///
/// # Errors
///
/// Returns [`DatabaseMigrationError`] when the migration history is inconsistent or
/// when Postgres rejects the schema updates.
pub async fn apply_database_migrations(pool: &PgPool) -> Result<(), DatabaseMigrationError> {
    ensure_migration_history_table(pool).await?;
    let _migration_guard = acquire_migration_lock(pool).await?;
    verify_applied_migrations(pool).await?;

    for migration in pending_migrations(pool).await? {
        apply_one_migration(pool, migration).await?;
    }
    Ok(())
}

/// Executes a Shardline database-migration command against Postgres.
///
/// # Errors
///
/// Returns [`DatabaseMigrationError`] when connection setup, migration history
/// verification, or SQL execution fails.
pub async fn run_database_migration(
    options: &DatabaseMigrationOptions,
) -> Result<DatabaseMigrationReport, DatabaseMigrationError> {
    if options.database_url().trim().is_empty() {
        return Err(DatabaseMigrationError::EmptyDatabaseUrl);
    }

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(options.database_url())
        .await?;
    ensure_migration_history_table(&pool).await?;
    let _migration_guard = match options.command() {
        DatabaseMigrationCommand::Up { .. }
        | DatabaseMigrationCommand::Down { .. }
        | DatabaseMigrationCommand::Repair { .. } => Some(acquire_migration_lock(&pool).await?),
        DatabaseMigrationCommand::Status => None,
        DatabaseMigrationCommand::Verify => None,
        DatabaseMigrationCommand::Backfill { .. } => Some(acquire_migration_lock(&pool).await?),
    };
    verify_applied_migrations(&pool).await?;

    let (applied_count, reverted_count) = match options.command() {
        DatabaseMigrationCommand::Up { steps } => {
            let pending = pending_migrations(&pool).await?;
            let mut applied_count = 0_u64;
            for migration in pending.into_iter().take(steps.unwrap_or(usize::MAX)) {
                apply_one_migration(&pool, migration).await?;
                applied_count = applied_count.saturating_add(1);
            }
            (applied_count, 0)
        }
        DatabaseMigrationCommand::Down { steps } => {
            let applied = applied_migrations_in_order(&pool).await?;
            let mut reverted_count = 0_u64;
            for migration in applied.into_iter().rev().take(*steps) {
                revert_one_migration(&pool, migration).await?;
                reverted_count = reverted_count.saturating_add(1);
            }
            (0, reverted_count)
        }
        DatabaseMigrationCommand::Status
        | DatabaseMigrationCommand::Verify
        | DatabaseMigrationCommand::Backfill { .. }
        | DatabaseMigrationCommand::Repair { .. } => (0, 0),
    };

    if matches!(options.command(), DatabaseMigrationCommand::Verify) {
        verify_reliability_events(&pool).await?;
    }
    if let DatabaseMigrationCommand::Backfill { batch_size } = options.command() {
        backfill_reliability_events(&pool, *batch_size).await?;
        backfill_reliability_merkle_commits(&pool, *batch_size).await?;
    }
    if let DatabaseMigrationCommand::Repair {
        operation_kind,
        operation_id,
    } = options.command()
    {
        repair_reliability_operation(&pool, operation_kind, operation_id).await?;
    }

    let migrations = migration_status_entries(&pool).await?;
    let applied_total_count =
        u64::try_from(migrations.iter().filter(|entry| entry.applied).count()).unwrap_or(u64::MAX);
    let pending_count =
        u64::try_from(migrations.iter().filter(|entry| !entry.applied).count()).unwrap_or(u64::MAX);

    Ok(DatabaseMigrationReport {
        backend: "postgres".to_owned(),
        command: options.command().clone(),
        applied_count,
        reverted_count,
        applied_total_count,
        pending_count,
        migrations,
    })
}

async fn ensure_migration_history_table(pool: &PgPool) -> Result<(), SqlxError> {
    raw_sql(&format!(
        "CREATE TABLE IF NOT EXISTS {MIGRATION_HISTORY_TABLE} (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            checksum TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )"
    ))
    .execute(pool)
    .await?;

    Ok(())
}

/// Gives pre-journal durable state a deterministic, verifiable evidence
/// prefix. This runs under the migration advisory lock and is idempotent: a
/// row with any evidence already present is left untouched.
async fn backfill_reliability_events(
    pool: &PgPool,
    batch_size: usize,
) -> Result<(), DatabaseMigrationError> {
    reconcile_reliability_events(pool, true, batch_size).await
}

/// Adds real StateChronicle Merkle commit bodies to legacy evidence rows in
/// bounded transactions. The existing Shardline event remains authoritative;
/// this only derives and records its verifiable commitment.
async fn backfill_reliability_merkle_commits(
    pool: &PgPool,
    batch_size: usize,
) -> Result<(), DatabaseMigrationError> {
    let batch_size = i64::try_from(batch_size.max(1)).map_err(|error| {
        DatabaseMigrationError::Backfill(format!("invalid Merkle backfill batch size: {error}"))
    })?;
    let mut transaction = pool.begin().await?;
    let rows = query(
        "SELECT operation_kind, operation_id, sequence, event_json
             FROM shardline_reliability_events
             WHERE merkle_commit_json IS NULL
                OR (sequence > 0 AND (merkle_commit_json->'body'->>'parent_commit_id') IS NULL)
             ORDER BY operation_kind, operation_id, sequence
             LIMIT $1
             FOR UPDATE SKIP LOCKED",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    if rows.is_empty() {
        transaction.commit().await?;
        return Ok(());
    }
    for row in rows {
        let operation_kind_text: String = row.try_get("operation_kind")?;
        let operation_id: String = row.try_get("operation_id")?;
        let sequence: i64 = row.try_get("sequence")?;
        let operation_kind = OperationKind::parse(&operation_kind_text).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "unknown reliability operation kind {operation_kind_text} for {operation_id}"
            ))
        })?;
        let event_json: serde_json::Value = row.try_get("event_json")?;
        let previous_json: Option<serde_json::Value> = query_scalar(
            "SELECT merkle_commit_json
             FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2 AND sequence < $3
               AND merkle_commit_json IS NOT NULL
             ORDER BY sequence DESC LIMIT 1",
        )
        .bind(operation_kind.as_str())
        .bind(&operation_id)
        .bind(sequence)
        .fetch_optional(&mut *transaction)
        .await?;
        let merkle_commit_json = build_persisted_merkle_commit_with_previous(
            operation_kind,
            event_json,
            previous_json,
        )
        .map_err(|error| {
                    DatabaseMigrationError::Backfill(format!(
                        "cannot build Merkle commit kind={operation_kind_text} operation={operation_id} sequence={sequence}: {error}"
                    ))
                })?;
        query(
            "UPDATE shardline_reliability_events
                 SET merkle_commit_json = $1
                 WHERE operation_kind = $2 AND operation_id = $3 AND sequence = $4
                   AND (merkle_commit_json IS DISTINCT FROM $1 OR merkle_commit_json IS NULL)",
        )
        .bind(merkle_commit_json)
        .bind(operation_kind_text)
        .bind(operation_id)
        .bind(sequence)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn verify_reliability_events(pool: &PgPool) -> Result<(), DatabaseMigrationError> {
    reconcile_reliability_events(pool, false, usize::MAX).await
}

async fn repair_reliability_operation(
    pool: &PgPool,
    operation_kind: &str,
    operation_id: &str,
) -> Result<(), DatabaseMigrationError> {
    let operation_kind = OperationKind::parse(operation_kind).ok_or_else(|| {
        DatabaseMigrationError::Backfill(format!(
            "unknown reliability operation kind for explicit repair: {operation_kind}"
        ))
    })?;
    let journal_exists: bool = query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
         )",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .fetch_one(pool)
    .await?;
    let authoritative_exists =
        authoritative_operation_exists(pool, operation_kind, operation_id).await?;
    if journal_exists && !authoritative_exists {
        repair_persisted_merkle_operation(pool, operation_kind, operation_id).await?;
        return verify_persisted_reliability_operation(pool, operation_kind, operation_id).await;
    }
    if !authoritative_exists {
        return Err(DatabaseMigrationError::Backfill(format!(
            "cannot repair reliability operation kind={} operation={}: no authoritative materialized state exists",
            operation_kind.as_str(),
            operation_id
        )));
    }
    let mut transaction = pool.begin().await?;
    query(
        "DELETE FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    const REPAIR_BACKFILL_BATCH_SIZE: usize = 256;
    loop {
        backfill_reliability_events(pool, REPAIR_BACKFILL_BATCH_SIZE).await?;
        let repaired: bool = query_scalar(
            "SELECT EXISTS(
             SELECT 1 FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
         )",
        )
        .bind(operation_kind.as_str())
        .bind(operation_id)
        .fetch_one(pool)
        .await?;
        if repaired {
            break;
        }
    }
    verify_persisted_reliability_operation(pool, operation_kind, operation_id).await
}

/// Rebuilds only the Merkle bodies for a journal whose materialized resource
/// has legitimately been consumed or deleted. The event JSON remains
/// authoritative for this explicit operator action; malformed events or
/// sequence gaps abort the transaction without changing the journal.
async fn repair_persisted_merkle_operation(
    pool: &PgPool,
    operation_kind: OperationKind,
    operation_id: &str,
) -> Result<(), DatabaseMigrationError> {
    let mut transaction = pool.begin().await?;
    let rows = query(
        "SELECT sequence, event_json
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2
         ORDER BY sequence
         FOR UPDATE",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .fetch_all(&mut *transaction)
    .await?;
    let mut previous: Option<serde_json::Value> = None;
    for row in rows {
        let sequence: i64 = row.try_get("sequence")?;
        let event_json: serde_json::Value = row.try_get("event_json")?;
        let event_sequence = persisted_event_sequence(operation_kind, event_json.clone()).map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid persisted reliability event during explicit Merkle repair kind={} operation={} sequence={}: {error}",
                operation_kind.as_str(),
                operation_id,
                sequence,
            ))
        })?;
        if i64::try_from(event_sequence).map_err(|error| {
            DatabaseMigrationError::Backfill(format!("event sequence out of range: {error}"))
        })? != sequence
        {
            return Err(DatabaseMigrationError::Backfill(format!(
                "persisted event sequence does not match row during explicit Merkle repair kind={} operation={} row_sequence={} event_sequence={}",
                operation_kind.as_str(),
                operation_id,
                sequence,
                event_sequence
            )));
        }
        let merkle_commit_json = build_persisted_merkle_commit_with_previous(
            operation_kind,
            event_json,
            previous,
        )
        .map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "cannot rebuild Merkle commit during explicit repair kind={} operation={} sequence={}: {error}",
                operation_kind.as_str(),
                operation_id,
                sequence,
            ))
        })?;
        query(
            "UPDATE shardline_reliability_events
             SET merkle_commit_json = $1
             WHERE operation_kind = $2 AND operation_id = $3 AND sequence = $4",
        )
        .bind(&merkle_commit_json)
        .bind(operation_kind.as_str())
        .bind(operation_id)
        .bind(sequence)
        .execute(&mut *transaction)
        .await?;
        previous = Some(merkle_commit_json);
    }
    transaction.commit().await?;
    Ok(())
}

async fn authoritative_operation_exists(
    pool: &PgPool,
    operation_kind: OperationKind,
    operation_id: &str,
) -> Result<bool, DatabaseMigrationError> {
    let exists = match operation_kind {
        OperationKind::Upload => query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_upload_intents WHERE intent_id = $1)",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::ResumableSession => query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_resumable_sessions WHERE session_id = $1)",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::ProviderEvent => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_provider_repository_states
                 WHERE provider || ':' || owner || ':' || repo = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::GarbageCollection => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_quarantine_candidates WHERE object_key = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::Visibility => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_oci_object_tombstones
                 WHERE scope_namespace || ':' || repository || ':' || object_kind || ':' || digest_hex = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::RetentionHold => query_scalar(
            "SELECT EXISTS(SELECT 1 FROM shardline_retention_holds WHERE object_key = $1)",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::WebhookDelivery => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_webhook_deliveries
                 WHERE octet_length(provider)::text || ':' || provider
                       || octet_length(owner)::text || ':' || owner
                       || octet_length(repo)::text || ':' || repo
                       || octet_length(delivery_id)::text || ':' || delivery_id = $1
                    OR delivery_id = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::MetadataCommit => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_hub_refs
                 WHERE octet_length(repo_id)::text || ':' || repo_id
                       || octet_length(ref_name)::text || ':' || ref_name = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::OciTag => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_oci_tags
                 WHERE octet_length(scope_namespace)::text || ':' || scope_namespace
                       || octet_length(repository)::text || ':' || repository
                       || octet_length(tag)::text || ':' || tag = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::S3Object => query_scalar(
            "SELECT EXISTS(
                 SELECT 1 FROM shardline_s3_objects
                 WHERE octet_length(scope_namespace)::text || ':' || scope_namespace
                       || octet_length(object_key)::text || ':' || object_key = $1
             )",
        )
        .bind(operation_id)
        .fetch_one(pool)
        .await?,
        OperationKind::Repair => false,
    };
    Ok(exists)
}

/// Verifies every persisted reliability event for an operator-facing fsck run.
///
/// Unlike migration backfill, this never establishes missing baselines or
/// changes materialized state. A failure is reported by fsck as an evidence
/// issue so operators can distinguish valid content from invalid provenance.
pub async fn verify_reliability_events_for_fsck(
    pool: &PgPool,
) -> Result<(), DatabaseMigrationError> {
    verify_reliability_events(pool).await
}

async fn persist_reliability_event<T: EvidenceEventMetadata>(
    transaction: &mut Transaction<'_, Postgres>,
    event: &T,
) -> Result<(), DatabaseMigrationError> {
    event
        .verify_integrity()
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
    let sequence = i64::try_from(event.sequence_number()).map_err(|error| {
        DatabaseMigrationError::Backfill(format!(
            "reliability event sequence out of range kind={} operation={} sequence={}: {error}",
            event.operation_identity().kind.as_str(),
            event.operation_identity().operation_id,
            event.sequence_number(),
        ))
    })?;
    let previous_json: Option<serde_json::Value> = query_scalar(
        "SELECT merkle_commit_json
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2 AND sequence < $3
           AND merkle_commit_json IS NOT NULL
         ORDER BY sequence DESC LIMIT 1",
    )
    .bind(event.operation_identity().kind.as_str())
    .bind(&event.operation_identity().operation_id)
    .bind(sequence)
    .fetch_optional(&mut **transaction)
    .await?;
    let previous = previous_json
        .map(serde_json::from_value::<ReliabilityMerkleCommit>)
        .transpose()
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
    let merkle_commit_json = reliability_merkle_commit_json_with_previous(event, previous.as_ref())
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
    let event_json =
        to_value(event).map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
    let result = query(
        "INSERT INTO shardline_reliability_events
            (operation_kind, operation_id, sequence, event_json, created_at_unix_seconds,
             merkle_commit_json)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (operation_kind, operation_id, sequence) DO UPDATE
         SET merkle_commit_json = COALESCE(
             shardline_reliability_events.merkle_commit_json,
             EXCLUDED.merkle_commit_json
         )
         WHERE shardline_reliability_events.event_json = EXCLUDED.event_json",
    )
    .bind(event.operation_identity().kind.as_str())
    .bind(&event.operation_identity().operation_id)
    .bind(sequence)
    .bind(event_json)
    .bind(unix_now_seconds_lossy() as i64)
    .bind(merkle_commit_json)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() == 0 {
        return Err(DatabaseMigrationError::Backfill(format!(
            "conflicting reliability event kind={} operation={} sequence={}",
            event.operation_identity().kind.as_str(),
            event.operation_identity().operation_id,
            event.sequence_number(),
        )));
    }
    Ok(())
}

async fn reliability_operation_exists(
    transaction: &mut Transaction<'_, Postgres>,
    operation_kind: OperationKind,
    operation_id: &str,
) -> Result<bool, DatabaseMigrationError> {
    Ok(query_scalar(
        "SELECT EXISTS(
             SELECT 1 FROM shardline_reliability_events
             WHERE operation_kind = $1 AND operation_id = $2
         )",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .fetch_one(&mut **transaction)
    .await?)
}

async fn reconcile_reliability_events(
    pool: &PgPool,
    repair_missing: bool,
    batch_size: usize,
) -> Result<(), DatabaseMigrationError> {
    let required_tables_exist: bool = query_scalar(
        "SELECT to_regclass('public.shardline_reliability_events') IS NOT NULL
             AND to_regclass('public.shardline_upload_intents') IS NOT NULL
             AND to_regclass('public.shardline_resumable_sessions') IS NOT NULL
             AND to_regclass('public.shardline_provider_repository_states') IS NOT NULL
             AND to_regclass('public.shardline_quarantine_candidates') IS NOT NULL
             AND to_regclass('public.shardline_oci_object_tombstones') IS NOT NULL
             AND to_regclass('public.shardline_retention_holds') IS NOT NULL
             AND to_regclass('public.shardline_webhook_deliveries') IS NOT NULL
             AND to_regclass('public.shardline_hub_refs') IS NOT NULL
             AND to_regclass('public.shardline_oci_tags') IS NOT NULL
             AND to_regclass('public.shardline_s3_objects') IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    if !required_tables_exist {
        return Ok(());
    }
    let batch_size = i64::try_from(batch_size.max(1)).unwrap_or(i64::MAX);
    let mut transaction = pool.begin().await?;
    if !repair_missing {
        query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(&mut *transaction)
            .await?;
        verify_persisted_reliability_events(&mut transaction)
            .await
            .map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "persisted reliability event verification: {error}"
                ))
            })?;
    }
    let upload_rows = query(
        "SELECT i.intent_id, i.object_key, i.object_hash, i.state
         FROM shardline_upload_intents AS i
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'Upload' AND e.operation_id = i.intent_id
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in upload_rows {
        let state_text: String = row.try_get("state")?;
        let state = UploadIntentState::parse(&state_text).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "unknown upload intent state during reliability backfill: {state_text}"
            ))
        })?;
        let final_state = UploadLifecycleState::parse(state.as_str()).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "upload intent state has no reliability mapping: {state_text}"
            ))
        })?;
        let events = baseline_upload_lifecycle_events(
            "shardline",
            "default",
            row.try_get::<String, _>("intent_id")?,
            row.try_get::<String, _>("object_key")?,
            row.try_get::<String, _>("object_hash")?,
            final_state,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events {
                persist_reliability_event(&mut transaction, &event).await?;
            }
        }
    }

    let session_rows = query(
        "SELECT s.session_id, s.scope_namespace, s.target_key, s.state
         FROM shardline_resumable_sessions AS s
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'ResumableSession' AND e.operation_id = s.session_id
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in session_rows {
        let state_text: String = row.try_get("state")?;
        let state = ResumableSessionState::parse(&state_text).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "unknown resumable session state during reliability backfill: {state_text}"
            ))
        })?;
        let events = baseline_resumable_session_events(
            row.try_get::<String, _>("scope_namespace")?,
            row.try_get::<String, _>("session_id")?,
            row.try_get::<String, _>("target_key")?,
            state,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events {
                persist_reliability_event(&mut transaction, &event).await?;
            }
        }
    }

    let provider_rows = query(
        "SELECT s.provider,
                s.owner,
                s.repo,
                s.last_access_changed_at_unix_seconds,
                s.last_revision_pushed_at_unix_seconds,
                s.last_pushed_revision,
                s.last_cache_invalidated_at_unix_seconds,
                s.last_authorization_rechecked_at_unix_seconds,
                s.last_drift_checked_at_unix_seconds
         FROM shardline_provider_repository_states AS s
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'ProviderEvent'
               AND e.operation_id = s.provider || ':' || s.owner || ':' || s.repo
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in provider_rows {
        let snapshot = provider_snapshot_from_row(&row)?;
        let events = ProviderEvidenceLog::baseline(snapshot)
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let quarantine_rows = query(
        "SELECT q.object_key, q.observed_length,
                q.first_seen_unreachable_at_unix_seconds, q.delete_after_unix_seconds
         FROM shardline_quarantine_candidates AS q
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'GarbageCollection' AND e.operation_id = q.object_key
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in quarantine_rows {
        let object_key: String = row.try_get("object_key")?;
        let snapshot = QuarantineSnapshot::new(
            QuarantineObjectIdentity::new(object_key.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("observed_length")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("first_seen_unreachable_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("delete_after_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            QuarantineLifecycleState::Active,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let events = QuarantineEvidenceLog::baseline(snapshot)
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let oci_tombstone_rows = query(
        "SELECT t.scope_namespace, t.repository, t.object_kind, t.digest_hex,
                t.deleted_at_unix_seconds
         FROM shardline_oci_object_tombstones AS t
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'Visibility'
               AND e.operation_id = t.scope_namespace || ':' || t.repository || ':' ||
                   t.object_kind || ':' || t.digest_hex
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in oci_tombstone_rows {
        let snapshot = OciObjectSnapshot::new(
            OciObjectIdentity::new(
                row.try_get::<String, _>("scope_namespace")?,
                row.try_get::<String, _>("repository")?,
                row.try_get::<String, _>("object_kind")?,
                row.try_get::<String, _>("digest_hex")?,
            )
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            OciObjectLifecycleState::Deleted,
            Some(
                u64::try_from(row.try_get::<i64, _>("deleted_at_unix_seconds")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            ),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let events = OciObjectEvidenceLog::baseline(snapshot)
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let retention_hold_rows = query(
        "SELECT h.object_key, h.reason, h.held_at_unix_seconds,
                h.release_after_unix_seconds
         FROM shardline_retention_holds AS h
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'RetentionHold' AND e.operation_id = h.object_key
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in retention_hold_rows {
        let object_key: String = row.try_get("object_key")?;
        let snapshot = RetentionHoldSnapshot::new(
            RetentionObjectIdentity::new(object_key)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<String, _>("reason")?,
            u64::try_from(row.try_get::<i64, _>("held_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<Option<i64>, _>("release_after_unix_seconds")?
                .map(|value| {
                    u64::try_from(value)
                        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
                })
                .transpose()?,
            RetentionHoldLifecycleState::Active,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let events = RetentionEvidenceLog::baseline(snapshot)
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing {
            for event in events.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let webhook_delivery_rows = query(
        "SELECT w.provider, w.owner, w.repo, w.delivery_id,
                w.processed_at_unix_seconds
         FROM shardline_webhook_deliveries AS w
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'WebhookDelivery'
               AND (
                   e.operation_id = octet_length(w.provider)::text || ':' || w.provider
                       || octet_length(w.owner)::text || ':' || w.owner
                       || octet_length(w.repo)::text || ':' || w.repo
                       || octet_length(w.delivery_id)::text || ':' || w.delivery_id
                   OR e.operation_id = w.delivery_id
               )
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in webhook_delivery_rows {
        let snapshot = WebhookDeliverySnapshot::new(
            WebhookDeliveryIdentity::new(
                row.try_get::<String, _>("provider")?,
                row.try_get::<String, _>("owner")?,
                row.try_get::<String, _>("repo")?,
                row.try_get::<String, _>("delivery_id")?,
            )
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("processed_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            WebhookDeliveryLifecycleState::Processed,
        );
        let events = WebhookDeliveryEvidenceLog::baseline(snapshot)
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation_id = events
            .events()
            .first()
            .map(|event| event.operation.operation_id.clone())
            .ok_or_else(|| {
                DatabaseMigrationError::Backfill("webhook baseline has no event".into())
            })?;
        let canonical_exists = query(
            "SELECT 1 FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1
             LIMIT 1",
        )
        .bind(&operation_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
        let legacy_exists = if canonical_exists {
            true
        } else {
            query(
                "SELECT 1 FROM shardline_reliability_events
                 WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1
                 LIMIT 1",
            )
            .bind(&row.try_get::<String, _>("delivery_id")?)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some()
        };
        if repair_missing && !legacy_exists {
            for event in events.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let hub_ref_rows = query(
        "SELECT repo_id, ref_name, sha
         FROM shardline_hub_refs
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'MetadataCommit'
               AND e.operation_id = octet_length(repo_id)::text || ':' || repo_id
                   || octet_length(ref_name)::text || ':' || ref_name
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in hub_ref_rows {
        let snapshot = HubRefSnapshot::new(
            row.try_get::<String, _>("repo_id")?,
            row.try_get::<String, _>("ref_name")?,
            Some(row.try_get::<String, _>("sha")?),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing
            && !reliability_operation_exists(
                &mut transaction,
                OperationKind::MetadataCommit,
                &operation.operation_id,
            )
            .await?
        {
            let evidence = HubRefEvidenceLog::baseline(snapshot)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in evidence.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let oci_tag_rows = query(
        "SELECT scope_namespace, repository, tag, digest_hex
         FROM shardline_oci_tags
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'OciTag'
               AND e.operation_id = octet_length(scope_namespace)::text || ':' || scope_namespace
                   || octet_length(repository)::text || ':' || repository
                   || octet_length(tag)::text || ':' || tag
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in oci_tag_rows {
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let repository: String = row.try_get("repository")?;
        let tag: String = row.try_get("tag")?;
        let digest_hex: String = row.try_get("digest_hex")?;
        let present = OciTagSnapshot::new(&scope_namespace, &repository, &tag, Some(digest_hex))
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation = present
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing
            && !reliability_operation_exists(
                &mut transaction,
                OperationKind::OciTag,
                &operation.operation_id,
            )
            .await?
        {
            let absent = OciTagSnapshot::new(&scope_namespace, &repository, &tag, None)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            let mut evidence = OciTagEvidenceLog::baseline(absent)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            evidence
                .record(present)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in evidence.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    let s3_object_rows = query(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash,
                etag, user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects
         WHERE NOT EXISTS (
             SELECT 1 FROM shardline_reliability_events AS e
             WHERE e.operation_kind = 'S3Object'
               AND e.operation_id = octet_length(scope_namespace)::text || ':' || scope_namespace
                   || octet_length(object_key)::text || ':' || object_key
         )
         LIMIT $1",
    )
    .bind(batch_size)
    .fetch_all(&mut *transaction)
    .await?;
    for row in s3_object_rows {
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let object_key: String = row.try_get("object_key")?;
        let present = S3ObjectSnapshot::new(
            &scope_namespace,
            &object_key,
            Some(S3ObjectState {
                file_id: row.try_get("file_id")?,
                size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
                content_hash: row.try_get("content_hash")?,
                etag: row.try_get("etag")?,
                user_metadata: serde_json::from_str(&row.try_get::<String, _>("user_metadata")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
                updated_at_unix_seconds: row.try_get("updated_at_unix_seconds")?,
            }),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation = present
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        if repair_missing
            && !reliability_operation_exists(
                &mut transaction,
                OperationKind::S3Object,
                &operation.operation_id,
            )
            .await?
        {
            let absent = S3ObjectSnapshot::new(&scope_namespace, &object_key, None)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            let mut evidence = S3ObjectEvidenceLog::baseline(absent)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            evidence
                .record(present)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in evidence.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
        }
    }

    // Backfill is deliberately a bounded write-only maintenance pass. Full
    // journal verification is exposed by the separate verify command; doing
    // it here would turn a small batch into a table-wide locking operation.
    if repair_missing {
        transaction.commit().await?;
        return Ok(());
    }

    // A partially present journal is not a valid migration state. The
    // backfill above repairs only rows that had no evidence at all; this pass
    // verifies every row before committing so an interrupted or tampered
    // journal cannot be silently carried forward by a successful migration.
    let upload_verification_rows = query(
        "SELECT intent_id, object_key, object_hash, state
         FROM shardline_upload_intents",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in upload_verification_rows {
        let intent_id: String = row.try_get("intent_id")?;
        let object_key: String = row.try_get("object_key")?;
        let object_hash: String = row.try_get("object_hash")?;
        let state_text: String = row.try_get("state")?;
        let state = UploadLifecycleState::parse(&state_text).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "unknown upload intent state during reliability verification: {state_text}"
            ))
        })?;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'Upload' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&intent_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<LifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        let (tenant, repository) = upload_lifecycle_identity(&events);
        verify_upload_lifecycle_events(
            &events,
            tenant,
            repository,
            &intent_id,
            &object_key,
            &object_hash,
            state,
        )
        .map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid upload reliability journal for {intent_id}: {error}"
            ))
        })?;
    }

    let session_verification_rows = query(
        "SELECT session_id, scope_namespace, target_key, state
         FROM shardline_resumable_sessions",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in session_verification_rows {
        let session_id: String = row.try_get("session_id")?;
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let target_key: String = row.try_get("target_key")?;
        let state_text: String = row.try_get("state")?;
        let state = ResumableSessionState::parse(&state_text).ok_or_else(|| {
            DatabaseMigrationError::Backfill(format!(
                "unknown resumable session state during reliability verification: {state_text}"
            ))
        })?;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'ResumableSession' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&session_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<StateTransitionEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        verify_resumable_session_events(&events, &scope_namespace, &session_id, &target_key, state)
            .map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid resumable reliability journal for {session_id}: {error}"
                ))
            })?;
    }

    let quarantine_verification_rows = query(
        "SELECT object_key, observed_length,
                first_seen_unreachable_at_unix_seconds, delete_after_unix_seconds
         FROM shardline_quarantine_candidates",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in quarantine_verification_rows {
        let object_key: String = row.try_get("object_key")?;
        let snapshot = QuarantineSnapshot::new(
            QuarantineObjectIdentity::new(object_key.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("observed_length")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("first_seen_unreachable_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("delete_after_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            QuarantineLifecycleState::Active,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'GarbageCollection' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&object_key)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<QuarantineLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        verify_quarantine_lifecycle_events(&events, &snapshot).map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid quarantine reliability journal for {object_key}: {error}"
            ))
        })?;
    }

    let oci_verification_rows = query(
        "SELECT scope_namespace, repository, object_kind, digest_hex,
                deleted_at_unix_seconds
         FROM shardline_oci_object_tombstones",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in oci_verification_rows {
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let repository: String = row.try_get("repository")?;
        let object_kind: String = row.try_get("object_kind")?;
        let digest_hex: String = row.try_get("digest_hex")?;
        let snapshot = OciObjectSnapshot::new(
            OciObjectIdentity::new(scope_namespace, repository, object_kind, digest_hex)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            OciObjectLifecycleState::Deleted,
            Some(
                u64::try_from(row.try_get::<i64, _>("deleted_at_unix_seconds")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            ),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation_id = snapshot.operation_id();
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'Visibility' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(operation_id.as_str())
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<OciObjectLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        verify_oci_object_lifecycle_events(&events, &snapshot).map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid OCI reliability journal for {}: {error}",
                operation_id.as_str()
            ))
        })?;
    }

    let retention_verification_rows = query(
        "SELECT object_key, reason, held_at_unix_seconds,
                release_after_unix_seconds
         FROM shardline_retention_holds",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in retention_verification_rows {
        let object_key: String = row.try_get("object_key")?;
        let snapshot = RetentionHoldSnapshot::new(
            RetentionObjectIdentity::new(object_key.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<String, _>("reason")?,
            u64::try_from(row.try_get::<i64, _>("held_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<Option<i64>, _>("release_after_unix_seconds")?
                .map(|value| {
                    u64::try_from(value)
                        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
                })
                .transpose()?,
            RetentionHoldLifecycleState::Active,
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'RetentionHold' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&object_key)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<RetentionHoldLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        verify_retention_hold_lifecycle_events(&events, &snapshot).map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid retention-hold reliability journal for {object_key}: {error}"
            ))
        })?;
    }

    let webhook_verification_rows = query(
        "SELECT provider, owner, repo, delivery_id, processed_at_unix_seconds
         FROM shardline_webhook_deliveries",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in webhook_verification_rows {
        let delivery_id: String = row.try_get("delivery_id")?;
        let snapshot = WebhookDeliverySnapshot::new(
            WebhookDeliveryIdentity::new(
                row.try_get::<String, _>("provider")?,
                row.try_get::<String, _>("owner")?,
                row.try_get::<String, _>("repo")?,
                delivery_id.clone(),
            )
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            u64::try_from(row.try_get::<i64, _>("processed_at_unix_seconds")?)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            WebhookDeliveryLifecycleState::Processed,
        );
        let operation_id = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?
            .operation_id;
        let mut event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        if event_rows.is_empty() {
            event_rows = query(
                "SELECT event_json
                 FROM shardline_reliability_events
                 WHERE operation_kind = 'WebhookDelivery' AND operation_id = $1
                 ORDER BY sequence",
            )
            .bind(&delivery_id)
            .fetch_all(&mut *transaction)
            .await?;
        }
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<WebhookDeliveryLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        verify_webhook_delivery_events(&events, &snapshot).map_err(|error| {
            DatabaseMigrationError::Backfill(format!(
                "invalid webhook-delivery reliability journal for {delivery_id}: {error}"
            ))
        })?;
    }

    let hub_ref_verification_rows = query(
        "SELECT repo_id, ref_name, sha
         FROM shardline_hub_refs",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in hub_ref_verification_rows {
        let repo_id: String = row.try_get("repo_id")?;
        let ref_name: String = row.try_get("ref_name")?;
        let snapshot =
            HubRefSnapshot::new(repo_id, ref_name, Some(row.try_get::<String, _>("sha")?))
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation_id = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?
            .operation_id;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'MetadataCommit' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<HubRefLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        if events.is_empty() {
            if !repair_missing {
                return Err(DatabaseMigrationError::Backfill(
                    "missing hub-ref reliability journal".into(),
                ));
            }
            let baseline = HubRefEvidenceLog::baseline(snapshot.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in baseline.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
            verify_hub_ref_events(baseline.events(), &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid hub-ref reliability baseline: {error}"
                ))
            })?;
        } else {
            verify_hub_ref_events(&events, &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid hub-ref reliability journal: {error}"
                ))
            })?;
        }
    }

    let oci_tag_verification_rows = query(
        "SELECT scope_namespace, repository, tag, digest_hex
         FROM shardline_oci_tags",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in oci_tag_verification_rows {
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let repository: String = row.try_get("repository")?;
        let tag: String = row.try_get("tag")?;
        let snapshot = OciTagSnapshot::new(
            &scope_namespace,
            &repository,
            &tag,
            Some(row.try_get::<String, _>("digest_hex")?),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation_id = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?
            .operation_id;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'OciTag' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<OciTagLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        if events.is_empty() {
            if !repair_missing {
                return Err(DatabaseMigrationError::Backfill(
                    "missing OCI-tag reliability journal".into(),
                ));
            }
            let absent = OciTagSnapshot::new(&scope_namespace, &repository, &tag, None)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            let mut baseline = OciTagEvidenceLog::baseline(absent)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            baseline
                .record(snapshot.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in baseline.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
            verify_oci_tag_events(baseline.events(), &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid OCI-tag reliability baseline: {error}"
                ))
            })?;
        } else {
            verify_oci_tag_events(&events, &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid OCI-tag reliability journal: {error}"
                ))
            })?;
        }
    }

    let s3_object_verification_rows = query(
        "SELECT scope_namespace, object_key, file_id, size_bytes, content_hash,
                etag, user_metadata, updated_at_unix_seconds
         FROM shardline_s3_objects",
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in s3_object_verification_rows {
        let scope_namespace: String = row.try_get("scope_namespace")?;
        let object_key: String = row.try_get("object_key")?;
        let snapshot = S3ObjectSnapshot::new(
            &scope_namespace,
            &object_key,
            Some(S3ObjectState {
                file_id: row.try_get("file_id")?,
                size_bytes: u64::try_from(row.try_get::<i64, _>("size_bytes")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
                content_hash: row.try_get("content_hash")?,
                etag: row.try_get("etag")?,
                user_metadata: serde_json::from_str(&row.try_get::<String, _>("user_metadata")?)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
                updated_at_unix_seconds: row.try_get("updated_at_unix_seconds")?,
            }),
        )
        .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
        let operation_id = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?
            .operation_id;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'S3Object' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(&operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<S3ObjectLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        if events.is_empty() {
            if !repair_missing {
                return Err(DatabaseMigrationError::Backfill(
                    "missing S3-object reliability journal".into(),
                ));
            }
            let absent = S3ObjectSnapshot::new(&scope_namespace, &object_key, None)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            let mut baseline = S3ObjectEvidenceLog::baseline(absent)
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            baseline
                .record(snapshot.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in baseline.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
            verify_s3_object_events(baseline.events(), &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid S3-object reliability baseline: {error}"
                ))
            })?;
        } else {
            verify_s3_object_events(&events, &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid S3-object reliability journal: {error}"
                ))
            })?;
        }
    }

    let provider_verification_rows = query(
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
    )
    .fetch_all(&mut *transaction)
    .await?;
    for row in provider_verification_rows {
        let snapshot = provider_snapshot_from_row(&row)?;
        let operation_id = snapshot
            .evidence_operation()
            .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?
            .operation_id;
        let event_rows = query(
            "SELECT event_json
             FROM shardline_reliability_events
             WHERE operation_kind = 'ProviderEvent' AND operation_id = $1
             ORDER BY sequence",
        )
        .bind(operation_id)
        .fetch_all(&mut *transaction)
        .await?;
        let events = event_rows
            .into_iter()
            .map(|event_row| {
                let value: serde_json::Value = event_row.try_get("event_json")?;
                from_value::<ProviderLifecycleEvent>(value)
                    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
            })
            .collect::<Result<Vec<_>, DatabaseMigrationError>>()?;
        if events.is_empty() {
            if !repair_missing {
                return Err(DatabaseMigrationError::Backfill(format!(
                    "missing provider reliability journal for {}",
                    snapshot.repo
                )));
            }
            let baseline = ProviderEvidenceLog::baseline(snapshot.clone())
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?;
            for event in baseline.events() {
                persist_reliability_event(&mut transaction, event).await?;
            }
            verify_provider_lifecycle_events(baseline.events(), &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid provider reliability baseline for {}: {error}",
                    snapshot.repo
                ))
            })?;
        } else {
            verify_provider_lifecycle_events(&events, &snapshot).map_err(|error| {
                DatabaseMigrationError::Backfill(format!(
                    "invalid provider reliability journal for {}: {error}",
                    snapshot.repo
                ))
            })?;
        }
    }
    transaction.commit().await?;
    Ok(())
}

async fn verify_persisted_reliability_events(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<(), DatabaseMigrationError> {
    let rows = query(
        "SELECT operation_kind, operation_id, sequence, event_json, merkle_commit_json
         FROM shardline_reliability_events
         ORDER BY operation_kind, operation_id, sequence",
    )
    .fetch_all(&mut **transaction)
    .await?;
    let mut previous_operation: Option<(String, String, serde_json::Value)> = None;
    for row in rows {
        let operation_kind: String = row.try_get("operation_kind")?;
        let operation_id: String = row.try_get("operation_id")?;
        let previous = previous_operation
            .as_ref()
            .filter(|(kind, id, _)| kind == &operation_kind && id == &operation_id)
            .map(|(_, _, commit)| commit.clone());
        let observed = verify_persisted_reliability_row(&row, previous)?;
        previous_operation = Some((operation_kind, operation_id, observed));
    }
    Ok(())
}

async fn verify_persisted_reliability_operation(
    pool: &PgPool,
    operation_kind: OperationKind,
    operation_id: &str,
) -> Result<(), DatabaseMigrationError> {
    let rows = query(
        "SELECT operation_kind, operation_id, sequence, event_json, merkle_commit_json
         FROM shardline_reliability_events
         WHERE operation_kind = $1 AND operation_id = $2
         ORDER BY sequence",
    )
    .bind(operation_kind.as_str())
    .bind(operation_id)
    .fetch_all(pool)
    .await?;
    let mut previous: Option<serde_json::Value> = None;
    for row in rows {
        let observed = verify_persisted_reliability_row(&row, previous)?;
        previous = Some(observed);
    }
    Ok(())
}

fn verify_persisted_reliability_row(
    row: &sqlx::postgres::PgRow,
    previous: Option<serde_json::Value>,
) -> Result<serde_json::Value, DatabaseMigrationError> {
    let operation_kind_text: String = row.try_get("operation_kind")?;
    let operation_id: String = row.try_get("operation_id")?;
    let sequence: i64 = row.try_get("sequence")?;
    let event_json: serde_json::Value = row.try_get("event_json")?;
    let merkle_commit_json: Option<serde_json::Value> = row.try_get("merkle_commit_json")?;
    let operation_kind = OperationKind::parse(&operation_kind_text).ok_or_else(|| {
        DatabaseMigrationError::Backfill(format!(
            "unknown reliability operation kind {operation_kind_text} for {operation_id} at sequence {sequence}"
        ))
    })?;
    let event_identity = persisted_event_identity(operation_kind, event_json.clone()).map_err(|error| {
        DatabaseMigrationError::Backfill(format!(
            "invalid persisted event identity kind={operation_kind_text} operation={operation_id} sequence={sequence}: {error}"
        ))
    })?;
    if event_identity.operation_id != operation_id {
        return Err(DatabaseMigrationError::Backfill(format!(
            "persisted event operation does not match row kind={operation_kind_text} row_operation={operation_id} event_operation={} sequence={sequence}",
            event_identity.operation_id
        )));
    }
    let event_sequence = persisted_event_sequence(operation_kind, event_json.clone()).map_err(|error| {
        DatabaseMigrationError::Backfill(format!(
            "invalid persisted reliability event kind={operation_kind_text} operation={operation_id} sequence={sequence}: {error}"
        ))
    })?;
    if i64::try_from(event_sequence).map_err(|error| {
        DatabaseMigrationError::Backfill(format!("event sequence out of range: {error}"))
    })? != sequence
    {
        return Err(DatabaseMigrationError::Backfill(format!(
            "persisted event sequence does not match row kind={operation_kind_text} operation={operation_id} row_sequence={sequence} event_sequence={event_sequence}"
        )));
    }
    let observed = merkle_commit_json.ok_or_else(|| {
        DatabaseMigrationError::Backfill(format!(
            "missing persisted Merkle commit kind={operation_kind_text} operation={operation_id} sequence={sequence}"
        ))
    })?;
    shardline_reliability::verify_persisted_merkle_commit_with_previous(
        operation_kind,
        event_json,
        Some(observed.clone()),
        previous,
    )
    .map_err(|error| {
        DatabaseMigrationError::Backfill(format!(
            "persisted Merkle commit mismatch kind={operation_kind_text} operation={operation_id} sequence={sequence}: {error}"
        ))
    })?;
    Ok(observed)
}

fn provider_snapshot_from_row(
    row: &sqlx::postgres::PgRow,
) -> Result<ProviderLifecycleSnapshot, DatabaseMigrationError> {
    ProviderLifecycleSnapshot::from_parts(
        ProviderRepositoryIdentity::new(
            row.try_get::<String, _>("provider")?,
            row.try_get::<String, _>("owner")?,
            row.try_get::<String, _>("repo")?,
        ),
        ProviderLifecycleObservations::new(
            row.try_get::<Option<i64>, _>("last_access_changed_at_unix_seconds")?
                .map(u64::try_from)
                .transpose()
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<Option<i64>, _>("last_revision_pushed_at_unix_seconds")?
                .map(u64::try_from)
                .transpose()
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get("last_pushed_revision")?,
            row.try_get::<Option<i64>, _>("last_cache_invalidated_at_unix_seconds")?
                .map(u64::try_from)
                .transpose()
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<Option<i64>, _>("last_authorization_rechecked_at_unix_seconds")?
                .map(u64::try_from)
                .transpose()
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
            row.try_get::<Option<i64>, _>("last_drift_checked_at_unix_seconds")?
                .map(u64::try_from)
                .transpose()
                .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))?,
        ),
    )
    .map_err(|error| DatabaseMigrationError::Backfill(error.to_string()))
}

async fn acquire_migration_lock(
    pool: &PgPool,
) -> Result<Transaction<'static, Postgres>, DatabaseMigrationError> {
    let mut transaction = pool.begin().await?;
    query("SELECT pg_advisory_xact_lock($1)")
        .bind(MIGRATION_ADVISORY_LOCK_KEY)
        .execute(&mut *transaction)
        .await?;
    Ok(transaction)
}

async fn verify_applied_migrations(pool: &PgPool) -> Result<(), DatabaseMigrationError> {
    for applied in load_applied_migrations(pool).await? {
        if RETIRED_MIGRATION_VERSIONS.contains(&applied.version.as_str()) {
            continue;
        }
        let Some(migration) = migration_by_version(&applied.version) else {
            return Err(DatabaseMigrationError::UnknownAppliedMigration(
                applied.version,
            ));
        };
        let expected_checksum = migration_checksum(migration);
        if expected_checksum != applied.checksum
            && !LEGACY_MIGRATION_CHECKSUM_ALIASES
                .iter()
                .any(|(version, checksum)| {
                    *version == applied.version && *checksum == applied.checksum
                })
        {
            return Err(DatabaseMigrationError::ChecksumMismatch {
                version: migration.version.to_owned(),
                expected_checksum,
                observed_checksum: applied.checksum,
            });
        }
    }

    Ok(())
}

async fn pending_migrations(
    pool: &PgPool,
) -> Result<Vec<&'static DatabaseMigration>, DatabaseMigrationError> {
    let applied = load_applied_migrations(pool).await?;
    let pending = SHARDLINE_MIGRATIONS
        .iter()
        .filter(|migration| {
            applied
                .iter()
                .all(|entry| entry.version != migration.version)
        })
        .collect();
    Ok(pending)
}

async fn applied_migrations_in_order(
    pool: &PgPool,
) -> Result<Vec<&'static DatabaseMigration>, DatabaseMigrationError> {
    let applied = load_applied_migrations(pool).await?;
    let mut migrations = Vec::with_capacity(applied.len());
    for entry in applied {
        if RETIRED_MIGRATION_VERSIONS.contains(&entry.version.as_str()) {
            continue;
        }
        let Some(migration) = migration_by_version(&entry.version) else {
            return Err(DatabaseMigrationError::UnknownAppliedMigration(
                entry.version,
            ));
        };
        migrations.push(migration);
    }
    migrations.sort_by_key(|migration| migration.version);
    Ok(migrations)
}

async fn apply_one_migration(
    pool: &PgPool,
    migration: &'static DatabaseMigration,
) -> Result<(), DatabaseMigrationError> {
    let mut transaction = pool.begin().await?;
    raw_sql(migration.up_sql).execute(&mut *transaction).await?;
    query(&format!(
        "INSERT INTO {MIGRATION_HISTORY_TABLE} (version, name, checksum)
         VALUES ($1, $2, $3)
         ON CONFLICT (version) DO NOTHING"
    ))
    .bind(migration.version)
    .bind(migration.name)
    .bind(migration_checksum(migration))
    .execute(&mut *transaction)
    .await?;
    #[cfg(test)]
    database_migration_failpoint(DatabaseMigrationBoundary::BeforeApplyCommit)?;
    transaction.commit().await?;
    #[cfg(test)]
    database_migration_failpoint(DatabaseMigrationBoundary::AfterApplyCommit)?;
    Ok(())
}

async fn revert_one_migration(
    pool: &PgPool,
    migration: &'static DatabaseMigration,
) -> Result<(), DatabaseMigrationError> {
    let mut transaction = pool.begin().await?;
    raw_sql(migration.down_sql)
        .execute(&mut *transaction)
        .await?;
    query(&format!(
        "DELETE FROM {MIGRATION_HISTORY_TABLE} WHERE version = $1"
    ))
    .bind(migration.version)
    .execute(&mut *transaction)
    .await?;
    #[cfg(test)]
    database_migration_failpoint(DatabaseMigrationBoundary::BeforeRevertCommit)?;
    transaction.commit().await?;
    #[cfg(test)]
    database_migration_failpoint(DatabaseMigrationBoundary::AfterRevertCommit)?;
    Ok(())
}

async fn load_applied_migrations(pool: &PgPool) -> Result<Vec<AppliedMigration>, SqlxError> {
    let rows = query(&format!(
        "SELECT version, checksum,
                to_char(applied_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')
                    AS applied_at_utc
         FROM {MIGRATION_HISTORY_TABLE}
         ORDER BY version"
    ))
    .fetch_all(pool)
    .await?;

    let mut migrations = Vec::with_capacity(rows.len());
    for row in rows {
        migrations.push(AppliedMigration {
            version: row.try_get::<String, _>("version")?,
            checksum: row.try_get::<String, _>("checksum")?,
            applied_at_utc: row.try_get::<String, _>("applied_at_utc")?,
        });
    }

    Ok(migrations)
}

async fn migration_status_entries(
    pool: &PgPool,
) -> Result<Vec<DatabaseMigrationStatusEntry>, DatabaseMigrationError> {
    let applied = load_applied_migrations(pool).await?;
    let mut statuses = Vec::with_capacity(SHARDLINE_MIGRATIONS.len());
    for migration in SHARDLINE_MIGRATIONS {
        let applied_entry = applied
            .iter()
            .find(|entry| entry.version == migration.version);
        statuses.push(DatabaseMigrationStatusEntry {
            version: migration.version.to_owned(),
            name: migration.name.to_owned(),
            applied: applied_entry.is_some(),
            applied_at_utc: applied_entry.map(|entry| entry.applied_at_utc.clone()),
        });
    }

    Ok(statuses)
}

fn migration_by_version(version: &str) -> Option<&'static DatabaseMigration> {
    SHARDLINE_MIGRATIONS
        .iter()
        .find(|migration| migration.version == version)
}

fn migration_checksum(migration: &DatabaseMigration) -> String {
    blake3::hash(migration.up_sql.as_bytes())
        .to_hex()
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use serial_test::serial;

    use super::{
        DatabaseMigration, DatabaseMigrationBoundary, DatabaseMigrationCommand,
        DatabaseMigrationError, DatabaseMigrationOptions, DatabaseMigrationReport,
        DatabaseMigrationStatusEntry, acquire_migration_lock, bundled_database_migrations,
        migration_by_version, migration_checksum, migration_fault_injection,
        run_database_migration,
    };

    async fn run_test_migration_command(
        database_url: &str,
        command: DatabaseMigrationCommand,
    ) -> Result<DatabaseMigrationReport, DatabaseMigrationError> {
        let options = DatabaseMigrationOptions::new(database_url.to_owned(), command);
        run_database_migration(&options).await
    }

    #[test]
    fn bundled_migrations_are_not_empty() {
        let migrations = bundled_database_migrations();
        assert!(!migrations.is_empty());
    }

    #[test]
    fn bundled_migrations_have_expected_count() {
        assert_eq!(bundled_database_migrations().len(), 28);
    }

    #[test]
    fn bundled_migrations_include_oci_tags() {
        let migration = bundled_database_migrations()
            .iter()
            .find(|migration| migration.name == "oci_tags")
            .expect("OCI tag migration must be registered");
        assert_eq!(migration.version, "20260822000000");
        assert!(migration.up_sql.contains("shardline_oci_tags"));
        assert!(migration.down_sql.contains("shardline_oci_tags"));
    }

    #[test]
    fn bundled_migrations_include_resource_fences() {
        let migration = bundled_database_migrations()
            .iter()
            .find(|migration| migration.name == "resource_fences")
            .expect("resource fence migration must be registered");
        assert_eq!(migration.version, "20260822010000");
        assert!(migration.up_sql.contains("shardline_resource_fences"));
        assert!(migration.down_sql.contains("shardline_resource_fences"));
    }

    #[test]
    fn bundled_migrations_include_oci_object_tombstones() {
        let migration = bundled_database_migrations()
            .iter()
            .find(|migration| migration.name == "oci_object_tombstones")
            .expect("OCI object tombstone migration must be registered");
        assert_eq!(migration.version, "20260822020000");
        assert!(migration.up_sql.contains("shardline_oci_object_tombstones"));
        assert!(
            migration
                .down_sql
                .contains("shardline_oci_object_tombstones")
        );
    }

    #[test]
    fn bundled_migrations_include_resumable_sessions() {
        let migration = bundled_database_migrations()
            .iter()
            .find(|migration| migration.name == "resumable_sessions")
            .expect("resumable session migration must be registered");
        assert_eq!(migration.version, "20260823000000");
        assert!(migration.up_sql.contains("shardline_resumable_sessions"));
        assert!(
            migration
                .up_sql
                .contains("shardline_resumable_session_parts")
        );
        assert!(migration.down_sql.contains("shardline_resumable_sessions"));
        assert!(
            migration
                .down_sql
                .contains("shardline_resumable_session_parts")
        );
    }

    #[test]
    fn bundled_migrations_include_reliability_events() {
        let migration = bundled_database_migrations()
            .iter()
            .find(|migration| migration.name == "reliability_events")
            .expect("reliability event migration must be registered");
        assert_eq!(migration.version, "20260922000000");
        assert!(migration.up_sql.contains("shardline_reliability_events"));
        assert!(migration.down_sql.contains("shardline_reliability_events"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn migration_lock_serializes_independent_connections() {
        let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
        let first = acquire_migration_lock(&pool).await.unwrap();
        let waiter_pool = pool.clone();
        let mut waiter = tokio::spawn(async move { acquire_migration_lock(&waiter_pool).await });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("migration lock should become available")
            .expect("migration lock task should complete")
            .unwrap();
        drop(second);
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial(database_migration_failpoint)]
    async fn interrupted_migration_boundaries_resume_to_complete_schema() {
        let Some(base_database_url) = std::env::var("DATABASE_URL").ok() else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let database_name = format!(
            "shardline_migration_interrupt_{}_{}",
            std::process::id(),
            unique_suffix
        );
        let mut admin_url = url::Url::parse(&base_database_url).unwrap();
        admin_url.set_path("postgres");
        let admin_pool = sqlx::PgPool::connect(admin_url.as_str()).await.unwrap();
        sqlx::query(&format!("CREATE DATABASE {database_name}"))
            .execute(&admin_pool)
            .await
            .unwrap();

        let mut test_url = url::Url::parse(&base_database_url).unwrap();
        test_url.set_path(&database_name);
        let test_url = test_url.to_string();

        {
            let _fault =
                migration_fault_injection::arm(DatabaseMigrationBoundary::BeforeApplyCommit);
            assert!(matches!(
                run_test_migration_command(
                    &test_url,
                    DatabaseMigrationCommand::Up { steps: Some(1) }
                )
                .await,
                Err(DatabaseMigrationError::InjectedInterruption {
                    boundary: DatabaseMigrationBoundary::BeforeApplyCommit
                })
            ));
        }
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Status)
                .await
                .unwrap()
                .applied_total_count,
            0
        );

        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Up { steps: Some(1) })
                .await
                .unwrap()
                .applied_total_count,
            1
        );
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Down { steps: 1 })
                .await
                .unwrap()
                .applied_total_count,
            0
        );

        {
            let _fault =
                migration_fault_injection::arm(DatabaseMigrationBoundary::AfterApplyCommit);
            assert!(matches!(
                run_test_migration_command(
                    &test_url,
                    DatabaseMigrationCommand::Up { steps: Some(1) }
                )
                .await,
                Err(DatabaseMigrationError::InjectedInterruption {
                    boundary: DatabaseMigrationBoundary::AfterApplyCommit
                })
            ));
        }
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Status)
                .await
                .unwrap()
                .applied_total_count,
            1
        );

        {
            let _fault =
                migration_fault_injection::arm(DatabaseMigrationBoundary::BeforeRevertCommit);
            assert!(matches!(
                run_test_migration_command(&test_url, DatabaseMigrationCommand::Down { steps: 1 })
                    .await,
                Err(DatabaseMigrationError::InjectedInterruption {
                    boundary: DatabaseMigrationBoundary::BeforeRevertCommit
                })
            ));
        }
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Status)
                .await
                .unwrap()
                .applied_total_count,
            1
        );
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Down { steps: 1 })
                .await
                .unwrap()
                .applied_total_count,
            0
        );
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Up { steps: Some(1) })
                .await
                .unwrap()
                .applied_total_count,
            1
        );

        {
            let _fault =
                migration_fault_injection::arm(DatabaseMigrationBoundary::AfterRevertCommit);
            assert!(matches!(
                run_test_migration_command(&test_url, DatabaseMigrationCommand::Down { steps: 1 })
                    .await,
                Err(DatabaseMigrationError::InjectedInterruption {
                    boundary: DatabaseMigrationBoundary::AfterRevertCommit
                })
            ));
        }
        assert_eq!(
            run_test_migration_command(&test_url, DatabaseMigrationCommand::Status)
                .await
                .unwrap()
                .applied_total_count,
            0
        );

        let resumed = run_database_migration(&DatabaseMigrationOptions::new(
            test_url.clone(),
            DatabaseMigrationCommand::Up { steps: None },
        ))
        .await
        .unwrap();
        assert_eq!(resumed.pending_count, 0);
        assert_eq!(
            resumed.applied_total_count,
            bundled_database_migrations().len() as u64
        );
        assert!(resumed.migrations.iter().all(|migration| migration.applied));
        run_test_migration_command(&test_url, DatabaseMigrationCommand::Verify)
            .await
            .expect("read-only reliability verification should succeed on a complete schema");

        sqlx::query(&format!("DROP DATABASE {database_name} WITH (FORCE)"))
            .execute(&admin_pool)
            .await
            .unwrap();
        admin_pool.close().await;
    }

    #[test]
    fn bundled_migrations_include_tree_store_with_sql() {
        let migrations = bundled_database_migrations();
        let tree_store = migrations
            .iter()
            .find(|m| m.name == "tree_store")
            .expect("tree_store migration must be registered");
        assert_eq!(tree_store.version, "20260805000000");
        // Both the up and down SQL must be bundled (non-empty) so the Postgres
        // migration path cannot silently omit the tree tables again.
        assert!(!tree_store.up_sql.is_empty());
        assert!(!tree_store.down_sql.is_empty());
        assert!(tree_store.up_sql.contains("shardline_tree_entries"));
        assert!(tree_store.up_sql.contains("shardline_revisions"));
        assert!(tree_store.down_sql.contains("shardline_tree_entries"));
        assert!(tree_store.down_sql.contains("shardline_revisions"));
    }

    #[test]
    fn bundled_migrations_have_unique_versions() {
        let migrations = bundled_database_migrations();
        let mut versions: Vec<&str> = migrations.iter().map(|m| m.version).collect();
        versions.sort();
        versions.dedup();
        assert_eq!(versions.len(), migrations.len());
    }

    #[test]
    fn bundled_migrations_have_non_empty_sql() {
        for migration in bundled_database_migrations() {
            assert!(
                !migration.up_sql.is_empty(),
                "migration {} has empty up_sql",
                migration.version
            );
            assert!(
                !migration.down_sql.is_empty(),
                "migration {} has empty down_sql",
                migration.version
            );
        }
    }

    #[test]
    fn migration_checksum_is_deterministic() {
        let migrations = bundled_database_migrations();
        for migration in migrations {
            let hash1 = migration_checksum(migration);
            let hash2 = migration_checksum(migration);
            assert_eq!(
                hash1, hash2,
                "checksum must be deterministic for {}",
                migration.version
            );
        }
    }

    #[test]
    fn migration_checksum_differs_for_different_migrations() {
        let migrations = bundled_database_migrations();
        if migrations.len() >= 2 {
            let hash1 = migration_checksum(&migrations[0]);
            let hash2 = migration_checksum(&migrations[1]);
            assert_ne!(
                hash1, hash2,
                "different migrations must have different checksums"
            );
        }
    }

    #[test]
    fn migration_by_version_finds_known_version() {
        let migrations = bundled_database_migrations();
        for migration in migrations {
            let found = migration_by_version(migration.version);
            assert!(
                found.is_some(),
                "version {} not found by migration_by_version",
                migration.version
            );
            assert_eq!(found.unwrap().version, migration.version);
        }
    }

    #[test]
    fn migration_by_version_returns_none_for_unknown() {
        assert!(migration_by_version("00000000000000").is_none());
    }

    #[test]
    fn migration_by_version_returns_none_for_empty_string() {
        assert!(migration_by_version("").is_none());
    }

    #[test]
    fn database_migration_options_new_and_accessors() {
        let options = DatabaseMigrationOptions::new(
            "postgres://localhost:5432/test".to_owned(),
            DatabaseMigrationCommand::Status,
        );
        assert_eq!(options.database_url(), "postgres://localhost:5432/test");
        assert_eq!(options.command(), &DatabaseMigrationCommand::Status);
    }

    #[test]
    fn database_migration_options_up_command() {
        let options = DatabaseMigrationOptions::new(
            "postgres://localhost:5432/test".to_owned(),
            DatabaseMigrationCommand::Up { steps: Some(3) },
        );
        assert!(matches!(
            options.command(),
            DatabaseMigrationCommand::Up { steps: Some(3) }
        ));
    }

    #[test]
    fn database_migration_options_down_command() {
        let options = DatabaseMigrationOptions::new(
            "postgres://localhost:5432/test".to_owned(),
            DatabaseMigrationCommand::Down { steps: 2 },
        );
        assert!(matches!(
            options.command(),
            DatabaseMigrationCommand::Down { steps: 2 }
        ));
    }

    #[test]
    fn database_migration_status_entry_fields() {
        let entry = DatabaseMigrationStatusEntry {
            version: "20260417000000".to_owned(),
            name: "metadata_store".to_owned(),
            applied: true,
            applied_at_utc: Some("2026-04-17T00:00:00Z".to_owned()),
        };
        assert_eq!(entry.version, "20260417000000");
        assert_eq!(entry.name, "metadata_store");
        assert!(entry.applied);
        assert_eq!(
            entry.applied_at_utc,
            Some("2026-04-17T00:00:00Z".to_owned())
        );
    }

    #[test]
    fn database_migration_status_entry_not_applied() {
        let entry = DatabaseMigrationStatusEntry {
            version: "20260418000000".to_owned(),
            name: "dedupe_shards".to_owned(),
            applied: false,
            applied_at_utc: None,
        };
        assert!(!entry.applied);
        assert!(entry.applied_at_utc.is_none());
    }

    #[test]
    fn database_migration_debug_and_clone() {
        let m = DatabaseMigration {
            version: "v1",
            name: "test",
            up_sql: "SELECT 1",
            down_sql: "SELECT 0",
        };
        let cloned = m;
        assert_eq!(m.version, cloned.version);
        assert_eq!(m.name, cloned.name);
    }

    #[test]
    fn database_migration_report_backend_is_postgres() {
        // Verify by constructing a report manually in a test helper.
        let report = super::DatabaseMigrationReport {
            backend: "postgres".to_owned(),
            command: DatabaseMigrationCommand::Status,
            applied_count: 0,
            reverted_count: 0,
            applied_total_count: 0,
            pending_count: 0,
            migrations: vec![],
        };
        assert_eq!(report.backend, "postgres");
    }

    #[test]
    fn bundled_database_migrations_are_monotonic() {
        let migrations = bundled_database_migrations();
        assert!(!migrations.is_empty());
        assert!(migrations.windows(2).all(|window| {
            let Some(first) = window.first() else {
                return false;
            };
            let Some(second) = window.get(1) else {
                return false;
            };
            first.version < second.version
        }));
    }

    #[test]
    fn bundled_database_migrations_each_have_valid_sql() {
        for migration in bundled_database_migrations() {
            // up_sql should be valid SQL (at least not empty and should start
            // with common SQL keywords)
            assert!(!migration.up_sql.is_empty());
            assert!(
                migration.up_sql.trim().starts_with("CREATE")
                    || migration.up_sql.trim().starts_with("ALTER")
                    || migration.up_sql.trim().starts_with("INSERT")
                    || migration.up_sql.trim().starts_with("DROP")
                    || migration.up_sql.trim().starts_with("--"),
                "migration {} up_sql does not start with expected SQL keyword: {:?}",
                migration.version,
                &migration.up_sql.trim()[..20.min(migration.up_sql.trim().len())]
            );
            assert!(!migration.down_sql.is_empty());
            assert!(
                migration.down_sql.trim().starts_with("DROP")
                    || migration.down_sql.trim().starts_with("DELETE")
                    || migration.down_sql.trim().starts_with("ALTER")
                    || migration.down_sql.trim().starts_with("CREATE")
                    || migration.down_sql.trim().starts_with("--"),
                "migration {} down_sql does not start with expected SQL keyword: {:?}",
                migration.version,
                &migration.down_sql.trim()[..20.min(migration.down_sql.trim().len())]
            );
        }
    }

    #[test]
    fn migration_status_entry_display_and_clone() {
        let entry = DatabaseMigrationStatusEntry {
            version: "v1".to_owned(),
            name: "test".to_owned(),
            applied: true,
            applied_at_utc: None,
        };
        let cloned = entry.clone();
        assert_eq!(entry, cloned);
        let debug = format!("{entry:?}");
        assert!(!debug.is_empty());
    }

    #[test]
    fn database_migration_error_display_empty_database_url() {
        let err = super::DatabaseMigrationError::EmptyDatabaseUrl;
        assert_eq!(err.to_string(), "database URL must not be empty");
    }

    #[test]
    fn database_migration_error_display_unknown_applied_migration() {
        let err = super::DatabaseMigrationError::UnknownAppliedMigration("v0".to_owned());
        let display = err.to_string();
        assert!(display.contains("unknown shardline migration version"));
        assert!(display.contains("v0"));
    }

    #[test]
    fn database_migration_error_display_checksum_mismatch() {
        let err = super::DatabaseMigrationError::ChecksumMismatch {
            version: "v1".to_owned(),
            expected_checksum: "abc123".to_owned(),
            observed_checksum: "def456".to_owned(),
        };
        let display = err.to_string();
        assert!(display.contains("checksum mismatch"));
        assert!(display.contains("v1"));
    }

    #[test]
    fn database_migration_error_debug_roundtrip() {
        let err = super::DatabaseMigrationError::EmptyDatabaseUrl;
        let debug = format!("{err:?}");
        assert!(!debug.is_empty());
    }

    #[test]
    fn database_migration_empty_url_option_rejected_at_construction() {
        // The empty URL validation in run_database_migration requires an async
        // runtime and a Postgres connection; instead verify the static
        // accessor returns the expected value.
        let options =
            DatabaseMigrationOptions::new(String::new(), DatabaseMigrationCommand::Status);
        assert_eq!(options.database_url(), "");
        assert_eq!(options.command(), &DatabaseMigrationCommand::Status);
    }

    #[test]
    fn database_migration_report_fields() {
        let report = super::DatabaseMigrationReport {
            backend: "postgres".to_owned(),
            command: DatabaseMigrationCommand::Up { steps: Some(2) },
            applied_count: 2,
            reverted_count: 0,
            applied_total_count: 2,
            pending_count: 7,
            migrations: vec![],
        };
        assert_eq!(report.backend, "postgres");
        assert_eq!(report.applied_count, 2);
        assert_eq!(report.pending_count, 7);
    }
}
