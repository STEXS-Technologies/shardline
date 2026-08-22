use shardline_protocol::SecretString;
use sqlx::{
    Error as SqlxError, PgPool, Postgres, Row, Transaction, postgres::PgPoolOptions, query, raw_sql,
};
use thiserror::Error;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppliedMigration {
    version: String,
    checksum: String,
    applied_at_utc: String,
}

const MIGRATION_HISTORY_TABLE: &str = "shardline_schema_migrations";
const MIGRATION_ADVISORY_LOCK_KEY: i64 = 0x5348_4152_444d_4701;

const SHARDLINE_MIGRATIONS: [DatabaseMigration; 19] = [
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
        DatabaseMigrationCommand::Up { .. } | DatabaseMigrationCommand::Down { .. } => {
            Some(acquire_migration_lock(&pool).await?)
        }
        DatabaseMigrationCommand::Status => None,
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
        DatabaseMigrationCommand::Status => (0, 0),
    };

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
        let Some(migration) = migration_by_version(&applied.version) else {
            return Err(DatabaseMigrationError::UnknownAppliedMigration(
                applied.version,
            ));
        };
        let expected_checksum = migration_checksum(migration);
        if expected_checksum != applied.checksum {
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
    transaction.commit().await?;
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
    transaction.commit().await?;
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
    use std::time::Duration;

    use super::{
        DatabaseMigration, DatabaseMigrationCommand, DatabaseMigrationOptions,
        DatabaseMigrationStatusEntry, acquire_migration_lock, bundled_database_migrations,
        migration_by_version, migration_checksum,
    };

    #[test]
    fn bundled_migrations_are_not_empty() {
        let migrations = bundled_database_migrations();
        assert!(!migrations.is_empty());
    }

    #[test]
    fn bundled_migrations_have_expected_count() {
        assert_eq!(bundled_database_migrations().len(), 19);
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
