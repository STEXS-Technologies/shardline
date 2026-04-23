use sqlx::{Error as SqlxError, PgPool, Row, postgres::PgPoolOptions, query, raw_sql};
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
    database_url: String,
    command: DatabaseMigrationCommand,
}

impl DatabaseMigrationOptions {
    /// Creates database-migration options.
    #[must_use]
    pub const fn new(database_url: String, command: DatabaseMigrationCommand) -> Self {
        Self {
            database_url,
            command,
        }
    }

    /// Returns the Postgres connection URL.
    #[must_use]
    pub fn database_url(&self) -> &str {
        &self.database_url
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

const SHARDLINE_MIGRATIONS: [DatabaseMigration; 6] = [
    DatabaseMigration {
        version: "20260417000000",
        name: "metadata_store",
        up_sql: include_str!("../../../migrations/20260417000000_metadata_store.up.sql"),
        down_sql: include_str!("../../../migrations/20260417000000_metadata_store.down.sql"),
    },
    DatabaseMigration {
        version: "20260417010000",
        name: "retention_holds",
        up_sql: include_str!("../../../migrations/20260417010000_retention_holds.up.sql"),
        down_sql: include_str!("../../../migrations/20260417010000_retention_holds.down.sql"),
    },
    DatabaseMigration {
        version: "20260418000000",
        name: "dedupe_shards",
        up_sql: include_str!("../../../migrations/20260418000000_dedupe_shards.up.sql"),
        down_sql: include_str!("../../../migrations/20260418000000_dedupe_shards.down.sql"),
    },
    DatabaseMigration {
        version: "20260418010000",
        name: "webhook_deliveries",
        up_sql: include_str!("../../../migrations/20260418010000_webhook_deliveries.up.sql"),
        down_sql: include_str!("../../../migrations/20260418010000_webhook_deliveries.down.sql"),
    },
    DatabaseMigration {
        version: "20260418020000",
        name: "provider_repository_states",
        up_sql: include_str!(
            "../../../migrations/20260418020000_provider_repository_states.up.sql"
        ),
        down_sql: include_str!(
            "../../../migrations/20260418020000_provider_repository_states.down.sql"
        ),
    },
    DatabaseMigration {
        version: "20260418110000",
        name: "provider_repository_reconciliation",
        up_sql: include_str!(
            "../../../migrations/20260418110000_provider_repository_reconciliation.up.sql"
        ),
        down_sql: include_str!(
            "../../../migrations/20260418110000_provider_repository_reconciliation.down.sql"
        ),
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
    use std::{env::var as env_var, error::Error, process::id as process_id};

    use sqlx::{PgPool, postgres::PgPoolOptions, query};
    use url::Url;

    use super::{
        DatabaseMigrationCommand, DatabaseMigrationOptions, apply_database_migrations,
        bundled_database_migrations, run_database_migration,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn database_migration_up_status_and_down_cover_full_lifecycle() {
        let result = exercise_database_migration_up_status_and_down_cover_full_lifecycle().await;
        let error = result.as_ref().err().map(ToString::to_string);
        assert!(
            result.is_ok(),
            "database migration lifecycle failed: {error:?}"
        );
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

    async fn exercise_database_migration_up_status_and_down_cover_full_lifecycle()
    -> Result<(), Box<dyn Error>> {
        let Some(base_url) = env_var("DATABASE_URL").ok() else {
            return Ok(());
        };

        let database_name = format!("shardline_db_migrate_{}", process_id());
        let admin_url = database_url_for(&base_url, "postgres")?;
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;
        recreate_database(&admin_pool, &database_name).await?;

        let database_url = database_url_for(&base_url, &database_name)?;
        let status_before = run_database_migration(&DatabaseMigrationOptions::new(
            database_url.clone(),
            DatabaseMigrationCommand::Status,
        ))
        .await?;
        assert_eq!(status_before.applied_count, 0);
        assert_eq!(status_before.reverted_count, 0);
        assert_eq!(status_before.applied_total_count, 0);
        assert_eq!(
            status_before.pending_count,
            u64::try_from(bundled_database_migrations().len())?
        );

        let up = run_database_migration(&DatabaseMigrationOptions::new(
            database_url.clone(),
            DatabaseMigrationCommand::Up { steps: Some(2) },
        ))
        .await?;
        assert_eq!(up.applied_count, 2);
        assert_eq!(up.reverted_count, 0);
        assert_eq!(up.applied_total_count, 2);

        let up_rest = run_database_migration(&DatabaseMigrationOptions::new(
            database_url.clone(),
            DatabaseMigrationCommand::Up { steps: None },
        ))
        .await?;
        assert_eq!(
            up_rest.applied_count,
            u64::try_from(bundled_database_migrations().len().saturating_sub(2))?
        );
        assert_eq!(up_rest.pending_count, 0);

        let down = run_database_migration(&DatabaseMigrationOptions::new(
            database_url.clone(),
            DatabaseMigrationCommand::Down { steps: 1 },
        ))
        .await?;
        assert_eq!(down.reverted_count, 1);
        assert_eq!(down.pending_count, 1);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;
        apply_database_migrations(&pool).await?;

        let final_status = run_database_migration(&DatabaseMigrationOptions::new(
            database_url,
            DatabaseMigrationCommand::Status,
        ))
        .await?;
        assert_eq!(final_status.pending_count, 0);
        assert_eq!(
            final_status.applied_total_count,
            u64::try_from(bundled_database_migrations().len())?
        );

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
}
