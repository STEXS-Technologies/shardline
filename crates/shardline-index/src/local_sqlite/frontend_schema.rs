//! Additive frontend tables, separate from the released core schema ledger.
//!
//! Installation is atomic and retains the original installation time when
//! adopting the two known prerelease core entries. No other history is moved.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{LOCAL_SCHEMA_MIGRATIONS_TABLE, LocalIndexStoreError, migration::FRONTEND_MIGRATIONS};

const HISTORY: &str = "shardline_frontend_schema_v1";
const S3_MULTIPART_INVALIDATION_TRIGGERS: &str = "
CREATE TRIGGER IF NOT EXISTS shardline_s3_multipart_counts_after_insert
AFTER INSERT ON shardline_s3_objects
BEGIN
    DELETE FROM shardline_s3_multipart_counts
    WHERE scope_namespace=NEW.scope_namespace AND object_key=NEW.object_key;
END;
CREATE TRIGGER IF NOT EXISTS shardline_s3_multipart_counts_after_update
AFTER UPDATE ON shardline_s3_objects
BEGIN
    DELETE FROM shardline_s3_multipart_counts
    WHERE scope_namespace=OLD.scope_namespace AND object_key=OLD.object_key;
    DELETE FROM shardline_s3_multipart_counts
    WHERE scope_namespace=NEW.scope_namespace AND object_key=NEW.object_key;
END;
CREATE TRIGGER IF NOT EXISTS shardline_s3_multipart_counts_after_delete
AFTER DELETE ON shardline_s3_objects
BEGIN
    DELETE FROM shardline_s3_multipart_counts
    WHERE scope_namespace=OLD.scope_namespace AND object_key=OLD.object_key;
END;
";

fn incompatible(name: &str) -> LocalIndexStoreError {
    LocalIndexStoreError::FrontendSchemaIncompatible(name.to_owned())
}

pub(super) fn install(connection: &mut Connection) -> Result<(), LocalIndexStoreError> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {HISTORY} (
            version TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            definition TEXT NOT NULL,
            applied_at_unix_seconds INTEGER NOT NULL
        );"
    ))?;
    reject_unknown_history(&transaction)?;
    for migration in FRONTEND_MIGRATIONS {
        let legacy: Option<(String, i64)> = transaction.query_row(
            &format!("SELECT name,applied_at_unix_seconds FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE} WHERE version=?1"),
            [migration.version],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if legacy
            .as_ref()
            .is_some_and(|(name, _)| name != migration.name)
        {
            return Err(incompatible(migration.name));
        }
        let installed: Option<(String, String, i64)> = transaction
            .query_row(
                &format!(
                    "SELECT name,definition,applied_at_unix_seconds FROM {HISTORY} WHERE version=?1"
                ),
                [migration.version],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        if let Some((name, definition, timestamp)) = installed {
            if name != migration.name
                || definition != migration.up_sql
                || legacy
                    .as_ref()
                    .is_some_and(|(_, legacy_time)| *legacy_time != timestamp)
            {
                return Err(incompatible(migration.name));
            }
        } else {
            // A prerelease ledger entry means its DDL has already run. Do not
            // repeat ALTER TABLE; check() below verifies every adopted object
            // before the history transfer can commit.
            if legacy.is_none() {
                transaction.execute_batch(migration.up_sql)?;
            }
            let timestamp = if let Some((_, timestamp)) = legacy.as_ref() {
                *timestamp
            } else {
                super::helpers::u64_to_i64(shardline_protocol::unix_now_seconds_lossy())?
            };
            transaction.execute(
                &format!("INSERT INTO {HISTORY} (version,name,definition,applied_at_unix_seconds) VALUES (?1,?2,?3,?4)"),
                params![migration.version, migration.name, migration.up_sql, timestamp],
            )?;
        }
        // Preserve the full original row in the dedicated history before
        // removing only this known additive entry from the core ledger.
        if legacy.is_some() {
            transaction.execute(
                &format!("DELETE FROM {LOCAL_SCHEMA_MIGRATIONS_TABLE} WHERE version=?1"),
                [migration.version],
            )?;
        }
    }
    transaction.execute_batch(S3_MULTIPART_INVALIDATION_TRIGGERS)?;
    check(&transaction)?;
    transaction.commit()?;
    Ok(())
}

/// Read-only startup verification, including tables, constraints and indexes.
pub(super) fn check(connection: &Connection) -> Result<(), LocalIndexStoreError> {
    let exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1",
            [HISTORY],
            |_| Ok(()),
        )
        .optional()?
        .is_some();
    if !exists {
        return Err(incompatible("frontend installation history"));
    }
    reject_unknown_history(connection)?;
    let expected = Connection::open_in_memory()?;
    // Creation policy extends the released repository table. Build that table
    // from its original definition so comparison covers the added columns too.
    let (repository_schema, _) = include_str!("../../migrations/20260629000000_hub_api.up.sql")
        .split_once(';')
        .ok_or_else(|| incompatible("repository schema definition"))?;
    expected.execute_batch(repository_schema)?;
    // Source tables belong to the unchanged core ledger. This stub lets the
    // derived frontend index install its triggers in the comparison database.
    expected.execute_batch(
        "CREATE TABLE shardline_file_records (
        record_key TEXT PRIMARY KEY, scope_key TEXT NOT NULL, record TEXT NOT NULL,
        file_id TEXT NOT NULL, record_kind TEXT NOT NULL
    );
    CREATE TABLE shardline_hub_file_entries (
        commit_sha TEXT NOT NULL, sha TEXT NOT NULL, is_lfs INTEGER NOT NULL,
        size INTEGER NOT NULL
    );",
    )?;
    for migration in FRONTEND_MIGRATIONS {
        let installed: Option<(String, String)> = connection
            .query_row(
                &format!("SELECT name,definition FROM {HISTORY} WHERE version=?1"),
                [migration.version],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if installed.as_ref().is_none_or(|(name, definition)| {
            name != migration.name || definition != migration.up_sql
        }) {
            return Err(incompatible(migration.name));
        }
        expected.execute_batch(migration.up_sql)?;
    }
    let mut statement = expected.prepare(
        "SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
         AND name NOT IN ('shardline_file_records','shardline_hub_file_entries','shardline_hub_repos')",
    )?;
    let definitions = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for definition in definitions {
        let (name, sql) = definition?;
        let actual: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE name=?1",
                [&name],
                |row| row.get(0),
            )
            .optional()?;
        if actual.as_ref() != Some(&sql) {
            return Err(incompatible(&name));
        }
    }
    // Existing core tables can have equivalent legacy formatting. Compare the
    // new declarations, including their defaults and CHECK, rather than making
    // their original whitespace a new compatibility requirement.
    let repository_sql = |database: &Connection| -> Result<String, LocalIndexStoreError> {
        Ok(database.query_row(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='shardline_hub_repos'",
            [],
            |row| row.get(0),
        )?)
    };
    let expected_repository = repository_sql(&expected)?;
    let actual_repository = repository_sql(connection)?;
    let expected_creation = expected_repository
        .split_once("creation_authorized")
        .map(|(_, suffix)| suffix);
    let actual_creation = actual_repository
        .split_once("creation_authorized")
        .map(|(_, suffix)| suffix);
    if expected_creation.is_none() || actual_creation != expected_creation {
        return Err(incompatible("repository creation columns"));
    }
    for name in [
        "shardline_s3_multipart_counts_after_insert",
        "shardline_s3_multipart_counts_after_update",
        "shardline_s3_multipart_counts_after_delete",
    ] {
        let sql: Option<String> = connection
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?1",
                [name],
                |row| row.get(0),
            )
            .optional()?;
        if sql
            .as_deref()
            .is_none_or(|sql| !sql.contains("shardline_s3_multipart_counts"))
        {
            return Err(incompatible(name));
        }
    }
    Ok(())
}

fn reject_unknown_history(connection: &Connection) -> Result<(), LocalIndexStoreError> {
    let mut statement =
        connection.prepare(&format!("SELECT version FROM {HISTORY} ORDER BY version"))?;
    let versions = statement.query_map([], |row| row.get::<_, String>(0))?;
    for version in versions {
        let version = version?;
        if !FRONTEND_MIGRATIONS
            .iter()
            .any(|migration| migration.version == version)
        {
            return Err(incompatible(&version));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
