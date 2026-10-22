//! Additive frontend tables, separate from the released core schema ledger.
//!
//! Installation is atomic and retains the original installation time when
//! adopting the two known prerelease core entries. No other history is moved.
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use super::{LOCAL_SCHEMA_MIGRATIONS_TABLE, LocalIndexStoreError, migration::FRONTEND_MIGRATIONS};

const HISTORY: &str = "shardline_frontend_schema_v1";

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
            transaction.execute_batch(migration.up_sql)?;
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
        "SELECT name,sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'",
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
