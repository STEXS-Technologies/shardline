use std::env::var;

use shardline_server::{
    DatabaseMigrationCommand, DatabaseMigrationError, DatabaseMigrationOptions,
    DatabaseMigrationReport, run_database_migration,
};
use thiserror::Error;

/// Database-migration runtime failure.
#[derive(Debug, Error)]
pub enum DbRuntimeError {
    /// No database URL was supplied and the active environment does not configure one.
    #[error(
        "no Postgres metadata URL configured; set SHARDLINE_INDEX_POSTGRES_URL or pass --database-url"
    )]
    MissingDatabaseUrl,
    /// Database migration execution failed.
    #[error(transparent)]
    Migration(#[from] DatabaseMigrationError),
}

/// Runs a Shardline database-migration command.
///
/// # Errors
///
/// Returns [`DbRuntimeError`] when no Postgres URL is available or the migration engine
/// fails.
pub async fn run_db_migration(
    database_url_override: Option<&str>,
    command: DatabaseMigrationCommand,
) -> Result<DatabaseMigrationReport, DbRuntimeError> {
    let database_url = if let Some(database_url) = database_url_override {
        database_url.to_owned()
    } else if let Ok(database_url) = var("SHARDLINE_INDEX_POSTGRES_URL") {
        database_url
    } else {
        return Err(DbRuntimeError::MissingDatabaseUrl);
    };

    let options = DatabaseMigrationOptions::new(database_url, command);
    Ok(run_database_migration(&options).await?)
}
