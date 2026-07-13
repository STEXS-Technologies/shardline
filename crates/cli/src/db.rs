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

#[cfg(test)]
mod tests {
    use shardline_server::{
        DatabaseMigrationCommand, DatabaseMigrationError, DatabaseMigrationOptions,
    };

    use super::DbRuntimeError;

    #[test]
    fn db_runtime_error_missing_url_message() {
        let err = DbRuntimeError::MissingDatabaseUrl;
        let msg = err.to_string();
        assert!(msg.contains("SHARDLINE_INDEX_POSTGRES_URL"));
        assert!(msg.contains("--database-url"));
    }

    #[test]
    fn db_runtime_error_debug() {
        let err = DbRuntimeError::MissingDatabaseUrl;
        let debug = format!("{err:?}");
        assert!(debug.contains("MissingDatabaseUrl"));
    }

    #[test]
    fn db_runtime_error_from_migration() {
        let migration_err = DatabaseMigrationError::EmptyDatabaseUrl;
        let err: DbRuntimeError = migration_err.into();
        let msg = err.to_string();
        assert!(msg.contains("database URL must not be empty"));
    }

    #[test]
    fn database_migration_options_holds_values() {
        let url = "postgres://localhost:5432/shardline".to_owned();
        let options = DatabaseMigrationOptions::new(url.clone(), DatabaseMigrationCommand::Status);
        assert_eq!(options.database_url(), url);
        assert_eq!(options.command(), &DatabaseMigrationCommand::Status);
    }

    #[test]
    fn database_migration_options_up_steps() {
        let url = "postgres://localhost/shardline".to_owned();
        let options = DatabaseMigrationOptions::new(
            url.clone(),
            DatabaseMigrationCommand::Up { steps: Some(3) },
        );
        assert_eq!(options.database_url(), url);
        assert_eq!(
            options.command(),
            &DatabaseMigrationCommand::Up { steps: Some(3) }
        );
    }

    #[test]
    fn database_migration_options_up_no_steps() {
        let options = DatabaseMigrationOptions::new(
            "postgres://localhost/shardline".to_owned(),
            DatabaseMigrationCommand::Up { steps: None },
        );
        assert_eq!(
            options.command(),
            &DatabaseMigrationCommand::Up { steps: None }
        );
    }

    #[test]
    fn database_migration_options_down_steps() {
        let options = DatabaseMigrationOptions::new(
            "postgres://localhost/shardline".to_owned(),
            DatabaseMigrationCommand::Down { steps: 2 },
        );
        assert_eq!(
            options.command(),
            &DatabaseMigrationCommand::Down { steps: 2 }
        );
    }

    #[test]
    fn database_migration_command_debug_and_clone() {
        let cmd = DatabaseMigrationCommand::Up { steps: Some(1) };
        let debug = format!("{cmd:?}");
        assert!(debug.contains("Up"));
        let cloned = cmd.clone();
        assert_eq!(cmd, cloned);
    }

    #[test]
    fn database_migration_error_empty_url() {
        let err = DatabaseMigrationError::EmptyDatabaseUrl;
        assert_eq!(err.to_string(), "database URL must not be empty");
    }

    #[test]
    fn database_migration_error_unknown_version() {
        let err = DatabaseMigrationError::UnknownAppliedMigration("v999".to_owned());
        let msg = err.to_string();
        assert!(msg.contains("unknown shardline migration version"));
        assert!(msg.contains("v999"));
    }

    #[test]
    fn database_migration_error_checksum_mismatch() {
        let err = DatabaseMigrationError::ChecksumMismatch {
            version: "v1".to_owned(),
            expected_checksum: "abc".to_owned(),
            observed_checksum: "def".to_owned(),
        };
        let msg = err.to_string();
        assert!(msg.contains("checksum mismatch"));
        assert!(msg.contains("v1"));
        assert!(msg.contains("abc"));
        assert!(msg.contains("def"));
    }

    #[test]
    fn run_db_migration_missing_url_display() {
        // Verify the error variant is constructible and has the right message.
        let err = DbRuntimeError::MissingDatabaseUrl;
        assert_eq!(
            err.to_string(),
            "no Postgres metadata URL configured; set SHARDLINE_INDEX_POSTGRES_URL or pass --database-url"
        );
    }

    #[test]
    fn database_migration_options_new() {
        let opts = DatabaseMigrationOptions::new(
            "postgres://localhost/mydb".to_owned(),
            DatabaseMigrationCommand::Status,
        );
        assert_eq!(opts.database_url(), "postgres://localhost/mydb");
        assert_eq!(opts.command(), &DatabaseMigrationCommand::Status);
    }

    #[test]
    fn database_migration_command_partial_eq() {
        assert_eq!(
            DatabaseMigrationCommand::Up { steps: Some(2) },
            DatabaseMigrationCommand::Up { steps: Some(2) }
        );
        assert_ne!(
            DatabaseMigrationCommand::Up { steps: Some(2) },
            DatabaseMigrationCommand::Up { steps: None }
        );
        assert_ne!(
            DatabaseMigrationCommand::Status,
            DatabaseMigrationCommand::Down { steps: 1 }
        );
    }
}
