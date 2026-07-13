use std::path::Path;

use shardline_index::{
    AsyncIndexStore, LifecycleStore, LocalIndexStore, LocalIndexStoreError, PostgresIndexStore,
    PostgresMetadataStoreError, RetentionHold, RetentionHoldError,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server::ServerConfigError;
use shardline_storage::{ObjectKey, ObjectKeyError};
use sqlx::{Error as SqlxError, postgres::PgPoolOptions};
use thiserror::Error;

use crate::config::load_server_config;

pub fn print_hold_summary(hold: &RetentionHold) {
    println!("object_key: {}", hold.object_key().as_str());
    println!("reason: {}", hold.reason());
    println!("held_at_unix_seconds: {}", hold.held_at_unix_seconds());
    match hold.release_after_unix_seconds() {
        Some(value) => println!("release_after_unix_seconds: {value}"),
        None => println!("release_after_unix_seconds: none"),
    }
}

pub fn print_hold_list_summary(root: &Path, active_only: bool, holds: &[RetentionHold]) {
    println!("root: {}", root.display());
    println!("active_only: {active_only}");
    println!("hold_count: {}", holds.len());
    for (index, hold) in holds.iter().enumerate() {
        println!("hold[{index}].object_key: {}", hold.object_key().as_str());
        println!("hold[{index}].reason: {}", hold.reason());
        println!(
            "hold[{index}].held_at_unix_seconds: {}",
            hold.held_at_unix_seconds()
        );
        match hold.release_after_unix_seconds() {
            Some(value) => {
                println!("hold[{index}].release_after_unix_seconds: {value}");
            }
            None => {
                println!("hold[{index}].release_after_unix_seconds: none");
            }
        }
    }
}

/// Retention-hold runtime failure.
#[derive(Debug, Error)]
pub enum HoldRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// The supplied object key was invalid.
    #[error(transparent)]
    ObjectKey(#[from] ObjectKeyError),
    /// The supplied hold record was invalid.
    #[error(transparent)]
    RetentionHold(#[from] RetentionHoldError),
    /// Local metadata access failed.
    #[error(transparent)]
    LocalIndex(#[from] LocalIndexStoreError),
    /// Postgres metadata access failed.
    #[error(transparent)]
    Postgres(#[from] PostgresMetadataStoreError),
    /// Postgres pool configuration failed.
    #[error("postgres metadata connection failed")]
    Sqlx(Box<SqlxError>),
    /// Timestamp arithmetic overflowed.
    #[error("retention hold timestamp overflowed")]
    Overflow,
}

impl From<SqlxError> for HoldRuntimeError {
    fn from(value: SqlxError) -> Self {
        Self::Sqlx(Box::new(value))
    }
}

/// Creates or updates one retention hold.
///
/// # Errors
///
/// Returns [`HoldRuntimeError`] when configuration, parsing, or metadata persistence
/// fails.
pub async fn run_hold_set(
    root: Option<&Path>,
    object_key: &str,
    reason: &str,
    ttl_seconds: Option<u64>,
) -> Result<RetentionHold, HoldRuntimeError> {
    let config = load_server_config(root)?;
    let object_key = ObjectKey::parse(object_key)?;
    let held_at_unix_seconds = unix_now_seconds_lossy();
    let release_after_unix_seconds = ttl_seconds
        .map(|ttl| {
            held_at_unix_seconds
                .checked_add(ttl)
                .ok_or(HoldRuntimeError::Overflow)
        })
        .transpose()?;
    let hold = RetentionHold::new(
        object_key,
        reason.to_owned(),
        held_at_unix_seconds,
        release_after_unix_seconds,
    )?;

    if let Some(index_postgres_url) = config.index_postgres_url() {
        let store = postgres_index_store(index_postgres_url)?;
        store.upsert_retention_hold(&hold).await?;
        return Ok(hold);
    }

    let store = LocalIndexStore::new(config.root_dir().to_path_buf())?;
    LifecycleStore::upsert_retention_hold(&store, &hold)?;
    Ok(hold)
}

/// Lists retention holds from the configured metadata backend.
///
/// # Errors
///
/// Returns [`HoldRuntimeError`] when configuration or metadata access fails.
pub async fn run_hold_list(
    root: Option<&Path>,
    active_only: bool,
) -> Result<Vec<RetentionHold>, HoldRuntimeError> {
    let config = load_server_config(root)?;
    let mut holds = if let Some(index_postgres_url) = config.index_postgres_url() {
        let store = postgres_index_store(index_postgres_url)?;
        store.list_retention_holds().await?
    } else {
        let store = LocalIndexStore::new(config.root_dir().to_path_buf())?;
        LifecycleStore::list_retention_holds(&store)?
    };

    if active_only {
        let now_unix_seconds = unix_now_seconds_lossy();
        holds.retain(|hold| hold.is_active_at(now_unix_seconds));
    }
    holds.sort_by(|left, right| left.object_key().as_str().cmp(right.object_key().as_str()));

    Ok(holds)
}

/// Deletes one retention hold from the configured metadata backend.
///
/// # Errors
///
/// Returns [`HoldRuntimeError`] when configuration, parsing, or metadata access fails.
pub async fn run_hold_release(
    root: Option<&Path>,
    object_key: &str,
) -> Result<bool, HoldRuntimeError> {
    let config = load_server_config(root)?;
    let object_key = ObjectKey::parse(object_key)?;

    if let Some(index_postgres_url) = config.index_postgres_url() {
        let store = postgres_index_store(index_postgres_url)?;
        return store
            .delete_retention_hold(&object_key)
            .await
            .map_err(Into::into);
    }

    let store = LocalIndexStore::new(config.root_dir().to_path_buf())?;
    LifecycleStore::delete_retention_hold(&store, &object_key).map_err(Into::into)
}

fn postgres_index_store(index_postgres_url: &str) -> Result<PostgresIndexStore, HoldRuntimeError> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(index_postgres_url)?;
    Ok(PostgresIndexStore::new(pool))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_runtime_error_config_display() {
        let err = HoldRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let msg = err.to_string();
        assert!(msg.contains("invalid server role"));
    }

    #[test]
    fn hold_runtime_error_object_key_display() {
        let err = HoldRuntimeError::ObjectKey(ObjectKeyError::Empty);
        let msg = err.to_string();
        assert!(msg.contains("empty"));
    }

    #[test]
    fn hold_runtime_error_retention_hold_display() {
        let err = HoldRuntimeError::RetentionHold(RetentionHoldError::EmptyReason);
        let msg = err.to_string();
        assert!(msg.contains("empty"));
    }

    #[test]
    fn hold_runtime_error_overflow_display() {
        let err = HoldRuntimeError::Overflow;
        assert_eq!(err.to_string(), "retention hold timestamp overflowed");
    }

    #[test]
    fn hold_runtime_error_debug() {
        let err = HoldRuntimeError::Overflow;
        let debug = format!("{err:?}");
        assert!(debug.contains("Overflow"));
    }

    #[test]
    fn hold_runtime_error_local_index_display() {
        use shardline_index::LocalIndexStoreError;
        let inner = LocalIndexStoreError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "db missing"));
        let err = HoldRuntimeError::LocalIndex(inner);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }
}
