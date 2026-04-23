use std::path::Path;

use shardline_index::{
    AsyncIndexStore, IndexStore, LocalIndexStore, LocalIndexStoreError, PostgresIndexStore,
    PostgresMetadataStoreError, RetentionHold, RetentionHoldError,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server::ServerConfigError;
use shardline_storage::{ObjectKey, ObjectKeyError};
use sqlx::{Error as SqlxError, postgres::PgPoolOptions};
use thiserror::Error;

use crate::config::load_server_config;

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
    IndexStore::upsert_retention_hold(&store, &hold)?;
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
        IndexStore::list_retention_holds(&store)?
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
    IndexStore::delete_retention_hold(&store, &object_key).map_err(Into::into)
}

fn postgres_index_store(index_postgres_url: &str) -> Result<PostgresIndexStore, HoldRuntimeError> {
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_lazy(index_postgres_url)?;
    Ok(PostgresIndexStore::new(pool))
}
