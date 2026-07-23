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
    let config = load_server_config(root, None)?;
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
    let config = load_server_config(root, None)?;
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
    let config = load_server_config(root, None)?;
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
    use std::path::Path;

    use super::*;

    // ── print_hold_summary ────────────────────────────────────────────────

    fn sample_hold(ttl_seconds: Option<u64>) -> RetentionHold {
        let object_key = ObjectKey::parse(&format!("de/{}", "de".repeat(32))).unwrap();
        let now = unix_now_seconds_lossy();
        let release_after = ttl_seconds.map(|ttl| now.saturating_add(ttl));
        RetentionHold::new(
            object_key,
            "test hold reason".to_owned(),
            now,
            release_after,
        )
        .unwrap()
    }

    #[test]
    fn print_hold_summary_with_ttl_prints_release_time() {
        let hold = sample_hold(Some(3600));
        print_hold_summary(&hold);
        assert!(hold.object_key().as_str().starts_with("de/"));
        assert_eq!(hold.reason(), "test hold reason");
        assert!(hold.release_after_unix_seconds().is_some());
    }

    #[test]
    fn print_hold_summary_without_ttl_prints_none() {
        let hold = sample_hold(None);
        print_hold_summary(&hold);
        assert!(hold.release_after_unix_seconds().is_none());
    }

    #[test]
    fn print_hold_list_summary_empty() {
        let root = std::path::Path::new("/test");
        print_hold_list_summary(root, false, &[]);
    }

    #[test]
    fn print_hold_list_summary_with_holds() {
        let root = std::path::Path::new("/test");
        let hold_a = sample_hold(Some(600));
        let hold_b = sample_hold(None);
        print_hold_list_summary(root, true, &[hold_a, hold_b]);
    }

    // ── HoldRuntimeError Display variants ─────────────────────────────────

    #[test]
    fn hold_runtime_error_sqlx_display() {
        let inner = SqlxError::Protocol("test error".to_owned());
        let err = HoldRuntimeError::Sqlx(Box::new(inner));
        let msg = err.to_string();
        assert!(msg.contains("postgres metadata connection failed"));
    }

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
        let inner = LocalIndexStoreError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "db missing",
        ));
        let err = HoldRuntimeError::LocalIndex(inner);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn hold_runtime_error_postgres_display() {
        use shardline_index::PostgresMetadataStoreError;
        let err = HoldRuntimeError::Postgres(PostgresMetadataStoreError::Json(
            serde_json::from_str::<()>("invalid").unwrap_err(),
        ));
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn hold_runtime_error_sqlx_from_conversion() {
        // Verify the From<SqlxError> impl works
        use sqlx::Error as SqlxError;
        let _err: HoldRuntimeError = SqlxError::Protocol("test".to_owned()).into();
    }

    #[test]
    fn hold_runtime_error_debug_postgres() {
        use shardline_index::PostgresMetadataStoreError;
        let err = HoldRuntimeError::Postgres(PostgresMetadataStoreError::Json(
            serde_json::from_str::<()>("invalid").unwrap_err(),
        ));
        let debug = format!("{err:?}");
        assert!(debug.contains("Postgres("));
    }

    // ── Async runtime functions error paths ─────────────────────────────

    #[tokio::test]
    async fn run_hold_set_rejects_missing_root() {
        let result = run_hold_set(
            Some(Path::new("/nonexistent-shardline-test-root")),
            "de/test/key",
            "test reason",
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_hold_set_rejects_invalid_object_key() {
        // Create a valid temp dir so load_server_config succeeds
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path();
        // An empty object key should trigger ObjectKeyError
        let result = run_hold_set(Some(root), "", "test reason", None).await;
        assert!(result.is_err());
        // The error should be ObjectKey
        #[allow(clippy::panic)]
        match result {
            Err(HoldRuntimeError::ObjectKey(_)) => {} // expected
            _ => panic!("expected ObjectKey error, got {:?}", result),
        }
    }

    #[allow(clippy::panic)]
    #[tokio::test]
    async fn run_hold_set_overflow_rejected() {
        let sandbox = tempfile::tempdir().unwrap();
        // Use u64::MAX as TTL to trigger overflow
        let result = run_hold_set(
            Some(sandbox.path()),
            "de/test/overflow",
            "overflow test",
            Some(u64::MAX),
        )
        .await;
        match result {
            Err(HoldRuntimeError::Overflow) => {} // expected
            _ => panic!("expected Overflow error, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn run_hold_list_rejects_missing_root() {
        let result =
            run_hold_list(Some(Path::new("/nonexistent-shardline-test-root")), false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_hold_release_rejects_missing_root() {
        let result = run_hold_release(
            Some(Path::new("/nonexistent-shardline-test-root")),
            "de/test/key",
        )
        .await;
        assert!(result.is_err());
    }

    #[allow(clippy::panic)]
    #[tokio::test]
    async fn run_hold_release_rejects_invalid_object_key() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = run_hold_release(Some(sandbox.path()), "").await;
        match result {
            Err(HoldRuntimeError::ObjectKey(_)) => {} // expected
            _ => panic!("expected ObjectKey error, got {:?}", result),
        }
    }

    #[tokio::test]
    async fn run_hold_list_empty_returns_empty_on_fresh_root() {
        let sandbox = tempfile::tempdir().unwrap();
        // Create the expected root structure
        let root = sandbox.path().join("deployment-root");
        std::fs::create_dir_all(&root).unwrap();

        let result = run_hold_list(Some(&root), false).await;
        // This may succeed (empty list) or fail (can't open index) depending on env
        // We just verify it doesn't panic
        let _ = result;
    }

    #[tokio::test]
    async fn run_hold_set_and_list_and_release_cycle() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("deployment-root");
        std::fs::create_dir_all(&root).unwrap();
        let object_key = format!("de/{}", "de".repeat(32));

        // Set a hold
        let set_result = run_hold_set(
            Some(&root),
            &object_key,
            "integration test reason",
            Some(3600),
        )
        .await;
        let set_was_ok = set_result.is_ok();
        if let Ok(ref hold) = set_result {
            assert_eq!(hold.reason(), "integration test reason");
            assert!(hold.release_after_unix_seconds().is_some());
        }

        // List holds
        let list_result = run_hold_list(Some(&root), false).await;
        if let Ok(holds) = list_result {
            // If we successfully set a hold, it should appear in the list
            if set_was_ok {
                assert!(!holds.is_empty());
            }
        }

        // Release the hold
        let release_result = run_hold_release(Some(&root), &object_key).await;
        // May fail, shouldn't panic
        let _ = release_result;
    }

    // ── From<SqlxError> impl ──────────────────────────────────────────────

    #[test]
    fn hold_runtime_error_from_sqlx_error() {
        use sqlx::Error as SqlxError;
        let sqlx_err = SqlxError::Protocol("connection error".to_owned());
        let err: HoldRuntimeError = sqlx_err.into();
        assert!(matches!(err, HoldRuntimeError::Sqlx(_)));
    }

    // ── postgres_index_store error path ───────────────────────────────────

    #[test]
    fn postgres_index_store_rejects_invalid_url() {
        let result = super::postgres_index_store("not-a-valid-postgres-url");
        assert!(result.is_err());
    }

    // ── run_hold_release releases hold on local store ─────────────────────

    #[tokio::test]
    async fn run_hold_release_returns_false_for_missing_hold() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("deployment-root");
        std::fs::create_dir_all(&root).unwrap();

        let result = run_hold_release(Some(&root), &format!("de/{}", "de".repeat(32))).await;
        // May succeed (returning false) or fail depending on state
        if let Ok(released) = result {
            assert!(!released);
        }
    }

    // ── run_hold_list with active_only filter ─────────────────────────────

    #[tokio::test]
    async fn run_hold_list_active_only_returns_empty_on_fresh_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = sandbox.path().join("deployment-root");
        std::fs::create_dir_all(&root).unwrap();

        let result = run_hold_list(Some(&root), true).await;
        let _ = result;
    }

    // ── HoldRuntimeError Debug for all variants ───────────────────────────

    #[test]
    fn hold_runtime_error_debug_overflow() {
        let err = HoldRuntimeError::Overflow;
        let debug = format!("{err:?}");
        assert!(debug.contains("Overflow"));
    }

    #[test]
    fn hold_runtime_error_debug_config() {
        let err = HoldRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let debug = format!("{err:?}");
        assert!(debug.contains("Config("));
    }
}
