use std::path::Path;

use shardline_index::{
    AsyncIndexStore, LifecycleStore, LocalIndexStore, LocalIndexStoreError, PostgresIndexStore,
    PostgresMetadataStoreError, RetentionHold, RetentionHoldError,
};
use shardline_protocol::unix_now_seconds_lossy;
use shardline_server::{ServerConfigError, ServerError};
use shardline_storage::{ObjectKey, ObjectKeyError};
use sqlx::{Error as SqlxError, postgres::PgPoolOptions};
use thiserror::Error;

use crate::config::load_server_config;

pub fn print_hold_summary(hold: &RetentionHold) {
    let _result = write_hold_summary(&mut std::io::stdout().lock(), hold);
}

/// Writes the report and flushes the destination.
///
/// # Errors
/// Returns the first write or flush error.
pub fn write_hold_summary<W: std::io::Write + ?Sized>(
    writer: &mut W,
    hold: &RetentionHold,
) -> std::io::Result<()> {
    writeln!(writer, "object_key: {}", hold.object_key().as_str())?;
    writeln!(writer, "reason: {}", hold.reason())?;
    writeln!(
        writer,
        "held_at_unix_seconds: {}",
        hold.held_at_unix_seconds()
    )?;
    match hold.release_after_unix_seconds() {
        Some(value) => writeln!(writer, "release_after_unix_seconds: {value}")?,
        None => writeln!(writer, "release_after_unix_seconds: none")?,
    }
    writer.flush()
}

pub fn print_hold_list_summary(root: &Path, active_only: bool, holds: &[RetentionHold]) {
    let _result = write_hold_list_summary(&mut std::io::stdout().lock(), root, active_only, holds);
}

/// Writes the report and flushes the destination.
///
/// # Errors
/// Returns the first write or flush error.
pub fn write_hold_list_summary<W: std::io::Write + ?Sized>(
    writer: &mut W,
    root: &Path,
    active_only: bool,
    holds: &[RetentionHold],
) -> std::io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    writeln!(writer, "active_only: {active_only}")?;
    writeln!(writer, "hold_count: {}", holds.len())?;
    for (index, hold) in holds.iter().enumerate() {
        writeln!(
            writer,
            "hold[{index}].object_key: {}",
            hold.object_key().as_str()
        )?;
        writeln!(writer, "hold[{index}].reason: {}", hold.reason())?;
        writeln!(
            writer,
            "hold[{index}].held_at_unix_seconds: {}",
            hold.held_at_unix_seconds()
        )?;
        match hold.release_after_unix_seconds() {
            Some(value) => {
                writeln!(writer, "hold[{index}].release_after_unix_seconds: {value}")?;
            }
            None => {
                writeln!(writer, "hold[{index}].release_after_unix_seconds: none")?;
            }
        }
    }
    writer.flush()
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
    /// Coordinated retention mutation failed.
    #[error(transparent)]
    Server(Box<ServerError>),
    /// Timestamp arithmetic overflowed.
    #[error("retention hold timestamp overflowed")]
    Overflow,
    /// A positive relative TTL expired while the metadata mutation was waiting.
    #[error("retention hold expired before acknowledgement")]
    Expired,
}

impl From<ServerError> for HoldRuntimeError {
    fn from(value: ServerError) -> Self {
        Self::Server(Box::new(value))
    }
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
    let hold = shardline_server::set_retention_hold_with(&config, || {
        let held_at_unix_seconds = unix_now_seconds_lossy();
        let release_after_unix_seconds = ttl_seconds
            .map(|ttl| {
                held_at_unix_seconds
                    .checked_add(ttl)
                    .ok_or(HoldRuntimeError::Overflow)
            })
            .transpose()?;
        RetentionHold::new(
            object_key,
            reason.to_owned(),
            held_at_unix_seconds,
            release_after_unix_seconds,
        )
        .map_err(HoldRuntimeError::from)
    })
    .await?;
    if ttl_seconds.is_some_and(|ttl| ttl > 0) && !hold.is_active_at(unix_now_seconds_lossy()) {
        // Keep the expired row: deleting it could remove a concurrent replacement hold.
        return Err(HoldRuntimeError::Expired);
    }
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

    shardline_server::release_retention_hold(&config, &object_key)
        .await
        .map_err(|error| HoldRuntimeError::Server(Box::new(error)))
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

    // ── HoldRuntimeError Display variants ─────────────────────────────────

    #[test]
    fn hold_runtime_error_sqlx_display() {
        let inner = SqlxError::Protocol("test error".to_owned());
        let err = HoldRuntimeError::Sqlx(Box::new(inner));
        let msg = err.to_string();
        assert!(msg.contains("postgres metadata connection failed"));
    }

    #[test]
    fn hold_runtime_error_overflow_display() {
        let err = HoldRuntimeError::Overflow;
        assert_eq!(err.to_string(), "retention hold timestamp overflowed");
    }

    // ── Async runtime functions error paths ─────────────────────────────

    #[tokio::test]
    async fn run_hold_set_rejects_file_root() {
        let root = tempfile::NamedTempFile::new().unwrap();
        let result = run_hold_set(Some(root.path()), "de/test/key", "test reason", None).await;
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
    async fn run_hold_list_rejects_file_root() {
        let root = tempfile::NamedTempFile::new().unwrap();
        let result = run_hold_list(Some(root.path()), false).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_hold_release_rejects_file_root() {
        let root = tempfile::NamedTempFile::new().unwrap();
        let result = run_hold_release(Some(root.path()), "de/test/key").await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_hold_lifecycle_preserves_values_filters_expiry_and_reports_release() {
        let sandbox = tempfile::tempdir().unwrap();
        let root = Some(sandbox.path());
        let key = format!("de/{}", "de".repeat(32));
        for active_only in [false, true] {
            assert!(run_hold_list(root, active_only).await.unwrap().is_empty());
        }
        assert!(!run_hold_release(root, &key).await.unwrap());

        for ttl in [3600, 0] {
            let hold = run_hold_set(root, &key, "integration test reason", Some(ttl))
                .await
                .unwrap();
            assert_eq!(hold.object_key().as_str(), key);
            assert_eq!(hold.reason(), "integration test reason");
            assert_eq!(
                hold.release_after_unix_seconds(),
                hold.held_at_unix_seconds().checked_add(ttl)
            );
            for active_only in [false, true] {
                let holds = run_hold_list(root, active_only).await.unwrap();
                if active_only && ttl == 0 {
                    assert!(holds.is_empty());
                } else {
                    assert_eq!(holds.len(), 1);
                    assert_eq!(holds[0], hold);
                }
            }
            assert!(run_hold_release(root, &key).await.unwrap());
            assert!(!run_hold_release(root, &key).await.unwrap());
            for active_only in [false, true] {
                assert!(run_hold_list(root, active_only).await.unwrap().is_empty());
            }
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn run_hold_set_waits_for_gc_before_acknowledging() {
        let storage = tempfile::tempdir().unwrap();
        let barrier = std::fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(storage.path().join(".gc-write-barrier.lock"))
            .unwrap();
        barrier.lock().unwrap();
        let root = storage.path().to_path_buf();
        let mut setter = tokio::spawn(async move {
            run_hold_set(Some(&root), "retention/cli-race", "GC coordination", None).await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), &mut setter)
                .await
                .is_err()
        );
        let index = LocalIndexStore::new(storage.path().to_path_buf()).unwrap();
        let key = ObjectKey::parse("retention/cli-race").unwrap();
        assert!(
            LifecycleStore::retention_hold(&index, &key)
                .unwrap()
                .is_none()
        );
        drop(barrier);
        let held = tokio::time::timeout(std::time::Duration::from_secs(2), setter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            LifecycleStore::retention_hold(&index, &key).unwrap(),
            Some(held)
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relative_ttl_starts_after_gc_barrier_and_zero_ttl_stays_expired() {
        let root = tempfile::tempdir().unwrap();
        let barrier = std::fs::File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.path().join(".gc-write-barrier.lock"))
            .unwrap();
        barrier.lock().unwrap();
        let path = root.path().to_path_buf();
        let mut setter = tokio::spawn(async move {
            run_hold_set(
                Some(&path),
                "retention/ttl-after-gc",
                "relative TTL",
                Some(2),
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(3), &mut setter)
                .await
                .is_err()
        );
        let unlocked_at = unix_now_seconds_lossy();
        drop(barrier);
        let hold = setter.await.unwrap().unwrap();
        assert!(hold.held_at_unix_seconds() >= unlocked_at);
        assert_eq!(
            hold.release_after_unix_seconds(),
            Some(hold.held_at_unix_seconds() + 2)
        );
        assert!(hold.is_active_at(unix_now_seconds_lossy()));
        let zero = run_hold_set(Some(root.path()), "retention/zero", "zero TTL", Some(0))
            .await
            .unwrap();
        assert_eq!(
            zero.release_after_unix_seconds(),
            Some(zero.held_at_unix_seconds())
        );
        assert!(!zero.is_active_at(unix_now_seconds_lossy()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn positive_ttl_expired_by_sqlite_wait_returns_typed_error_without_deleting_row() {
        let root = tempfile::tempdir().unwrap();
        let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        let writer = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        // Establish that warmed store setup completes with the same writer held:
        // the subsequent wait is in the mutation after the hold factory runs.
        let warm_path = root.path().to_path_buf();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                tokio::task::spawn_blocking(move || LocalIndexStore::new(warm_path))
            )
            .await
            .unwrap()
            .unwrap()
            .is_ok()
        );

        let path = root.path().to_path_buf();
        let mut setter = tokio::spawn(async move {
            run_hold_set(
                Some(&path),
                "retention/expired-write",
                "slow metadata",
                Some(1),
            )
            .await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(150), &mut setter)
                .await
                .is_err()
        );
        tokio::time::sleep(std::time::Duration::from_millis(2200)).await;
        writer.execute_batch("COMMIT").unwrap();
        assert!(matches!(
            setter.await.unwrap(),
            Err(HoldRuntimeError::Expired)
        ));
        let key = ObjectKey::parse("retention/expired-write").unwrap();
        let stored = LifecycleStore::retention_hold(&index, &key)
            .unwrap()
            .unwrap();
        assert!(!stored.is_active_at(unix_now_seconds_lossy()));
    }
}

#[cfg(test)]
pub(crate) mod output_test_support {
    use std::io::{self, Write};

    struct FaultWriter {
        remaining: usize,
        fail_flush: bool,
    }
    impl Write for FaultWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_flush {
                return Ok(bytes.len());
            }
            if self.remaining == 0 {
                return Err(io::Error::from_raw_os_error(28));
            }
            let length = bytes.len().min(self.remaining);
            self.remaining = self.remaining.saturating_sub(length);
            Ok(length)
        }
        fn flush(&mut self) -> io::Result<()> {
            if self.fail_flush {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "flush failure"))
            } else {
                Ok(())
            }
        }
    }

    pub(crate) fn assert_writer_errors(mut write: impl FnMut(&mut dyn Write) -> io::Result<()>) {
        for remaining in [0, 7] {
            let mut partial = FaultWriter {
                remaining,
                fail_flush: false,
            };
            assert!(matches!(write(&mut partial), Err(error) if error.raw_os_error() == Some(28)));
        }
        let mut flush = FaultWriter {
            remaining: 0,
            fail_flush: true,
        };
        assert!(
            matches!(write(&mut flush), Err(error) if error.kind() == io::ErrorKind::BrokenPipe && error.to_string() == "flush failure")
        );
    }
}

#[cfg(test)]
mod writer_tests {
    use super::*;
    #[test]
    fn hold_writers_preserve_text_and_propagate_write_and_flush_errors() {
        let hold = RetentionHold::new(
            ObjectKey::parse("future/key").expect("key"),
            "reason".to_owned(),
            10,
            Some(20),
        )
        .expect("hold");
        let mut bytes = Vec::new();
        write_hold_summary(&mut bytes, &hold).expect("summary");
        assert_eq!(bytes, b"object_key: future/key\nreason: reason\nheld_at_unix_seconds: 10\nrelease_after_unix_seconds: 20\n");
        output_test_support::assert_writer_errors(|writer| write_hold_summary(writer, &hold));
        output_test_support::assert_writer_errors(|writer| {
            write_hold_list_summary(
                writer,
                Path::new("/root"),
                true,
                std::slice::from_ref(&hold),
            )
        });
        bytes.clear();
        write_hold_list_summary(
            &mut bytes,
            Path::new("/root"),
            true,
            std::slice::from_ref(&hold),
        )
        .expect("list");
        assert_eq!(bytes, b"root: /root\nactive_only: true\nhold_count: 1\nhold[0].object_key: future/key\nhold[0].reason: reason\nhold[0].held_at_unix_seconds: 10\nhold[0].release_after_unix_seconds: 20\n");
        let no_ttl =
            RetentionHold::new(hold.object_key().clone(), "reason".to_owned(), 10, None).unwrap();
        bytes.clear();
        write_hold_summary(&mut bytes, &no_ttl).unwrap();
        assert_eq!(bytes, b"object_key: future/key\nreason: reason\nheld_at_unix_seconds: 10\nrelease_after_unix_seconds: none\n");
        bytes.clear();
        write_hold_list_summary(&mut bytes, Path::new("/root"), true, &[no_ttl]).unwrap();
        assert_eq!(bytes, b"root: /root\nactive_only: true\nhold_count: 1\nhold[0].object_key: future/key\nhold[0].reason: reason\nhold[0].held_at_unix_seconds: 10\nhold[0].release_after_unix_seconds: none\n");
        bytes.clear();
        write_hold_list_summary(&mut bytes, Path::new("/root"), false, &[]).unwrap();
        assert_eq!(bytes, b"root: /root\nactive_only: false\nhold_count: 0\n");
    }
}
