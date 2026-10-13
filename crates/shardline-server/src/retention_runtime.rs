use shardline_index::{AsyncIndexStore, LocalIndexStore, PostgresIndexStore, RetentionHold};
use shardline_storage::ObjectKey;

use crate::{ServerConfig, ServerError, maintenance_barrier};

/// A shared GC/write barrier held until this guard is dropped.
///
/// Keep it alive throughout the metadata and object-store operation being
/// coordinated. It excludes mutating GC on the configured deployment backend.
#[derive(Debug)]
pub struct MetadataWriteBarrier {
    guard: MetadataWriteBarrierGuard,
}

#[derive(Debug)]
enum MetadataWriteBarrierGuard {
    Local(maintenance_barrier::MaintenanceBarrierGuard),
    Postgres(sqlx::PgPool),
}

impl MetadataWriteBarrier {
    /// Returns the metadata pool whose session owns this barrier, when using Postgres.
    ///
    /// Use this pool for protected reads and writes instead of opening another
    /// pool. It cannot reconnect after ownership loss. Keep the barrier alive
    /// until the operation finishes, including any blocking work. Dropping the
    /// barrier closes the pool to further acquisitions, including cloned handles.
    /// This binds database work; it does not fence external object-store requests.
    #[must_use]
    pub const fn postgres_metadata_pool(&self) -> Option<&sqlx::PgPool> {
        match &self.guard {
            MetadataWriteBarrierGuard::Local(_guard) => None,
            MetadataWriteBarrierGuard::Postgres(pool) => Some(pool),
        }
    }
}

impl Drop for MetadataWriteBarrier {
    fn drop(&mut self) {
        if let MetadataWriteBarrierGuard::Postgres(pool) = &self.guard {
            // SQLx marks the pool closed synchronously; awaiting only finishes
            // graceful connection cleanup. Even retained clones cannot reconnect.
            drop(pool.close());
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let pool = pool.clone();
                runtime.spawn(async move { pool.close().await });
            }
        }
    }
}

/// Acquires the deployment's shared GC/write barrier for a library operation.
///
/// This uses the same local root or Postgres advisory key as [`crate::run_gc`].
/// Acquire it before resource locks to preserve the server's lock order. For
/// Postgres, use [`MetadataWriteBarrier::postgres_metadata_pool`] for protected
/// database work; a separately opened pool is not bound to this ownership.
///
/// # Errors
///
/// Returns [`ServerError`] when backend configuration or lock acquisition fails.
pub async fn acquire_metadata_write_barrier(
    config: &ServerConfig,
) -> Result<MetadataWriteBarrier, ServerError> {
    let guard = if let Some(url) = config.index_postgres_url() {
        MetadataWriteBarrierGuard::Postgres(
            maintenance_barrier::postgres_shared_work_pool(url).await?,
        )
    } else {
        MetadataWriteBarrierGuard::Local(
            maintenance_barrier::acquire_local_shared(config.root_dir()).await?,
        )
    };
    Ok(MetadataWriteBarrier { guard })
}

/// Persists an administrative hold while excluding mutating garbage collection.
///
/// The shared barrier remains owned until the write completes. A previously
/// acknowledged active hold is therefore visible to the next GC run. If GC owns
/// the barrier first, this operation waits; it cannot restore deleted bytes.
/// Keys need not already exist, allowing holds to protect future objects.
///
/// # Errors
///
/// Returns [`ServerError`] when barrier acquisition or metadata persistence fails.
pub async fn set_retention_hold(
    config: &ServerConfig,
    hold: &RetentionHold,
) -> Result<(), ServerError> {
    set_retention_hold_with(config, || Ok::<_, ServerError>(hold.clone()))
        .await
        .map(|_| ())
}

/// Constructs and persists an administrative hold after acquiring the GC barrier.
///
/// The synchronous factory runs exactly once while the shared barrier is held.
/// Relative expiration can start after GC has finished; metadata lock waits may
/// still consume that duration. The factory must not reacquire this barrier.
/// Absolute-time callers should use [`set_retention_hold`] instead.
///
/// # Errors
///
/// Returns the factory error or a converted [`ServerError`] from coordination or storage.
pub async fn set_retention_hold_with<Build, E>(
    config: &ServerConfig,
    build_hold: Build,
) -> Result<RetentionHold, E>
where
    Build: FnOnce() -> Result<RetentionHold, E>,
    E: From<ServerError>,
{
    let barrier = acquire_metadata_write_barrier(config).await?;
    if let Some(pool) = barrier.postgres_metadata_pool() {
        let hold = build_hold()?;
        let result = PostgresIndexStore::new(pool.clone())
            .upsert_retention_hold(&hold)
            .await
            .map_err(ServerError::from);
        pool.close().await;
        result?;
        Ok(hold)
    } else {
        let store =
            LocalIndexStore::new(config.root_dir().to_path_buf()).map_err(ServerError::from)?;
        let hold = build_hold()?;
        store
            .upsert_retention_hold(&hold)
            .await
            .map_err(ServerError::from)?;
        Ok(hold)
    }
}

/// Releases an administrative hold while excluding mutating garbage collection.
///
/// # Errors
///
/// Returns [`ServerError`] when barrier acquisition or metadata persistence fails.
pub async fn release_retention_hold(
    config: &ServerConfig,
    object_key: &ObjectKey,
) -> Result<bool, ServerError> {
    let barrier = acquire_metadata_write_barrier(config).await?;
    if let Some(pool) = barrier.postgres_metadata_pool() {
        let result = PostgresIndexStore::new(pool.clone())
            .delete_retention_hold(object_key)
            .await
            .map_err(ServerError::from);
        pool.close().await;
        result
    } else {
        Ok(LocalIndexStore::new(config.root_dir().to_path_buf())?
            .delete_retention_hold(object_key)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::{num::NonZeroUsize, time::Duration};

    use shardline_index::LifecycleStore;
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};

    use super::*;

    static POSTGRES_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn config(root: &std::path::Path) -> ServerConfig {
        ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.to_path_buf(),
            NonZeroUsize::new(4096).unwrap(),
        )
    }

    fn hold() -> RetentionHold {
        RetentionHold::new(
            ObjectKey::parse("retention/barrier-regression").unwrap(),
            "barrier regression".to_owned(),
            shardline_protocol::unix_now_seconds_lossy(),
            None,
        )
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_hold_mutations_wait_for_complete_gc_critical_section() {
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path());
        let hold = hold();
        let objects = crate::object_store::object_store_from_config(&config).unwrap();
        let body = b"GC race bytes";
        objects
            .put_if_absent(
                hold.object_key(),
                ObjectBody::Borrowed(body),
                &ObjectIntegrity::new(shardline_server_core::chunk_hash(body), body.len() as u64),
            )
            .unwrap();
        let index = LocalIndexStore::new(storage.path().to_path_buf()).unwrap();
        let gc_guard = maintenance_barrier::acquire_local_exclusive(storage.path())
            .await
            .unwrap();
        // GC has already looked up holds and still owns its lock through deletion.
        assert!(
            LifecycleStore::retention_hold(&index, hold.object_key())
                .unwrap()
                .is_none()
        );
        let task_config = config.clone();
        let task_hold = hold.clone();
        let mut setter =
            tokio::spawn(async move { set_retention_hold(&task_config, &task_hold).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut setter)
                .await
                .is_err()
        );
        assert!(
            LifecycleStore::retention_hold(&index, hold.object_key())
                .unwrap()
                .is_none()
        );
        // The physical delete occurs after the lookup while the barrier remains
        // owned. The concurrent hold cannot have succeeded in this interval.
        objects.delete_if_present(hold.object_key()).unwrap();
        assert!(!objects.contains(hold.object_key()).unwrap());
        drop(gc_guard);
        tokio::time::timeout(Duration::from_secs(2), setter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            LifecycleStore::retention_hold(&index, hold.object_key()).unwrap(),
            Some(hold.clone())
        );
        let gc_guard = maintenance_barrier::acquire_local_exclusive(storage.path())
            .await
            .unwrap();
        let task_config = config.clone();
        let task_key = hold.object_key().clone();
        let mut releaser =
            tokio::spawn(async move { release_retention_hold(&task_config, &task_key).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut releaser)
                .await
                .is_err()
        );
        assert_eq!(
            LifecycleStore::retention_hold(&index, hold.object_key()).unwrap(),
            Some(hold)
        );
        drop(gc_guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), releaser)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_pending_remote_delete_returns_clear_retryable_hold_error() {
        use axum::{
            http::{StatusCode, header::RETRY_AFTER},
            response::IntoResponse,
        };
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let index = PostgresIndexStore::new(pool.clone());
        let key =
            ObjectKey::parse(&format!("retention/pending-delete-{}", std::process::id())).unwrap();
        index.initialize_remote_delete_guard().await.unwrap();
        let hold = RetentionHold::new(
            key.clone(),
            "pending delete protection".to_owned(),
            shardline_protocol::unix_now_seconds_lossy(),
            None,
        )
        .unwrap();
        let owner = maintenance_barrier::postgres_exclusive_work_pool(&url)
            .await
            .unwrap();
        let permit = PostgresIndexStore::new(owner.clone())
            .begin_remote_delete(&key)
            .await
            .unwrap();
        drop(permit);
        owner.close().await;
        let error = set_retention_hold(&config, &hold).await.unwrap_err();
        assert!(error.is_remote_delete_pending());
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            body.get("error").unwrap(),
            "object has an unresolved remote delete; retry after reconciliation"
        );
        assert!(index.retention_hold(&key).await.unwrap().is_none());
        let unrelated = RetentionHold::new(
            ObjectKey::parse(&format!("{}/other", key.as_str())).unwrap(),
            "unrelated protection".to_owned(),
            hold.held_at_unix_seconds(),
            None,
        )
        .unwrap();
        set_retention_hold(&config, &unrelated).await.unwrap();
        assert!(
            release_retention_hold(&config, unrelated.object_key())
                .await
                .unwrap()
        );
        // This fixture never issued a remote request. Remove only its marker.
        sqlx::query("DELETE FROM shardline_resource_fences WHERE domain='maintenance-remote-delete' AND resource=$1")
            .bind(key.as_str()).execute(&pool).await.unwrap();
        pool.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[allow(clippy::panic)] // Deliberate unwind verifies ownership cleanup.
    async fn postgres_hold_factory_failure_releases_maintenance_ownership() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let result =
            set_retention_hold_with(&config, || Err::<RetentionHold, _>(ServerError::Overflow))
                .await;
        assert!(matches!(result, Err(ServerError::Overflow)));
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::postgres_exclusive_work_pool(&url),
        )
        .await
        .unwrap()
        .unwrap();
        replacement.close().await;

        let panicking = tokio::spawn(async move {
            set_retention_hold_with(&config, || -> Result<RetentionHold, ServerError> {
                panic!("synthetic hold factory panic");
            })
            .await
        });
        assert!(panicking.await.unwrap_err().is_panic());
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::postgres_exclusive_work_pool(&url),
        )
        .await
        .unwrap()
        .unwrap();
        replacement.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_metadata_barrier_closes_retained_pool_handles_on_drop() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let barrier = acquire_metadata_write_barrier(&config).await.unwrap();
        let retained = barrier.postgres_metadata_pool().unwrap().clone();
        drop(barrier);
        assert!(
            retained.is_closed(),
            "dropping ownership must immediately close retained clones"
        );
        assert!(matches!(
            retained.acquire().await,
            Err(sqlx::Error::PoolClosed)
        ));
        // Await physical closure before proving a competing owner can acquire.
        retained.close().await;
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::acquire_postgres_exclusive(&pool),
        )
        .await
        .unwrap()
        .unwrap();
        drop(replacement);
        pool.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_hold_mutations_wait_for_complete_gc_critical_section() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let index = PostgresIndexStore::new(pool.clone());
        let hold = hold();
        let objects = crate::object_store::object_store_from_config(&config).unwrap();
        let body = b"GC race bytes";
        objects
            .put_if_absent(
                hold.object_key(),
                ObjectBody::Borrowed(body),
                &ObjectIntegrity::new(shardline_server_core::chunk_hash(body), body.len() as u64),
            )
            .unwrap();
        index
            .delete_retention_hold(hold.object_key())
            .await
            .unwrap();
        let gc_guard = maintenance_barrier::acquire_postgres_exclusive(&pool)
            .await
            .unwrap();
        let task_config = config.clone();
        let task_hold = hold.clone();
        let mut setter =
            tokio::spawn(async move { set_retention_hold(&task_config, &task_hold).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut setter)
                .await
                .is_err()
        );
        assert!(
            index
                .retention_hold(hold.object_key())
                .await
                .unwrap()
                .is_none()
        );
        objects.delete_if_present(hold.object_key()).unwrap();
        assert!(!objects.contains(hold.object_key()).unwrap());
        drop(gc_guard);
        tokio::time::timeout(Duration::from_secs(5), setter)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            index.retention_hold(hold.object_key()).await.unwrap(),
            Some(hold.clone())
        );
        let gc_guard = maintenance_barrier::acquire_postgres_exclusive(&pool)
            .await
            .unwrap();
        let task_config = config.clone();
        let task_key = hold.object_key().clone();
        let mut releaser =
            tokio::spawn(async move { release_retention_hold(&task_config, &task_key).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut releaser)
                .await
                .is_err()
        );
        assert_eq!(
            index.retention_hold(hold.object_key()).await.unwrap(),
            Some(hold)
        );
        drop(gc_guard);
        assert!(
            tokio::time::timeout(Duration::from_secs(5), releaser)
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_hold_cannot_commit_after_barrier_connection_is_lost() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let index = PostgresIndexStore::new(pool.clone());
        let hold = RetentionHold::new(
            ObjectKey::parse(&format!("retention/lost-owner-{}", std::process::id())).unwrap(),
            "lost owner regression".to_owned(),
            shardline_protocol::unix_now_seconds_lossy(),
            None,
        )
        .unwrap();
        index
            .delete_retention_hold(hold.object_key())
            .await
            .unwrap();
        let entered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory_entered = entered.clone();
        let (resume, paused) = std::sync::mpsc::channel();
        let task_hold = hold.clone();
        let setter = tokio::spawn(async move {
            set_retention_hold_with(&config, move || {
                factory_entered.store(true, std::sync::atomic::Ordering::Release);
                tokio::task::block_in_place(|| {
                    paused.recv_timeout(Duration::from_secs(5)).unwrap();
                });
                Ok::<_, ServerError>(task_hold)
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !entered.load(std::sync::atomic::Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let owner = sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted
             AND objsubid = 1
             AND classid::bigint = (($1::bigint >> 32) & 4294967295)
             AND objid::bigint = ($1::bigint & 4294967295)",
        )
        .bind(0x5348_4152_4447_4301_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::acquire_postgres_exclusive(&pool),
        )
        .await
        .unwrap()
        .unwrap();
        resume.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_secs(15), setter)
            .await
            .unwrap()
            .unwrap();
        let observed = index.retention_hold(hold.object_key()).await.unwrap();
        drop(replacement);
        index
            .delete_retention_hold(hold.object_key())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "a former shared-barrier owner committed during exclusive takeover"
        );
        assert!(
            observed.is_none(),
            "the stale retention hold became visible"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_hold_release_cannot_delete_after_ownership_connection_is_lost() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let index = PostgresIndexStore::new(pool.clone());
        let hold = RetentionHold::new(
            ObjectKey::parse(&format!("retention/lost-release-{}", std::process::id())).unwrap(),
            "release ownership regression".to_owned(),
            shardline_protocol::unix_now_seconds_lossy(),
            None,
        )
        .unwrap();
        index.upsert_retention_hold(&hold).await.unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE shardline_retention_holds IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let key = hold.object_key().clone();
        let releaser = tokio::spawn(async move { release_retention_hold(&config, &key).await });
        let owner = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let owner = sqlx::query_scalar::<_, i32>(
                    "SELECT locks.pid FROM pg_locks locks JOIN pg_stat_activity activity USING (pid)
                     WHERE locks.locktype = 'advisory' AND locks.granted AND locks.objsubid = 1
                       AND locks.classid::bigint = (($1::bigint >> 32) & 4294967295)
                       AND locks.objid::bigint = ($1::bigint & 4294967295)
                       AND activity.wait_event = 'relation'
                       AND activity.query LIKE '%shardline_retention_holds%'",
                ).bind(0x5348_4152_4447_4301_i64).fetch_optional(&pool).await.unwrap();
                if let Some(owner) = owner {
                    break owner;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::acquire_postgres_exclusive(&pool),
        )
        .await
        .unwrap()
        .unwrap();
        blocker.rollback().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(2), releaser)
            .await
            .unwrap()
            .unwrap();
        let observed = index.retention_hold(hold.object_key()).await.unwrap();
        drop(replacement);
        index
            .delete_retention_hold(hold.object_key())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "the old release continued after losing its owning session"
        );
        assert_eq!(observed, Some(hold), "the protected hold was removed");
    }

    async fn acknowledged_hold_protects_oci_bytes(config: &ServerConfig) {
        use shardline_index::{OciObjectKey, OciObjectKind, OciObjectStore};
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
        let object = OciObjectKey {
            scope_namespace: "global".to_owned(),
            // Each temporary storage root gets a distinct immutable OCI
            // history, including when the fixture database is reused.
            repository: format!(
                "retention/barrier-{}",
                shardline_index::xet_hash_hex_string(shardline_server_core::chunk_hash(
                    config.root_dir().as_os_str().as_encoded_bytes()
                ))
            ),
            kind: OciObjectKind::Blob,
            digest_hex: "ab".repeat(32),
        };
        let key =
            crate::oci_adapter::oci_blob_key(&object.repository, &object.digest_hex, None).unwrap();
        let objects = crate::object_store::object_store_from_config(config).unwrap();
        let body = b"retained OCI object";
        objects
            .put_if_absent(
                &key,
                ObjectBody::Borrowed(body),
                &ObjectIntegrity::new(shardline_server_core::chunk_hash(body), body.len() as u64),
            )
            .unwrap();
        if let Some(url) = config.index_postgres_url() {
            let pool = sqlx::PgPool::connect(url).await.unwrap();
            PostgresIndexStore::new(pool)
                .delete_oci_object(&object)
                .await
                .unwrap();
            // Postgres stores whole seconds rounded from its clock; let this
            // tombstone become eligible under the GC process's floored clock.
            tokio::time::sleep(Duration::from_millis(1100)).await;
        } else {
            LocalIndexStore::new(config.root_dir().to_path_buf())
                .unwrap()
                .delete_oci_object(&object)
                .await
                .unwrap();
        }
        let hold = RetentionHold::new(
            key.clone(),
            "acknowledged before GC".to_owned(),
            shardline_protocol::unix_now_seconds_lossy(),
            None,
        )
        .unwrap();
        set_retention_hold(config, &hold).await.unwrap();
        let options = crate::LocalGcOptions {
            mark: false,
            sweep: true,
            retention_seconds: 0,
            max_revisions_per_repo: None,
        };
        crate::run_gc(config.clone(), options).await.unwrap();
        assert!(
            objects.contains(&key).unwrap(),
            "acknowledged active hold must protect bytes"
        );
        assert!(release_retention_hold(config, &key).await.unwrap());
        crate::run_gc(config.clone(), options).await.unwrap();
        assert!(
            !objects.contains(&key).unwrap(),
            "released hold must permit reclamation"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_acknowledged_hold_protects_physical_oci_reclamation() {
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_server_frontends([crate::ServerFrontend::Oci])
            .unwrap();
        acknowledged_hold_protects_oci_bytes(&config).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_acknowledged_hold_protects_physical_oci_reclamation() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url)
            .unwrap()
            .with_server_frontends([crate::ServerFrontend::Oci])
            .unwrap();
        acknowledged_hold_protects_oci_bytes(&config).await;
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn local_lifecycle_repair_waits_for_gc_barrier() {
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path());
        let gc_guard = maintenance_barrier::acquire_local_exclusive(storage.path())
            .await
            .unwrap();
        let mut repair = tokio::spawn(async move {
            crate::run_lifecycle_repair(config, crate::LifecycleRepairOptions::default()).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut repair)
                .await
                .is_err()
        );
        drop(gc_guard);
        tokio::time::timeout(Duration::from_secs(2), repair)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let gc_guard = maintenance_barrier::acquire_local_exclusive(storage.path())
            .await
            .unwrap();
        let root = storage.path().to_path_buf();
        let mut local_repair = tokio::spawn(async move {
            crate::run_local_lifecycle_repair(root, crate::LifecycleRepairOptions::default()).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut local_repair)
                .await
                .is_err()
        );
        drop(gc_guard);
        tokio::time::timeout(Duration::from_secs(2), local_repair)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_lifecycle_repair_waits_for_gc_barrier() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let gc_guard = maintenance_barrier::acquire_postgres_exclusive(&pool)
            .await
            .unwrap();
        let mut repair = tokio::spawn(async move {
            crate::run_lifecycle_repair(config, crate::LifecycleRepairOptions::default()).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut repair)
                .await
                .is_err()
        );
        drop(gc_guard);
        tokio::time::timeout(Duration::from_secs(5), repair)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn postgres_lifecycle_repair_cannot_mutate_after_barrier_connection_is_lost() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let _test_guard = POSTGRES_TEST_LOCK.lock().await;
        let storage = tempfile::tempdir().unwrap();
        let config = config(storage.path())
            .with_index_postgres_url(url.clone())
            .unwrap();
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        let index = PostgresIndexStore::new(pool.clone());
        let now = shardline_protocol::unix_now_seconds_lossy();
        let hold = RetentionHold::new(
            ObjectKey::parse(&format!("retention/repair-owner-{}", std::process::id())).unwrap(),
            "repair ownership regression".to_owned(),
            now.saturating_sub(100),
            Some(now.saturating_sub(10)),
        )
        .unwrap();
        index.upsert_retention_hold(&hold).await.unwrap();
        let mut blocker = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE shardline_file_records IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .unwrap();
        let repair = tokio::spawn(async move {
            crate::run_lifecycle_repair(config, crate::LifecycleRepairOptions::default()).await
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let blocked = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'relation'
                     AND query LIKE '%shardline_file_records%')",
                )
                .fetch_one(&pool)
                .await
                .unwrap();
                if blocked {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let owner = sqlx::query_scalar::<_, i32>(
            "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted AND objsubid = 1
             AND classid::bigint = (($1::bigint >> 32) & 4294967295)
             AND objid::bigint = ($1::bigint & 4294967295)",
        )
        .bind(0x5348_4152_4447_4301_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap()
        );
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            maintenance_barrier::acquire_postgres_exclusive(&pool),
        )
        .await
        .unwrap()
        .unwrap();
        blocker.rollback().await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(15), repair)
            .await
            .unwrap()
            .unwrap();
        let observed = index.retention_hold(hold.object_key()).await.unwrap();
        drop(replacement);
        index
            .delete_retention_hold(hold.object_key())
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "the former repair owner mutated metadata during GC takeover"
        );
        assert_eq!(
            observed,
            Some(hold),
            "the expired hold was removed without ownership"
        );
    }

    #[tokio::test]
    async fn hold_factory_error_is_preserved_and_absolute_expiration_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let result =
            set_retention_hold_with(&config, || Err::<RetentionHold, _>(ServerError::Overflow))
                .await;
        assert!(matches!(result, Err(ServerError::Overflow)));
        let index = LocalIndexStore::new(root.path().to_path_buf()).unwrap();
        assert!(
            LifecycleStore::list_retention_holds(&index)
                .unwrap()
                .is_empty()
        );
        let absolute = RetentionHold::new(
            ObjectKey::parse("retention/absolute").unwrap(),
            "absolute".into(),
            10,
            Some(11),
        )
        .unwrap();
        set_retention_hold(&config, &absolute).await.unwrap();
        assert_eq!(
            LifecycleStore::retention_hold(&index, absolute.object_key()).unwrap(),
            Some(absolute)
        );
    }
}
