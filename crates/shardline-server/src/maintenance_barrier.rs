use std::{
    fs::{File, TryLockError},
    path::{Path, PathBuf},
    time::Duration,
};

use sha2::{Digest, Sha256};
use shardline_index::ResourceLockKey;
use sqlx::{
    PgConnection, PgPool, Postgres, Transaction, pool::PoolConnection, query, query_scalar,
};

use crate::ServerError;

/// Stable application-level advisory-lock key for GC versus visible metadata writers.
const GC_WRITE_BARRIER_KEY: i64 = 0x5348_4152_4447_4301;
const LOCAL_BARRIER_FILE_NAME: &str = ".gc-write-barrier.lock";
const LOCAL_RESOURCE_LOCK_DIR: &str = ".resource-locks";

/// Held shared or exclusive maintenance barrier.
///
/// Dropping the local file releases its advisory lock. Dropping the Postgres
/// transaction rolls it back and releases its transaction-scoped advisory lock.
pub(crate) enum MaintenanceBarrierGuard {
    Local {
        file: File,
    },
    Postgres {
        _transaction: Transaction<'static, Postgres>,
    },
}

impl std::fmt::Debug for MaintenanceBarrierGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { .. } => formatter.write_str("MaintenanceBarrierGuard::Local"),
            Self::Postgres { .. } => formatter.write_str("MaintenanceBarrierGuard::Postgres"),
        }
    }
}

impl Drop for MaintenanceBarrierGuard {
    fn drop(&mut self) {
        if let Self::Local { file } = self {
            let _ignored = file.unlock();
        }
    }
}

/// Exclusive ownership of one mutable application resource.
///
/// Postgres guards retain a dedicated session-level advisory lock and a durable,
/// monotonically increasing fencing epoch. The connection is always closed rather
/// than returned to the pool so a session lock cannot leak into an unrelated request.
pub(crate) enum ResourceWriteGuard {
    Local {
        files: Vec<File>,
    },
    Postgres {
        connection: PoolConnection<Postgres>,
        fences: Vec<(ResourceLockKey, i64)>,
    },
}

impl std::fmt::Debug for ResourceWriteGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { .. } => formatter.write_str("ResourceWriteGuard::Local"),
            Self::Postgres { fences, .. } => formatter
                .debug_struct("ResourceWriteGuard::Postgres")
                .field("fences", fences)
                .finish_non_exhaustive(),
        }
    }
}

impl Drop for ResourceWriteGuard {
    fn drop(&mut self) {
        if let Self::Local { files } = self {
            for file in files {
                let _ignored = file.unlock();
            }
        }
    }
}

impl ResourceWriteGuard {
    pub(crate) fn postgres_fences(&self) -> Option<Vec<shardline_index::PostgresResourceFence>> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { fences, .. } => Some(
                fences
                    .iter()
                    .map(|(key, epoch)| {
                        shardline_index::PostgresResourceFence::new(key.clone(), *epoch)
                    })
                    .collect(),
            ),
        }
    }

    pub(crate) fn postgres_connection_mut(&mut self) -> Option<&mut PgConnection> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { connection, .. } => Some(&mut **connection),
        }
    }

    /// Checks every fence using the session that owns all of the locks.
    pub(crate) async fn assert_current(&mut self) -> Result<(), ServerError> {
        let Self::Postgres { connection, fences } = self else {
            return Ok(());
        };
        for (key, epoch) in fences {
            let current = query_scalar::<_, i64>(
                "SELECT epoch FROM shardline_resource_fences WHERE domain = $1 AND resource = $2",
            )
            .bind(key.domain().as_str())
            .bind(key.resource())
            .fetch_optional(&mut **connection)
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
            if current != Some(*epoch) {
                return Err(ServerError::StaleResourceFence);
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn epoch(&self) -> Option<i64> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { fences, .. } => fences.first().map(|(_, epoch)| *epoch),
        }
    }
}

/// Long-lived advisory guards must never borrow from the metadata work pool.
/// Each backend owns separate bounded GC and resource coordination pools.
pub(crate) fn postgres_coordination_pool(url: &str) -> Result<PgPool, ServerError> {
    crate::postgres_backend::connect_postgres_metadata_pool(url, 4)
}

fn ordered_resources(keys: &[ResourceLockKey]) -> Vec<ResourceLockKey> {
    let mut keys = keys.to_vec();
    keys.sort();
    keys.dedup();
    keys
}

pub(crate) async fn acquire_local_shared(
    root: &Path,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    acquire_local(root, false).await
}

pub(crate) async fn acquire_local_exclusive(
    root: &Path,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    acquire_local(root, true).await
}

async fn acquire_local(
    root: &Path,
    exclusive: bool,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    let path = root.join(LOCAL_BARRIER_FILE_NAME);
    let file = acquire_local_file_lock(path, exclusive).await?;
    Ok(MaintenanceBarrierGuard::Local { file })
}

/// Waits cooperatively for the OS lock. Only opening the file uses a blocking
/// worker: cancellation must not leave a worker waiting for another process to
/// release its lock, or exhaust the blocking pool and prevent runtime shutdown.
async fn acquire_local_file_lock(path: PathBuf, exclusive: bool) -> Result<File, ServerError> {
    let file = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| std::io::Error::other("maintenance lock has no parent"))?,
        )?;
        File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)
    })
    .await
    .map_err(|error| ServerError::Io(std::io::Error::other(error)))?
    .map_err(ServerError::Io)?;
    let mut retry_delay = Duration::from_millis(10);
    loop {
        let result = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        match result {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) => {}
            Err(TryLockError::Error(error)) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(TryLockError::Error(error)) => return Err(ServerError::Io(error)),
        }
        tokio::time::sleep(retry_delay).await;
        retry_delay = retry_delay.saturating_mul(2).min(Duration::from_millis(50));
    }
}

/// Acquires an exclusive application-resource lock through the shared local root.
///
/// The lock filename is a digest of the domain and resource, so caller-controlled
/// repository names never become path components.
pub(crate) async fn acquire_local_resource_exclusive(
    root: &Path,
    key: &ResourceLockKey,
) -> Result<ResourceWriteGuard, ServerError> {
    acquire_local_resources_exclusive(root, std::slice::from_ref(key)).await
}

pub(crate) async fn acquire_local_resources_exclusive(
    root: &Path,
    keys: &[ResourceLockKey],
) -> Result<ResourceWriteGuard, ServerError> {
    let mut files = Vec::new();
    for key in ordered_resources(keys) {
        let digest = resource_lock_digest(&key);
        let path = root
            .join(LOCAL_RESOURCE_LOCK_DIR)
            .join(format!("{digest}.lock"));
        files.push(acquire_local_file_lock(path, true).await?);
    }
    Ok(ResourceWriteGuard::Local { files })
}

pub(crate) async fn acquire_postgres_shared(
    pool: &PgPool,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    acquire_postgres(pool, false).await
}

pub(crate) async fn acquire_postgres_exclusive(
    pool: &PgPool,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    acquire_postgres(pool, true).await
}

async fn acquire_postgres(
    pool: &PgPool,
    exclusive: bool,
) -> Result<MaintenanceBarrierGuard, ServerError> {
    let mut transaction = pool
        .begin()
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    let function = if exclusive {
        "pg_advisory_xact_lock"
    } else {
        "pg_advisory_xact_lock_shared"
    };
    sqlx::query(&format!("SELECT {function}($1)"))
        .bind(GC_WRITE_BARRIER_KEY)
        .execute(&mut *transaction)
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    Ok(MaintenanceBarrierGuard::Postgres {
        _transaction: transaction,
    })
}

/// Acquires an exclusive session-scoped advisory lock for an application resource.
pub(crate) async fn acquire_postgres_resource_exclusive(
    pool: &PgPool,
    key: &ResourceLockKey,
) -> Result<ResourceWriteGuard, ServerError> {
    acquire_postgres_resources_exclusive(pool, std::slice::from_ref(key)).await
}

/// A bundle uses one session regardless of resource count. Sorted acquisition
/// prevents opposing renames from deadlocking. Closing the session also releases
/// partial bundles when acquisition is cancelled or a fencing update fails.
pub(crate) async fn acquire_postgres_resources_exclusive(
    pool: &PgPool,
    keys: &[ResourceLockKey],
) -> Result<ResourceWriteGuard, ServerError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    connection.close_on_drop();
    let mut fences = Vec::new();
    for key in ordered_resources(keys) {
        query("SELECT pg_advisory_lock($1)")
            .bind(resource_lock_key(&key))
            .execute(&mut *connection)
            .await
            .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        let epoch = query_scalar::<_, i64>(
            "INSERT INTO shardline_resource_fences (domain, resource, epoch)
             VALUES ($1, $2, 1)
             ON CONFLICT (domain, resource)
             DO UPDATE SET epoch = shardline_resource_fences.epoch + 1
             RETURNING epoch",
        )
        .bind(key.domain().as_str())
        .bind(key.resource())
        .fetch_one(&mut *connection)
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        fences.push((key, epoch));
    }
    Ok(ResourceWriteGuard::Postgres { connection, fences })
}

fn resource_lock_digest(key: &ResourceLockKey) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"shardline-resource-lock\0");
    hasher.update(key.domain().as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(key.resource().as_bytes());
    hex::encode(hasher.finalize())
}

fn resource_lock_key(key: &ResourceLockKey) -> i64 {
    let digest = Sha256::digest(resource_lock_digest(key).as_bytes());
    let mut bytes = [0_u8; 8];
    if let Some(prefix) = digest.get(..bytes.len()) {
        bytes.copy_from_slice(prefix);
    }
    i64::from_be_bytes(bytes)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use super::*;

    #[test]
    fn cancelled_local_lock_waits_leave_blocking_pool_and_shutdown_available() {
        let storage = shardline_test_support::TempStorage::new();
        let barrier_path = storage.path().join(LOCAL_BARRIER_FILE_NAME);
        let resource_key = ResourceLockKey::oci_repository("global", "cancelled-waiter");
        let resource_directory = storage.path().join(LOCAL_RESOURCE_LOCK_DIR);
        std::fs::create_dir_all(&resource_directory).unwrap();
        let resource_path =
            resource_directory.join(format!("{}.lock", resource_lock_digest(&resource_key)));
        let open_locked = |path| {
            let file = File::options()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(path)
                .unwrap();
            file.lock().unwrap();
            file
        };
        // Independent file handles model a GC/maintenance process that keeps
        // owning its OS locks after HTTP request cancellation and shutdown.
        let barrier_owner = open_locked(barrier_path);
        let resource_owner = open_locked(resource_path);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_time()
            .build()
            .unwrap();
        let (shared_cancelled, exclusive_cancelled, resource_cancelled, unrelated_completed) =
            runtime.block_on(async {
                let deadline = Duration::from_millis(50);
                let shared_cancelled =
                    tokio::time::timeout(deadline, acquire_local_shared(storage.path()))
                        .await
                        .is_err();
                let exclusive_cancelled =
                    tokio::time::timeout(deadline, acquire_local_exclusive(storage.path()))
                        .await
                        .is_err();
                let resource_cancelled = tokio::time::timeout(
                    deadline,
                    acquire_local_resource_exclusive(storage.path(), &resource_key),
                )
                .await
                .is_err();
                let unrelated_completed = matches!(
                    tokio::time::timeout(
                        Duration::from_secs(2),
                        tokio::task::spawn_blocking(|| 42)
                    )
                    .await,
                    Ok(Ok(42))
                );
                (
                    shared_cancelled,
                    exclusive_cancelled,
                    resource_cancelled,
                    unrelated_completed,
                )
            });
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let shutdown = std::thread::spawn(move || {
            drop(runtime);
            shutdown_tx.send(()).unwrap();
        });
        let shutdown_completed = shutdown_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        // Release before assertions so a regression cannot hang the test suite
        // while waiting for the old detached blocking tasks to terminate.
        drop((barrier_owner, resource_owner));
        shutdown.join().unwrap();
        assert!(shared_cancelled && exclusive_cancelled && resource_cancelled);
        assert!(
            unrelated_completed,
            "cancelled lock waits exhausted the blocking pool"
        );
        assert!(
            shutdown_completed,
            "runtime shutdown waited for an externally held lock"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_exclusive_waits_for_shared_guard() {
        let storage = shardline_test_support::TempStorage::new();
        let shared = acquire_local_shared(storage.path()).await.unwrap();
        let root = storage.path_buf();
        let mut waiter = tokio::spawn(async move { acquire_local_exclusive(&root).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );
        drop(shared);
        let exclusive = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("exclusive lock should become available")
            .expect("exclusive lock task should complete")
            .unwrap();
        drop(exclusive);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_shared_guards_can_coexist() {
        let storage = shardline_test_support::TempStorage::new();
        let first = acquire_local_shared(storage.path()).await.unwrap();
        let second =
            tokio::time::timeout(Duration::from_secs(2), acquire_local_shared(storage.path()))
                .await
                .expect("second shared lock should not block")
                .unwrap();
        drop((first, second));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn local_resource_lock_serializes_only_the_same_resource() {
        let storage = shardline_test_support::TempStorage::new();
        let first_key = ResourceLockKey::oci_repository("global", "team/a");
        let first = acquire_local_resource_exclusive(storage.path(), &first_key)
            .await
            .unwrap();

        let other_key = ResourceLockKey::oci_repository("global", "team/b");
        let other = tokio::time::timeout(
            Duration::from_secs(2),
            acquire_local_resource_exclusive(storage.path(), &other_key),
        )
        .await
        .expect("unrelated resource must not block")
        .unwrap();
        drop(other);

        let root = storage.path_buf();
        let waiter_key = first_key.clone();
        let mut waiter =
            tokio::spawn(async move { acquire_local_resource_exclusive(&root, &waiter_key).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("same-resource lock should become available")
            .expect("same-resource lock task should complete")
            .unwrap();
        drop(second);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_one_connection_pools_keep_nested_writers_and_gc_moving() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let work = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let shared_pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let exclusive_pool =
            crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let resources = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let shared = acquire_postgres_shared(&shared_pool).await.unwrap();
        let keys = vec![
            ResourceLockKey::oci_repository("pool-test", "b"),
            ResourceLockKey::oci_repository("pool-test", "a"),
            ResourceLockKey::oci_repository("pool-test", "a"),
        ];
        let mut bundle = acquire_postgres_resources_exclusive(&resources, &keys)
            .await
            .unwrap();
        assert_eq!(bundle.postgres_fences().unwrap().len(), 2);
        let mut gc =
            tokio::spawn(async move { acquire_postgres_exclusive(&exclusive_pool).await.unwrap() });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut gc)
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), query("SELECT 1").execute(&work))
            .await
            .unwrap()
            .unwrap();
        bundle.assert_current().await.unwrap();
        drop(bundle);
        let replacement = tokio::time::timeout(
            Duration::from_secs(2),
            acquire_postgres_resources_exclusive(&resources, &keys),
        )
        .await
        .unwrap()
        .unwrap();
        drop(replacement);
        drop(shared);
        let exclusive = tokio::time::timeout(Duration::from_secs(2), gc)
            .await
            .unwrap()
            .unwrap();
        let next_pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let mut writer =
            tokio::spawn(async move { acquire_postgres_shared(&next_pool).await.unwrap() });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut writer)
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), query("SELECT 1").execute(&work))
            .await
            .unwrap()
            .unwrap();
        drop(exclusive);
        drop(
            tokio::time::timeout(Duration::from_secs(2), writer)
                .await
                .unwrap()
                .unwrap(),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_cancelled_partial_bundle_releases_every_session_lock() {
        let Ok(url) = std::env::var("DATABASE_URL") else {
            return;
        };
        let first_pool = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let waiting_pool =
            crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        let a = ResourceLockKey::oci_repository("bundle-cancel", "a");
        let b = ResourceLockKey::oci_repository("bundle-cancel", "b");
        let held = acquire_postgres_resource_exclusive(&first_pool, &b)
            .await
            .unwrap();
        let keys = vec![b.clone(), a.clone()];
        let waiter = tokio::spawn(async move {
            acquire_postgres_resources_exclusive(&waiting_pool, &keys).await
        });
        let observer = crate::postgres_backend::connect_postgres_metadata_pool(&url, 1).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let owned = query_scalar::<_, bool>("SELECT NOT pg_try_advisory_lock($1)")
                    .bind(resource_lock_key(&a))
                    .fetch_one(&observer)
                    .await
                    .unwrap();
                if owned {
                    break;
                }
                query("SELECT pg_advisory_unlock($1)")
                    .bind(resource_lock_key(&a))
                    .execute(&observer)
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        let recovered = tokio::time::timeout(
            Duration::from_secs(2),
            acquire_postgres_resource_exclusive(&observer, &a),
        )
        .await
        .unwrap()
        .unwrap();
        drop((recovered, held));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_exclusive_waits_for_cross_connection_shared_guard() {
        let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPool::connect(&database_url).await.unwrap();
        let shared = acquire_postgres_shared(&pool).await.unwrap();
        let waiter_pool = pool.clone();
        let mut waiter =
            tokio::spawn(async move { acquire_postgres_exclusive(&waiter_pool).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );
        drop(shared);
        let exclusive = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("exclusive Postgres lock should become available")
            .expect("exclusive Postgres lock task should complete")
            .unwrap();
        drop(exclusive);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_resource_lock_waits_across_connections() {
        let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPool::connect(&database_url).await.unwrap();
        let key = ResourceLockKey::oci_repository("global", "team/a");
        let first = acquire_postgres_resource_exclusive(&pool, &key)
            .await
            .unwrap();
        let first_epoch = first.epoch().unwrap();
        let waiter_pool = pool.clone();
        let waiter_key = key.clone();
        let mut waiter = tokio::spawn(async move {
            acquire_postgres_resource_exclusive(&waiter_pool, &waiter_key).await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut waiter)
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("same Postgres resource lock should become available")
            .expect("same Postgres resource lock task should complete")
            .unwrap();
        assert!(second.epoch().unwrap() > first_epoch);
        drop(second);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_resource_fence_rejects_terminated_owner() {
        let Some(database_url) = std::env::var("DATABASE_URL").ok() else {
            eprintln!("skipping: no DATABASE_URL");
            return;
        };
        let pool = PgPool::connect(&database_url).await.unwrap();
        let key = ResourceLockKey::provider_repository("github", "test", "terminated-owner");
        let mut first = acquire_postgres_resource_exclusive(&pool, &key)
            .await
            .unwrap();
        let first_epoch = first.epoch().unwrap();
        let backend_pid = query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(
                first
                    .postgres_connection_mut()
                    .expect("Postgres acquisition must return a Postgres guard"),
            )
            .await
            .unwrap();

        let terminated = query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(backend_pid)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(terminated);

        let second = tokio::time::timeout(
            Duration::from_secs(2),
            acquire_postgres_resource_exclusive(&pool, &key),
        )
        .await
        .expect("replacement owner should acquire the released server-side lock")
        .unwrap();
        assert!(second.epoch().unwrap() > first_epoch);
        assert!(first.assert_current().await.is_err());
    }
}
