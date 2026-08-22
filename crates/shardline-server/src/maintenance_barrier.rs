use std::{fs::File, path::Path};

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
        file: File,
    },
    Postgres {
        connection: PoolConnection<Postgres>,
        key: ResourceLockKey,
        epoch: i64,
    },
}

impl std::fmt::Debug for ResourceWriteGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { .. } => formatter.write_str("ResourceWriteGuard::Local"),
            Self::Postgres { key, epoch, .. } => formatter
                .debug_struct("ResourceWriteGuard::Postgres")
                .field("key", key)
                .field("epoch", epoch)
                .finish_non_exhaustive(),
        }
    }
}

impl Drop for ResourceWriteGuard {
    fn drop(&mut self) {
        if let Self::Local { file } = self {
            let _ignored = file.unlock();
        }
    }
}

impl ResourceWriteGuard {
    /// Returns a copy of this guard's durable Postgres fence identity.
    pub(crate) fn postgres_fence(&self) -> Option<shardline_index::PostgresResourceFence> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { key, epoch, .. } => Some(shardline_index::PostgresResourceFence::new(
                key.clone(),
                *epoch,
            )),
        }
    }

    /// Returns the dedicated lock-owning Postgres connection, when applicable.
    pub(crate) fn postgres_connection_mut(&mut self) -> Option<&mut PgConnection> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { connection, .. } => Some(&mut **connection),
        }
    }

    /// Verifies that this guard still owns the latest durable fencing epoch.
    ///
    /// The query uses the same dedicated Postgres session that owns the advisory
    /// lock. A partitioned or superseded writer therefore fails closed.
    pub(crate) async fn assert_current(&mut self) -> Result<(), ServerError> {
        let Self::Postgres {
            connection,
            key,
            epoch,
        } = self
        else {
            return Ok(());
        };
        let current = query_scalar::<_, i64>(
            "SELECT epoch
             FROM shardline_resource_fences
             WHERE domain = $1 AND resource = $2",
        )
        .bind(key.domain().as_str())
        .bind(key.resource())
        .fetch_optional(&mut **connection)
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
        if current == Some(*epoch) {
            Ok(())
        } else {
            Err(ServerError::StaleResourceFence)
        }
    }

    #[cfg(test)]
    const fn epoch(&self) -> Option<i64> {
        match self {
            Self::Local { .. } => None,
            Self::Postgres { epoch, .. } => Some(*epoch),
        }
    }
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
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| std::io::Error::other("maintenance lock has no parent"))?,
        )?;
        let file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        if exclusive {
            file.lock()?;
        } else {
            file.lock_shared()?;
        }
        Ok::<_, std::io::Error>(MaintenanceBarrierGuard::Local { file })
    })
    .await
    .map_err(|error| ServerError::Io(std::io::Error::other(error)))?
    .map_err(ServerError::Io)
}

/// Acquires an exclusive application-resource lock through the shared local root.
///
/// The lock filename is a digest of the domain and resource, so caller-controlled
/// repository names never become path components.
pub(crate) async fn acquire_local_resource_exclusive(
    root: &Path,
    key: &ResourceLockKey,
) -> Result<ResourceWriteGuard, ServerError> {
    let digest = resource_lock_digest(key);
    let path = root
        .join(LOCAL_RESOURCE_LOCK_DIR)
        .join(format!("{digest}.lock"));
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(
            path.parent()
                .ok_or_else(|| std::io::Error::other("resource lock has no parent"))?,
        )?;
        let file = File::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.lock()?;
        Ok::<_, std::io::Error>(ResourceWriteGuard::Local { file })
    })
    .await
    .map_err(|error| ServerError::Io(std::io::Error::other(error)))?
    .map_err(ServerError::Io)
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

/// Acquires an exclusive transaction-scoped advisory lock for an application resource.
pub(crate) async fn acquire_postgres_resource_exclusive(
    pool: &PgPool,
    key: &ResourceLockKey,
) -> Result<ResourceWriteGuard, ServerError> {
    let advisory_key = resource_lock_key(key);
    let mut connection = pool
        .acquire()
        .await
        .map_err(shardline_index::PostgresMetadataStoreError::from)?;
    connection.close_on_drop();
    query("SELECT pg_advisory_lock($1)")
        .bind(advisory_key)
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
    Ok(ResourceWriteGuard::Postgres {
        connection,
        key: key.clone(),
        epoch,
    })
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
