use shardline_index::{AsyncIndexStore, LocalIndexStore, PostgresIndexStore, RetentionHold};
use shardline_storage::ObjectKey;

use crate::{
    ServerConfig, ServerError, maintenance_barrier,
    postgres_backend::connect_postgres_metadata_pool,
};

/// A shared GC/write barrier held until this guard is dropped.
///
/// Keep it alive throughout the metadata and object-store operation being
/// coordinated. It excludes mutating GC on the configured deployment backend.
#[derive(Debug)]
pub struct MetadataWriteBarrier {
    _guard: maintenance_barrier::MaintenanceBarrierGuard,
}

/// Acquires the deployment's shared GC/write barrier for a library operation.
///
/// This uses the same local root or Postgres advisory key as [`crate::run_gc`].
/// Acquire it before resource locks to preserve the server's lock order.
///
/// # Errors
///
/// Returns [`ServerError`] when backend configuration or lock acquisition fails.
pub async fn acquire_metadata_write_barrier(
    config: &ServerConfig,
) -> Result<MetadataWriteBarrier, ServerError> {
    let guard = if let Some(url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(url, 4)?;
        maintenance_barrier::acquire_postgres_shared(&pool).await?
    } else {
        maintenance_barrier::acquire_local_shared(config.root_dir()).await?
    };
    Ok(MetadataWriteBarrier { _guard: guard })
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
    if let Some(url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(url, 4)?;
        let _barrier = maintenance_barrier::acquire_postgres_shared(&pool).await?;
        PostgresIndexStore::new(pool)
            .upsert_retention_hold(hold)
            .await?;
    } else {
        let _barrier = maintenance_barrier::acquire_local_shared(config.root_dir()).await?;
        LocalIndexStore::new(config.root_dir().to_path_buf())?
            .upsert_retention_hold(hold)
            .await?;
    }
    Ok(())
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
    if let Some(url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(url, 4)?;
        let _barrier = maintenance_barrier::acquire_postgres_shared(&pool).await?;
        Ok(PostgresIndexStore::new(pool)
            .delete_retention_hold(object_key)
            .await?)
    } else {
        let _barrier = maintenance_barrier::acquire_local_shared(config.root_dir()).await?;
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
    async fn acknowledged_hold_protects_oci_bytes(config: &ServerConfig) {
        use shardline_index::{OciObjectKey, OciObjectKind, OciObjectStore};
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
        let object = OciObjectKey {
            scope_namespace: "global".to_owned(),
            repository: "retention/barrier".to_owned(),
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
}
