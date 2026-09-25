use crate::{
    config::ServerConfig,
    error, gc, maintenance_barrier,
    object_store::{ServerObjectStore, object_store_from_config},
    postgres_backend::connect_postgres_metadata_pool,
};
use shardline_index::{LocalIndexStore, LocalRecordStore, PostgresIndexStore, PostgresRecordStore};
use shardline_storage::{AsyncObjectStore as _, ObjectPrefix};

async fn reclaim_postgres_resumable_staging(
    index_store: &PostgresIndexStore,
    object_store: &ServerObjectStore,
    sweep: bool,
) -> Result<ResumableStagingGcReport, error::ServerError> {
    const EXPIRY_BATCH: usize = 10_000;
    if sweep {
        loop {
            let expired = index_store.expire_resumable_sessions(EXPIRY_BATCH).await?;
            if expired.len() < EXPIRY_BATCH {
                break;
            }
        }
    }
    let inventory = index_store.resumable_session_gc_inventory().await?;
    let protected = inventory
        .protected_staging_keys()
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    let mut report =
        reclaim_unprotected_resumable_staging_objects(object_store, &protected, sweep).await?;
    if sweep {
        report.reclaimed_sessions = index_store
            .delete_reclaimable_resumable_sessions(inventory.reclaimable_sessions())
            .await?;
    }
    Ok(report)
}

async fn reclaim_unprotected_resumable_staging_objects(
    object_store: &ServerObjectStore,
    protected: &std::collections::HashSet<String>,
    sweep: bool,
) -> Result<ResumableStagingGcReport, error::ServerError> {
    let prefix = ObjectPrefix::parse("staging/resumable/")
        .map_err(|_error| error::ServerError::InvalidPath)?;
    let objects = object_store.list_prefix(&prefix).await?;
    let mut report = ResumableStagingGcReport {
        scanned_objects: u64::try_from(objects.len())?,
        protected_objects: u64::try_from(
            objects
                .iter()
                .filter(|metadata| protected.contains(metadata.key().as_str()))
                .count(),
        )?,
        ..ResumableStagingGcReport::default()
    };
    if !sweep {
        return Ok(report);
    }
    for metadata in objects {
        if protected.contains(metadata.key().as_str()) {
            continue;
        }
        object_store.delete_if_present(metadata.key()).await?;
        report.reclaimed_objects = report
            .reclaimed_objects
            .checked_add(1)
            .ok_or(error::ServerError::Overflow)?;
        report.reclaimed_bytes = report
            .reclaimed_bytes
            .checked_add(metadata.length())
            .ok_or(error::ServerError::Overflow)?;
    }
    Ok(report)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct ResumableStagingGcReport {
    scanned_objects: u64,
    protected_objects: u64,
    reclaimed_objects: u64,
    reclaimed_bytes: u64,
    reclaimed_sessions: u64,
}

/// Runs garbage collection against the configured metadata backend and local chunk storage.
///
/// # Examples
///
/// ```no_run
/// use shardline_server::{LocalGcOptions, ServerConfig, run_gc};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = ServerConfig::from_env()?;
///     // Start with a dry run, then move to `mark_and_sweep(retention_seconds)`.
///     let report = run_gc(config, LocalGcOptions::dry_run()).await?;
///     assert_eq!(report.deleted_chunks, 0);
///     Ok(())
/// }
/// ```
///
/// See [`LocalGcOptions`] for the supported modes: `dry_run`, `mark_only`,
/// `sweep_only`, and `mark_and_sweep`.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc(
    config: ServerConfig,
    options: gc::LocalGcOptions,
) -> Result<gc::LocalGcReport, error::ServerError> {
    Ok(run_gc_diagnostics(config, options).await?.report)
}

/// Runs garbage collection and returns operator diagnostics.
///
/// # Errors
///
/// Returns [`ServerError`] when metadata cannot be read, quarantine state cannot be
/// updated, or deletion fails.
pub async fn run_gc_diagnostics(
    config: ServerConfig,
    options: gc::LocalGcOptions,
) -> Result<gc::LocalGcDiagnostics, error::ServerError> {
    let object_store = object_store_from_config(&config)?;
    if let Some(index_postgres_url) = config.index_postgres_url() {
        let pool = connect_postgres_metadata_pool(index_postgres_url, 4)?;
        let _gc_barrier = if options.mark || options.sweep {
            Some(maintenance_barrier::acquire_postgres_exclusive(&pool).await?)
        } else {
            None
        };
        let index_store = PostgresIndexStore::new(pool.clone());
        let record_store = PostgresRecordStore::new(pool);
        let staging_report =
            reclaim_postgres_resumable_staging(&index_store, &object_store, options.sweep).await?;
        let mut diagnostics = gc::run_gc_with_oci_tombstones(
            &record_store,
            &index_store,
            &object_store,
            config.server_frontends(),
            options,
        )
        .await
        .map_err(error::ServerError::from)?;
        diagnostics.report.scanned_resumable_staging_objects = staging_report.scanned_objects;
        diagnostics.report.protected_resumable_staging_objects = staging_report.protected_objects;
        diagnostics.report.reclaimed_resumable_staging_objects = staging_report.reclaimed_objects;
        diagnostics.report.reclaimed_resumable_staging_bytes = staging_report.reclaimed_bytes;
        diagnostics.report.reclaimed_resumable_sessions = staging_report.reclaimed_sessions;
        return Ok(diagnostics);
    }

    let _gc_barrier = if options.mark || options.sweep {
        Some(maintenance_barrier::acquire_local_exclusive(config.root_dir()).await?)
    } else {
        None
    };
    let index_store = LocalIndexStore::open(config.root_dir().to_path_buf());
    let record_store = LocalRecordStore::open(config.root_dir().to_path_buf());
    gc::run_gc_with_oci_tombstones(
        &record_store,
        &index_store,
        &object_store,
        config.server_frontends(),
        options,
    )
    .await
    .map_err(Into::into)
}

#[cfg(test)]
mod lib_tests {
    use std::collections::HashSet;

    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey};

    use super::*;

    fn put_test_staging_object(object_store: &ServerObjectStore, key: &ObjectKey, bytes: &[u8]) {
        let integrity = ObjectIntegrity::new(
            shardline_server_core::chunk_hash(bytes),
            u64::try_from(bytes.len()).unwrap(),
        );
        shardline_storage::ObjectStore::put_if_absent(
            object_store,
            key,
            ObjectBody::Borrowed(bytes),
            &integrity,
        )
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_gc_succeeds_on_valid_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::ServerConfig::new(
            std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            tmp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let options = super::gc::LocalGcOptions::default();
        let result = run_gc(config, options).await;
        // On a valid temp directory with default options (mark=false, sweep=false),
        // GC should succeed as a no-op
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_gc_diagnostics_succeeds_on_valid_local_path() {
        let tmp = tempfile::tempdir().unwrap();
        let config = crate::config::ServerConfig::new(
            std::net::SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST), 8080),
            "http://localhost:8080".to_owned(),
            tmp.path().to_path_buf(),
            std::num::NonZeroUsize::new(4096).unwrap(),
        );
        let options = super::gc::LocalGcOptions::default();
        let result = run_gc_diagnostics(config, options).await;
        assert!(result.is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resumable_staging_gc_preserves_live_keys_and_reclaims_every_other_attempt() {
        let tmp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(tmp.path().join("objects")).unwrap();
        let protected_key = ObjectKey::parse("staging/resumable/lfs/live/1/hash").unwrap();
        let replaced_key = ObjectKey::parse("staging/resumable/lfs/live/1/old-hash").unwrap();
        let terminal_key = ObjectKey::parse("staging/resumable/s3/terminal/1/hash").unwrap();
        put_test_staging_object(&object_store, &protected_key, b"live");
        put_test_staging_object(&object_store, &replaced_key, b"old");
        put_test_staging_object(&object_store, &terminal_key, b"terminal");
        let protected = HashSet::from([protected_key.as_str().to_owned()]);

        let report = reclaim_unprotected_resumable_staging_objects(&object_store, &protected, true)
            .await
            .unwrap();
        assert_eq!(report.scanned_objects, 3);
        assert_eq!(report.protected_objects, 1);
        assert_eq!(report.reclaimed_objects, 2);
        assert_eq!(report.reclaimed_bytes, 11);
        assert!(shardline_storage::ObjectStore::contains(&object_store, &protected_key).unwrap());
        assert!(!shardline_storage::ObjectStore::contains(&object_store, &replaced_key).unwrap());
        assert!(!shardline_storage::ObjectStore::contains(&object_store, &terminal_key).unwrap());
    }
}
