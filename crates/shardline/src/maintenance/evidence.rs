//! Command-lifetime evidence I/O, separate from the lock-owning work session.
use super::{MaintenanceOperation, Result};
use shardline_server::{ServerConfig, task_tracking::TaskEvidenceStore};

pub(super) fn store(
    operation: &MaintenanceOperation,
    config: &ServerConfig,
    private_state: Option<&std::path::Path>,
) -> Result<TaskEvidenceStore> {
    // Copy endpoints do not establish a PostgreSQL association. In particular,
    // copying local payloads must not start depending on an unavailable DB.
    if matches!(operation, MaintenanceOperation::StorageMigrate { .. }) {
        if config.object_storage_adapter() == shardline_server::ObjectStorageAdapter::S3 {
            // No verified metadata association exists for a raw S3 endpoint.
            // Keep control data in the existing host task state, never payload.
            let root =
                private_state.ok_or("storage-copy history has no writable private archive")?;
            super::artifact::private_directory(root)?;
            let archive = super::source_config(
                root,
                shardline_server::StorageMigrationEndpoint::LocalStateRoot(root.to_path_buf()),
            )?;
            return Ok(TaskEvidenceStore::from_config(&archive)?);
        }
        return Ok(TaskEvidenceStore::from_config(config)?);
    }
    let Some(url) = config.index_postgres_url() else {
        return Ok(TaskEvidenceStore::from_config(config)?.require_metadata_publication());
    };
    let handle = tokio::runtime::Handle::try_current()?;
    handle.block_on(async {
        // One lazy evidence connection survives closure of the work pool. No
        // process, runtime, background worker or execution authority is added.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect_lazy(url)?;
        match TaskEvidenceStore::from_postgres_pool(config, &pool).await {
            Ok((store, scope)) => {
                // Unsupported/pre-upgrade metadata must release its probe
                // session before the legacy command needs that same DB slot.
                if scope.is_none() {
                    pool.close().await;
                }
                Ok(store.require_metadata_publication())
            }
            Err(error) => {
                pool.close().await;
                tracing::warn!(%error, "task metadata unavailable; existing command continues without payload history");
                Ok(TaskEvidenceStore::from_config(config)?.require_metadata_publication())
            }
        }
    })
}

#[cfg(test)]
mod tests;
