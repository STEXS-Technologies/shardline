//! Command-lifetime evidence I/O, separate from the lock-owning work session.
use super::{MaintenanceOperation, Result};
use shardline_server::{ServerConfig, task_tracking::TaskEvidenceStore};

pub(super) fn store(
    operation: &MaintenanceOperation,
    config: &ServerConfig,
) -> Result<TaskEvidenceStore> {
    // Copy endpoints do not establish a PostgreSQL association. In particular,
    // copying local payloads must not start depending on an unavailable DB.
    if matches!(operation, MaintenanceOperation::StorageMigrate { .. }) {
        return Ok(TaskEvidenceStore::from_config(config)?);
    }
    let Some(url) = config.index_postgres_url() else {
        return Ok(TaskEvidenceStore::from_config(config)?);
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
                Ok(store)
            }
            Err(error) => {
                pool.close().await;
                tracing::warn!(%error, "task metadata unavailable; existing command and legacy tracking continue");
                Ok(TaskEvidenceStore::from_config(config)?)
            }
        }
    })
}
