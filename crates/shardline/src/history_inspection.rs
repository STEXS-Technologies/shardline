//! Read-only history inspection using existing deployment configuration.
use shardline_server::task_tracking::{TaskEvidenceStore, deployment_fingerprint};
use std::path::Path;

pub(crate) fn failure_guidance(error: &(dyn std::error::Error + 'static)) -> Option<&'static str> {
    let mut current = Some(error);
    while let Some(cause) = current {
        if shardline_index::LocalIndexStoreError::read_only_wal_needs_sidecars(cause) {
            return Some(
                "SQLite history requires readable metadata.sqlite3-wal and metadata.sqlite3-shm files when the deployment directory is read-only. Inspect the running deployment with its existing sidecars, or a consistent offline backup. Preserve the database and its WAL together; inspection did not change database contents.",
            );
        }
        current = cause.source();
    }
    None
}

pub(crate) async fn run(
    root: Option<&Path>,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let config = crate::config::load_server_config(root, None)?;
    let deployment = deployment_fingerprint(&config)?;
    // Inspection never creates a server backend or runs its startup sweeps.
    let pool = config
        .index_postgres_url()
        .map(|url| {
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(std::time::Duration::from_secs(5))
                .connect_lazy(url)
        })
        .transpose()?;
    let (store, scope) = TaskEvidenceStore::for_inspection(&config, pool.as_ref()).await?;
    let result = tokio::task::spawn_blocking(move || {
        store.inspection_page(deployment, scope, after.as_deref(), limit)
    })
    .await;
    if let Some(pool) = pool {
        pool.close().await;
    }
    Ok(result??)
}
