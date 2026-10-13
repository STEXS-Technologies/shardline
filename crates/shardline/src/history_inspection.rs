//! Read-only history inspection using existing deployment configuration.
use shardline_server::task_tracking::{TaskEvidenceStore, deployment_fingerprint};
use std::path::Path;

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
