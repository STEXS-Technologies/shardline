//! Read-only history inspection using existing deployment configuration.
use shardline_server::task_tracking::{TaskEvidenceStore, deployment_fingerprint};
use std::path::Path;

#[cfg(target_os = "linux")]
mod archive;

pub(crate) async fn run_archive(
    root: Option<&Path>,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(target_os = "linux")]
    {
        archive::run(
            root.ok_or("history --archive requires --root")?,
            after,
            limit,
        )
        .await
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (root, after, limit);
        Err("private maintenance archive inspection requires Linux".into())
    }
}

pub(crate) fn failure_guidance(error: &(dyn std::error::Error + 'static)) -> Option<&'static str> {
    let mut current = Some(error);
    while let Some(cause) = current {
        let database_error = cause
            .downcast_ref::<sqlx::Error>()
            .or_else(|| cause.downcast_ref::<Box<sqlx::Error>>().map(Box::as_ref));
        if let Some(sqlx::Error::PoolTimedOut | sqlx::Error::Io(_) | sqlx::Error::Tls(_)) =
            database_error
        {
            return Some(
                "Check PostgreSQL availability, network access and the database configuration used by this deployment, then retry inspection. Do not rerun maintenance just to recover its history; this inspection did not start maintenance work.",
            );
        }
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
    validate_page(after.as_deref(), limit)?;
    let config = crate::config::load_server_config(root, None)?;
    inspect_config(config, after, limit).await
}

pub(crate) async fn run_local(
    root: Option<&Path>,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    validate_page(after.as_deref(), limit)?;
    #[cfg(target_os = "linux")]
    {
        let root = match root {
            Some(root) => root.to_path_buf(),
            None => crate::config::project_state_root(&std::env::current_dir()?),
        };
        let root = crate::local_output::resolved_output_path(&root)?;
        if !root.join("metadata.sqlite3").try_exists()? {
            return Err(
                "No local SQLite metadata exists at this root; inspection did not initialize it."
                    .into(),
            );
        }
        let mut page =
            inspect_config(crate::maintenance::metadata_config(root)?, after, limit).await?;
        let object = page.as_object_mut().ok_or("invalid history page")?;
        object.insert("history_source".into(), serde_json::json!("local_sqlite"));
        object.insert("inspection_notice".into(), serde_json::json!("Local SQLite history only; configured PostgreSQL history is not included. Inspection starts no maintenance."));
        Ok(page)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (root, after);
        Err("host-local history inspection requires Linux".into())
    }
}

fn validate_page(
    after: Option<&str>,
    limit: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if after.as_ref().is_some_and(|cursor| cursor.len() > 128) {
        return Err(
            "history --after must be a next_after value of at most 128 bytes from the history page"
                .into(),
        );
    }
    if !(1..=100).contains(&limit) {
        return Err("history --limit must be between 1 and 100".into());
    }
    Ok(())
}

async fn inspect_config(
    config: shardline_server::ServerConfig,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
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
