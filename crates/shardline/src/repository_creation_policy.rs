//! Owner-managed shared policy; no extra service or per-replica configuration.
use shardline_index::{LocalIndexStore, PostgresIndexStore, hub::HubCreationPolicy};
use std::{io::Read, path::Path};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) async fn run(
    namespace: &str,
    root: Option<&Path>,
    policy_file: Option<&Path>,
    clear: bool,
) -> Result<serde_json::Value> {
    if clear && policy_file.is_some() {
        return Err("choose --clear or --policy-file".into());
    }
    let policy = policy_file.map(read_policy).transpose()?;
    let defaults = HubCreationPolicy::default();
    policy.as_ref().unwrap_or(&defaults).validate(namespace)?;
    let config = crate::config::load_server_config(root, None)?;
    let namespace = namespace.to_owned();
    let policy = if let Some(url) = config.index_postgres_url() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .connect(url)
            .await?;
        let result = async {
            shardline_server::check_database_schema_compatibility(&pool).await?;
            let store = PostgresIndexStore::new(pool.clone());
            tokio::task::spawn_blocking(move || -> Result<Option<HubCreationPolicy>> {
                if clear || policy.is_some() {
                    store.set_hub_creation_policy(&namespace, policy.as_ref())?;
                }
                Ok(store.hub_creation_policy(&namespace)?)
            })
            .await?
        }
        .await;
        pool.close().await;
        result?
    } else {
        // Match the Hub frontend's separate local metadata store. The root
        // metadata database holds storage-domain state, not Hub repositories.
        let storage_root = config.root_dir().join("hub");
        tokio::task::spawn_blocking(move || -> Result<Option<HubCreationPolicy>> {
            let store = LocalIndexStore::open(storage_root);
            store.check_schema_compatibility()?;
            if clear || policy.is_some() {
                store.set_hub_creation_policy(&namespace, policy.as_ref())?;
            }
            Ok(store.hub_creation_policy(&namespace)?)
        })
        .await??
    };
    Ok(serde_json::to_value(policy)?)
}

fn read_policy(path: &Path) -> Result<HubCreationPolicy> {
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err("policy input must be a regular JSON file".into());
    }
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("policy JSON exceeds 64 KiB".into());
    }
    Ok(serde_json::from_slice(&bytes)?)
}
