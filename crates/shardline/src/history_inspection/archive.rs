//! Explicit owner-selected archive inspection; no deployment scope is inferred.
use shardline_server::task_tracking::TaskEvidenceStore;
use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::Path,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(super) async fn run(
    root: &Path,
    after: Option<String>,
    limit: usize,
) -> Result<serde_json::Value> {
    if !(1..=100).contains(&limit) || after.as_ref().is_some_and(|id| id.len() > 128) {
        return Err("archive history requires --limit 1–100 and its own next_after cursor".into());
    }
    let root = crate::local_output::resolved_output_path(root)?;
    let metadata = std::fs::symlink_metadata(&root)?;
    if !metadata.is_dir()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.permissions().mode() & 0o077 != 0
    {
        return Err(
            "inspect a private archive as its owner; preserve its existing permissions".into(),
        );
    }
    if root.join("identity.json").exists() || root.join("tasks.sqlite").exists() {
        return tokio::task::spawn_blocking(move || {
            crate::maintenance::inspection::run(&root, after.as_deref())
        })
        .await?;
    }
    let config = crate::maintenance::metadata_config(root)?;
    let (store, _) = TaskEvidenceStore::for_inspection(&config, None).await?;
    Ok(
        tokio::task::spawn_blocking(move || store.inspection_archive_page(after.as_deref(), limit))
            .await??,
    )
}
