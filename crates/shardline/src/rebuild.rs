use std::path::Path;

use shardline_server::{
    LocalIndexRebuildReport, ServerConfigError, ServerError,
    run_index_rebuild as run_server_index_rebuild,
};
use thiserror::Error;

use crate::config::load_server_config;

/// Local index-rebuild runtime failure.
#[derive(Debug, Error)]
pub enum RebuildRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Local index rebuild failed due to an operational server-side error.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Rebuilds latest-record state from immutable version records for the active deployment.
///
/// # Errors
///
/// Returns [`RebuildRuntimeError`] when the configured deployment cannot be scanned or updated.
pub async fn run_index_rebuild(
    root: Option<&Path>,
) -> Result<LocalIndexRebuildReport, RebuildRuntimeError> {
    let config = load_server_config(root, None)?;
    Ok(run_server_index_rebuild(config).await?)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn run_index_rebuild_rejects_missing_root() {
        let result = run_index_rebuild(Some(Path::new("/nonexistent-shardline-test-root"))).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_index_rebuild_with_valid_config() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = run_index_rebuild(Some(sandbox.path())).await;
        // On an empty deployment with a valid temp dir, rebuild should complete
        assert!(
            result.is_ok(),
            "run_index_rebuild should succeed on empty deployment: {result:?}"
        );
    }

    #[tokio::test]
    async fn run_index_rebuild_rejects_symlinked_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("real-root");
        std::fs::create_dir_all(&target).unwrap();
        #[cfg(unix)]
        let link = {
            let link = sandbox.path().join("root-link");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            link
        };
        #[cfg(not(unix))]
        let link = target.clone();
        let result = run_index_rebuild(Some(&link)).await;
        #[cfg(unix)]
        assert!(result.is_err());
    }
}
