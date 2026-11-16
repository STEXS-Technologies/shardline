use std::path::Path;

use shardline_server::{
    LocalFsckReport, ServerConfigError, ServerError, run_fsck as run_server_fsck,
};
use thiserror::Error;

use crate::config::load_server_config;

/// Local fsck runtime failure.
#[derive(Debug, Error)]
pub enum FsckRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Local integrity verification failed due to an operational server-side error.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Runs Shardline integrity checks for the active deployment.
///
/// # Errors
///
/// Returns [`FsckRuntimeError`] when the configured deployment cannot be scanned.
pub async fn run_fsck(root: Option<&Path>) -> Result<LocalFsckReport, FsckRuntimeError> {
    let config = load_server_config(root, None)?;
    Ok(run_server_fsck(config).await?)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[tokio::test]
    async fn run_fsck_rejects_missing_root() {
        let result = run_fsck(Some(Path::new("/nonexistent-shardline-test-root"))).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_fsck_with_valid_config() {
        let sandbox = tempfile::tempdir().unwrap();
        let result = run_fsck(Some(sandbox.path())).await;
        // On an empty deployment with a valid temp dir, fsck should complete
        assert!(
            result.is_ok(),
            "run_fsck should succeed on empty deployment: {result:?}"
        );
    }

    #[tokio::test]
    async fn run_fsck_rejects_symlinked_root() {
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
        let result = run_fsck(Some(&link)).await;
        #[cfg(unix)]
        assert!(result.is_err());
    }
}
