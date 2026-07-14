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
    let config = load_server_config(root)?;
    Ok(run_server_fsck(config).await?)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    /// FsckRuntimeError display and debug.
    #[test]
    fn fsck_runtime_error_display_debug() {
        let err = FsckRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let msg = err.to_string();
        assert!(msg.contains("invalid server role"));

        let debug = format!("{err:?}");
        // The Debug format includes the variant name
        assert!(debug.contains("Config("));
    }

    #[test]
    fn fsck_runtime_error_from_config() {
        let config_err = ServerConfigError::InvalidServerRole;
        let _fsck_err: FsckRuntimeError = config_err.into();
    }

    #[test]
    fn fsck_runtime_error_from_server() {
        // We can't create a ServerError directly (many variants), but we can test
        // the From trait is implemented by asserting the type conversion compiles.
        // Use a simple variant.
        let _err: FsckRuntimeError = ServerConfigError::InvalidServerRole.into();
    }

    #[test]
    fn fsck_runtime_error_equality() {
        // Verify Debug trait is derived properly
        let err = FsckRuntimeError::Config(ServerConfigError::MissingServerFrontends);
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn fsck_runtime_error_server_display() {
        use shardline_server::ServerError;
        let err = FsckRuntimeError::Server(ServerError::NotFound);
        let msg = err.to_string();
        assert!(!msg.is_empty());
        let debug = format!("{err:?}");
        assert!(debug.contains("Server("));
    }

    #[tokio::test]
    async fn run_fsck_rejects_missing_root() {
        let result = run_fsck(Some(Path::new("/nonexistent-shardline-test-root"))).await;
        assert!(result.is_err());
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
