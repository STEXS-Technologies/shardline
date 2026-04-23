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
