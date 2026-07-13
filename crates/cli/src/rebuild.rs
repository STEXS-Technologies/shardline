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
    let config = load_server_config(root)?;
    Ok(run_server_index_rebuild(config).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebuild_runtime_error_config_display() {
        let err = RebuildRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let msg = err.to_string();
        assert!(msg.contains("invalid server role"));
    }

    #[test]
    fn rebuild_runtime_error_debug() {
        let err = RebuildRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let debug = format!("{err:?}");
        assert!(debug.contains("Config("));
    }
}
