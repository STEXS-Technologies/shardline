use std::path::Path;

use shardline_server::{
    LifecycleRepairOptions, LifecycleRepairReport, LocalFsckReport, LocalIndexRebuildReport,
    ServerConfigError, ServerError, run_fsck as run_server_fsck,
    run_index_rebuild as run_server_index_rebuild,
    run_lifecycle_repair as run_server_lifecycle_repair,
};
use thiserror::Error;

use crate::config::load_server_config;

/// Report produced by the top-level repair orchestrator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairReport {
    /// Derived index rebuild report.
    pub index_rebuild: LocalIndexRebuildReport,
    /// Lifecycle metadata repair report.
    pub lifecycle_repair: LifecycleRepairReport,
    /// Final verification report after repair steps complete.
    pub fsck: LocalFsckReport,
}

/// Local lifecycle-repair runtime failure.
#[derive(Debug, Error)]
pub enum RepairRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Lifecycle repair failed due to an operational server-side error.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Repairs stale lifecycle metadata for the active Shardline deployment.
///
/// # Errors
///
/// Returns [`RepairRuntimeError`] when configuration or metadata access fails.
pub async fn run_lifecycle_repair(
    root: Option<&Path>,
    webhook_retention_seconds: u64,
) -> Result<LifecycleRepairReport, RepairRuntimeError> {
    let options = LifecycleRepairOptions {
        webhook_retention_seconds,
    };
    let config = load_server_config(root)?;
    Ok(run_server_lifecycle_repair(config, options).await?)
}

/// Runs the full local repair sequence for the active Shardline deployment.
///
/// The sequence rebuilds derived index state, repairs lifecycle metadata, and then runs
/// fsck so the caller can fail the command when integrity issues remain.
///
/// # Errors
///
/// Returns [`RepairRuntimeError`] when configuration loading or any repair step fails.
pub async fn run_repair(
    root: Option<&Path>,
    webhook_retention_seconds: u64,
) -> Result<RepairReport, RepairRuntimeError> {
    let options = LifecycleRepairOptions {
        webhook_retention_seconds,
    };
    let config = load_server_config(root)?;
    let index_rebuild = run_server_index_rebuild(config.clone()).await?;
    let lifecycle_repair = run_server_lifecycle_repair(config.clone(), options).await?;
    let fsck = run_server_fsck(config).await?;

    Ok(RepairReport {
        index_rebuild,
        lifecycle_repair,
        fsck,
    })
}

#[cfg(test)]
mod tests {
    use shardline_server::DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS;

    use super::LifecycleRepairOptions;

    #[test]
    fn lifecycle_repair_default_retention_matches_server_default() {
        let options = LifecycleRepairOptions::default();

        assert_eq!(
            options.webhook_retention_seconds,
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
        );
    }
}
