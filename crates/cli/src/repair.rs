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

impl RepairReport {
    pub fn print_summary(&self) {
        println!(
            "index_rebuild.scanned_version_records: {}",
            self.index_rebuild.scanned_version_records
        );
        println!(
            "index_rebuild.scanned_retained_shards: {}",
            self.index_rebuild.scanned_retained_shards
        );
        println!(
            "index_rebuild.rebuilt_latest_records: {}",
            self.index_rebuild.rebuilt_latest_records
        );
        println!(
            "index_rebuild.unchanged_latest_records: {}",
            self.index_rebuild.unchanged_latest_records
        );
        println!(
            "index_rebuild.removed_stale_latest_records: {}",
            self.index_rebuild.removed_stale_latest_records
        );
        println!(
            "index_rebuild.scanned_reconstructions: {}",
            self.index_rebuild.scanned_reconstructions
        );
        println!(
            "index_rebuild.unchanged_reconstructions: {}",
            self.index_rebuild.unchanged_reconstructions
        );
        println!(
            "index_rebuild.removed_stale_reconstructions: {}",
            self.index_rebuild.removed_stale_reconstructions
        );
        println!(
            "index_rebuild.rebuilt_dedupe_shard_mappings: {}",
            self.index_rebuild.rebuilt_dedupe_shard_mappings
        );
        println!(
            "index_rebuild.unchanged_dedupe_shard_mappings: {}",
            self.index_rebuild.unchanged_dedupe_shard_mappings
        );
        println!(
            "index_rebuild.removed_stale_dedupe_shard_mappings: {}",
            self.index_rebuild.removed_stale_dedupe_shard_mappings
        );
        println!(
            "index_rebuild.issue_count: {}",
            self.index_rebuild.issue_count()
        );
        self.lifecycle_repair
            .print_summary_prefixed("lifecycle_repair");
        println!("fsck.latest_records: {}", self.fsck.latest_records);
        println!("fsck.version_records: {}", self.fsck.version_records);
        println!(
            "fsck.inspected_chunk_references: {}",
            self.fsck.inspected_chunk_references
        );
        println!(
            "fsck.inspected_dedupe_shard_mappings: {}",
            self.fsck.inspected_dedupe_shard_mappings
        );
        println!(
            "fsck.inspected_reconstructions: {}",
            self.fsck.inspected_reconstructions
        );
        println!(
            "fsck.inspected_webhook_deliveries: {}",
            self.fsck.inspected_webhook_deliveries
        );
        println!(
            "fsck.inspected_provider_repository_states: {}",
            self.fsck.inspected_provider_repository_states
        );
        println!("fsck.issue_count: {}", self.fsck.issue_count());
    }

    pub fn print_cli_summary(&self, root: &Path, webhook_retention_seconds: u64) {
        println!("root: {}", root.display());
        println!("webhook_retention_seconds: {webhook_retention_seconds}");
        self.print_summary();
    }

    pub fn print_issues(&self) {
        for issue in &self.index_rebuild.issues {
            eprintln!(
                "index_rebuild.issue: {} location={} detail={}",
                issue.kind.as_str(),
                issue.location,
                issue.detail
            );
        }
        for issue in &self.fsck.issues {
            eprintln!(
                "fsck.issue: {} location={} detail={}",
                issue.kind.as_str(),
                issue.location,
                issue.detail
            );
        }
    }
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
