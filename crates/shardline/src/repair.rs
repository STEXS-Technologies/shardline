use std::path::Path;

use shardline_server::{
    LifecycleRepairOptions, LifecycleRepairReport, LocalFsckReport, LocalIndexRebuildReport,
    ServerConfigError, ServerError, run_fsck as run_server_fsck,
    run_index_rebuild as run_server_index_rebuild,
    run_lifecycle_repair as run_server_lifecycle_repair,
};
use thiserror::Error;

use crate::{config::load_server_config, report_output};

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
        for location in &self
            .index_rebuild
            .preserved_latest_records_unreadable_version
        {
            println!(
                "index_rebuild.kept_latest_record_unreadable_version: {}",
                location
            );
        }
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
        report_output::print_lifecycle_repair_summary_prefixed(
            &self.lifecycle_repair,
            "lifecycle_repair",
        );
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
    let config = load_server_config(root, None)?;
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
    let config = load_server_config(root, None)?;
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
    use std::path::Path;

    use shardline_server::{
        DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS, FsckIssueDetail, FsckIssueKind,
        IndexRebuildIssueDetail, LifecycleRepairReport, LocalFsckIssue, LocalFsckReport,
        LocalIndexRebuildIssue, LocalIndexRebuildIssueKind, LocalIndexRebuildReport,
        ServerConfigError,
    };

    use super::{LifecycleRepairOptions, RepairReport, RepairRuntimeError};

    #[test]
    fn lifecycle_repair_default_retention_matches_server_default() {
        let options = LifecycleRepairOptions::default();

        assert_eq!(
            options.webhook_retention_seconds,
            DEFAULT_WEBHOOK_DELIVERY_RETENTION_SECONDS
        );
    }

    // ── RepairRuntimeError Display / Debug ──────────────────────────────

    #[test]
    fn repair_runtime_error_config_display() {
        let err = RepairRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let msg = err.to_string();
        assert!(msg.contains("invalid server role"));
    }

    #[test]
    fn repair_runtime_error_server_display() {
        use shardline_server::ServerError;
        let err = RepairRuntimeError::Server(ServerError::NotFound);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn repair_runtime_error_debug() {
        let err = RepairRuntimeError::Config(ServerConfigError::InvalidServerRole);
        let debug = format!("{err:?}");
        assert!(debug.contains("Config("));
    }

    // ── RepairReport smoke tests ────────────────────────────────────────

    fn empty_repair_report() -> RepairReport {
        RepairReport {
            index_rebuild: LocalIndexRebuildReport {
                scanned_version_records: 0,
                scanned_retained_shards: 0,
                rebuilt_latest_records: 0,
                unchanged_latest_records: 0,
                removed_stale_latest_records: 0,
                scanned_reconstructions: 0,
                unchanged_reconstructions: 0,
                removed_stale_reconstructions: 0,
                rebuilt_dedupe_shard_mappings: 0,
                unchanged_dedupe_shard_mappings: 0,
                removed_stale_dedupe_shard_mappings: 0,
                preserved_latest_records_unreadable_version: vec![],
                issues: vec![],
            },
            lifecycle_repair: LifecycleRepairReport {
                scanned_records: 0,
                referenced_objects: 0,
                scanned_quarantine_candidates: 0,
                removed_missing_quarantine_candidates: 0,
                removed_reachable_quarantine_candidates: 0,
                removed_held_quarantine_candidates: 0,
                scanned_retention_holds: 0,
                removed_expired_retention_holds: 0,
                removed_missing_retention_holds: 0,
                scanned_webhook_deliveries: 0,
                removed_stale_webhook_deliveries: 0,
                removed_future_webhook_deliveries: 0,
            },
            fsck: LocalFsckReport {
                latest_records: 0,
                version_records: 0,
                inspected_chunk_references: 0,
                inspected_dedupe_shard_mappings: 0,
                inspected_reconstructions: 0,
                inspected_webhook_deliveries: 0,
                inspected_provider_repository_states: 0,
                issues: vec![],
            },
        }
    }

    fn report_with_issues() -> RepairReport {
        RepairReport {
            index_rebuild: LocalIndexRebuildReport {
                scanned_version_records: 10,
                scanned_retained_shards: 5,
                rebuilt_latest_records: 3,
                unchanged_latest_records: 6,
                removed_stale_latest_records: 1,
                scanned_reconstructions: 8,
                unchanged_reconstructions: 6,
                removed_stale_reconstructions: 2,
                rebuilt_dedupe_shard_mappings: 4,
                unchanged_dedupe_shard_mappings: 10,
                removed_stale_dedupe_shard_mappings: 1,
                preserved_latest_records_unreadable_version: vec![],
                issues: vec![LocalIndexRebuildIssue {
                    kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                    location: "records/abc".to_owned(),
                    detail: IndexRebuildIssueDetail::RecordJsonInvalid,
                }],
            },
            lifecycle_repair: LifecycleRepairReport {
                scanned_records: 100,
                referenced_objects: 50,
                scanned_quarantine_candidates: 20,
                removed_missing_quarantine_candidates: 2,
                removed_reachable_quarantine_candidates: 5,
                removed_held_quarantine_candidates: 1,
                scanned_retention_holds: 10,
                removed_expired_retention_holds: 3,
                removed_missing_retention_holds: 1,
                scanned_webhook_deliveries: 200,
                removed_stale_webhook_deliveries: 50,
                removed_future_webhook_deliveries: 5,
            },
            fsck: LocalFsckReport {
                latest_records: 100,
                version_records: 200,
                inspected_chunk_references: 1500,
                inspected_dedupe_shard_mappings: 50,
                inspected_reconstructions: 25,
                inspected_webhook_deliveries: 10,
                inspected_provider_repository_states: 5,
                issues: vec![LocalFsckIssue {
                    kind: FsckIssueKind::MissingChunk,
                    location: "chunks/xyz".to_owned(),
                    detail: FsckIssueDetail::RecordJsonInvalid,
                }],
            },
        }
    }

    #[test]
    fn repair_report_constructs_defaults() {
        let report = empty_repair_report();
        assert_eq!(report.index_rebuild.scanned_version_records, 0);
        assert_eq!(report.lifecycle_repair.scanned_records, 0);
        assert_eq!(report.fsck.latest_records, 0);
    }

    #[test]
    fn repair_report_with_data_reports_correct_counts() {
        let report = report_with_issues();
        assert_eq!(report.index_rebuild.scanned_version_records, 10);
        assert_eq!(report.index_rebuild.issue_count(), 1);
        assert_eq!(report.lifecycle_repair.scanned_records, 100);
        assert_eq!(report.fsck.issue_count(), 1);
        assert!(!report.fsck.is_clean());
        assert!(!report.index_rebuild.is_clean());
    }

    #[test]
    fn repair_report_print_summary_runs() {
        let report = empty_repair_report();
        report.print_summary();
    }

    #[test]
    fn repair_report_print_summary_with_data_runs() {
        let report = report_with_issues();
        report.print_summary();
    }

    #[test]
    fn repair_report_print_cli_summary_runs() {
        let report = empty_repair_report();
        report.print_cli_summary(Path::new("/root"), 3600);
    }

    #[test]
    fn repair_report_print_issues_with_data() {
        let report = report_with_issues();
        report.print_issues();
    }

    #[test]
    fn repair_report_print_issues_empty() {
        let report = empty_repair_report();
        report.print_issues();
    }

    #[tokio::test]
    async fn run_lifecycle_repair_rejects_missing_root() {
        let result =
            super::run_lifecycle_repair(Some(Path::new("/nonexistent-shardline-test-root")), 3600)
                .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_repair_rejects_missing_root() {
        let result =
            super::run_repair(Some(Path::new("/nonexistent-shardline-test-root")), 3600).await;
        assert!(result.is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn full_repair_is_idempotent_on_the_same_durable_root() {
        let root = tempfile::tempdir().expect("repair root");

        let first = super::run_repair(Some(root.path()), 3_600)
            .await
            .expect("first repair pass");
        let second = super::run_repair(Some(root.path()), 3_600)
            .await
            .expect("second repair pass");

        assert_eq!(second, first, "a quiescent repair rerun must converge");
        assert!(second.index_rebuild.is_clean());
        assert!(second.fsck.is_clean());
    }
}
