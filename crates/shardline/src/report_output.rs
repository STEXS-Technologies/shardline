use std::io::{self, Write};
use std::path::Path;

use shardline_server::{
    BackupManifestReport, ConfigCheckReport, DatabaseMigrationReport, LifecycleRepairReport,
    LocalFsckReport, LocalGcReport, LocalIndexRebuildReport, StorageMigrationReport,
};

pub fn print_config_check_summary(report: &ConfigCheckReport) {
    println!("status: {}", report.status);
    println!("server_role: {}", report.server_role);
    println!("server_frontends: {}", report.server_frontends.join(","));
    println!("metadata_backend: {}", report.metadata_backend);
    println!("object_backend: {}", report.object_backend);
    println!("cache_backend: {}", report.cache_backend);
    println!("auth_enabled: {}", report.auth_enabled);
    println!(
        "provider_tokens_enabled: {}",
        report.provider_tokens_enabled
    );
}

pub fn print_database_migration_summary(report: &DatabaseMigrationReport) {
    println!("backend: {}", report.backend);
    println!("applied_count: {}", report.applied_count);
    println!("reverted_count: {}", report.reverted_count);
    println!("applied_total_count: {}", report.applied_total_count);
    println!("pending_count: {}", report.pending_count);
    for migration in &report.migrations {
        println!(
            "migration: version={} name={} applied={} applied_at_utc={}",
            migration.version,
            migration.name,
            migration.applied,
            migration.applied_at_utc.as_deref().unwrap_or("-")
        );
    }
}

// Compatibility printers retain their unit-returning APIs. The CLI uses the
// fallible writer functions below to propagate write and flush failures.
pub fn print_fsck_summary(report: &LocalFsckReport) {
    let _output_result = write_fsck_summary(&mut io::stdout().lock(), report);
}

pub fn print_fsck_cli_summary(report: &LocalFsckReport, root: &Path) {
    let _output_result = write_fsck_cli_summary(&mut io::stdout().lock(), report, root);
}

pub fn print_fsck_issues(report: &LocalFsckReport) {
    let _output_result = write_fsck_issues(&mut io::stderr().lock(), report);
}

pub fn print_index_rebuild_summary(report: &LocalIndexRebuildReport) {
    let _output_result = write_index_rebuild_summary(&mut io::stdout().lock(), report);
}

pub fn print_index_rebuild_cli_summary(report: &LocalIndexRebuildReport, root: &Path) {
    let _output_result = write_index_rebuild_cli_summary(&mut io::stdout().lock(), report, root);
}

pub fn print_index_rebuild_issues(report: &LocalIndexRebuildReport) {
    let _output_result = write_index_rebuild_issues(&mut io::stderr().lock(), report);
}

pub fn print_lifecycle_repair_summary(report: &LifecycleRepairReport) {
    print_lifecycle_repair_summary_prefixed(report, "");
}

pub fn print_lifecycle_repair_summary_prefixed(report: &LifecycleRepairReport, prefix: &str) {
    let sep = if prefix.is_empty() { "" } else { "." };
    println!("{prefix}{sep}scanned_records: {}", report.scanned_records);
    println!(
        "{prefix}{sep}referenced_objects: {}",
        report.referenced_objects
    );
    println!(
        "{prefix}{sep}scanned_quarantine_candidates: {}",
        report.scanned_quarantine_candidates
    );
    println!(
        "{prefix}{sep}removed_missing_quarantine_candidates: {}",
        report.removed_missing_quarantine_candidates
    );
    println!(
        "{prefix}{sep}removed_reachable_quarantine_candidates: {}",
        report.removed_reachable_quarantine_candidates
    );
    println!(
        "{prefix}{sep}removed_held_quarantine_candidates: {}",
        report.removed_held_quarantine_candidates
    );
    println!(
        "{prefix}{sep}scanned_retention_holds: {}",
        report.scanned_retention_holds
    );
    println!(
        "{prefix}{sep}removed_expired_retention_holds: {}",
        report.removed_expired_retention_holds
    );
    println!(
        "{prefix}{sep}removed_missing_retention_holds: {}",
        report.removed_missing_retention_holds
    );
    println!(
        "{prefix}{sep}scanned_webhook_deliveries: {}",
        report.scanned_webhook_deliveries
    );
    println!(
        "{prefix}{sep}removed_stale_webhook_deliveries: {}",
        report.removed_stale_webhook_deliveries
    );
    println!(
        "{prefix}{sep}removed_future_webhook_deliveries: {}",
        report.removed_future_webhook_deliveries
    );
}

pub fn print_lifecycle_repair_cli_summary(
    report: &LifecycleRepairReport,
    root: &Path,
    webhook_retention_seconds: u64,
) {
    println!("root: {}", root.display());
    println!("webhook_retention_seconds: {webhook_retention_seconds}");
    print_lifecycle_repair_summary(report);
}

pub fn print_backup_manifest_summary(report: &BackupManifestReport) {
    let _output_result = write_backup_manifest_summary(&mut io::stdout().lock(), report);
}

pub fn print_backup_manifest_cli_summary(
    report: &BackupManifestReport,
    root: &Path,
    output: &Path,
) {
    let _output_result =
        write_backup_manifest_cli_summary(&mut io::stdout().lock(), report, root, output);
}

pub fn print_storage_migration_summary(report: &StorageMigrationReport) {
    println!("source_backend: {}", report.source_backend);
    println!("destination_backend: {}", report.destination_backend);
    println!("prefix: {}", report.prefix);
    println!("dry_run: {}", report.dry_run);
    println!("scanned_objects: {}", report.scanned_objects);
    println!("scanned_bytes: {}", report.scanned_bytes);
    println!("inserted_objects: {}", report.inserted_objects);
    println!(
        "already_present_objects: {}",
        report.already_present_objects
    );
    println!("copied_bytes: {}", report.copied_bytes);
}

pub fn print_local_gc_summary(report: &LocalGcReport) {
    println!(
        "retention_deferred_clock: {}",
        report.retention_deferred_clock
    );
    println!("scanned_records: {}", report.scanned_records);
    println!("referenced_chunks: {}", report.referenced_chunks);
    println!("orphan_chunks: {}", report.orphan_chunks);
    println!("orphan_chunk_bytes: {}", report.orphan_chunk_bytes);
    println!(
        "active_quarantine_candidates: {}",
        report.active_quarantine_candidates
    );
    println!(
        "new_quarantine_candidates: {}",
        report.new_quarantine_candidates
    );
    println!(
        "retained_quarantine_candidates: {}",
        report.retained_quarantine_candidates
    );
    println!(
        "released_quarantine_candidates: {}",
        report.released_quarantine_candidates
    );
    println!("deleted_chunks: {}", report.deleted_chunks);
    println!("deleted_bytes: {}", report.deleted_bytes);
    println!(
        "pruned_revisions_over_cap: {}",
        report.pruned_revisions_over_cap
    );
    println!("scanned_oci_tombstones: {}", report.scanned_oci_tombstones);
    println!(
        "eligible_oci_tombstones: {}",
        report.eligible_oci_tombstones
    );
    println!(
        "reclaimed_oci_tombstones: {}",
        report.reclaimed_oci_tombstones
    );
    println!(
        "scanned_resumable_staging_objects: {}",
        report.scanned_resumable_staging_objects
    );
    println!(
        "protected_resumable_staging_objects: {}",
        report.protected_resumable_staging_objects
    );
    println!(
        "reclaimed_resumable_staging_objects: {}",
        report.reclaimed_resumable_staging_objects
    );
    println!(
        "reclaimed_resumable_staging_bytes: {}",
        report.reclaimed_resumable_staging_bytes
    );
    println!(
        "reclaimed_resumable_sessions: {}",
        report.reclaimed_resumable_sessions
    );
}

pub fn print_local_gc_cli_summary(
    report: &LocalGcReport,
    mode: &str,
    root: &Path,
    retention_seconds: u64,
    mark: bool,
    retention_report: Option<&Path>,
    orphan_inventory: Option<&Path>,
) {
    println!("mode: {}", mode);
    println!("root: {}", root.display());
    if mark {
        println!("retention_seconds: {}", retention_seconds);
    }
    if let Some(path) = retention_report {
        println!("retention_report: {}", path.display());
    }
    if let Some(path) = orphan_inventory {
        println!("orphan_inventory: {}", path.display());
    }
    print_local_gc_summary(report);
}

pub(crate) fn write_fsck_summary(
    writer: &mut impl Write,
    report: &LocalFsckReport,
) -> io::Result<()> {
    writeln!(writer, "latest_records: {}", report.latest_records)?;
    writeln!(writer, "version_records: {}", report.version_records)?;
    writeln!(
        writer,
        "inspected_chunk_references: {}",
        report.inspected_chunk_references
    )?;
    writeln!(
        writer,
        "inspected_dedupe_shard_mappings: {}",
        report.inspected_dedupe_shard_mappings
    )?;
    writeln!(
        writer,
        "inspected_reconstructions: {}",
        report.inspected_reconstructions
    )?;
    writeln!(
        writer,
        "inspected_webhook_deliveries: {}",
        report.inspected_webhook_deliveries
    )?;
    writeln!(
        writer,
        "inspected_provider_repository_states: {}",
        report.inspected_provider_repository_states
    )?;
    writeln!(writer, "issue_count: {}", report.issue_count())?;

    writer.flush()
}

pub(crate) fn write_fsck_cli_summary(
    writer: &mut impl Write,
    report: &LocalFsckReport,
    root: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    write_fsck_summary(writer, report)
}

pub(crate) fn write_fsck_issues(
    writer: &mut impl Write,
    report: &LocalFsckReport,
) -> io::Result<()> {
    for issue in &report.issues {
        writeln!(
            writer,
            "issue: {} location={} detail={}",
            issue.kind.as_str(),
            issue.location,
            issue.detail
        )?;
    }

    writer.flush()
}

pub(crate) fn write_index_rebuild_summary(
    writer: &mut impl Write,
    report: &LocalIndexRebuildReport,
) -> io::Result<()> {
    writeln!(
        writer,
        "scanned_version_records: {}",
        report.scanned_version_records
    )?;
    writeln!(
        writer,
        "scanned_retained_shards: {}",
        report.scanned_retained_shards
    )?;
    writeln!(
        writer,
        "rebuilt_latest_records: {}",
        report.rebuilt_latest_records
    )?;
    writeln!(
        writer,
        "unchanged_latest_records: {}",
        report.unchanged_latest_records
    )?;
    writeln!(
        writer,
        "removed_stale_latest_records: {}",
        report.removed_stale_latest_records
    )?;
    for location in &report.preserved_latest_records_unreadable_version {
        writeln!(
            writer,
            "kept_latest_record_unreadable_version: {}",
            location
        )?;
    }
    writeln!(
        writer,
        "scanned_reconstructions: {}",
        report.scanned_reconstructions
    )?;
    writeln!(
        writer,
        "unchanged_reconstructions: {}",
        report.unchanged_reconstructions
    )?;
    writeln!(
        writer,
        "removed_stale_reconstructions: {}",
        report.removed_stale_reconstructions
    )?;
    writeln!(
        writer,
        "rebuilt_dedupe_shard_mappings: {}",
        report.rebuilt_dedupe_shard_mappings
    )?;
    writeln!(
        writer,
        "unchanged_dedupe_shard_mappings: {}",
        report.unchanged_dedupe_shard_mappings
    )?;
    writeln!(
        writer,
        "removed_stale_dedupe_shard_mappings: {}",
        report.removed_stale_dedupe_shard_mappings
    )?;
    writeln!(writer, "issue_count: {}", report.issue_count())?;

    writer.flush()
}

pub(crate) fn write_index_rebuild_cli_summary(
    writer: &mut impl Write,
    report: &LocalIndexRebuildReport,
    root: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    write_index_rebuild_summary(writer, report)
}

pub(crate) fn write_index_rebuild_issues(
    writer: &mut impl Write,
    report: &LocalIndexRebuildReport,
) -> io::Result<()> {
    for issue in &report.issues {
        writeln!(
            writer,
            "issue: {} location={} detail={}",
            issue.kind.as_str(),
            issue.location,
            issue.detail
        )?;
    }

    writer.flush()
}

pub(crate) fn write_backup_manifest_summary(
    writer: &mut impl Write,
    report: &BackupManifestReport,
) -> io::Result<()> {
    writeln!(writer, "manifest_version: {}", report.manifest_version)?;
    writeln!(writer, "metadata_backend: {}", report.metadata_backend)?;
    writeln!(writer, "object_backend: {}", report.object_backend)?;
    writeln!(writer, "object_count: {}", report.object_count)?;
    writeln!(writer, "object_bytes: {}", report.object_bytes)?;
    writeln!(writer, "latest_records: {}", report.latest_records)?;
    writeln!(writer, "version_records: {}", report.version_records)?;
    writeln!(
        writer,
        "reconstruction_rows: {}",
        report.reconstruction_rows
    )?;
    writeln!(
        writer,
        "dedupe_shard_mappings: {}",
        report.dedupe_shard_mappings
    )?;
    writeln!(
        writer,
        "quarantine_candidates: {}",
        report.quarantine_candidates
    )?;
    writeln!(writer, "retention_holds: {}", report.retention_holds)?;
    writeln!(writer, "webhook_deliveries: {}", report.webhook_deliveries)?;
    writeln!(
        writer,
        "provider_repository_states: {}",
        report.provider_repository_states
    )?;

    writer.flush()
}

pub(crate) fn write_backup_manifest_cli_summary(
    writer: &mut impl Write,
    report: &BackupManifestReport,
    root: &Path,
    output: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    writeln!(writer, "output: {}", output.display())?;
    write_backup_manifest_summary(writer, report)
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};
    use std::path::Path;

    use shardline_server::{
        BackupManifestReport, ConfigCheckReport, DatabaseMigrationCommand, DatabaseMigrationReport,
        DatabaseMigrationStatusEntry, FsckIssueDetail, FsckIssueKind, IndexRebuildIssueDetail,
        LifecycleRepairReport, LocalFsckIssue, LocalFsckReport, LocalGcReport,
        LocalIndexRebuildIssue, LocalIndexRebuildIssueKind, LocalIndexRebuildReport,
        StorageMigrationReport,
    };

    use super::*;

    // -----------------------------------------------------------------------
    // print_config_check_summary — smoke test (no panic)
    // -----------------------------------------------------------------------

    #[test]
    fn config_check_summary_runs() {
        let report = ConfigCheckReport {
            status: "ok".to_owned(),
            server_role: "all".to_owned(),
            server_frontends: vec!["xet".to_owned()],
            metadata_backend: "local".to_owned(),
            object_backend: "local".to_owned(),
            cache_backend: "memory".to_owned(),
            auth_enabled: true,
            provider_tokens_enabled: false,
        };
        print_config_check_summary(&report);
    }

    // -----------------------------------------------------------------------
    // print_database_migration_summary — smoke test (no panic)
    // -----------------------------------------------------------------------

    #[test]
    fn database_migration_summary_runs() {
        let report = DatabaseMigrationReport {
            backend: "postgres".to_owned(),
            command: DatabaseMigrationCommand::Status,
            applied_count: 3,
            reverted_count: 1,
            applied_total_count: 7,
            pending_count: 2,
            migrations: vec![
                DatabaseMigrationStatusEntry {
                    version: "v1".to_owned(),
                    name: "m1".to_owned(),
                    applied: true,
                    applied_at_utc: Some("2026-01-01T00:00:00Z".to_owned()),
                },
                DatabaseMigrationStatusEntry {
                    version: "v2".to_owned(),
                    name: "m2".to_owned(),
                    applied: false,
                    applied_at_utc: None,
                },
            ],
        };
        print_database_migration_summary(&report);
    }

    #[test]
    fn database_migration_no_migrations() {
        let report = DatabaseMigrationReport {
            backend: "postgres".to_owned(),
            command: DatabaseMigrationCommand::Status,
            applied_count: 0,
            reverted_count: 0,
            applied_total_count: 0,
            pending_count: 0,
            migrations: vec![],
        };
        print_database_migration_summary(&report);
    }

    // -----------------------------------------------------------------------
    // print_fsck_summary / cli / issues
    // -----------------------------------------------------------------------

    #[test]
    fn fsck_summary_runs() {
        let report = LocalFsckReport {
            latest_records: 100,
            version_records: 200,
            inspected_chunk_references: 1500,
            inspected_dedupe_shard_mappings: 50,
            inspected_reconstructions: 25,
            inspected_webhook_deliveries: 10,
            inspected_provider_repository_states: 5,
            issues: vec![],
        };
        print_fsck_summary(&report);
    }

    #[test]
    fn fsck_cli_summary_runs() {
        let report = LocalFsckReport {
            latest_records: 1,
            version_records: 2,
            inspected_chunk_references: 3,
            inspected_dedupe_shard_mappings: 4,
            inspected_reconstructions: 5,
            inspected_webhook_deliveries: 6,
            inspected_provider_repository_states: 7,
            issues: vec![],
        };
        print_fsck_cli_summary(&report, Path::new("/root"));
    }

    #[test]
    fn fsck_summary_reports_issue_count() {
        let report = LocalFsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: vec![
                LocalFsckIssue {
                    kind: FsckIssueKind::MissingChunk,
                    location: "chunks/a".to_owned(),
                    detail: FsckIssueDetail::MissingVersionRecord {
                        version_locator: "r/a".to_owned(),
                    },
                },
                LocalFsckIssue {
                    kind: FsckIssueKind::ChunkHashMismatch,
                    location: "chunks/b".to_owned(),
                    detail: FsckIssueDetail::RecordJsonInvalid,
                },
            ],
        };
        assert_eq!(report.issue_count(), 2);
        print_fsck_summary(&report);
        // issue_count() is internally derived from issues.len()
        assert!(!report.is_clean());
    }

    #[test]
    fn fsck_issues_runs() {
        let report = LocalFsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: vec![LocalFsckIssue {
                kind: FsckIssueKind::MissingChunk,
                location: "chunks/abc".to_owned(),
                detail: FsckIssueDetail::MissingVersionRecord {
                    version_locator: "records/abc".to_owned(),
                },
            }],
        };
        print_fsck_issues(&report);
    }

    #[test]
    fn fsck_empty_issues_is_clean() {
        let report = LocalFsckReport {
            issues: vec![],
            ..empty_fsck_report()
        };
        assert!(report.is_clean());
        assert_eq!(report.issue_count(), 0);
    }

    // -----------------------------------------------------------------------
    // print_index_rebuild_summary / cli / issues
    // -----------------------------------------------------------------------

    fn empty_index_rebuild_report() -> LocalIndexRebuildReport {
        LocalIndexRebuildReport {
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
        }
    }

    #[test]
    fn index_rebuild_summary_runs() {
        print_index_rebuild_summary(&empty_index_rebuild_report());
    }

    #[test]
    fn index_rebuild_cli_summary_runs() {
        print_index_rebuild_cli_summary(&empty_index_rebuild_report(), Path::new("/root"));
    }

    #[test]
    fn index_rebuild_issues_runs() {
        let report = LocalIndexRebuildReport {
            issues: vec![LocalIndexRebuildIssue {
                kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                location: "records/abc".to_owned(),
                detail: IndexRebuildIssueDetail::RecordJsonInvalid,
            }],
            ..empty_index_rebuild_report()
        };
        print_index_rebuild_issues(&report);
    }

    #[test]
    fn index_rebuild_issue_count_and_clean() {
        let report = LocalIndexRebuildReport {
            issues: vec![LocalIndexRebuildIssue {
                kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                location: "r".to_owned(),
                detail: IndexRebuildIssueDetail::RecordJsonInvalid,
            }],
            ..empty_index_rebuild_report()
        };
        assert_eq!(report.issue_count(), 1);
        assert!(!report.is_clean());

        let clean = empty_index_rebuild_report();
        assert_eq!(clean.issue_count(), 0);
        assert!(clean.is_clean());
    }

    // -----------------------------------------------------------------------
    // print_lifecycle_repair_summary / prefixed / cli
    // -----------------------------------------------------------------------

    fn empty_lifecycle_repair_report() -> LifecycleRepairReport {
        LifecycleRepairReport {
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
        }
    }

    #[test]
    fn lifecycle_repair_summary_runs() {
        print_lifecycle_repair_summary(&empty_lifecycle_repair_report());
    }

    #[test]
    fn lifecycle_repair_summary_prefixed_runs() {
        let report = empty_lifecycle_repair_report();
        print_lifecycle_repair_summary_prefixed(&report, "repair");
        // empty prefix — no dot separator
        print_lifecycle_repair_summary_prefixed(&report, "");
    }

    #[test]
    fn lifecycle_repair_cli_summary_runs() {
        print_lifecycle_repair_cli_summary(
            &empty_lifecycle_repair_report(),
            Path::new("/root"),
            2592000,
        );
    }

    // -----------------------------------------------------------------------
    // print_backup_manifest_summary / cli
    // -----------------------------------------------------------------------

    fn empty_backup_report() -> BackupManifestReport {
        BackupManifestReport {
            manifest_version: 1,
            metadata_backend: "local".to_owned(),
            object_backend: "fs".to_owned(),
            object_count: 0,
            object_bytes: 0,
            latest_records: 0,
            version_records: 0,
            reconstruction_rows: 0,
            dedupe_shard_mappings: 0,
            quarantine_candidates: 0,
            retention_holds: 0,
            webhook_deliveries: 0,
            provider_repository_states: 0,
        }
    }

    #[test]
    fn backup_manifest_summary_runs() {
        print_backup_manifest_summary(&empty_backup_report());
    }

    #[test]
    fn backup_manifest_cli_summary_runs() {
        print_backup_manifest_cli_summary(
            &empty_backup_report(),
            Path::new("/root"),
            Path::new("/out.json"),
        );
    }

    // -----------------------------------------------------------------------
    // print_storage_migration_summary
    // -----------------------------------------------------------------------

    #[test]
    fn storage_migration_summary_runs() {
        let report = StorageMigrationReport {
            source_backend: "local".to_owned(),
            destination_backend: "s3".to_owned(),
            prefix: "chunks/".to_owned(),
            dry_run: true,
            scanned_objects: 1000,
            scanned_bytes: 500_000_000,
            inserted_objects: 800,
            already_present_objects: 200,
            copied_bytes: 400_000_000,
        };
        print_storage_migration_summary(&report);
    }

    // -----------------------------------------------------------------------
    // print_local_gc_summary / cli
    // -----------------------------------------------------------------------

    fn empty_gc_report() -> LocalGcReport {
        LocalGcReport {
            scanned_records: 0,
            referenced_chunks: 0,
            orphan_chunks: 0,
            orphan_chunk_bytes: 0,
            active_quarantine_candidates: 0,
            new_quarantine_candidates: 0,
            retained_quarantine_candidates: 0,
            released_quarantine_candidates: 0,
            deleted_chunks: 0,
            deleted_bytes: 0,
            reaped_stale_temporary_chunks: 0,
            reaped_stale_temporary_bytes: 0,
            pruned_revisions_over_cap: 0,
            ..LocalGcReport::default()
        }
    }

    #[test]
    fn local_gc_summary_runs() {
        print_local_gc_summary(&empty_gc_report());
    }

    #[test]
    fn local_gc_cli_summary_runs() {
        // mark=true — retention_seconds printed
        print_local_gc_cli_summary(
            &empty_gc_report(),
            "mark",
            Path::new("/root"),
            7200,
            true,
            Some(Path::new("/ret.json")),
            None,
        );
        // mark=false — retention_seconds not printed, orphan_inventory printed
        print_local_gc_cli_summary(
            &empty_gc_report(),
            "sweep",
            Path::new("/root"),
            3600,
            false,
            None,
            Some(Path::new("/orphans.json")),
        );
        // Both optional paths present
        print_local_gc_cli_summary(
            &empty_gc_report(),
            "mark-and-sweep",
            Path::new("/root"),
            3600,
            true,
            Some(Path::new("/ret.json")),
            Some(Path::new("/orphans.json")),
        );
    }

    // -----------------------------------------------------------------------
    // Helper: empty default report for fsck
    // -----------------------------------------------------------------------

    fn empty_fsck_report() -> LocalFsckReport {
        LocalFsckReport {
            latest_records: 0,
            version_records: 0,
            inspected_chunk_references: 0,
            inspected_dedupe_shard_mappings: 0,
            inspected_reconstructions: 0,
            inspected_webhook_deliveries: 0,
            inspected_provider_repository_states: 0,
            issues: vec![],
        }
    }
    struct FailingReportWriter {
        fail_write: bool,
        written: Vec<u8>,
    }

    impl Write for FailingReportWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.fail_write {
                return Err(io::Error::from_raw_os_error(28));
            }
            self.written.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::from_raw_os_error(28))
        }
    }

    #[test]
    fn fallible_cli_summaries_propagate_write_and_flush_errors() {
        for fail_write in [true, false] {
            let mut writer = FailingReportWriter {
                fail_write,
                written: Vec::new(),
            };
            assert_eq!(
                write_fsck_cli_summary(&mut writer, &empty_fsck_report(), Path::new("/root"))
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_index_rebuild_cli_summary(
                    &mut writer,
                    &empty_index_rebuild_report(),
                    Path::new("/root")
                )
                .unwrap_err()
                .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_backup_manifest_cli_summary(
                    &mut writer,
                    &empty_backup_report(),
                    Path::new("/root"),
                    Path::new("/manifest.json")
                )
                .unwrap_err()
                .raw_os_error(),
                Some(28)
            );
            assert_eq!(writer.written.is_empty(), fail_write);
        }
    }

    #[test]
    fn fallible_issue_outputs_propagate_write_and_flush_errors() {
        let fsck = LocalFsckReport {
            issues: vec![LocalFsckIssue {
                kind: FsckIssueKind::MissingChunk,
                location: "chunks/abc".to_owned(),
                detail: FsckIssueDetail::RecordJsonInvalid,
            }],
            ..empty_fsck_report()
        };
        let rebuild = LocalIndexRebuildReport {
            issues: vec![LocalIndexRebuildIssue {
                kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                location: "records/abc".to_owned(),
                detail: IndexRebuildIssueDetail::RecordJsonInvalid,
            }],
            ..empty_index_rebuild_report()
        };
        for fail_write in [true, false] {
            let mut writer = FailingReportWriter {
                fail_write,
                written: Vec::new(),
            };
            assert_eq!(
                write_fsck_issues(&mut writer, &fsck)
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_index_rebuild_issues(&mut writer, &rebuild)
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(writer.written.is_empty(), fail_write);
        }
    }

    #[test]
    fn fallible_cli_summaries_preserve_report_format() {
        let mut writer = Vec::new();
        write_backup_manifest_cli_summary(
            &mut writer,
            &empty_backup_report(),
            Path::new("/root"),
            Path::new("/manifest.json"),
        )
        .unwrap();
        let text = String::from_utf8(writer).unwrap();
        assert!(text.starts_with("root: /root\noutput: /manifest.json\nmanifest_version: 1\n"));
        assert!(text.contains("object_count: 0\n"));
        assert!(text.ends_with("provider_repository_states: 0\n"));
    }
}
