use std::io::{self, Write};
use std::path::Path;

use shardline_server::{
    BackupManifestReport, ConfigCheckReport, DatabaseMigrationReport, LifecycleRepairReport,
    LocalFsckReport, LocalGcReport, LocalIndexRebuildReport, StorageMigrationReport,
};

pub fn print_config_check_summary(report: &ConfigCheckReport) {
    let _output_result = write_config_check_summary(&mut io::stdout().lock(), report);
}

pub fn print_database_migration_summary(report: &DatabaseMigrationReport) {
    let _output_result = write_database_migration_summary(&mut io::stdout().lock(), report);
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
    let _output_result = write_lifecycle_repair_summary(&mut io::stdout().lock(), report);
}

pub fn print_lifecycle_repair_summary_prefixed(report: &LifecycleRepairReport, prefix: &str) {
    let _output_result =
        write_lifecycle_repair_summary_prefixed(&mut io::stdout().lock(), report, prefix);
}

pub fn print_lifecycle_repair_cli_summary(
    report: &LifecycleRepairReport,
    root: &Path,
    webhook_retention_seconds: u64,
) {
    let _output_result = write_lifecycle_repair_cli_summary(
        &mut io::stdout().lock(),
        report,
        root,
        webhook_retention_seconds,
    );
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
    let _output_result = write_storage_migration_summary(&mut io::stdout().lock(), report);
}

pub fn print_local_gc_summary(report: &LocalGcReport) {
    let _output_result = write_local_gc_summary(&mut io::stdout().lock(), report);
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
    let _output_result = write_local_gc_cli_summary(
        &mut io::stdout().lock(),
        report,
        mode,
        root,
        retention_seconds,
        mark,
        retention_report,
        orphan_inventory,
    );
}

pub(crate) fn write_fsck_summary(
    writer: &mut (impl Write + ?Sized),
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
    writer: &mut (impl Write + ?Sized),
    report: &LocalFsckReport,
    root: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    write_fsck_summary(writer, report)
}

pub(crate) fn write_fsck_issues(
    writer: &mut (impl Write + ?Sized),
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
    writer: &mut (impl Write + ?Sized),
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
    writer: &mut (impl Write + ?Sized),
    report: &LocalIndexRebuildReport,
    root: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    write_index_rebuild_summary(writer, report)
}

pub(crate) fn write_index_rebuild_issues(
    writer: &mut (impl Write + ?Sized),
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
    writer: &mut (impl Write + ?Sized),
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
    writer: &mut (impl Write + ?Sized),
    report: &BackupManifestReport,
    root: &Path,
    output: &Path,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    writeln!(writer, "output: {}", output.display())?;
    write_backup_manifest_summary(writer, report)
}

pub(crate) fn write_config_check_summary(
    writer: &mut (impl Write + ?Sized),
    report: &ConfigCheckReport,
) -> io::Result<()> {
    writeln!(writer, "status: {}", report.status)?;
    writeln!(writer, "server_role: {}", report.server_role)?;
    writeln!(
        writer,
        "server_frontends: {}",
        report.server_frontends.join(",")
    )?;
    writeln!(writer, "metadata_backend: {}", report.metadata_backend)?;
    writeln!(writer, "object_backend: {}", report.object_backend)?;
    writeln!(writer, "cache_backend: {}", report.cache_backend)?;
    writeln!(writer, "auth_enabled: {}", report.auth_enabled)?;
    writeln!(
        writer,
        "provider_tokens_enabled: {}",
        report.provider_tokens_enabled
    )?;

    writer.flush()
}

pub(crate) fn write_database_migration_summary(
    writer: &mut (impl Write + ?Sized),
    report: &DatabaseMigrationReport,
) -> io::Result<()> {
    writeln!(writer, "backend: {}", report.backend)?;
    writeln!(writer, "applied_count: {}", report.applied_count)?;
    writeln!(writer, "reverted_count: {}", report.reverted_count)?;
    writeln!(
        writer,
        "applied_total_count: {}",
        report.applied_total_count
    )?;
    writeln!(writer, "pending_count: {}", report.pending_count)?;
    for migration in &report.migrations {
        writeln!(
            writer,
            "migration: version={} name={} applied={} applied_at_utc={}",
            migration.version,
            migration.name,
            migration.applied,
            migration.applied_at_utc.as_deref().unwrap_or("-")
        )?;
    }

    writer.flush()
}

pub(crate) fn write_lifecycle_repair_summary(
    writer: &mut (impl Write + ?Sized),
    report: &LifecycleRepairReport,
) -> io::Result<()> {
    write_lifecycle_repair_summary_prefixed(writer, report, "")
}

pub(crate) fn write_lifecycle_repair_summary_prefixed(
    writer: &mut (impl Write + ?Sized),
    report: &LifecycleRepairReport,
    prefix: &str,
) -> io::Result<()> {
    let sep = if prefix.is_empty() { "" } else { "." };
    writeln!(
        writer,
        "{prefix}{sep}scanned_records: {}",
        report.scanned_records
    )?;
    writeln!(
        writer,
        "{prefix}{sep}referenced_objects: {}",
        report.referenced_objects
    )?;
    writeln!(
        writer,
        "{prefix}{sep}scanned_quarantine_candidates: {}",
        report.scanned_quarantine_candidates
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_missing_quarantine_candidates: {}",
        report.removed_missing_quarantine_candidates
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_reachable_quarantine_candidates: {}",
        report.removed_reachable_quarantine_candidates
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_held_quarantine_candidates: {}",
        report.removed_held_quarantine_candidates
    )?;
    writeln!(
        writer,
        "{prefix}{sep}scanned_retention_holds: {}",
        report.scanned_retention_holds
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_expired_retention_holds: {}",
        report.removed_expired_retention_holds
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_missing_retention_holds: {}",
        report.removed_missing_retention_holds
    )?;
    writeln!(
        writer,
        "{prefix}{sep}scanned_webhook_deliveries: {}",
        report.scanned_webhook_deliveries
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_stale_webhook_deliveries: {}",
        report.removed_stale_webhook_deliveries
    )?;
    writeln!(
        writer,
        "{prefix}{sep}removed_future_webhook_deliveries: {}",
        report.removed_future_webhook_deliveries
    )?;

    writer.flush()
}

pub(crate) fn write_lifecycle_repair_cli_summary(
    writer: &mut (impl Write + ?Sized),
    report: &LifecycleRepairReport,
    root: &Path,
    webhook_retention_seconds: u64,
) -> io::Result<()> {
    writeln!(writer, "root: {}", root.display())?;
    writeln!(
        writer,
        "webhook_retention_seconds: {webhook_retention_seconds}"
    )?;
    write_lifecycle_repair_summary(writer, report)
}

pub(crate) fn write_storage_migration_summary(
    writer: &mut (impl Write + ?Sized),
    report: &StorageMigrationReport,
) -> io::Result<()> {
    writeln!(writer, "source_backend: {}", report.source_backend)?;
    writeln!(
        writer,
        "destination_backend: {}",
        report.destination_backend
    )?;
    writeln!(writer, "prefix: {}", report.prefix)?;
    writeln!(writer, "dry_run: {}", report.dry_run)?;
    writeln!(writer, "scanned_objects: {}", report.scanned_objects)?;
    writeln!(writer, "scanned_bytes: {}", report.scanned_bytes)?;
    writeln!(writer, "inserted_objects: {}", report.inserted_objects)?;
    writeln!(
        writer,
        "already_present_objects: {}",
        report.already_present_objects
    )?;
    writeln!(writer, "copied_bytes: {}", report.copied_bytes)?;

    writer.flush()
}

pub(crate) fn write_local_gc_summary(
    writer: &mut (impl Write + ?Sized),
    report: &LocalGcReport,
) -> io::Result<()> {
    writeln!(
        writer,
        "retention_deferred_clock: {}",
        report.retention_deferred_clock
    )?;
    writeln!(writer, "scanned_records: {}", report.scanned_records)?;
    writeln!(writer, "referenced_chunks: {}", report.referenced_chunks)?;
    writeln!(writer, "orphan_chunks: {}", report.orphan_chunks)?;
    writeln!(writer, "orphan_chunk_bytes: {}", report.orphan_chunk_bytes)?;
    writeln!(
        writer,
        "active_quarantine_candidates: {}",
        report.active_quarantine_candidates
    )?;
    writeln!(
        writer,
        "new_quarantine_candidates: {}",
        report.new_quarantine_candidates
    )?;
    writeln!(
        writer,
        "retained_quarantine_candidates: {}",
        report.retained_quarantine_candidates
    )?;
    writeln!(
        writer,
        "released_quarantine_candidates: {}",
        report.released_quarantine_candidates
    )?;
    writeln!(writer, "deleted_chunks: {}", report.deleted_chunks)?;
    writeln!(writer, "deleted_bytes: {}", report.deleted_bytes)?;
    writeln!(
        writer,
        "pruned_revisions_over_cap: {}",
        report.pruned_revisions_over_cap
    )?;
    writeln!(
        writer,
        "scanned_oci_tombstones: {}",
        report.scanned_oci_tombstones
    )?;
    writeln!(
        writer,
        "eligible_oci_tombstones: {}",
        report.eligible_oci_tombstones
    )?;
    writeln!(
        writer,
        "reclaimed_oci_tombstones: {}",
        report.reclaimed_oci_tombstones
    )?;
    writeln!(
        writer,
        "scanned_resumable_staging_objects: {}",
        report.scanned_resumable_staging_objects
    )?;
    writeln!(
        writer,
        "protected_resumable_staging_objects: {}",
        report.protected_resumable_staging_objects
    )?;
    writeln!(
        writer,
        "reclaimed_resumable_staging_objects: {}",
        report.reclaimed_resumable_staging_objects
    )?;
    writeln!(
        writer,
        "reclaimed_resumable_staging_bytes: {}",
        report.reclaimed_resumable_staging_bytes
    )?;
    writeln!(
        writer,
        "reclaimed_resumable_sessions: {}",
        report.reclaimed_resumable_sessions
    )?;

    writer.flush()
}

// Preserve the legacy summary argument set while adding the fallible writer.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_local_gc_cli_summary(
    writer: &mut (impl Write + ?Sized),
    report: &LocalGcReport,
    mode: &str,
    root: &Path,
    retention_seconds: u64,
    mark: bool,
    retention_report: Option<&Path>,
    orphan_inventory: Option<&Path>,
) -> io::Result<()> {
    writeln!(writer, "mode: {}", mode)?;
    writeln!(writer, "root: {}", root.display())?;
    if mark {
        writeln!(writer, "retention_seconds: {}", retention_seconds)?;
    }
    if let Some(path) = retention_report {
        writeln!(writer, "retention_report: {}", path.display())?;
    }
    if let Some(path) = orphan_inventory {
        writeln!(writer, "orphan_inventory: {}", path.display())?;
    }
    write_local_gc_summary(writer, report)
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
    #[test]
    fn remaining_summary_writers_propagate_write_and_flush_errors() {
        let config = ConfigCheckReport {
            status: "ok".to_owned(),
            server_role: "all".to_owned(),
            server_frontends: vec!["xet".to_owned()],
            metadata_backend: "local".to_owned(),
            object_backend: "local".to_owned(),
            cache_backend: "memory".to_owned(),
            auth_enabled: true,
            provider_tokens_enabled: false,
        };
        let database = DatabaseMigrationReport {
            backend: "postgres".to_owned(),
            command: DatabaseMigrationCommand::Status,
            applied_count: 0,
            reverted_count: 0,
            applied_total_count: 1,
            pending_count: 0,
            migrations: vec![DatabaseMigrationStatusEntry {
                version: "v1".to_owned(),
                name: "migration".to_owned(),
                applied: true,
                applied_at_utc: None,
            }],
        };
        let storage = StorageMigrationReport {
            source_backend: "local".to_owned(),
            destination_backend: "local".to_owned(),
            prefix: String::new(),
            dry_run: true,
            scanned_objects: 0,
            scanned_bytes: 0,
            inserted_objects: 0,
            already_present_objects: 0,
            copied_bytes: 0,
        };
        for fail_write in [true, false] {
            let mut writer = FailingReportWriter {
                fail_write,
                written: Vec::new(),
            };
            assert_eq!(
                write_config_check_summary(&mut writer, &config)
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_database_migration_summary(&mut writer, &database)
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_storage_migration_summary(&mut writer, &storage)
                    .unwrap_err()
                    .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_lifecycle_repair_cli_summary(
                    &mut writer,
                    &empty_lifecycle_repair_report(),
                    Path::new("/root"),
                    3600
                )
                .unwrap_err()
                .raw_os_error(),
                Some(28)
            );
            assert_eq!(
                write_local_gc_cli_summary(
                    &mut writer,
                    &empty_gc_report(),
                    "dry-run",
                    Path::new("/root"),
                    3600,
                    false,
                    None,
                    None
                )
                .unwrap_err()
                .raw_os_error(),
                Some(28)
            );
            assert_eq!(writer.written.is_empty(), fail_write);
        }
    }
}
