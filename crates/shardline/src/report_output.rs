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
    for (name, value) in [
        ("latest_records", report.latest_records),
        ("version_records", report.version_records),
        (
            "inspected_chunk_references",
            report.inspected_chunk_references,
        ),
        (
            "inspected_dedupe_shard_mappings",
            report.inspected_dedupe_shard_mappings,
        ),
        (
            "inspected_reconstructions",
            report.inspected_reconstructions,
        ),
        (
            "inspected_webhook_deliveries",
            report.inspected_webhook_deliveries,
        ),
        (
            "inspected_provider_repository_states",
            report.inspected_provider_repository_states,
        ),
    ] {
        writeln!(writer, "{name}: {value}")?;
    }

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
    for (name, value) in [
        ("scanned_version_records", report.scanned_version_records),
        ("scanned_retained_shards", report.scanned_retained_shards),
        ("rebuilt_latest_records", report.rebuilt_latest_records),
        ("unchanged_latest_records", report.unchanged_latest_records),
        (
            "removed_stale_latest_records",
            report.removed_stale_latest_records,
        ),
    ] {
        writeln!(writer, "{name}: {value}")?;
    }
    for location in &report.preserved_latest_records_unreadable_version {
        writeln!(
            writer,
            "kept_latest_record_unreadable_version: {}",
            location
        )?;
    }
    for (name, value) in [
        ("scanned_reconstructions", report.scanned_reconstructions),
        (
            "unchanged_reconstructions",
            report.unchanged_reconstructions,
        ),
        (
            "removed_stale_reconstructions",
            report.removed_stale_reconstructions,
        ),
        (
            "rebuilt_dedupe_shard_mappings",
            report.rebuilt_dedupe_shard_mappings,
        ),
        (
            "unchanged_dedupe_shard_mappings",
            report.unchanged_dedupe_shard_mappings,
        ),
        (
            "removed_stale_dedupe_shard_mappings",
            report.removed_stale_dedupe_shard_mappings,
        ),
    ] {
        writeln!(writer, "{name}: {value}")?;
    }
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
    for (name, value) in [
        ("object_count", report.object_count),
        ("object_bytes", report.object_bytes),
        ("latest_records", report.latest_records),
        ("version_records", report.version_records),
        ("reconstruction_rows", report.reconstruction_rows),
        ("dedupe_shard_mappings", report.dedupe_shard_mappings),
        ("quarantine_candidates", report.quarantine_candidates),
        ("retention_holds", report.retention_holds),
        ("webhook_deliveries", report.webhook_deliveries),
        (
            "provider_repository_states",
            report.provider_repository_states,
        ),
    ] {
        writeln!(writer, "{name}: {value}")?;
    }

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
    for (name, value) in [
        ("scanned_records", report.scanned_records),
        ("referenced_objects", report.referenced_objects),
        (
            "scanned_quarantine_candidates",
            report.scanned_quarantine_candidates,
        ),
        (
            "removed_missing_quarantine_candidates",
            report.removed_missing_quarantine_candidates,
        ),
        (
            "removed_reachable_quarantine_candidates",
            report.removed_reachable_quarantine_candidates,
        ),
        (
            "removed_held_quarantine_candidates",
            report.removed_held_quarantine_candidates,
        ),
        ("scanned_retention_holds", report.scanned_retention_holds),
        (
            "removed_expired_retention_holds",
            report.removed_expired_retention_holds,
        ),
        (
            "removed_missing_retention_holds",
            report.removed_missing_retention_holds,
        ),
        (
            "scanned_webhook_deliveries",
            report.scanned_webhook_deliveries,
        ),
        (
            "removed_stale_webhook_deliveries",
            report.removed_stale_webhook_deliveries,
        ),
        (
            "removed_future_webhook_deliveries",
            report.removed_future_webhook_deliveries,
        ),
    ] {
        writeln!(writer, "{prefix}{sep}{name}: {value}")?;
    }

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
    for (name, value) in [
        ("scanned_records", report.scanned_records),
        ("referenced_chunks", report.referenced_chunks),
        ("orphan_chunks", report.orphan_chunks),
        ("orphan_chunk_bytes", report.orphan_chunk_bytes),
        (
            "active_quarantine_candidates",
            report.active_quarantine_candidates,
        ),
        (
            "new_quarantine_candidates",
            report.new_quarantine_candidates,
        ),
        (
            "retained_quarantine_candidates",
            report.retained_quarantine_candidates,
        ),
        (
            "released_quarantine_candidates",
            report.released_quarantine_candidates,
        ),
        ("deleted_chunks", report.deleted_chunks),
        ("deleted_bytes", report.deleted_bytes),
        (
            "pruned_revisions_over_cap",
            report.pruned_revisions_over_cap,
        ),
        ("scanned_oci_tombstones", report.scanned_oci_tombstones),
        ("eligible_oci_tombstones", report.eligible_oci_tombstones),
        ("reclaimed_oci_tombstones", report.reclaimed_oci_tombstones),
        (
            "scanned_resumable_staging_objects",
            report.scanned_resumable_staging_objects,
        ),
        (
            "protected_resumable_staging_objects",
            report.protected_resumable_staging_objects,
        ),
        (
            "reclaimed_resumable_staging_objects",
            report.reclaimed_resumable_staging_objects,
        ),
        (
            "reclaimed_resumable_staging_bytes",
            report.reclaimed_resumable_staging_bytes,
        ),
        (
            "reclaimed_resumable_sessions",
            report.reclaimed_resumable_sessions,
        ),
    ] {
        writeln!(writer, "{name}: {value}")?;
    }

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
    use std::path::Path;

    use shardline_server::{
        BackupManifestReport, ConfigCheckReport, DatabaseMigrationCommand, DatabaseMigrationReport,
        DatabaseMigrationStatusEntry, FsckIssueDetail, FsckIssueKind, IndexRebuildIssueDetail,
        LifecycleRepairReport, LocalFsckIssue, LocalFsckReport, LocalGcReport,
        LocalIndexRebuildIssue, LocalIndexRebuildIssueKind, LocalIndexRebuildReport,
        StorageMigrationReport,
    };

    use super::*;
    use crate::hold::output_test_support::assert_writer_errors;

    // -----------------------------------------------------------------------
    // Config check summary output and errors
    // -----------------------------------------------------------------------

    #[test]
    fn config_check_summary_preserves_output_and_propagates_errors() {
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
        let mut bytes = Vec::new();
        write_config_check_summary(&mut bytes, &report).unwrap();
        assert_eq!(
            bytes,
            concat!(
                "status: ok\n",
                "server_role: all\n",
                "server_frontends: xet\n",
                "metadata_backend: local\n",
                "object_backend: local\n",
                "cache_backend: memory\n",
                "auth_enabled: true\n",
                "provider_tokens_enabled: false\n",
            )
            .as_bytes()
        );
        assert_writer_errors(|writer| write_config_check_summary(writer, &report));
    }

    // -----------------------------------------------------------------------
    // Database migration summary output and errors
    // -----------------------------------------------------------------------

    #[test]
    fn database_migration_summary_preserves_output_and_propagates_errors() {
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
        let mut bytes = Vec::new();
        write_database_migration_summary(&mut bytes, &report).unwrap();
        assert_eq!(
            bytes,
            concat!(
                "backend: postgres\n",
                "applied_count: 3\n",
                "reverted_count: 1\n",
                "applied_total_count: 7\n",
                "pending_count: 2\n",
                "migration: version=v1 name=m1 applied=true applied_at_utc=2026-01-01T00:00:00Z\n",
                "migration: version=v2 name=m2 applied=false applied_at_utc=-\n",
            )
            .as_bytes()
        );
        assert_writer_errors(|writer| write_database_migration_summary(writer, &report));
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
    fn fsck_summaries_preserve_fields_order_and_issue_counts() {
        let mut report = LocalFsckReport {
            latest_records: 1,
            version_records: 2,
            inspected_chunk_references: 3,
            inspected_dedupe_shard_mappings: 4,
            inspected_reconstructions: 5,
            inspected_webhook_deliveries: 6,
            inspected_provider_repository_states: 7,
            ..LocalFsckReport::default()
        };
        for (issue_count, issues) in [
            (0, vec![]),
            (
                2,
                vec![
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
            ),
        ] {
            report.issues = issues;
            let expected = format!(
                concat!(
                    "latest_records: 1\nversion_records: 2\n",
                    "inspected_chunk_references: 3\ninspected_dedupe_shard_mappings: 4\n",
                    "inspected_reconstructions: 5\ninspected_webhook_deliveries: 6\n",
                    "inspected_provider_repository_states: 7\nissue_count: {}\n",
                ),
                issue_count
            );
            let mut bytes = Vec::new();
            write_fsck_summary(&mut bytes, &report).unwrap();
            assert_eq!(bytes, expected.as_bytes());
            bytes.clear();
            write_fsck_cli_summary(&mut bytes, &report, Path::new("/root")).unwrap();
            assert_eq!(bytes, format!("root: /root\n{expected}").as_bytes());
            assert_eq!(report.issue_count(), issue_count);
            assert_eq!(report.is_clean(), issue_count == 0);
        }
    }

    #[test]
    fn fsck_issues_runs() {
        let report = LocalFsckReport {
            issues: vec![LocalFsckIssue {
                kind: FsckIssueKind::MissingChunk,
                location: "chunks/abc".to_owned(),
                detail: FsckIssueDetail::MissingVersionRecord {
                    version_locator: "records/abc".to_owned(),
                },
            }],
            ..LocalFsckReport::default()
        };
        print_fsck_issues(&report);
    }

    // -----------------------------------------------------------------------
    // print_index_rebuild_summary / cli / issues
    // -----------------------------------------------------------------------

    #[test]
    fn index_rebuild_summary_preserves_fields_and_order() {
        let report = LocalIndexRebuildReport {
            scanned_version_records: 1,
            scanned_retained_shards: 2,
            rebuilt_latest_records: 3,
            unchanged_latest_records: 4,
            removed_stale_latest_records: 5,
            preserved_latest_records_unreadable_version: vec!["records/kept".to_owned()],
            scanned_reconstructions: 6,
            unchanged_reconstructions: 7,
            removed_stale_reconstructions: 8,
            rebuilt_dedupe_shard_mappings: 9,
            unchanged_dedupe_shard_mappings: 10,
            removed_stale_dedupe_shard_mappings: 11,
            ..LocalIndexRebuildReport::default()
        };
        let mut bytes = Vec::new();
        write_index_rebuild_cli_summary(&mut bytes, &report, Path::new("/root")).unwrap();
        assert_eq!(
            bytes,
            concat!(
                "root: /root\n",
                "scanned_version_records: 1\n",
                "scanned_retained_shards: 2\n",
                "rebuilt_latest_records: 3\n",
                "unchanged_latest_records: 4\n",
                "removed_stale_latest_records: 5\n",
                "kept_latest_record_unreadable_version: records/kept\n",
                "scanned_reconstructions: 6\n",
                "unchanged_reconstructions: 7\n",
                "removed_stale_reconstructions: 8\n",
                "rebuilt_dedupe_shard_mappings: 9\n",
                "unchanged_dedupe_shard_mappings: 10\n",
                "removed_stale_dedupe_shard_mappings: 11\n",
                "issue_count: 0\n",
            )
            .as_bytes()
        );
    }

    #[test]
    fn index_rebuild_issues_runs() {
        let report = LocalIndexRebuildReport {
            issues: vec![LocalIndexRebuildIssue {
                kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                location: "records/abc".to_owned(),
                detail: IndexRebuildIssueDetail::RecordJsonInvalid,
            }],
            ..LocalIndexRebuildReport::default()
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
            ..LocalIndexRebuildReport::default()
        };
        assert_eq!(report.issue_count(), 1);
        assert!(!report.is_clean());

        let clean = LocalIndexRebuildReport::default();
        assert_eq!(clean.issue_count(), 0);
        assert!(clean.is_clean());
    }

    // -----------------------------------------------------------------------
    // print_lifecycle_repair_summary / prefixed / cli
    // -----------------------------------------------------------------------

    #[test]
    fn lifecycle_repair_summary_runs() {
        print_lifecycle_repair_summary(&LifecycleRepairReport::default());
    }

    #[test]
    fn lifecycle_repair_summary_prefixed_runs() {
        let report = LifecycleRepairReport::default();
        print_lifecycle_repair_summary_prefixed(&report, "repair");
        // empty prefix — no dot separator
        print_lifecycle_repair_summary_prefixed(&report, "");
    }

    #[test]
    fn lifecycle_repair_cli_summary_runs() {
        print_lifecycle_repair_cli_summary(
            &LifecycleRepairReport::default(),
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

    // -----------------------------------------------------------------------
    // print_storage_migration_summary
    // -----------------------------------------------------------------------

    #[test]
    fn storage_migration_summary_preserves_output_and_propagates_errors() {
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
        let mut bytes = Vec::new();
        write_storage_migration_summary(&mut bytes, &report).unwrap();
        assert_eq!(
            bytes,
            concat!(
                "source_backend: local\n",
                "destination_backend: s3\n",
                "prefix: chunks/\n",
                "dry_run: true\n",
                "scanned_objects: 1000\n",
                "scanned_bytes: 500000000\n",
                "inserted_objects: 800\n",
                "already_present_objects: 200\n",
                "copied_bytes: 400000000\n",
            )
            .as_bytes()
        );
        assert_writer_errors(|writer| write_storage_migration_summary(writer, &report));
    }

    // -----------------------------------------------------------------------
    // print_local_gc_summary / cli
    // -----------------------------------------------------------------------

    #[test]
    fn local_gc_summary_preserves_fields_and_order() {
        let report = LocalGcReport {
            retention_deferred_clock: true,
            scanned_records: 1,
            referenced_chunks: 2,
            orphan_chunks: 3,
            orphan_chunk_bytes: 4,
            active_quarantine_candidates: 5,
            new_quarantine_candidates: 6,
            retained_quarantine_candidates: 7,
            released_quarantine_candidates: 8,
            deleted_chunks: 9,
            deleted_bytes: 10,
            pruned_revisions_over_cap: 11,
            scanned_oci_tombstones: 12,
            eligible_oci_tombstones: 13,
            reclaimed_oci_tombstones: 14,
            scanned_resumable_staging_objects: 15,
            protected_resumable_staging_objects: 16,
            reclaimed_resumable_staging_objects: 17,
            reclaimed_resumable_staging_bytes: 18,
            reclaimed_resumable_sessions: 19,
            ..LocalGcReport::default()
        };
        let mut bytes = Vec::new();
        write_local_gc_summary(&mut bytes, &report).unwrap();
        assert_eq!(
            bytes,
            concat!(
                "retention_deferred_clock: true\n",
                "scanned_records: 1\n",
                "referenced_chunks: 2\n",
                "orphan_chunks: 3\n",
                "orphan_chunk_bytes: 4\n",
                "active_quarantine_candidates: 5\n",
                "new_quarantine_candidates: 6\n",
                "retained_quarantine_candidates: 7\n",
                "released_quarantine_candidates: 8\n",
                "deleted_chunks: 9\n",
                "deleted_bytes: 10\n",
                "pruned_revisions_over_cap: 11\n",
                "scanned_oci_tombstones: 12\n",
                "eligible_oci_tombstones: 13\n",
                "reclaimed_oci_tombstones: 14\n",
                "scanned_resumable_staging_objects: 15\n",
                "protected_resumable_staging_objects: 16\n",
                "reclaimed_resumable_staging_objects: 17\n",
                "reclaimed_resumable_staging_bytes: 18\n",
                "reclaimed_resumable_sessions: 19\n",
            )
            .as_bytes()
        );
        assert_writer_errors(|writer| write_local_gc_summary(writer, &report));
    }

    #[test]
    fn local_gc_cli_summary_preserves_mode_headers_and_errors() {
        let report = LocalGcReport::default();
        let mut summary = Vec::new();
        write_local_gc_summary(&mut summary, &report).unwrap();
        for (mode, retention, mark, retention_report, orphan_inventory, header) in [
            (
                "mark",
                7200,
                true,
                Some("/ret.json"),
                None,
                "mode: mark\nroot: /root\nretention_seconds: 7200\nretention_report: /ret.json\n",
            ),
            (
                "sweep",
                3600,
                false,
                None,
                Some("/orphans.json"),
                "mode: sweep\nroot: /root\norphan_inventory: /orphans.json\n",
            ),
            (
                "mark-and-sweep",
                3600,
                true,
                Some("/ret.json"),
                Some("/orphans.json"),
                "mode: mark-and-sweep\nroot: /root\nretention_seconds: 3600\nretention_report: /ret.json\norphan_inventory: /orphans.json\n",
            ),
            (
                "dry-run",
                3600,
                false,
                None,
                None,
                "mode: dry-run\nroot: /root\n",
            ),
        ] {
            let write = |writer: &mut dyn Write| {
                write_local_gc_cli_summary(
                    writer,
                    &report,
                    mode,
                    Path::new("/root"),
                    retention,
                    mark,
                    retention_report.map(Path::new),
                    orphan_inventory.map(Path::new),
                )
            };
            let mut bytes = Vec::new();
            write(&mut bytes).unwrap();
            assert_eq!(bytes, [header.as_bytes(), &summary].concat(), "{mode}");
            assert_writer_errors(write);
        }
    }

    #[test]
    fn fallible_cli_summaries_propagate_write_and_flush_errors() {
        assert_writer_errors(|writer| {
            write_fsck_cli_summary(writer, &LocalFsckReport::default(), Path::new("/root"))
        });
        assert_writer_errors(|writer| {
            write_index_rebuild_cli_summary(
                writer,
                &LocalIndexRebuildReport::default(),
                Path::new("/root"),
            )
        });
        assert_writer_errors(|writer| {
            write_backup_manifest_cli_summary(
                writer,
                &empty_backup_report(),
                Path::new("/root"),
                Path::new("/manifest.json"),
            )
        });
    }

    #[test]
    fn fallible_issue_outputs_propagate_write_and_flush_errors() {
        let fsck = LocalFsckReport {
            issues: vec![LocalFsckIssue {
                kind: FsckIssueKind::MissingChunk,
                location: "chunks/abc".to_owned(),
                detail: FsckIssueDetail::RecordJsonInvalid,
            }],
            ..LocalFsckReport::default()
        };
        let rebuild = LocalIndexRebuildReport {
            issues: vec![LocalIndexRebuildIssue {
                kind: LocalIndexRebuildIssueKind::InvalidVersionRecordJson,
                location: "records/abc".to_owned(),
                detail: IndexRebuildIssueDetail::RecordJsonInvalid,
            }],
            ..LocalIndexRebuildReport::default()
        };
        assert_writer_errors(|writer| write_fsck_issues(writer, &fsck));
        assert_writer_errors(|writer| write_index_rebuild_issues(writer, &rebuild));
    }

    #[test]
    fn backup_manifest_writer_preserves_fields_and_order() {
        let report = BackupManifestReport {
            object_count: 1,
            object_bytes: 2,
            latest_records: 3,
            version_records: 4,
            reconstruction_rows: 5,
            dedupe_shard_mappings: 6,
            quarantine_candidates: 7,
            retention_holds: 8,
            webhook_deliveries: 9,
            provider_repository_states: 10,
            ..empty_backup_report()
        };
        let mut writer = Vec::new();
        write_backup_manifest_cli_summary(
            &mut writer,
            &report,
            Path::new("/root"),
            Path::new("/manifest.json"),
        )
        .unwrap();
        assert_eq!(
            writer,
            concat!(
                "root: /root\noutput: /manifest.json\nmanifest_version: 1\n",
                "metadata_backend: local\nobject_backend: fs\n",
                "object_count: 1\n",
                "object_bytes: 2\n",
                "latest_records: 3\n",
                "version_records: 4\n",
                "reconstruction_rows: 5\n",
                "dedupe_shard_mappings: 6\n",
                "quarantine_candidates: 7\n",
                "retention_holds: 8\n",
                "webhook_deliveries: 9\n",
                "provider_repository_states: 10\n",
            )
            .as_bytes()
        );
    }

    #[test]
    fn lifecycle_summary_propagates_write_and_flush_errors() {
        assert_writer_errors(|writer| {
            write_lifecycle_repair_cli_summary(
                writer,
                &LifecycleRepairReport::default(),
                Path::new("/root"),
                3600,
            )
        });
    }
}
