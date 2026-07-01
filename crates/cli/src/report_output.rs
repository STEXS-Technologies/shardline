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

pub fn print_fsck_summary(report: &LocalFsckReport) {
    println!("latest_records: {}", report.latest_records);
    println!("version_records: {}", report.version_records);
    println!(
        "inspected_chunk_references: {}",
        report.inspected_chunk_references
    );
    println!(
        "inspected_dedupe_shard_mappings: {}",
        report.inspected_dedupe_shard_mappings
    );
    println!(
        "inspected_reconstructions: {}",
        report.inspected_reconstructions
    );
    println!(
        "inspected_webhook_deliveries: {}",
        report.inspected_webhook_deliveries
    );
    println!(
        "inspected_provider_repository_states: {}",
        report.inspected_provider_repository_states
    );
    println!("issue_count: {}", report.issue_count());
}

pub fn print_fsck_cli_summary(report: &LocalFsckReport, root: &Path) {
    println!("root: {}", root.display());
    print_fsck_summary(report);
}

pub fn print_fsck_issues(report: &LocalFsckReport) {
    for issue in &report.issues {
        eprintln!(
            "issue: {} location={} detail={}",
            issue.kind.as_str(),
            issue.location,
            issue.detail
        );
    }
}

pub fn print_index_rebuild_summary(report: &LocalIndexRebuildReport) {
    println!(
        "scanned_version_records: {}",
        report.scanned_version_records
    );
    println!(
        "scanned_retained_shards: {}",
        report.scanned_retained_shards
    );
    println!("rebuilt_latest_records: {}", report.rebuilt_latest_records);
    println!(
        "unchanged_latest_records: {}",
        report.unchanged_latest_records
    );
    println!(
        "removed_stale_latest_records: {}",
        report.removed_stale_latest_records
    );
    println!(
        "scanned_reconstructions: {}",
        report.scanned_reconstructions
    );
    println!(
        "unchanged_reconstructions: {}",
        report.unchanged_reconstructions
    );
    println!(
        "removed_stale_reconstructions: {}",
        report.removed_stale_reconstructions
    );
    println!(
        "rebuilt_dedupe_shard_mappings: {}",
        report.rebuilt_dedupe_shard_mappings
    );
    println!(
        "unchanged_dedupe_shard_mappings: {}",
        report.unchanged_dedupe_shard_mappings
    );
    println!(
        "removed_stale_dedupe_shard_mappings: {}",
        report.removed_stale_dedupe_shard_mappings
    );
    println!("issue_count: {}", report.issue_count());
}

pub fn print_index_rebuild_cli_summary(report: &LocalIndexRebuildReport, root: &Path) {
    println!("root: {}", root.display());
    print_index_rebuild_summary(report);
}

pub fn print_index_rebuild_issues(report: &LocalIndexRebuildReport) {
    for issue in &report.issues {
        eprintln!(
            "issue: {} location={} detail={}",
            issue.kind.as_str(),
            issue.location,
            issue.detail
        );
    }
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
    println!("manifest_version: {}", report.manifest_version);
    println!("metadata_backend: {}", report.metadata_backend);
    println!("object_backend: {}", report.object_backend);
    println!("object_count: {}", report.object_count);
    println!("object_bytes: {}", report.object_bytes);
    println!("latest_records: {}", report.latest_records);
    println!("version_records: {}", report.version_records);
    println!("reconstruction_rows: {}", report.reconstruction_rows);
    println!("dedupe_shard_mappings: {}", report.dedupe_shard_mappings);
    println!("quarantine_candidates: {}", report.quarantine_candidates);
    println!("retention_holds: {}", report.retention_holds);
    println!("webhook_deliveries: {}", report.webhook_deliveries);
    println!(
        "provider_repository_states: {}",
        report.provider_repository_states
    );
}

pub fn print_backup_manifest_cli_summary(
    report: &BackupManifestReport,
    root: &Path,
    output: &Path,
) {
    println!("root: {}", root.display());
    println!("output: {}", output.display());
    print_backup_manifest_summary(report);
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
