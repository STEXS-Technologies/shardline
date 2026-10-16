use super::{Input, MaintenanceOperation, Result, artifact, target};
use penelope::core::task::{TaskGeneration, TaskScope};
use shardline_reliability::{MaintenanceArtifactKind, ProcessWorker};
use std::path::Path;

pub(crate) fn run(path: &Path, scope: String, generation: u32) -> Result<super::CommandOutput> {
    if scope.len() > 4096 {
        return Err("invalid worker scope".into());
    }
    let scope: TaskScope = serde_json::from_slice(&hex::decode(scope)?)?;
    let generation = TaskGeneration::new(generation);
    let mut worker = ProcessWorker::claim(path, scope, generation)?;
    let input: Input = serde_json::from_slice(worker.input())?;
    if input.schema != 1 {
        return Err("unsupported maintenance input".into());
    }
    crate::config::set_cli_config_override(input.config);
    let config = super::operation_config(&input.operation, input.command.is_some())?;
    if target(&config, &input.operation)? != input.target {
        return Err("maintenance deployment target changed".into());
    }
    let directory = path.parent().ok_or("journal has no state directory")?;
    // This checkpoint certifies the authorized starting boundary. It is never
    // interpreted as permission to skip scans or reuse stale reachability data.
    let checkpoint = artifact::publish(
        directory,
        scope,
        generation,
        MaintenanceArtifactKind::Checkpoint,
        1,
        b"shardline.maintenance.full-scan-boundary.v1",
    )?;
    worker.checkpoint(&checkpoint, 1)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut report = tempfile::NamedTempFile::new_in(directory)?;
    let mut writer = artifact::ReportWriter::new(report.as_file_mut());
    if let Some(arguments) = input.command {
        use std::os::unix::ffi::OsStringExt;
        let arguments = arguments.into_iter().map(std::ffi::OsString::from_vec);
        let status = runtime.block_on(Box::pin(crate::entry::run_direct(arguments)));
        use std::io::Write as _;
        std::io::stdout().flush()?;
        std::io::stderr().flush()?;
        super::capture::publish(directory, generation, status, &mut writer)?;
    } else {
        runtime.block_on(execute(input.operation.clone(), config, &mut writer))?;
    }
    let output = artifact::publish_file(
        directory,
        scope,
        generation,
        input.operation.kind(),
        2,
        report.as_file(),
    )?;
    worker.complete(&output, 1)?;
    Ok(super::CommandOutput {
        exit_code: None,
        evidence_published: false,
        text: String::new(),
        success: true,
        capture: None,
    })
}

async fn execute(
    operation: MaintenanceOperation,
    config: shardline_server::ServerConfig,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    use shardline_server::{
        LifecycleRepairOptions, run_fsck, run_index_rebuild, run_lifecycle_repair,
    };
    match operation {
        MaintenanceOperation::StorageMigrate {
            from,
            from_root,
            to,
            to_root,
            prefix,
            dry_run,
            ..
        } => {
            let report = crate::run_storage_migration(
                from.adapter(),
                from_root.as_deref(),
                to.adapter(),
                to_root.as_deref(),
                prefix,
                dry_run,
            )?;
            writeln!(writer, "maintenance.success: true")?;
            crate::report_output::write_storage_migration_summary(writer, &report)?;
        }
        MaintenanceOperation::Fsck { .. } => {
            let report = run_fsck(config).await?;
            writeln!(writer, "maintenance.success: {}", report.is_clean())?;
            crate::report_output::write_fsck_summary(writer, &report)?;
            crate::report_output::write_fsck_issues(writer, &report)?;
        }
        MaintenanceOperation::IndexRebuild { .. } => {
            let report = run_index_rebuild(config).await?;
            writeln!(writer, "maintenance.success: {}", report.is_clean())?;
            crate::report_output::write_index_rebuild_summary(writer, &report)?;
            crate::report_output::write_index_rebuild_issues(writer, &report)?;
        }
        MaintenanceOperation::Gc {
            mark,
            sweep,
            retention_seconds,
            ..
        } => {
            let options = shardline_server::LocalGcOptions {
                mark,
                sweep,
                retention_seconds: retention_seconds.max(crate::MINIMUM_GC_RETENTION_SECONDS),
                max_revisions_per_repo: Some(config.max_revisions_per_repo().get()),
            };
            let diagnostics = shardline_server::run_gc_diagnostics(config, options).await?;
            writeln!(writer, "maintenance.success: true")?;
            crate::report_output::write_local_gc_summary(writer, &diagnostics.report)?;
            serde_json::to_writer_pretty(&mut *writer, &diagnostics.retention_report)?;
            serde_json::to_writer_pretty(writer, &diagnostics.orphan_inventory)?;
        }
        MaintenanceOperation::LifecycleRepair {
            webhook_retention_seconds,
            ..
        } => {
            let report = run_lifecycle_repair(
                config,
                LifecycleRepairOptions {
                    webhook_retention_seconds,
                },
            )
            .await?;
            writeln!(writer, "maintenance.success: true")?;
            crate::report_output::write_lifecycle_repair_summary(writer, &report)?;
        }
    }
    Ok(())
}
