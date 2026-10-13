use super::{MaintenanceOperation, Result};
use crate::CliCommand;
use std::{ffi::OsString, os::unix::ffi::OsStrExt};

pub(crate) struct PreparedMaintenance {
    operation: MaintenanceOperation,
    configuration: super::configuration::Configuration,
}

pub(crate) fn prepare(command: &CliCommand) -> Result<Option<PreparedMaintenance>> {
    if let CliCommand::StorageMigrate {
        from,
        from_root,
        to,
        to_root,
        prefix,
        dry_run,
    } = command
    {
        let source = crate::storage_migration::endpoint(*from, from_root.as_deref(), "source")?;
        let destination =
            crate::storage_migration::endpoint(*to, to_root.as_deref(), "destination")?;
        let root = match &source {
            shardline_server::StorageMigrationEndpoint::LocalStateRoot(root) => root.clone(),
            shardline_server::StorageMigrationEndpoint::S3(_) => std::env::current_dir()?,
        };
        let operation = MaintenanceOperation::StorageMigrate {
            root,
            from: super::MaintenanceStorageAdapter::from_adapter(*from),
            from_root: from_root.clone(),
            to: super::MaintenanceStorageAdapter::from_adapter(*to),
            to_root: to_root.clone(),
            prefix: prefix.clone(),
            dry_run: *dry_run,
        };
        return Ok(Some(PreparedMaintenance {
            configuration: super::configuration::Configuration {
                config: super::source_config(operation.root(), source.clone())?,
                command: command.clone(),
                config_path: crate::config::selected_config_path(None),
                migration: Some((source, destination)),
            },
            operation,
        }));
    }
    let requested_root = match command {
        CliCommand::Fsck { root }
        | CliCommand::IndexRebuild { root }
        | CliCommand::Gc { root, .. }
        | CliCommand::Repair { root, .. }
        | CliCommand::RepairLifecycle { root, .. } => root.as_deref(),
        CliCommand::ProviderlessSetup
        | CliCommand::Serve { .. }
        | CliCommand::ConfigCheck
        | CliCommand::DbMigrate { .. }
        | CliCommand::DbMigrateLocalUp { .. }
        | CliCommand::AdminToken { .. }
        | CliCommand::GcScheduleInstall { .. }
        | CliCommand::GcScheduleUninstall { .. }
        | CliCommand::RepairHubTree { .. }
        | CliCommand::RepairLfsEvidence { .. }
        | CliCommand::BackupManifest { .. }
        | CliCommand::StorageMigrate { .. }
        | CliCommand::HoldSet { .. }
        | CliCommand::HoldList { .. }
        | CliCommand::HoldRelease { .. }
        | CliCommand::Bench { .. }
        | CliCommand::Health { .. }
        | CliCommand::Completion { .. }
        | CliCommand::Manpage { .. } => return Ok(None),
    };
    let config = crate::config::load_server_config(requested_root, None)?;
    let root = config.root_dir().to_path_buf();
    if config.index_postgres_url().is_none() && !root.is_dir() {
        // Original local commands bootstrap an empty deployment. Create its
        // directory with normal storage permissions before private task state;
        // recursive 0700 task-directory creation must not narrow its parents.
        // The storage helper rejects symlinked components and makes new names
        // durable without changing existing directory permissions.
        let _root = shardline_storage::open_directory_chain(&root, true, None, || {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid deployment root")
        })?;
    }
    let operation = match command {
        CliCommand::Fsck { .. } => MaintenanceOperation::Fsck { root },
        CliCommand::IndexRebuild { .. } => MaintenanceOperation::IndexRebuild { root },
        CliCommand::Gc {
            mark,
            sweep,
            retention_seconds,
            ..
        } => MaintenanceOperation::Gc {
            root,
            mark: *mark,
            sweep: *sweep,
            retention_seconds: *retention_seconds,
        },
        CliCommand::Repair {
            webhook_retention_seconds,
            ..
        }
        | CliCommand::RepairLifecycle {
            webhook_retention_seconds,
            ..
        } => MaintenanceOperation::LifecycleRepair {
            root,
            webhook_retention_seconds: *webhook_retention_seconds,
        },
        CliCommand::ProviderlessSetup
        | CliCommand::Serve { .. }
        | CliCommand::ConfigCheck
        | CliCommand::DbMigrate { .. }
        | CliCommand::DbMigrateLocalUp { .. }
        | CliCommand::AdminToken { .. }
        | CliCommand::GcScheduleInstall { .. }
        | CliCommand::GcScheduleUninstall { .. }
        | CliCommand::RepairHubTree { .. }
        | CliCommand::RepairLfsEvidence { .. }
        | CliCommand::BackupManifest { .. }
        | CliCommand::StorageMigrate { .. }
        | CliCommand::HoldSet { .. }
        | CliCommand::HoldList { .. }
        | CliCommand::HoldRelease { .. }
        | CliCommand::Bench { .. }
        | CliCommand::Health { .. }
        | CliCommand::Completion { .. }
        | CliCommand::Manpage { .. } => return Ok(None),
    };
    Ok(Some(PreparedMaintenance {
        operation,
        configuration: super::configuration::Configuration {
            config,
            command: command.clone(),
            config_path: crate::config::selected_config_path(None),
            migration: None,
        },
    }))
}

pub(crate) fn run(
    prepared: &PreparedMaintenance,
    arguments: Vec<OsString>,
) -> Result<super::CommandOutput> {
    super::configuration::scope(prepared.configuration.clone(), || {
        run_inner(&prepared.operation, arguments)
    })
}

fn run_inner(
    operation: &MaintenanceOperation,
    arguments: Vec<OsString>,
) -> Result<super::CommandOutput> {
    let config = super::operation_config(operation, true)?;
    // Read-only diagnostics must not acquire a new filesystem write requirement.
    if operation.read_only() {
        return Ok(super::shared_inline::run(operation, arguments, &config));
    }
    let ephemeral = config.index_postgres_url().is_some()
        || matches!(operation, MaintenanceOperation::StorageMigrate { .. });
    let state = {
        if ephemeral {
            shardline_server::task_tracking::deployment_fingerprint(&config).map(|deployment| {
                std::env::temp_dir().join(format!(
                    "shardline-maintenance-{}-{}",
                    rustix::process::geteuid().as_raw(),
                    hex::encode(deployment.0)
                ))
            })
        } else {
            Ok(config.root_dir().join(".shardline-maintenance"))
        }
    }?;
    let mut nonce = [0_u8; 16];
    if let Err(error) = getrandom::fill(&mut nonce) {
        tracing::debug!(
            "private maintenance identity unavailable; no application work started; existing command continues with available metadata history: {error}"
        );
        return Ok(super::shared_inline::run(operation, arguments, &config));
    }
    let id = hex::encode(nonce);
    let directory = state.join(hex::encode(
        penelope_core::task::Digest::of(id.as_bytes()).0,
    ));
    let writable = super::workspace_admission::ensure(&state, &directory);
    if let Err(error) = writable {
        // No application work has started. Keep the original handler and its
        // domain barriers; only post-work housekeeping inspects retained state.
        // Private scratch is optional when deployment history can record the
        // outcome. That fallback is normal on restricted roots; shared_inline
        // warns separately if recording actually fails.
        tracing::debug!(workspace = %state.display(), "private maintenance tracking unavailable; existing command continues with available metadata history: {error}");
        let output = super::shared_inline::run(operation, arguments, &config);
        // Admission pressure must not starve exports and verified cleanup.
        // The shared path already retired history; avoid a second retention batch.
        match super::publication_config(operation, &config) {
            Ok(export_config) => retry_private_exports(operation, &export_config, &state, false),
            Err(config_error) => tracing::warn!(
                "retained private history unavailable; original command result is preserved: {config_error}"
            ),
        }
        return Ok(output);
    }
    // Preserve configuration validation before any application mutation.
    let export_config = super::publication_config(operation, &config)?;
    let command = arguments
        .iter()
        .map(|argument| argument.as_os_str().as_bytes().to_vec())
        .collect();
    let output = super::inline::run(&state, &id, operation, command, arguments);
    retry_private_exports(operation, &export_config, &state, true);
    if output
        .as_ref()
        .is_ok_and(|result| result.evidence_published)
    {
        // Evidence and streamed output are confirmed in the selected history
        // store. Open capture descriptors survive removal of the scratch files.
        let digest = penelope_core::task::Digest::of(id.as_bytes());
        if let Err(error) = super::cleanup::completed(&state.join(hex::encode(digest.0))) {
            // Work and its receipt are already complete. A scratch-cleanup
            // error must not change the handler's result or authorize a retry.
            tracing::warn!(workspace = %state.display(), "completed maintenance scratch could not be removed; original command result is preserved: {error}");
        }
    }
    output
}

// Runs only after the original handler. Shared fallback already retires history;
// both paths must still make progress exporting and cleaning verified private runs.
fn retry_private_exports(
    operation: &MaintenanceOperation,
    export_config: &shardline_server::ServerConfig,
    state: &std::path::Path,
    retire_shared: bool,
) {
    if let Err(error) = super::cleanup::retry(state) {
        tracing::warn!(workspace = %state.display(), "confirmed scratch cleanup unavailable; original command result is preserved: {error}");
    }
    let retry = (|| -> Result<()> {
        let store = super::evidence::store(operation, export_config, Some(state))?;
        let deployment = shardline_server::task_tracking::deployment_fingerprint(export_config)?;
        if retire_shared {
            super::evidence::retire_finished_history(&store, deployment);
        }
        super::journal::retry_exports(state, &store, deployment)
    })();
    if let Err(error) = retry {
        tracing::warn!(workspace = %state.display(), "retained evidence could not be inspected or exported; original command result is preserved; no application retry authorized: {error}");
    }
}

#[cfg(test)]
mod tests;
