use super::{MaintenanceOperation, Result};
use crate::CliCommand;
use std::{
    ffi::OsString,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
};

pub(crate) fn prepare(command: &CliCommand) -> Result<Option<MaintenanceOperation>> {
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
        let _destination =
            crate::storage_migration::endpoint(*to, to_root.as_deref(), "destination")?;
        let root = match source {
            shardline_server::StorageMigrationEndpoint::LocalStateRoot(root) => root,
            shardline_server::StorageMigrationEndpoint::S3(_) => std::env::current_dir()?,
        };
        return Ok(Some(MaintenanceOperation::StorageMigrate {
            root,
            from: super::MaintenanceStorageAdapter::from_adapter(*from),
            from_root: from_root.clone(),
            to: super::MaintenanceStorageAdapter::from_adapter(*to),
            to_root: to_root.clone(),
            prefix: prefix.clone(),
            dry_run: *dry_run,
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
        // Preserve original validation for a missing local deployment. No work
        // can be authorized against an absent local metadata directory.
        return Err("local deployment root does not exist".into());
    }
    Ok(Some(match command {
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
    }))
}

pub(crate) fn run(
    operation: &MaintenanceOperation,
    arguments: Vec<OsString>,
) -> Result<super::CommandOutput> {
    let config = super::operation_config(operation, true)?;
    let temporary = if operation.read_only() {
        Some(
            tempfile::Builder::new()
                .prefix("shardline-maintenance-")
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir()?,
        )
    } else {
        None
    };
    let ephemeral = config.index_postgres_url().is_some()
        || matches!(
            operation,
            MaintenanceOperation::StorageMigrate {
                from: super::MaintenanceStorageAdapter::S3,
                ..
            }
        );
    let state = temporary.as_ref().map_or_else(
        || {
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
        },
        |dir| Ok(dir.path().to_path_buf()),
    )?;
    if !operation.read_only() {
        let export_config = super::publication_config(operation, &config)?;
        let retry = (|| -> Result<()> {
            let store =
                shardline_server::task_tracking::TaskEvidenceStore::from_config(&export_config)?;
            let deployment =
                shardline_server::task_tracking::deployment_fingerprint(&export_config)?;
            if let Err(error) =
                store.prune_completed(deployment, shardline_protocol::unix_now_seconds_lossy())
            {
                tracing::warn!(
                    "maintenance evidence retention unavailable; existing command continues: {error}"
                );
            }
            super::journal::retry_exports(&state, &store, deployment)
        })();
        if let Err(error) = retry {
            tracing::warn!(workspace = %state.display(), "retained evidence could not be inspected or exported; no application retry authorized: {error}");
        }
    }
    let mut nonce = [0_u8; 16];
    getrandom::fill(&mut nonce).map_err(|error| error.to_string())?;
    let id = hex::encode(nonce);
    let command = arguments
        .iter()
        .map(|argument| argument.as_os_str().as_bytes().to_vec())
        .collect();
    let output = super::inline::run(&state, &id, operation, command, arguments);
    if output
        .as_ref()
        .is_ok_and(|result| result.evidence_published)
    {
        // Evidence and streamed output are already confirmed in the deployment
        // store. Open capture descriptors survive removal of the scratch files.
        let digest = penelope_core::task::Digest::of(id.as_bytes());
        std::fs::remove_dir_all(state.join(hex::encode(digest.0)))?;
    } else if let Some(directory) = temporary {
        // Retain ambiguous work for inspection, never silently erase its journal.
        let retained = directory.keep();
        tracing::warn!(workspace = %retained.display(), "retained maintenance evidence; shared completion is unconfirmed");
    }
    output
}
