//! Opt-in maintenance execution. Existing commands and deployment schemas stay intact.
mod artifact;
mod automatic;
mod capture;
mod controller;
mod history;
mod inline;
mod journal;
mod outbox;
mod path_codec;
mod shared_inline;
mod verified;
mod worker;

use clap::{Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use shardline_reliability::MaintenanceArtifactKind;
use std::path::PathBuf;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

pub(crate) struct CommandOutput {
    pub text: String,
    pub success: bool,
    pub capture: Option<capture::CapturedOutput>,
    pub evidence_published: bool,
    pub exit_code: Option<u8>,
}

/// Maintenance workflows supported by the durable Linux controller.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand, Serialize, Deserialize)]
pub enum MaintenanceOperation {
    /// Copy immutable objects between local or S3 adapters; preserves verified-copy semantics.
    StorageMigrate {
        #[arg(long)]
        #[serde(with = "path_codec")]
        root: PathBuf,
        #[arg(long, value_enum)]
        from: MaintenanceStorageAdapter,
        #[arg(long)]
        #[serde(with = "path_codec::optional")]
        from_root: Option<PathBuf>,
        #[arg(long, value_enum)]
        to: MaintenanceStorageAdapter,
        #[arg(long)]
        #[serde(with = "path_codec::optional")]
        to_root: Option<PathBuf>,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long)]
        dry_run: bool,
    },
    /// Scan integrity; retries perform a complete fresh scan.
    Fsck {
        #[arg(long)]
        #[serde(with = "path_codec")]
        root: PathBuf,
    },
    /// Rebuild derived indexes under the existing exclusive maintenance barrier.
    IndexRebuild {
        #[arg(long)]
        #[serde(with = "path_codec")]
        root: PathBuf,
    },
    /// Run GC with existing quarantine, retention and reachability checks.
    Gc {
        #[arg(long)]
        #[serde(with = "path_codec")]
        root: PathBuf,
        #[arg(long)]
        mark: bool,
        #[arg(long)]
        sweep: bool,
        #[arg(long, default_value_t = 3600)]
        retention_seconds: u64,
    },
    /// Repair lifecycle metadata under the existing application contract.
    LifecycleRepair {
        #[arg(long)]
        #[serde(with = "path_codec")]
        root: PathBuf,
        #[arg(long, default_value_t = 604800)]
        webhook_retention_seconds: u64,
    },
}

/// Object-storage adapter for supervised immutable-object copies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
pub enum MaintenanceStorageAdapter {
    Local,
    S3,
}
impl MaintenanceStorageAdapter {
    const fn from_adapter(adapter: shardline_server::ObjectStorageAdapter) -> Self {
        match adapter {
            shardline_server::ObjectStorageAdapter::Local => Self::Local,
            shardline_server::ObjectStorageAdapter::S3 => Self::S3,
        }
    }
    const fn adapter(self) -> shardline_server::ObjectStorageAdapter {
        match self {
            Self::Local => shardline_server::ObjectStorageAdapter::Local,
            Self::S3 => shardline_server::ObjectStorageAdapter::S3,
        }
    }
}

impl MaintenanceOperation {
    fn root(&self) -> &std::path::Path {
        match self {
            Self::Fsck { root }
            | Self::IndexRebuild { root }
            | Self::Gc { root, .. }
            | Self::StorageMigrate { root, .. }
            | Self::LifecycleRepair { root, .. } => root,
        }
    }
    const fn kind(&self) -> MaintenanceArtifactKind {
        match self {
            Self::StorageMigrate { .. } => MaintenanceArtifactKind::StorageMigration,
            Self::Fsck { .. } => MaintenanceArtifactKind::Fsck,
            Self::IndexRebuild { .. } => MaintenanceArtifactKind::IndexRebuild,
            Self::Gc { .. } => MaintenanceArtifactKind::GarbageCollection,
            Self::LifecycleRepair { .. } => MaintenanceArtifactKind::LifecycleRepair,
        }
    }
    const fn restart_safe(&self) -> bool {
        matches!(
            self,
            Self::Fsck { .. }
                | Self::Gc {
                    mark: false,
                    sweep: false,
                    ..
                }
                | Self::StorageMigrate { dry_run: true, .. }
        )
    }
    const fn read_only(&self) -> bool {
        matches!(
            self,
            Self::Gc {
                mark: false,
                sweep: false,
                ..
            } | Self::StorageMigrate { dry_run: true, .. }
        )
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    schema: u32,
    operation: MaintenanceOperation,
    #[serde(with = "path_codec::optional")]
    config: Option<PathBuf>,
    target: penelope_core::task::Digest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command: Option<Vec<Vec<u8>>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    command_digest: Option<penelope_core::task::Digest>,
}

fn target(
    config: &shardline_server::ServerConfig,
    operation: &MaintenanceOperation,
) -> Result<penelope_core::task::Digest> {
    // Credentials are never persisted. Logical S3 addressing and all metadata
    // targets are bound; credential rotation alone does not change a task.
    let migration = if let MaintenanceOperation::StorageMigrate {
        from,
        from_root,
        to,
        to_root,
        ..
    } = operation
    {
        format!(
            "{:?}|{:?}",
            crate::storage_migration::endpoint(from.adapter(), from_root.as_deref(), "source")?,
            crate::storage_migration::endpoint(to.adapter(), to_root.as_deref(), "destination")?
        )
    } else {
        String::new()
    };
    Ok(penelope_core::task::Digest::of(
        format!(
            "{:?}|{:?}|{:?}|{:?}|{migration}",
            config.root_dir(),
            shardline_server::task_tracking::deployment_fingerprint(config)?,
            config.server_frontends(),
            config.max_revisions_per_repo()
        )
        .as_bytes(),
    ))
}

fn operation_config(
    operation: &MaintenanceOperation,
    automatic: bool,
) -> Result<shardline_server::ServerConfig> {
    if automatic
        && let MaintenanceOperation::StorageMigrate {
            from, from_root, ..
        } = operation
    {
        let source =
            crate::storage_migration::endpoint(from.adapter(), from_root.as_deref(), "source")?;
        let root = match &source {
            shardline_server::StorageMigrationEndpoint::LocalStateRoot(root) => root.clone(),
            shardline_server::StorageMigrationEndpoint::S3(_) => operation.root().to_path_buf(),
        };
        let root = crate::local_output::resolved_output_path(&root)?;
        let config = shardline_server::ServerConfig::new(
            "127.0.0.1:0".parse()?,
            "http://localhost".to_owned(),
            root,
            std::num::NonZeroUsize::new(4096).ok_or("invalid maintenance chunk size")?,
        );
        return Ok(match source {
            shardline_server::StorageMigrationEndpoint::LocalStateRoot(_) => config,
            shardline_server::StorageMigrationEndpoint::S3(s3) => {
                config.with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(s3))
            }
        });
    }
    Ok(crate::config::load_server_config(
        Some(operation.root()),
        None,
    )?)
}

fn publication_config(
    operation: &MaintenanceOperation,
    config: &shardline_server::ServerConfig,
) -> Result<shardline_server::ServerConfig> {
    if let MaintenanceOperation::StorageMigrate { to, to_root, .. } = operation {
        return Ok(
            match crate::storage_migration::endpoint(
                to.adapter(),
                to_root.as_deref(),
                "destination",
            )? {
                shardline_server::StorageMigrationEndpoint::LocalStateRoot(root) => config
                    .clone()
                    .with_root_dir(crate::local_output::resolved_output_path(&root)?)
                    .with_object_storage(shardline_server::ObjectStorageAdapter::Local, None),
                shardline_server::StorageMigrationEndpoint::S3(s3) => config
                    .clone()
                    .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(s3)),
            },
        );
    }
    Ok(config.clone())
}

pub(crate) use automatic::{prepare as prepare_automatic, run as run_automatic};
pub(crate) use controller::run;
pub(crate) use worker::run as run_worker;
