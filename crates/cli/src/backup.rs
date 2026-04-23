use std::{io::Error as IoError, path::Path};

use shardline_server::{
    BackupManifestReport, ServerConfigError, ServerError,
    write_backup_manifest as write_server_backup_manifest,
};
use thiserror::Error;

use crate::{config::load_server_config, local_output::AtomicOutputFile};

/// Backup command runtime failure.
#[derive(Debug, Error)]
pub enum BackupRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// Backup manifest writing failed due to an operational server-side error.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// Output file creation failed.
    #[error("backup manifest output file operation failed")]
    Io(#[from] IoError),
}

/// Writes a backup manifest for the active deployment.
///
/// # Errors
///
/// Returns [`BackupRuntimeError`] when configuration loading, output creation, metadata
/// enumeration, or object inventory fails.
pub async fn run_backup_manifest(
    root: Option<&Path>,
    output: &Path,
) -> Result<BackupManifestReport, BackupRuntimeError> {
    let config = load_server_config(root)?;
    let mut writer = AtomicOutputFile::create(output, false)?;
    let report = write_server_backup_manifest(config, &mut writer).await?;
    writer.commit()?;
    Ok(report)
}
