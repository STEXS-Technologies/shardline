use std::{fs, path::Path};

use shardline_server::{
    LfsPatchEvidenceRepairInput, ServerConfigError, ServerError, repair_lfs_patch_evidence,
};
use thiserror::Error;

use crate::load_server_config;

/// Failure while loading or applying an explicit LFS evidence repair.
#[derive(Debug, Error)]
pub enum LfsRepairRuntimeError {
    /// The deployment root could not be resolved.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// The operator state file could not be read.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// The operator state file was not valid JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// The authoritative materialized state failed validation.
    #[error(transparent)]
    Server(#[from] ServerError),
}

/// Rebuilds one LFS patch evidence envelope from an explicit operator state file.
///
/// # Errors
///
/// Returns an error when configuration, the operator state file, or the
/// authoritative LFS materialized state cannot be read or validated.
pub fn run_lfs_evidence_repair(
    root: Option<&Path>,
    state_file: &Path,
) -> Result<(), LfsRepairRuntimeError> {
    let config = load_server_config(root, None)?;
    let bytes = fs::read(state_file)?;
    let input: LfsPatchEvidenceRepairInput = serde_json::from_slice(&bytes)?;
    let patch_dir = config.root_dir().join("tmp").join("lfs-patch");
    repair_lfs_patch_evidence(&patch_dir, &input)?;
    Ok(())
}
