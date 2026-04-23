use std::{io::Error as IoError, path::Path};

use serde::Serialize;
use serde_json::{Error as JsonError, to_vec_pretty};
use shardline_server::{
    LocalGcDiagnostics, LocalGcOptions, LocalGcReport, ServerConfigError, ServerError,
    run_gc_diagnostics as run_server_gc_diagnostics,
};
use thiserror::Error;

use crate::{config::load_server_config, local_output::write_output_bytes};

/// Garbage-collection runtime failure.
#[derive(Debug, Error)]
pub enum GcRuntimeError {
    /// Configuration loading failed.
    #[error(transparent)]
    Config(#[from] ServerConfigError),
    /// The server-side collector failed.
    #[error(transparent)]
    Server(#[from] ServerError),
    /// Diagnostics artifact serialization failed.
    #[error(transparent)]
    Json(#[from] JsonError),
    /// Diagnostics artifact persistence failed.
    #[error(transparent)]
    Io(#[from] IoError),
}

/// Runs garbage collection against the active Shardline deployment.
///
/// # Errors
///
/// Returns [`GcRuntimeError`] when scanning or sweeping storage fails.
pub async fn run_gc(
    root: Option<&Path>,
    mark: bool,
    sweep: bool,
    retention_seconds: u64,
    retention_report_path: Option<&Path>,
    orphan_inventory_path: Option<&Path>,
) -> Result<LocalGcReport, GcRuntimeError> {
    Ok(run_gc_diagnostics(
        root,
        mark,
        sweep,
        retention_seconds,
        retention_report_path,
        orphan_inventory_path,
    )
    .await?
    .report)
}

/// Runs garbage collection against the active Shardline deployment and returns full diagnostics.
///
/// # Errors
///
/// Returns [`GcRuntimeError`] when scanning, sweeping, serializing, or writing export
/// artifacts fails.
pub async fn run_gc_diagnostics(
    root: Option<&Path>,
    mark: bool,
    sweep: bool,
    retention_seconds: u64,
    retention_report_path: Option<&Path>,
    orphan_inventory_path: Option<&Path>,
) -> Result<LocalGcDiagnostics, GcRuntimeError> {
    let options = LocalGcOptions {
        mark,
        sweep,
        retention_seconds,
    };
    let config = load_server_config(root)?;
    let diagnostics = run_server_gc_diagnostics(config, options).await?;
    write_optional_artifact(retention_report_path, &diagnostics.retention_report)?;
    write_optional_artifact(orphan_inventory_path, &diagnostics.orphan_inventory)?;
    Ok(diagnostics)
}

fn write_optional_artifact<Value>(path: Option<&Path>, value: &Value) -> Result<(), GcRuntimeError>
where
    Value: Serialize,
{
    let Some(path) = path else {
        return Ok(());
    };
    let bytes = to_vec_pretty(value)?;
    write_output_bytes(path, &bytes, true)?;
    Ok(())
}
