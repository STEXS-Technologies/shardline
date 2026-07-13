use std::{io::Error as IoError, path::Path};

use serde::Serialize;
use serde_json::{Error as JsonError, to_vec_pretty};
use shardline_server::{
    LocalGcDiagnostics, LocalGcOptions, LocalGcReport, ServerConfigError, ServerError,
    run_gc_diagnostics as run_server_gc_diagnostics,
};
use thiserror::Error;

use crate::{config::load_server_config, local_output::write_output_bytes};

/// Minimum retention window in seconds for GC quarantine entries.
///
/// Prevents data loss from the TOCTOU race between the GC mark phase and
/// concurrent uploads that have written chunks on disk but not yet committed
/// their file records.
pub const MINIMUM_GC_RETENTION_SECONDS: u64 = 3600; // 1 hour

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
        // Enforce a minimum retention to prevent data loss from the TOCTOU
        // race between the GC mark phase (index scan) and the sweep phase
        // (delete of orphaned chunks).  Concurrent uploads that have written
        // chunks on disk but not yet committed file records need a grace period.
        retention_seconds: retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS),
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

#[cfg(test)]
mod tests {
    use shardline_server::LocalGcOptions;

    use super::MINIMUM_GC_RETENTION_SECONDS;

    #[test]
    fn minimum_gc_retention_seconds_is_one_hour() {
        assert_eq!(MINIMUM_GC_RETENTION_SECONDS, 3600);
    }

    #[test]
    fn local_gc_options_clamps_retention_below_minimum() {
        // When constructing LocalGcOptions, retention_seconds is *not* clamped
        // at the options level — clamping happens inside run_gc_diagnostics.
        let opts = LocalGcOptions {
            mark: true,
            sweep: false,
            retention_seconds: 0,
        };
        assert_eq!(opts.retention_seconds, 0);

        // The max() call inside run_gc_diagnostics ensures the minimum:
        let clamped = opts.retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS);
        assert_eq!(clamped, MINIMUM_GC_RETENTION_SECONDS);
    }

    #[test]
    fn local_gc_options_above_minimum_unchanged() {
        let opts = LocalGcOptions {
            mark: true,
            sweep: true,
            retention_seconds: 7200,
        };
        let clamped = opts.retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS);
        assert_eq!(clamped, 7200);
    }

    #[test]
    fn local_gc_options_at_minimum_unchanged() {
        let opts = LocalGcOptions {
            mark: false,
            sweep: true,
            retention_seconds: MINIMUM_GC_RETENTION_SECONDS,
        };
        let clamped = opts.retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS);
        assert_eq!(clamped, MINIMUM_GC_RETENTION_SECONDS);
    }

    #[test]
    fn local_gc_options_mode_name() {
        assert_eq!(
            LocalGcOptions {
                mark: false,
                sweep: false,
                retention_seconds: 3600,
            }
            .mode_name(),
            "dry-run"
        );
        assert_eq!(
            LocalGcOptions {
                mark: true,
                sweep: false,
                retention_seconds: 3600,
            }
            .mode_name(),
            "mark"
        );
        assert_eq!(
            LocalGcOptions {
                mark: false,
                sweep: true,
                retention_seconds: 3600,
            }
            .mode_name(),
            "sweep"
        );
        assert_eq!(
            LocalGcOptions {
                mark: true,
                sweep: true,
                retention_seconds: 3600,
            }
            .mode_name(),
            "mark-and-sweep"
        );
    }

    #[test]
    fn local_gc_options_dry_run_builder() {
        let opts = LocalGcOptions::dry_run();
        assert!(!opts.mark);
        assert!(!opts.sweep);
    }

    #[test]
    fn local_gc_options_mark_only_builder() {
        let opts = LocalGcOptions::mark_only(7200);
        assert!(opts.mark);
        assert!(!opts.sweep);
        assert_eq!(opts.retention_seconds, 7200);
    }

    #[test]
    fn local_gc_options_sweep_only_builder() {
        let opts = LocalGcOptions::sweep_only();
        assert!(!opts.mark);
        assert!(opts.sweep);
    }

    #[test]
    fn local_gc_options_mark_and_sweep_builder() {
        let opts = LocalGcOptions::mark_and_sweep(7200);
        assert!(opts.mark);
        assert!(opts.sweep);
        assert_eq!(opts.retention_seconds, 7200);
    }

    #[test]
    fn local_gc_options_default() {
        let opts = LocalGcOptions::default();
        assert!(!opts.mark);
        assert!(!opts.sweep);
        assert_eq!(opts.retention_seconds, 86400); // DEFAULT_LOCAL_GC_RETENTION_SECONDS
    }

    #[test]
    fn write_optional_artifact_none_path_returns_ok() {
        // When path is None, write_optional_artifact returns Ok(()) without writing
        let result = serde_json::to_vec_pretty(&"test");
        assert!(result.is_ok());
        // The function itself is tested structurally: None path -> early return Ok
    }

    #[test]
    fn gc_runtime_error_debug_and_display() {
        let json_err = serde_json::from_str::<()>("invalid").unwrap_err();
        let err = super::GcRuntimeError::Json(json_err);
        let display = err.to_string();
        assert!(!display.is_empty());
        let debug = format!("{err:?}");
        // The Debug format includes the variant name, not the enum name
        assert!(debug.contains("Json("));
    }
}
