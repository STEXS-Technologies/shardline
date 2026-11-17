use std::{io::Error as IoError, path::Path};

use serde::Serialize;
use serde_json::{Error as JsonError, to_vec_pretty};
use shardline_server::{
    LocalGcDiagnostics, LocalGcOptions, LocalGcReport, ServerConfigError, ServerError,
    run_gc_diagnostics as run_server_gc_diagnostics,
};
use thiserror::Error;

use crate::{
    config::load_server_config,
    local_output::{validate_deployment_outputs, write_output_bytes},
};

pub use shardline_server::MINIMUM_GC_RETENTION_SECONDS;

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
    let config = load_server_config(root, None)?;
    let outputs = [retention_report_path, orphan_inventory_path]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    validate_deployment_outputs(&config, &outputs)?;
    let options = LocalGcOptions {
        mark,
        sweep,
        // Enforce a minimum retention to prevent data loss from the TOCTOU
        // race between the GC mark phase (index scan) and the sweep phase
        // (delete of orphaned chunks).  Concurrent uploads that have written
        // chunks on disk but not yet committed file records need a grace period.
        retention_seconds: retention_seconds.max(MINIMUM_GC_RETENTION_SECONDS),
        // GC-time enforcement of the F-75 per-repo revision cap: thread the
        // configured value into the run so the GC runner prunes over-cap
        // revision-registry rows (oldest first) for every repository.
        max_revisions_per_repo: Some(config.max_revisions_per_repo().get()),
    };
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
    use std::path::Path;

    use shardline_server::LocalGcOptions;

    use super::MINIMUM_GC_RETENTION_SECONDS;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gc_enforces_minimum_retention_on_quarantine_deadlines() {
        assert_eq!(MINIMUM_GC_RETENTION_SECONDS, 3600);
        for (requested, expected) in [(0, 3600), (3599, 3600), (3600, 3600), (7200, 7200)] {
            let root = tempfile::tempdir().unwrap();
            let hash = "de".repeat(32);
            let directory = root.path().join("chunks/de");
            std::fs::create_dir_all(&directory).unwrap();
            let object = directory.join(hash);
            std::fs::write(&object, b"orphan").unwrap();
            let diagnostics =
                super::run_gc_diagnostics(Some(root.path()), true, true, requested, None, None)
                    .await
                    .unwrap();
            assert_eq!(diagnostics.report.new_quarantine_candidates, 1);
            assert_eq!(diagnostics.report.deleted_chunks, 0);
            assert_eq!(diagnostics.retention_report.len(), 1);
            let candidate = &diagnostics.retention_report[0];
            assert_eq!(
                candidate.delete_after_unix_seconds
                    - candidate.first_seen_unreachable_at_unix_seconds,
                expected,
                "requested retention: {requested}",
            );
            assert!(!candidate.expired);
            assert_eq!(std::fs::read(object).unwrap(), b"orphan");
        }
    }

    #[test]
    fn local_gc_options_builders_preserve_modes_and_retention() {
        for (options, mark, sweep, retention, mode) in [
            (LocalGcOptions::default(), false, false, 86400, "dry-run"),
            (LocalGcOptions::dry_run(), false, false, 86400, "dry-run"),
            (LocalGcOptions::mark_only(0), true, false, 0, "mark"),
            (LocalGcOptions::mark_only(7200), true, false, 7200, "mark"),
            (LocalGcOptions::sweep_only(), false, true, 86400, "sweep"),
            (
                LocalGcOptions::mark_and_sweep(7200),
                true,
                true,
                7200,
                "mark-and-sweep",
            ),
        ] {
            assert_eq!(options.mode_name(), mode);
            assert_eq!(options.mark, mark, "{mode}");
            assert_eq!(options.sweep, sweep, "{mode}");
            assert_eq!(options.retention_seconds, retention, "{mode}");
            assert_eq!(options.max_revisions_per_repo, None, "{mode}");
        }
    }

    #[test]
    fn write_optional_artifact_none_path_returns_ok() {
        // When path is None, write_optional_artifact returns Ok(()) without writing
        let result = super::write_optional_artifact(None::<&std::path::Path>, &"test");
        assert!(result.is_ok());
    }

    #[test]
    fn write_optional_artifact_writes_to_some_path() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("artifact.json");
        let data = serde_json::json!({"key": "value", "count": 42});
        let result = super::write_optional_artifact(Some(&path), &data);
        assert!(result.is_ok());
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"key\""));
        assert!(content.contains("\"count\""));
        assert!(content.contains("42"));
    }

    #[test]
    fn write_optional_artifact_retention_report_round_trip() {
        let sandbox = tempfile::tempdir().unwrap();
        let path = sandbox.path().join("retention.json");
        let report = serde_json::json!({
            "quarantined_objects": [
                {"object_key": "aa/hash1", "added_at": 1000},
                {"object_key": "bb/hash2", "added_at": 2000}
            ],
            "total_orphan_bytes": 5000
        });
        let result = super::write_optional_artifact(Some(&path), &report);
        assert!(result.is_ok());
        let content = std::fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["total_orphan_bytes"], serde_json::json!(5000));
        assert_eq!(parsed["quarantined_objects"].as_array().unwrap().len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserved_exports_preserve_database_and_objects_before_gc() {
        let sandbox = tempfile::tempdir().unwrap();
        crate::backup::run_backup_manifest(
            Some(sandbox.path()),
            &sandbox.path().join("manifest.json"),
        )
        .await
        .unwrap();
        let database = sandbox.path().join("metadata.sqlite3");
        let database_bytes = std::fs::read(&database).unwrap();
        let object = sandbox.path().join("chunks/aa/object");
        std::fs::create_dir_all(object.parent().unwrap()).unwrap();
        std::fs::write(&object, b"preserved object").unwrap();
        for path in [&database, &object] {
            for retention_export in [true, false] {
                let result = super::run_gc_diagnostics(
                    Some(sandbox.path()),
                    true,
                    true,
                    3600,
                    retention_export.then_some(path.as_path()),
                    (!retention_export).then_some(path.as_path()),
                )
                .await;
                assert!(matches!(result, Err(super::GcRuntimeError::Io(_))));
                assert_eq!(std::fs::read(&database).unwrap(), database_bytes);
                assert_eq!(std::fs::read(&object).unwrap(), b"preserved object");
            }
        }
        // A bad second export must be rejected before creating the first report
        // or running the collector.
        let report = sandbox.path().join("reports/retention.json");
        let result = super::run_gc_diagnostics(
            Some(sandbox.path()),
            true,
            true,
            3600,
            Some(&report),
            Some(&database),
        )
        .await;
        assert!(matches!(result, Err(super::GcRuntimeError::Io(_))));
        assert!(!report.parent().unwrap().exists());
        assert_eq!(std::fs::read(&database).unwrap(), database_bytes);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn overlapping_exports_are_rejected_before_gc_or_output_creation() {
        for (retention_name, orphan_name) in [
            ("reports/shared.json", "reports/shared.json"),
            ("reports/shared.json", "reports/../reports/shared.json"),
            ("reports", "reports/orphans.json"),
            ("reports/retention.json", "reports"),
        ] {
            let sandbox = tempfile::tempdir().unwrap();
            let root = sandbox.path();
            let retention = root.join(retention_name);
            let orphan = root.join(orphan_name);
            let result = super::run_gc_diagnostics(
                Some(root),
                true,
                true,
                3600,
                Some(&retention),
                Some(&orphan),
            )
            .await;
            assert!(matches!(result, Err(super::GcRuntimeError::Io(error))
                if error.kind() == std::io::ErrorKind::InvalidInput));
            assert!(!root.join("metadata.sqlite3").exists());
            assert!(!root.join("reports").exists());
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn ordinary_reports_under_deployment_root_are_supported() {
        let sandbox = tempfile::tempdir().unwrap();
        let retention = sandbox.path().join("reports/retention.json");
        let orphans = sandbox.path().join("reports/orphans.json");
        super::run_gc_diagnostics(
            Some(sandbox.path()),
            false,
            false,
            3600,
            Some(&retention),
            Some(&orphans),
        )
        .await
        .unwrap();
        for report in [retention, orphans] {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(report).unwrap()).unwrap();
            assert!(value.is_array());
        }
    }

    #[tokio::test]
    async fn run_gc_rejects_missing_root() {
        let result = super::run_gc(
            Some(Path::new("/nonexistent-shardline-test-root")),
            true,
            true,
            3600,
            None,
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_gc_diagnostics_rejects_missing_root() {
        let result = super::run_gc_diagnostics(
            Some(Path::new("/nonexistent-shardline-test-root")),
            true,
            false,
            3600,
            None,
            None,
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn run_gc_diagnostics_rejects_symlinked_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let target = sandbox.path().join("real-root");
        std::fs::create_dir_all(&target).unwrap();
        #[cfg(unix)]
        let link = {
            let link = sandbox.path().join("root-link");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            link
        };
        #[cfg(not(unix))]
        let link = target.clone();
        let result = super::run_gc_diagnostics(Some(&link), false, false, 3600, None, None).await;
        #[cfg(unix)]
        assert!(result.is_err());
    }
}
