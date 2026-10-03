use std::{
    io::{BufWriter, Error as IoError, Write},
    path::Path,
};

use shardline_server::{
    BackupManifestReport, ServerConfigError, ServerError,
    write_backup_manifest as write_server_backup_manifest,
};
use thiserror::Error;

use crate::{
    config::load_server_config,
    local_output::{AtomicOutputFile, validate_deployment_output},
};

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
    let config = load_server_config(root, None)?;
    validate_deployment_output(&config, output)?;
    let mut output_file = AtomicOutputFile::create(output, false)?;
    let report = {
        let mut writer = BufWriter::new(&mut output_file);
        let report = write_server_backup_manifest(config, &mut writer).await?;
        writer.flush()?;
        report
    };
    output_file.commit()?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use shardline_server::BackupManifestReport;

    use super::{BackupRuntimeError, run_backup_manifest};

    #[test]
    fn backup_manifest_report_new_defaults() {
        let report = BackupManifestReport {
            manifest_version: 1,
            metadata_backend: "postgres".to_owned(),
            object_backend: "fs".to_owned(),
            object_count: 0,
            object_bytes: 0,
            latest_records: 0,
            version_records: 0,
            reconstruction_rows: 0,
            dedupe_shard_mappings: 0,
            quarantine_candidates: 0,
            retention_holds: 0,
            webhook_deliveries: 0,
            provider_repository_states: 0,
        };
        assert_eq!(report.manifest_version, 1);
        assert_eq!(report.metadata_backend, "postgres");
        assert_eq!(report.object_backend, "fs");
    }

    #[test]
    fn backup_manifest_report_with_counts() {
        let report = BackupManifestReport {
            manifest_version: 1,
            metadata_backend: "local".to_owned(),
            object_backend: "s3".to_owned(),
            object_count: 100,
            object_bytes: 1_000_000,
            latest_records: 10,
            version_records: 50,
            reconstruction_rows: 5,
            dedupe_shard_mappings: 20,
            quarantine_candidates: 3,
            retention_holds: 2,
            webhook_deliveries: 15,
            provider_repository_states: 1,
        };
        assert_eq!(report.object_count, 100);
        assert_eq!(report.object_bytes, 1_000_000);
        assert_eq!(report.latest_records, 10);
        assert_eq!(report.version_records, 50);
        assert_eq!(report.reconstruction_rows, 5);
        assert_eq!(report.dedupe_shard_mappings, 20);
        assert_eq!(report.quarantine_candidates, 3);
        assert_eq!(report.retention_holds, 2);
        assert_eq!(report.webhook_deliveries, 15);
        assert_eq!(report.provider_repository_states, 1);
    }

    #[test]
    fn backup_manifest_report_serializable() {
        let report = BackupManifestReport {
            manifest_version: 1,
            metadata_backend: "test".to_owned(),
            object_backend: "test".to_owned(),
            object_count: 5,
            object_bytes: 100,
            latest_records: 1,
            version_records: 2,
            reconstruction_rows: 3,
            dedupe_shard_mappings: 4,
            quarantine_candidates: 5,
            retention_holds: 6,
            webhook_deliveries: 7,
            provider_repository_states: 8,
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("\"manifest_version\":1"));
        assert!(json.contains("\"metadata_backend\":\"test\""));
        assert!(json.contains("\"object_backend\":\"test\""));
    }

    #[test]
    fn backup_runtime_error_display() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = BackupRuntimeError::Io(io_err);
        let msg = err.to_string();
        assert!(msg.contains("backup manifest output file operation failed"));
    }

    #[test]
    fn backup_runtime_error_debug() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "test");
        let err = BackupRuntimeError::Io(io_err);
        let debug = format!("{err:?}");
        // The Debug format includes the variant name, not the enum name
        assert!(debug.contains("Io(") || debug.starts_with("Io("));
    }

    #[test]
    fn backup_runtime_error_config_display() {
        use shardline_server::ServerConfigError;
        let config_err = ServerConfigError::InvalidServerRole;
        let err = BackupRuntimeError::Config(config_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn backup_runtime_error_server_display() {
        use shardline_server::ServerError;
        let server_err = ServerError::NotFound;
        let err = BackupRuntimeError::Server(server_err);
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn run_backup_manifest_with_valid_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("manifest.json");
        let result = run_backup_manifest(Some(sandbox.path()), &output).await;
        // On an empty deployment with a valid temp dir, backup should complete
        assert!(
            result.is_ok(),
            "run_backup_manifest should succeed on empty deployment: {result:?}"
        );
        assert!(output.exists(), "manifest output file should be created");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reserved_metadata_destination_preserves_database_bytes() {
        let sandbox = tempfile::tempdir().unwrap();
        run_backup_manifest(Some(sandbox.path()), &sandbox.path().join("manifest.json"))
            .await
            .unwrap();
        let database = sandbox.path().join("metadata.sqlite3");
        let previous = std::fs::read(&database).unwrap();
        assert!(previous.starts_with(b"SQLite format 3"));
        let result = run_backup_manifest(Some(sandbox.path()), &database).await;
        assert!(matches!(result, Err(BackupRuntimeError::Io(_))));
        assert_eq!(std::fs::read(&database).unwrap(), previous);
    }

    #[tokio::test]
    async fn run_backup_manifest_rejects_missing_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("manifest.json");
        let result =
            run_backup_manifest(Some(Path::new("/nonexistent-shardline-test-root")), &output).await;
        assert!(result.is_err());
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn streamed_manifest_matches_server_output() {
        let sandbox = tempfile::tempdir().unwrap();
        std::fs::create_dir(sandbox.path().join("chunks")).unwrap();
        std::fs::write(sandbox.path().join("chunks/object"), b"payload").unwrap();
        let config = crate::config::load_server_config(Some(sandbox.path()), None).unwrap();
        let mut expected = Vec::new();
        let expected_report = shardline_server::write_backup_manifest(config, &mut expected)
            .await
            .unwrap();
        let output = sandbox.path().join("manifest.json");
        let report = run_backup_manifest(Some(sandbox.path()), &output)
            .await
            .unwrap();
        assert_eq!(report, expected_report);
        assert_eq!(std::fs::read(output).unwrap(), expected);
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn failed_inventory_preserves_previous_manifest_and_cleans_temporary_output() {
        use std::os::unix::ffi::OsStringExt;
        let sandbox = tempfile::tempdir().unwrap();
        let output_dir = tempfile::tempdir().unwrap();
        let output = output_dir.path().join("manifest.json");
        std::fs::write(&output, b"previous manifest").unwrap();
        std::fs::create_dir(sandbox.path().join("chunks")).unwrap();
        let invalid_key = std::ffi::OsString::from_vec(vec![0xff]);
        std::fs::write(sandbox.path().join("chunks").join(invalid_key), b"payload").unwrap();
        let result = run_backup_manifest(Some(sandbox.path()), &output).await;
        assert!(matches!(result, Err(BackupRuntimeError::Server(_))));
        assert_eq!(std::fs::read(&output).unwrap(), b"previous manifest");
        assert_eq!(std::fs::read_dir(output_dir.path()).unwrap().count(), 1);
    }
    #[tokio::test]
    async fn manifest_output_inside_object_store_is_rejected_before_creation() {
        let sandbox = tempfile::tempdir().unwrap();
        let objects = sandbox.path().join("chunks");
        std::fs::create_dir(&objects).unwrap();
        let result =
            run_backup_manifest(Some(sandbox.path()), &objects.join("manifest.json")).await;
        assert!(matches!(result, Err(BackupRuntimeError::Io(_))));
        assert_eq!(std::fs::read_dir(objects).unwrap().count(), 0);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn aliased_object_store_destination_preserves_existing_target() {
        let sandbox = tempfile::tempdir().unwrap();
        let objects = sandbox.path().join("chunks");
        std::fs::create_dir(&objects).unwrap();
        std::fs::write(objects.join("manifest.json"), b"previous").unwrap();
        let alias = sandbox.path().join("alias");
        std::os::unix::fs::symlink(&objects, &alias).unwrap();
        let result = run_backup_manifest(Some(sandbox.path()), &alias.join("manifest.json")).await;
        assert!(matches!(result, Err(BackupRuntimeError::Io(_))));
        assert_eq!(
            std::fs::read(objects.join("manifest.json")).unwrap(),
            b"previous"
        );
        assert_eq!(std::fs::read_dir(objects).unwrap().count(), 1);
    }
}
