use std::{io::Error as IoError, path::Path};

use shardline_server::{
    BackupManifestReport, ServerConfigError, ServerError,
    write_backup_manifest as write_server_backup_manifest,
};
use thiserror::Error;

use crate::{config::load_server_config, local_output::write_output_bytes};

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
    let mut manifest = Vec::new();
    let report = write_server_backup_manifest(config, &mut manifest).await?;
    write_output_bytes(output, &manifest, false)?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use shardline_server::BackupManifestReport;

    use super::{run_backup_manifest, BackupRuntimeError};

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

    #[tokio::test]
    async fn run_backup_manifest_rejects_missing_root() {
        let sandbox = tempfile::tempdir().unwrap();
        let output = sandbox.path().join("manifest.json");
        let result = run_backup_manifest(Some(Path::new("/nonexistent-shardline-test-root")), &output).await;
        assert!(result.is_err());
    }
}
