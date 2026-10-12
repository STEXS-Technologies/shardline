#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use shardline_server::{
    ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskRunState, deployment_fingerprint},
};
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread")]
async fn unusable_private_tracking_preserves_the_original_command_and_operator_file() {
    let root = tempfile::tempdir().unwrap();
    let private_state = root.path().join(".shardline-maintenance");
    std::fs::write(&private_state, b"existing operator file").unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let configured = config.clone();
    let work_root = root.path().to_path_buf();
    let output = tokio::task::spawn_blocking(move || {
        let prepared = PreparedMaintenance {
            operation: MaintenanceOperation::IndexRebuild {
                root: work_root.clone(),
            },
            configuration: super::super::configuration::Configuration {
                config: configured,
                command: CliCommand::IndexRebuild {
                    root: Some(work_root.clone()),
                },
                config_path: None,
                migration: None,
            },
        };
        run(
            &prepared,
            vec![
                OsString::from("shardline"),
                OsString::from("index"),
                OsString::from("rebuild"),
                OsString::from("--root"),
                work_root.into_os_string(),
            ],
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert!(output.evidence_published);
    assert_eq!(
        std::fs::read(&private_state).unwrap(),
        b"existing operator file"
    );
    let history = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .summaries(deployment_fingerprint(&config).unwrap(), None, 10)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, TaskRunState::Completed);
}
