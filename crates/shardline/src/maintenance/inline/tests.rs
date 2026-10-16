#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use shardline_server::{
    ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskRunState, deployment_fingerprint},
};
use std::{num::NonZeroUsize, os::unix::fs::PermissionsExt};

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_private_journal_does_not_stop_the_original_command() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("private-state");
    let id = "admission-unavailable";
    let directory = state.join(hex::encode(Digest::of(id.as_bytes()).0));
    artifact::private_directory(&directory).unwrap();
    let journal = directory.join("tasks.sqlite");
    std::fs::write(&journal, b"corrupt existing journal").unwrap();
    std::fs::set_permissions(&journal, std::fs::Permissions::from_mode(0o600)).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let configured = config.clone();
    let work_root = root.path().to_path_buf();
    let task_state = state.clone();
    let output = tokio::task::spawn_blocking(move || {
        let operation = MaintenanceOperation::IndexRebuild {
            root: work_root.clone(),
        };
        super::super::configuration::scope(
            super::super::configuration::Configuration {
                config: configured,
                command: crate::CliCommand::IndexRebuild {
                    root: Some(work_root.clone()),
                },
                config_path: None,
                migration: None,
            },
            || {
                run(
                    &task_state,
                    id,
                    &operation,
                    vec![b"index rebuild".to_vec()],
                    vec![
                        OsString::from("shardline"),
                        OsString::from("index"),
                        OsString::from("rebuild"),
                        OsString::from("--root"),
                        work_root.into_os_string(),
                    ],
                )
            },
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert!(output.evidence_published);
    assert_eq!(
        std::fs::read(&journal).unwrap(),
        b"corrupt existing journal"
    );
    let history = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .summaries(deployment_fingerprint(&config).unwrap(), None, 10)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, TaskRunState::Completed);
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_completion_artifact_preserves_success_without_claiming_completion() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("private-state");
    let id = "completion-unavailable";
    let directory = state.join(hex::encode(Digest::of(id.as_bytes()).0));
    let artifacts = directory.join("artifacts");
    artifact::private_directory(&artifacts).unwrap();
    let report = format!(
        "maintenance.success: true\n{}",
        serde_json::to_string(&serde_json::json!({"schema": 1, "exit_code": 0})).unwrap()
    );
    let report_path = artifacts.join(hex::encode(Digest::of(report.as_bytes()).0));
    std::fs::write(&report_path, b"corrupt existing report").unwrap();
    std::fs::set_permissions(&report_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    );
    let configured = config.clone();
    let work_root = root.path().to_path_buf();
    let task_state = state.clone();
    let output = tokio::task::spawn_blocking(move || {
        let operation = MaintenanceOperation::IndexRebuild {
            root: work_root.clone(),
        };
        super::super::configuration::scope(
            super::super::configuration::Configuration {
                config: configured,
                command: crate::CliCommand::IndexRebuild {
                    root: Some(work_root.clone()),
                },
                config_path: None,
                migration: None,
            },
            || {
                run(
                    &task_state,
                    id,
                    &operation,
                    vec![b"index rebuild".to_vec()],
                    vec![
                        OsString::from("shardline"),
                        OsString::from("index"),
                        OsString::from("rebuild"),
                        OsString::from("--root"),
                        work_root.into_os_string(),
                    ],
                )
            },
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert!(!output.evidence_published);
    assert_eq!(
        std::fs::read(&report_path).unwrap(),
        b"corrupt existing report"
    );
    assert!(directory.join("tasks.sqlite").is_file());
    let history = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .summaries(deployment_fingerprint(&config).unwrap(), None, 10)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_ne!(history[0].state, TaskRunState::Completed);
}
