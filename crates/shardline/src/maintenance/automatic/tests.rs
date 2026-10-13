#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use shardline_server::{
    ServerConfig,
    task_tracking::{TaskEvidenceStore, TaskRunState, deployment_fingerprint},
};
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread")]
async fn full_private_archive_runs_fresh_work_with_shared_history_and_preserves_evidence() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join(".shardline-maintenance");
    super::super::artifact::private_directory(&state).unwrap();
    for id in 0..4096 {
        let directory = state.join(format!("{id:064x}"));
        super::super::artifact::private_directory(&directory).unwrap();
        std::fs::write(
            directory.join("retained-evidence"),
            b"preserve unfinished work",
        )
        .unwrap();
    }
    let command = CliCommand::IndexRebuild {
        root: Some(root.path().to_owned()),
    };
    let output = tokio::task::spawn_blocking(move || {
        let prepared = prepare(&command).unwrap().unwrap();
        run(
            &prepared,
            vec![OsString::from("shardline"), OsString::from("index-rebuild")],
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(output.success);
    assert_eq!(output.exit_code, Some(0));
    assert!(output.evidence_published);
    assert_eq!(
        std::fs::read_dir(&state)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| entry.file_name().to_str().is_some_and(
                |name| name.len() == 64 && name.bytes().all(|byte| byte.is_ascii_hexdigit())
            ))
            .count(),
        4096
    );
    for id in 0..4096 {
        assert_eq!(
            std::fs::read(state.join(format!("{id:064x}")).join("retained-evidence")).unwrap(),
            b"preserve unfinished work"
        );
    }
    let configured = crate::config::load_server_config(Some(root.path()), None).unwrap();
    let history = TaskEvidenceStore::from_config(&configured)
        .unwrap()
        .summaries(deployment_fingerprint(&configured).unwrap(), None, 10)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history.first().unwrap().state, TaskRunState::Completed);
    assert_eq!(history.first().unwrap().owner.pid, std::process::id());
}

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

#[tokio::test(flavor = "multi_thread")]
async fn full_archive_still_exports_completed_history_and_restores_admission() {
    let root = tempfile::tempdir().unwrap();
    let work_root = root.path().to_owned();
    let state = work_root.join(".shardline-maintenance");
    let work_state = state.clone();
    tokio::task::spawn_blocking(move || {
        let command = CliCommand::IndexRebuild {
            root: Some(work_root.clone()),
        };
        let configured = crate::config::load_server_config(Some(&work_root), None).unwrap();
        let configuration = super::super::configuration::Configuration {
            config: configured.clone(),
            command: command.clone(),
            config_path: None,
            migration: None,
        };
        let id = "completed-before-capacity";
        let operation = MaintenanceOperation::IndexRebuild {
            root: work_root.clone(),
        };
        let output = super::super::configuration::scope(configuration, || {
            super::super::inline::run(
                &work_state,
                id,
                &operation,
                vec![b"index-rebuild".to_vec()],
                vec![OsString::from("shardline"), OsString::from("index-rebuild")],
            )
        })
        .unwrap();
        assert!(output.success && output.evidence_published);
        let name = hex::encode(penelope_core::task::Digest::of(id.as_bytes()).0);
        let directory = work_state.join(&name);
        let identity_path = directory.join("identity.json");
        let mut identity: shardline_server::task_tracking::TaskRunIdentity =
            serde_json::from_slice(&std::fs::read(&identity_path).unwrap()).unwrap();
        // Model a completed predecessor whose process is gone. Keep its exact
        // private/public identity aligned; native records and proofs are unchanged.
        identity.owner.pid = u32::MAX;
        assert_eq!(identity.owner.alive_here(), Some(false));
        let bytes = serde_json::to_vec(&identity).unwrap();
        crate::write_output_bytes(&identity_path, &bytes, false).unwrap();
        let connection = rusqlite::Connection::open(work_root.join("metadata.sqlite3")).unwrap();
        assert_eq!(
            connection
                .execute(
                    "UPDATE shardline_task_artifacts_v1 SET payload=?1 WHERE artifact_key=?2",
                    rusqlite::params![bytes, format!("_shardline_tasks/v1/manifests/{id}")],
                )
                .unwrap(),
            1
        );
        for number in 0..4095 {
            let retained = work_state.join(format!("{number:064x}"));
            super::super::artifact::private_directory(&retained).unwrap();
            std::fs::write(
                retained.join("retained-evidence"),
                b"preserve unfinished work",
            )
            .unwrap();
        }
        // Select the verified predecessor on the next fair export page.
        let cursor = format!("{:064x}", 4095);
        assert!(name > cursor);
        crate::write_output_bytes(&work_state.join(".export-cursor"), cursor.as_bytes(), false)
            .unwrap();
        for expected in [2, 3] {
            let prepared = prepare(&command).unwrap().unwrap();
            let fresh = run(
                &prepared,
                vec![OsString::from("shardline"), OsString::from("index-rebuild")],
            )
            .unwrap();
            assert!(fresh.success && fresh.evidence_published);
            assert_eq!(fresh.exit_code, Some(0));
            let store = TaskEvidenceStore::from_config(&configured).unwrap();
            let history = store
                .summaries(deployment_fingerprint(&configured).unwrap(), None, 10)
                .unwrap();
            assert_eq!(history.len(), expected);
            assert!(
                history
                    .iter()
                    .all(|run| run.state == TaskRunState::Completed)
            );
            assert!(!directory.exists());
        }
        for number in 0..4095 {
            assert_eq!(
                std::fs::read(
                    work_state
                        .join(format!("{number:064x}"))
                        .join("retained-evidence")
                )
                .unwrap(),
                b"preserve unfinished work"
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn full_retired_queue_with_cleanup_faults_keeps_fresh_work_available() {
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join(".shardline-maintenance");
    let retired = state.join(super::super::cleanup::RETIRED);
    super::super::artifact::private_directory(&retired).unwrap();
    let outside = root.path().join("outside-evidence");
    std::fs::write(&outside, b"operator evidence").unwrap();
    // Invalid queued symlinks consume capacity and make cleanup fail safely,
    // including when the test runs as root. They must never be followed.
    for id in 0..4096 {
        std::os::unix::fs::symlink(&outside, retired.join(format!("{id:064x}"))).unwrap();
    }
    let command = CliCommand::IndexRebuild {
        root: Some(root.path().to_owned()),
    };
    let output = tokio::task::spawn_blocking(move || {
        let prepared = prepare(&command).unwrap().unwrap();
        run(
            &prepared,
            vec![OsString::from("shardline"), OsString::from("index-rebuild")],
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(output.success && output.evidence_published);
    assert_eq!(output.exit_code, Some(0));
    for id in 0..4096 {
        assert!(
            std::fs::symlink_metadata(retired.join(format!("{id:064x}")))
                .unwrap()
                .is_symlink()
        );
    }
    assert_eq!(std::fs::read(&outside).unwrap(), b"operator evidence");
    let configured = crate::config::load_server_config(Some(root.path()), None).unwrap();
    let history = TaskEvidenceStore::from_config(&configured)
        .unwrap()
        .summaries(deployment_fingerprint(&configured).unwrap(), None, 10)
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history.first().unwrap().state, TaskRunState::Completed);
    assert_eq!(history.first().unwrap().owner.pid, std::process::id());
}
