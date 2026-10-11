#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;
use shardline_server::{ServerConfig, task_tracking::TaskRunState};
use std::{fs, num::NonZeroUsize, path::Path};

fn config(root: &Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

#[test]
fn shared_receipts_verify_native_success_failure_and_materialized_tampering() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let operation = MaintenanceOperation::Fsck {
        root: root.path().to_owned(),
    };
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    for exit_code in [0_u8, 1, 2] {
        let id = format!("shared-inline-{exit_code}");
        let (input, definition, scope) =
            super::super::inline::prepare(&config, &id, &operation, vec![b"fsck".to_vec()])
                .unwrap();
        let mut receipt =
            Receipt::begin(&operation, &id, &config, &input, definition, scope).unwrap();
        receipt.finish(operation.kind(), exit_code).unwrap();
        let tasks = store.summaries(deployment, None, 10).unwrap();
        let task = tasks.iter().find(|task| task.id == id).unwrap();
        assert_eq!(
            task.state,
            if exit_code == 0 {
                TaskRunState::Completed
            } else {
                TaskRunState::Failed
            }
        );
    }
    // No signing key, SQLite journal or scratch directory exists beside the
    // shared object adapter. Its native body verification detects later damage.
    assert!(!root.path().join(".shardline-maintenance").exists());
    let artifacts = root
        .path()
        .join("chunks/_shardline_tasks/v1/runs/shared-inline-0/artifacts");
    for entry in fs::read_dir(artifacts).unwrap() {
        fs::write(entry.unwrap().path(), b"damaged materialized result").unwrap();
    }
    let tasks = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.id == "shared-inline-0")
            .unwrap()
            .state,
        TaskRunState::Corrupt
    );
}

#[test]
fn shared_completion_write_failure_never_claims_a_finished_task() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let operation = MaintenanceOperation::Fsck {
        root: root.path().to_owned(),
    };
    let id = "shared-inline-incomplete";
    let (input, definition, scope) =
        super::super::inline::prepare(&config, id, &operation, vec![b"fsck".to_vec()]).unwrap();
    let mut receipt = Receipt::begin(&operation, id, &config, &input, definition, scope).unwrap();
    let report = b"maintenance.success: true\n{\"exit_code\":0,\"schema\":1}";
    fs::create_dir(root.path().join(format!(
        "chunks/_shardline_tasks/v1/runs/{id}/artifacts/{}",
        hex::encode(Digest::of(report).0),
    )))
    .unwrap();
    assert!(receipt.finish(operation.kind(), 0).is_err());
    assert!(
        receipt
            .store
            .read(&format!("runs/{id}/records/0000000004"), 65536)
            .unwrap()
            .is_none()
    );
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let tasks = receipt.store.summaries(deployment, None, 10).unwrap();
    assert!(!matches!(
        tasks[0].state,
        TaskRunState::Completed | TaskRunState::Failed
    ));
}
