#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use shardline_reliability::MaintenanceArtifactKind;
use std::num::NonZeroUsize;

fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

#[test]
fn embedded_tasks_publish_verified_success_and_truthful_failure() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "test-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let before = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, TaskRunState::Running);
    assert_eq!(before[0].execution, TaskExecutionMode::Embedded);
    receipt
        .finish(true, b"inspected: 4; upgraded: 2; failures: 0")
        .unwrap();
    let failed = tracker
        .begin(
            "incomplete-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    failed.finish(false, b"failures: 1").unwrap();
    let after = store.summaries(deployment, None, 10).unwrap();
    assert!(
        after
            .iter()
            .any(|task| task.state == TaskRunState::Completed && task.operation == "test-sweep")
    );
    assert!(
        after
            .iter()
            .any(|task| task.state == TaskRunState::NeedsReconciliation
                && task.operation == "incomplete-sweep")
    );
    assert!(
        store
            .summaries(
                penelope_core::task::Digest::of(b"other-deployment"),
                None,
                10
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn background_report_tampering_and_record_gaps_are_visible() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "test-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    receipt.finish(true, b"report").unwrap();
    let summary = store.summaries(deployment, None, 10).unwrap();
    let run = &summary[0].id;
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/runs")
            .join(run)
            .join("artifacts")
            .join(hex::encode(penelope_core::task::Digest::of(b"report").0)),
        b"edited",
    )
    .unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Corrupt
    );
    std::fs::remove_file(
        root.path()
            .join("chunks/_shardline_tasks/v1/runs")
            .join(run)
            .join("records/0000000001"),
    )
    .unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Corrupt
    );
}

#[test]
fn dropping_an_unfinished_background_job_never_leaves_a_live_success_claim() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "interrupted",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    drop(receipt);
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::NeedsReconciliation
    );
}

#[test]
fn evidence_retention_removes_only_verified_finished_runs() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    tracker
        .begin(
            "finished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"complete")
        .unwrap();
    let unfinished = tracker
        .begin(
            "unfinished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let corrupt = tracker
        .begin(
            "corrupt",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    corrupt.finish(true, b"original").unwrap();
    let corrupt_id = store
        .summaries(deployment, None, 10)
        .unwrap()
        .into_iter()
        .find(|summary| summary.operation == "corrupt")
        .unwrap()
        .id;
    std::fs::write(
        root.path().join(format!(
            "chunks/_shardline_tasks/v1/runs/{corrupt_id}/artifacts/{}",
            hex::encode(penelope_core::task::Digest::of(b"original").0)
        )),
        b"tampered",
    )
    .unwrap();
    assert_eq!(
        store
            .prune_completed(deployment, shardline_protocol::unix_now_seconds_lossy())
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .prune_completed(
                deployment,
                shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60)
            )
            .unwrap(),
        1
    );
    let remaining = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|task| task.operation != "finished"));
    drop(unfinished);
}

#[test]
fn credential_source_changes_do_not_change_a_shared_deployment_target() {
    use shardline_protocol::SecretString;
    let root = tempfile::tempdir().unwrap();
    let s3 = shardline_storage::S3ObjectStoreConfig::new("bucket".to_owned(), "region".to_owned());
    let iam =
        config(root.path()).with_object_storage(crate::ObjectStorageAdapter::S3, Some(s3.clone()));
    let explicit = config(root.path()).with_object_storage(
        crate::ObjectStorageAdapter::S3,
        Some(s3.with_credentials(
            Some(SecretString::new("access".to_owned())),
            Some(SecretString::new("secret".to_owned())),
            None,
        )),
    );
    assert_eq!(
        deployment_fingerprint(&iam).unwrap(),
        deployment_fingerprint(&explicit).unwrap()
    );
}

#[test]
fn postgres_password_rotation_preserves_deployment_identity() {
    let root = tempfile::tempdir().unwrap();
    let initial = config(root.path())
        .with_index_postgres_url("postgres://service:old@localhost/shardline".to_owned())
        .unwrap();
    let rotated = config(root.path())
        .with_index_postgres_url("postgres://service:new@localhost/shardline".to_owned())
        .unwrap();
    let other = config(root.path())
        .with_index_postgres_url("postgres://service:new@localhost/another".to_owned())
        .unwrap();
    assert_eq!(
        deployment_fingerprint(&initial).unwrap(),
        deployment_fingerprint(&rotated).unwrap()
    );
    assert_ne!(
        deployment_fingerprint(&initial).unwrap(),
        deployment_fingerprint(&other).unwrap()
    );
}

#[tokio::test]
async fn normal_router_starts_tracker_and_exposes_authenticated_job_status() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path())
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"test-admin-token".to_vec())
        .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .header("Authorization", "Bearer test-admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["supervision"], "embedded");
    let jobs = body["tasks"].as_array().unwrap();
    assert!(
        jobs.iter()
            .any(|job| job["operation"] == "lfs-startup-session-sweep"
                && job["state"] == "completed"
                && job["execution"] == "embedded"
                && job["owner"]["pid"] == std::process::id())
    );
}
