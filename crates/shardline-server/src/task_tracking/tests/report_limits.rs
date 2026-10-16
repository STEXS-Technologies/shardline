//! Optional report storage limits must never invent successful completion.
use super::*;

#[test]
fn automatic_report_limit_preserves_unresolved_work_and_existing_evidence() {
    let root = tempfile::tempdir().unwrap();
    let configured = config(root.path());
    let store = TaskEvidenceStore::from_config(&configured)
        .unwrap()
        .require_metadata_publication();
    let deployment = deployment_fingerprint(&configured).unwrap();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    let payload = root.path().join("user-payload");
    std::fs::write(&payload, b"preserved application bytes").unwrap();
    tracker
        .begin(
            "maximum-report",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, &vec![b'a'; 65536])
        .unwrap();
    let receipt = tracker
        .begin(
            "oversized-report",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let oversized_id = store
        .summaries(deployment, None, 10)
        .unwrap()
        .into_iter()
        .find(|run| run.operation == "oversized-report")
        .unwrap()
        .id;
    assert!(receipt.finish(true, &vec![b'b'; 65537]).is_err());
    // This synchronous fixture runs Drop's reconciliation publication before
    // inspection. A failed optional report cannot become completed history.
    let history = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history
            .iter()
            .find(|run| run.operation == "maximum-report")
            .unwrap()
            .state,
        TaskRunState::Completed
    );
    assert_eq!(
        history
            .iter()
            .find(|run| run.id == oversized_id)
            .unwrap()
            .state,
        TaskRunState::NeedsReconciliation
    );
    let report = store
        .embedded_report(deployment, None, &oversized_id)
        .unwrap()
        .unwrap();
    assert_eq!(report["report_available"], false);
    assert!(report["report"].is_null());
    assert!(
        store
            .read(
                &format!(
                    "runs/{oversized_id}/artifacts/{}",
                    hex::encode(penelope::core::task::Digest::of(&vec![b'b'; 65537]).0)
                ),
                65537
            )
            .unwrap()
            .is_none()
    );
    assert_eq!(
        std::fs::read(payload).unwrap(),
        b"preserved application bytes"
    );
    assert!(
        store
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .is_empty()
    );
}
