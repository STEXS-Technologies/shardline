#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use shardline_reliability::MaintenanceArtifactKind;
use std::num::NonZeroUsize;

mod history_pagination;
mod incremental_capacity;
mod legacy_retention_capacity;
mod local_retention;
mod postgres_capacity;
mod postgres_retention;
mod report_limits;
mod startup_recording;

fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

#[test]
fn metadata_only_publication_preserves_legacy_reads_without_new_payload_objects() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let legacy = TaskEvidenceStore::from_objects(object_store_from_config(&config).unwrap());
    legacy
        .publish("existing-history", b"original bytes")
        .unwrap();
    let automatic = legacy.clone().require_metadata_publication();
    assert_eq!(
        automatic.read("existing-history", 1024).unwrap(),
        Some(b"original bytes".to_vec())
    );
    assert!(automatic.publish("new-history", b"new bytes").is_err());
    assert!(automatic.read("new-history", 1024).unwrap().is_none());
    assert_eq!(
        automatic
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .len(),
        1
    );
    // The explicit legacy API keeps its existing publication contract.
    legacy
        .publish("explicit-legacy-history", b"compatible bytes")
        .unwrap();
}

#[test]
fn local_metadata_history_is_verified_without_entering_payload_inventory() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .require_metadata_publication();
    let deployment = deployment_fingerprint(&config).unwrap();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    tracker
        .begin(
            "metadata-run",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"verified report")
        .unwrap();
    assert!(
        store
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .is_empty()
    );
    let reopened = TaskEvidenceStore::from_config(&config).unwrap();
    let history = reopened.summaries(deployment, None, 10).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].state, TaskRunState::Completed);
    assert_eq!(
        reopened
            .embedded_report(deployment, None, &history[0].id)
            .unwrap()
            .unwrap()["report"],
        "verified report"
    );

    // A legacy writer is still visible alongside new evidence after an upgrade.
    EmbeddedTaskTracker::new(store.objects, deployment)
        .begin(
            "legacy-run",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"legacy report")
        .unwrap();
    let history = reopened.summaries(deployment, None, 10).unwrap();
    assert_eq!(history.len(), 2);
    assert!(
        history
            .iter()
            .all(|run| run.state == TaskRunState::Completed)
    );
    assert_eq!(
        reopened
            .prune_completed(
                deployment,
                shardline_protocol::unix_now_seconds_lossy() + 31 * 86400
            )
            .unwrap(),
        2
    );
    assert!(reopened.summaries(deployment, None, 10).unwrap().is_empty());
    let mut retained = 0;
    reopened
        .visit_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap(), |_| {
            retained += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(retained, 0);
}

#[test]
fn local_metadata_conflicts_and_duplicate_legacy_evidence_remain_visible() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    store.publish("fixture", b"exact bytes").unwrap();
    store.publish("fixture", b"exact bytes").unwrap();
    assert!(store.publish("fixture", b"other bytes").is_err());
    assert!(matches!(
        store.read("fixture", 2),
        Err(ServerError::RequestBodyTooLarge)
    ));
    let legacy = TaskEvidenceStore::from_objects(store.objects.clone());
    legacy.publish("fixture", b"exact bytes").unwrap();
    let mut count = 0;
    store
        .visit_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap(), |_| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 1);
    store
        .delete_artifact(&TaskEvidenceStore::key("fixture").unwrap())
        .unwrap();
    assert_eq!(store.read("fixture", 100).unwrap(), None);
    assert_eq!(legacy.read("fixture", 100).unwrap(), None);
    store.publish("conflicting-copy", b"safe").unwrap();
    legacy.publish("conflicting-copy", b"wrong").unwrap();
    assert!(matches!(
        store.read("conflicting-copy", 100),
        Err(ServerError::InvalidContentHash)
    ));
    assert!(store.publish("conflicting-copy", b"safe").is_err());
}

#[test]
fn evidence_reader_crosses_pages_and_preserves_legacy_checks() {
    let root = tempfile::tempdir().unwrap();
    let store = TaskEvidenceStore::from_config(&config(root.path())).unwrap();
    let legacy = TaskEvidenceStore::from_objects(store.objects.clone());
    for number in 0..67 {
        let suffix = format!("manifests/{number:03}");
        store.publish(&suffix, suffix.as_bytes()).unwrap();
    }
    legacy
        .publish("manifests/064", b"different legacy bytes")
        .unwrap();
    legacy.publish("manifests/legacy", b"legacy only").unwrap();
    store
        .publish("manifests/oversized", &vec![0; 65537])
        .unwrap();
    let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}manifests/")).unwrap();
    let deleted_cursor = TaskEvidenceStore::key("manifests/031").unwrap();
    store.delete_artifact(&deleted_cursor).unwrap();
    let mut reader = super::evidence_reader::EvidencePageReader::new(&store, &prefix, None);
    for number in 0..67 {
        let suffix = format!("manifests/{number:03}");
        let key = TaskEvidenceStore::key(&suffix).unwrap();
        let read = reader.read(key.as_str(), &suffix);
        if number == 31 {
            assert!(read.unwrap().is_none());
        } else if number == 64 {
            assert!(matches!(read, Err(ServerError::InvalidContentHash)));
        } else {
            assert_eq!(read.unwrap().as_deref(), Some(suffix.as_bytes()));
        }
    }
    let oversized = TaskEvidenceStore::key("manifests/oversized").unwrap();
    assert!(matches!(
        reader.read(oversized.as_str(), "manifests/oversized"),
        Err(ServerError::RequestBodyTooLarge)
    ));
    // Legacy enumeration can move backwards after the ordered metadata pass.
    let key = TaskEvidenceStore::key("manifests/legacy").unwrap();
    assert_eq!(
        reader
            .read(key.as_str(), "manifests/legacy")
            .unwrap()
            .as_deref(),
        Some(b"legacy only".as_slice())
    );
    let key = TaskEvidenceStore::key("manifests/000").unwrap();
    assert_eq!(
        reader
            .read(key.as_str(), "manifests/000")
            .unwrap()
            .as_deref(),
        Some(b"manifests/000".as_slice())
    );
    // Nested keys must never substitute their bytes for the canonical manifest.
    store
        .publish("manifests/nested/000", b"wrong nested bytes")
        .unwrap();
    let nested = TaskEvidenceStore::key("manifests/nested/000").unwrap();
    assert_eq!(
        reader
            .read(nested.as_str(), "manifests/000")
            .unwrap()
            .as_deref(),
        Some(b"manifests/000".as_slice())
    );
    let mut resumed = super::evidence_reader::EvidencePageReader::new(
        &store,
        &prefix,
        Some(deleted_cursor.as_str()),
    );
    let key = TaskEvidenceStore::key("manifests/032").unwrap();
    assert_eq!(
        resumed
            .read(key.as_str(), "manifests/032")
            .unwrap()
            .as_deref(),
        Some(b"manifests/032".as_slice())
    );
}

pub(super) fn verify_bounded_record_reader(store: &TaskEvidenceStore) {
    let prefix =
        ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/reader/records/")).unwrap();
    for sequence in 0..67 {
        let suffix = format!("runs/reader/records/{sequence:010}");
        store.publish(&suffix, suffix.as_bytes()).unwrap();
    }
    for (sequence, length) in [(67, 16384), (68, 16385), (69, 65537)] {
        store
            .publish(
                &format!("runs/reader/records/{sequence:010}"),
                &vec![7; length],
            )
            .unwrap();
    }
    let conflicting = "runs/reader/records/0000000064";
    TaskEvidenceStore::from_objects(store.objects.clone())
        .publish(conflicting, b"conflicting legacy record")
        .unwrap();
    let mut reader = super::evidence_reader::EvidencePageReader::new(store, &prefix, None);
    for sequence in 0..70 {
        let suffix = format!("runs/reader/records/{sequence:010}");
        let key = TaskEvidenceStore::key(&suffix).unwrap();
        let read = reader.read_bounded(key.as_str(), &suffix, 16384);
        match sequence {
            64 => assert!(matches!(read, Err(ServerError::InvalidContentHash))),
            67 => assert_eq!(read.unwrap().unwrap(), vec![7; 16384]),
            68 | 69 => assert!(matches!(read, Err(ServerError::RequestBodyTooLarge))),
            _ => assert_eq!(read.unwrap().as_deref(), Some(suffix.as_bytes())),
        }
    }
}

#[test]
fn bounded_record_reader_preserves_pages_conflicts_and_record_limits() {
    let root = tempfile::tempdir().unwrap();
    let store = TaskEvidenceStore::from_config(&config(root.path())).unwrap();
    verify_bounded_record_reader(&store);
}

#[test]
fn batched_native_records_keep_oversized_and_gapped_history_corrupt_and_retained() {
    let root = tempfile::tempdir().unwrap();
    let configured = config(root.path());
    let deployment = deployment_fingerprint(&configured).unwrap();
    let store = TaskEvidenceStore::from_config(&configured).unwrap();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    for operation in ["valid", "oversized", "gapped"] {
        tracker
            .begin(
                operation,
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let runs = store.summaries(deployment, None, 10).unwrap();
    let oversized = runs
        .iter()
        .find(|run| run.operation == "oversized")
        .unwrap();
    let gapped = runs.iter().find(|run| run.operation == "gapped").unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    assert_eq!(connection.execute(
        "UPDATE shardline_task_artifacts_v1 SET payload = zeroblob(16385) WHERE artifact_key = ?1",
        [format!("{TASK_EVIDENCE_PREFIX}runs/{}/records/0000000001", oversized.id)],
    ).unwrap(), 1);
    store
        .delete_artifact(
            &TaskEvidenceStore::key(&format!("runs/{}/records/0000000001", gapped.id)).unwrap(),
        )
        .unwrap();
    let states = store.summaries(deployment, None, 10).unwrap();
    for run in states {
        assert_eq!(
            run.state,
            if run.operation == "valid" {
                TaskRunState::Completed
            } else {
                TaskRunState::Corrupt
            }
        );
    }
    assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
    let retained = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(retained.len(), 2);
    assert!(
        retained
            .iter()
            .all(|run| run.state == TaskRunState::Corrupt)
    );
}

#[test]
fn batched_blobs_and_reports_retain_damaged_and_conflicting_evidence() {
    let root = tempfile::tempdir().unwrap();
    let configured = config(root.path());
    let deployment = deployment_fingerprint(&configured).unwrap();
    let store = TaskEvidenceStore::from_config(&configured).unwrap();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    for operation in [
        "valid",
        "oversized-blob",
        "oversized-report",
        "damaged-report",
        "conflicting-blob",
    ] {
        tracker
            .begin(
                operation,
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    let legacy = TaskEvidenceStore::from_objects(store.objects.clone());
    for run in store.summaries(deployment, None, 10).unwrap() {
        let category = if matches!(
            run.operation.as_str(),
            "damaged-report" | "oversized-report"
        ) {
            "artifacts"
        } else {
            "blobs"
        };
        let prefix = format!("{TASK_EVIDENCE_PREFIX}runs/{}/{category}/", run.id);
        match run.operation.as_str() {
            "oversized-blob" | "oversized-report" | "damaged-report" => {
                let size = if run.operation == "damaged-report" {
                    1
                } else {
                    65537
                };
                assert!(connection.execute(
                    "UPDATE shardline_task_artifacts_v1 SET payload = zeroblob(?1) WHERE artifact_key LIKE ?2",
                    rusqlite::params![size, format!("{prefix}%")],
                ).unwrap() > 0);
            }
            "conflicting-blob" => {
                store
                    .visit_prefix(&ObjectPrefix::parse(&prefix).unwrap(), |metadata| {
                        legacy.publish(
                            metadata
                                .key()
                                .as_str()
                                .strip_prefix(TASK_EVIDENCE_PREFIX)
                                .unwrap(),
                            b"conflicting legacy bytes",
                        )?;
                        Ok(())
                    })
                    .unwrap();
            }
            _ => {}
        }
    }
    for run in store.summaries(deployment, None, 10).unwrap() {
        assert_eq!(
            run.state,
            if run.operation == "valid" {
                TaskRunState::Completed
            } else {
                TaskRunState::Corrupt
            }
        );
    }
    assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
    assert_eq!(store.summaries(deployment, None, 10).unwrap().len(), 4);
}

#[test]
fn shared_history_preserves_legacy_fallback_and_scoped_pagination() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let objects = object_store_from_config(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let legacy = penelope_core::task::Digest::of(b"first login");
    let foreign = penelope_core::task::Digest::of(b"second login");
    let shared = penelope_core::task::Digest::of(b"shared schema");
    let other = penelope_core::task::Digest::of(b"other schema");
    for (index, (deployment, scope, operation)) in [
        (legacy, Some(shared), "own-bound"),
        (foreign, Some(shared), "foreign-bound"),
        (legacy, Some(other), "own-other-schema"),
        (legacy, None, "own-legacy"),
        (foreign, None, "foreign-legacy"),
    ]
    .into_iter()
    .enumerate()
    {
        let mut tracker = if index % 2 == 0 {
            EmbeddedTaskTracker::from_store(store.clone(), deployment)
        } else {
            EmbeddedTaskTracker::new(objects.clone(), deployment)
        };
        if let Some(scope) = scope {
            tracker = tracker.with_history_scope(scope);
        }
        tracker
            .begin(
                operation,
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"report")
            .unwrap();
    }
    let history = store
        .inspect_summaries_with_scope(legacy, Some(shared), None, 10)
        .unwrap();
    assert_eq!(history.len(), 3);
    let mut operations = Vec::new();
    let mut foreign_id = String::new();
    for entry in &history {
        if let super::status::TaskEvidenceEntry::Verified(summary) = entry {
            operations.push(summary.operation.as_str());
            if summary.operation == "foreign-bound" {
                foreign_id.clone_from(&summary.id);
            }
        }
    }
    operations.sort_unstable();
    assert_eq!(operations, ["foreign-bound", "own-bound", "own-legacy"]);
    assert_eq!(
        store
            .embedded_report(legacy, Some(shared), &foreign_id)
            .unwrap()
            .unwrap()["report"],
        "report"
    );
    assert!(
        store
            .embedded_report(legacy, None, &foreign_id)
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .embedded_report(legacy, Some(other), &foreign_id)
            .unwrap()
            .is_none()
    );
    let mut after = None;
    for expected in &history {
        let page = store
            .inspect_summaries_with_scope(legacy, Some(shared), after.as_deref(), 1)
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].id(), expected.id());
        after = Some(page[0].id().to_owned());
    }
    assert!(
        store
            .inspect_summaries_with_scope(legacy, Some(shared), after.as_deref(), 1)
            .unwrap()
            .is_empty()
    );
    // Legacy readers keep their existing three own-login records.
    assert_eq!(store.summaries(legacy, None, 10).unwrap().len(), 3);
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/scope-bindings")
            .join(&foreign_id),
        b"corrupt",
    )
    .unwrap();
    assert!(
        store
            .embedded_report(legacy, Some(shared), &foreign_id)
            .is_err()
    );
    let corrupt = store
        .inspect_summaries_with_scope(legacy, Some(shared), None, 10)
        .unwrap();
    assert!(corrupt.iter().any(|entry| matches!(entry,
        super::status::TaskEvidenceEntry::Unreadable { id, state: TaskRunState::Corrupt, identity_verified: false }
        if id == &foreign_id)));
    let unreadable = corrupt
        .iter()
        .find(|entry| entry.id() == foreign_id)
        .unwrap();
    let view = serde_json::to_value(unreadable.operator_view()).unwrap();
    assert_eq!(view["state"], "corrupt");
    assert_eq!(view["identity_verified"], false);
    assert_eq!(view["id"], foreign_id);
    assert!(view.get("owner").is_none());
    assert_eq!(
        view["next_step"],
        "Preserve the stored evidence and inspect storage and original command logs. Do not rely on this record or retry uncertain mutations from it."
    );
    assert_eq!(store.summaries(foreign, None, 10).unwrap().len(), 2);
}

#[test]
fn history_scope_bindings_preserve_legacy_results_and_reject_substitution() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let scope = penelope_core::task::Digest::of(b"resolved scope");
    let other_scope = penelope_core::task::Digest::of(b"other scope");
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment)
        .with_history_scope(scope);
    let receipt = tracker
        .begin(
            "bound-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let summary = store.summaries(deployment, None, 10).unwrap();
    let id = &summary[0].id;
    let manifest = store
        .read(&format!("manifests/{id}"), 65536)
        .unwrap()
        .unwrap();
    let identity: TaskRunIdentity = serde_json::from_slice(&manifest).unwrap();
    let binding = store
        .read(&format!("scope-bindings/{id}"), 2048)
        .unwrap()
        .unwrap();
    assert_eq!(identity.deployment, deployment);
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    assert!(
        !store
            .history_scope_binding_matches(&identity, other_scope)
            .unwrap()
    );
    receipt.bind_history_scope(scope).unwrap();
    assert_eq!(
        store
            .read(&format!("scope-bindings/{id}"), 2048)
            .unwrap()
            .unwrap(),
        binding
    );
    assert!(receipt.bind_history_scope(other_scope).is_err());
    assert!(
        store
            .publish_history_scope_binding(
                &identity,
                &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
                scope
            )
            .is_err()
    );
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    receipt.finish(true, b"done").unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Completed
    );
    assert_eq!(
        store
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .unwrap(),
        manifest
    );
    let binding_path = root
        .path()
        .join("chunks/_shardline_tasks/v1/scope-bindings")
        .join(id);
    let mut tampered: serde_json::Value = serde_json::from_slice(&binding).unwrap();
    tampered["scope"] = serde_json::to_value(other_scope).unwrap();
    std::fs::write(&binding_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .is_err()
    );
    assert!(
        store
            .history_scope_binding_matches(&identity, other_scope)
            .is_err()
    );
    std::fs::write(&binding_path, &binding).unwrap();
    let mut forged = identity.clone();
    forged.operation.push_str("-forged");
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/manifests")
            .join(id),
        serde_json::to_vec(&forged).unwrap(),
    )
    .unwrap();
    assert!(store.history_scope_binding_matches(&forged, scope).is_err());
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/manifests")
            .join(id),
        &manifest,
    )
    .unwrap();
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    std::fs::remove_file(&binding_path).unwrap();
    assert!(
        !store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    std::fs::write(&binding_path, &binding).unwrap();
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
    let unfinished = store
        .embedded_report(deployment, None, &before[0].id)
        .unwrap()
        .unwrap();
    assert_eq!(unfinished["state"], "running");
    assert_eq!(unfinished["report_available"], false);
    assert_eq!(unfinished["report"], serde_json::Value::Null);
    assert_eq!(
        unfinished["next_step"],
        "The recorded process is alive, but no completion is confirmed. The work may still be running or its result may not have been saved. Check the original command and logs before starting work again."
    );
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
    let failed_id = &after
        .iter()
        .find(|task| task.operation == "incomplete-sweep")
        .unwrap()
        .id;
    let diagnostic = store
        .embedded_report(deployment, None, failed_id)
        .unwrap()
        .unwrap();
    assert_eq!(diagnostic["state"], "needs_reconciliation");
    assert_eq!(diagnostic["report"], "failures: 1");
    assert_eq!(diagnostic["report_available"], true);
    assert!(
        store
            .embedded_report(penelope_core::task::Digest::of(b"foreign"), None, failed_id)
            .unwrap()
            .is_none()
    );
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
    let report_path = root
        .path()
        .join("chunks/_shardline_tasks/v1/runs")
        .join(failed_id)
        .join("artifacts")
        .join(hex::encode(
            penelope_core::task::Digest::of(b"failures: 1").0,
        ));
    std::fs::write(&report_path, b"failures: 0").unwrap();
    assert!(store.embedded_report(deployment, None, failed_id).is_err());
    assert_eq!(
        store
            .summaries(deployment, None, 10)
            .unwrap()
            .iter()
            .find(|task| task.id == *failed_id)
            .unwrap()
            .state,
        TaskRunState::Corrupt
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
    let id = store.summaries(deployment, None, 10).unwrap().remove(0).id;
    let diagnostic = store
        .embedded_report(deployment, None, &id)
        .unwrap()
        .unwrap();
    assert_eq!(diagnostic["report_available"], false);
    assert!(diagnostic["report"].is_null());
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
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment)
        .with_history_scope(penelope_core::task::Digest::of(b"retention scope"));
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
    for task in &remaining {
        assert!(
            store
                .read(&format!("scope-bindings/{}", task.id), 2048)
                .unwrap()
                .is_some()
        );
    }
    let prefix =
        shardline_storage::ObjectPrefix::parse("_shardline_tasks/v1/scope-bindings/").unwrap();
    use shardline_storage::ObjectStore as _;
    assert_eq!(store.objects.list_prefix(&prefix).unwrap().len(), 2);
    drop(unfinished);
}

#[test]
fn interrupted_evidence_cleanup_resumes_without_replaying_work() {
    use penelope_core::task::Digest;
    use shardline_storage::{ObjectPrefix, ObjectStore};
    // Crash after the marker, after hiding status, during artifact deletion,
    // and after all run objects were deleted but before clearing the marker.
    for boundary in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let deployment = deployment_fingerprint(&config).unwrap();
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        let objects = object_store_from_config(&config).unwrap();
        let tracker = EmbeddedTaskTracker::new(objects.clone(), deployment);
        tracker
            .begin(
                "finished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
        let id = store.summaries(deployment, None, 10).unwrap()[0].id.clone();
        let identity: TaskRunIdentity = serde_json::from_slice(
            &store
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(store.begin_retirement(&identity).unwrap());
        let run_prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        if boundary >= 1 {
            std::fs::remove_file(
                root.path()
                    .join(format!("chunks/{TASK_EVIDENCE_PREFIX}manifests/{id}")),
            )
            .unwrap();
        }
        if boundary >= 2 {
            std::fs::remove_file(root.path().join(format!(
                "chunks/{TASK_EVIDENCE_PREFIX}runs/{id}/artifacts/{}",
                hex::encode(penelope_core::task::Digest::of(b"complete").0)
            )))
            .unwrap();
        }
        if boundary >= 3 {
            objects
                .visit_prefix(&run_prefix, |metadata| {
                    objects.delete_if_present(metadata.key()).unwrap();
                    Ok::<(), ServerError>(())
                })
                .unwrap();
        }
        let reopened = TaskEvidenceStore::from_config(&config).unwrap();
        let expired = identity.created_at.saturating_add(31 * 24 * 60 * 60);
        assert_eq!(
            reopened
                .prune_completed(Digest::of(b"another-deployment"), expired)
                .unwrap(),
            0
        );
        assert_eq!(reopened.prune_completed(deployment, expired).unwrap(), 1);
        assert!(reopened.summaries(deployment, None, 10).unwrap().is_empty());
        assert!(
            reopened
                .read(&format!("retiring/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        let mut remaining = 0;
        objects
            .visit_prefix(&run_prefix, |_metadata| {
                remaining += 1;
                Ok::<(), ServerError>(())
            })
            .unwrap();
        assert_eq!(remaining, 0);
        assert_eq!(reopened.prune_completed(deployment, expired).unwrap(), 0);
    }
}

#[test]
fn unfinished_history_does_not_starve_finished_history_retention() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    for _ in 0..65 {
        drop(
            tracker
                .begin(
                    "unfinished",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap(),
        );
    }
    for summary in store.summaries(deployment, None, 100).unwrap() {
        let path = root.path().join(format!(
            "chunks/{TASK_EVIDENCE_PREFIX}manifests/{}",
            summary.id
        ));
        let mut identity: TaskRunIdentity =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        identity.created_at = 0;
        std::fs::write(path, serde_json::to_vec(&identity).unwrap()).unwrap();
    }
    tracker
        .begin(
            "finished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"complete")
        .unwrap();
    let now = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60);
    assert_eq!(store.prune_completed(deployment, now).unwrap(), 1);
    let remaining = store.summaries(deployment, None, 100).unwrap();
    assert_eq!(remaining.len(), 65);
    assert!(remaining.iter().all(|task| task.operation == "unfinished"));
}

#[test]
fn metadata_backlog_paginates_and_drains_without_removing_unfinished_or_corrupt_history() {
    verify_metadata_backlog(130);
}

#[test]
fn larger_metadata_backlog_preserves_pagination_and_safe_retention() {
    verify_metadata_backlog(1024);
}

#[test]
#[ignore = "full-capacity scale drill; run verify_maintenance_history_capacity.sh in release mode"]
fn full_automatic_metadata_capacity_preserves_inspection_and_cleanup() {
    // Include one unfinished and one corrupt run in the automatic 4,096-run
    // budget. Neither may disappear when the finished backlog is retired.
    verify_metadata_backlog(4094);
}

#[test]
fn metadata_retention_uses_creation_time_instead_of_key_order() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .require_metadata_publication();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    for _ in 0..65 {
        tracker
            .begin(
                "finished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let mut ids: Vec<_> = store
        .summaries(deployment, None, 100)
        .unwrap()
        .into_iter()
        .map(|summary| summary.id)
        .collect();
    ids.sort();
    for (position, id) in ids.iter().enumerate() {
        let path = format!("manifests/{id}");
        let mut identity: TaskRunIdentity =
            serde_json::from_slice(&store.read(&path, 65536).unwrap().unwrap()).unwrap();
        // Fixture timestamps deliberately oppose ordered metadata keys.
        identity.created_at = 65_u64
            .checked_sub(u64::try_from(position).unwrap())
            .unwrap();
        store
            .delete_artifact(&ObjectKey::parse(&format!("{TASK_EVIDENCE_PREFIX}{path}")).unwrap())
            .unwrap();
        store
            .publish(&path, &serde_json::to_vec(&identity).unwrap())
            .unwrap();
    }
    assert_eq!(store.prune_completed(deployment, 31 * 86400).unwrap(), 64);
    let remaining = store.summaries(deployment, None, 100).unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(&remaining.first().unwrap().id, ids.first().unwrap());
    assert_eq!(remaining.first().unwrap().state, TaskRunState::Completed);
    assert_eq!(store.prune_completed(deployment, 31 * 86400).unwrap(), 1);
    assert!(store.summaries(deployment, None, 100).unwrap().is_empty());
}

#[test]
fn batched_local_retention_preserves_conflicting_legacy_identity() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .require_metadata_publication();
    EmbeddedTaskTracker::from_store(store.clone(), deployment)
        .begin(
            "finished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"complete")
        .unwrap();
    let id = store
        .summaries(deployment, None, 10)
        .unwrap()
        .first()
        .unwrap()
        .id
        .clone();
    let suffix = format!("manifests/{id}");
    let original = store.read(&suffix, 65536).unwrap();
    TaskEvidenceStore::from_objects(store.objects.clone())
        .publish(&suffix, b"conflicting legacy identity")
        .unwrap();
    assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
    assert!(matches!(
        store.read(&suffix, 65536),
        Err(ServerError::InvalidContentHash)
    ));
    assert_eq!(
        store.read_objects(&suffix, 65536).unwrap().as_deref(),
        Some(b"conflicting legacy identity".as_slice())
    );
    assert_eq!(
        store
            .local_metadata
            .as_ref()
            .unwrap()
            .read_task_artifact(TaskEvidenceStore::key(&suffix).unwrap().as_str(), 65536)
            .unwrap(),
        original
    );
}

fn verify_metadata_backlog(finished_count: usize) {
    let total_count = finished_count.checked_add(2).unwrap();
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .require_metadata_publication();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    std::fs::create_dir_all(root.path().join("chunks")).unwrap();
    let payload = root.path().join("chunks/user-payload");
    std::fs::write(&payload, b"existing payload bytes").unwrap();
    for _ in 0..finished_count {
        tracker
            .begin(
                "finished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let unfinished = tracker
        .begin(
            "unfinished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    store
        .publish("manifests/unreadable", b"invalid legacy identity")
        .unwrap();
    let original = store.read("manifests/unreadable", 100).unwrap();

    let began = std::time::Instant::now();
    let mut ids = std::collections::BTreeSet::new();
    let mut after = None;
    loop {
        let page = store
            .inspect_summaries(deployment, after.as_deref(), 100)
            .unwrap();
        assert!(page.len() <= 100);
        if page.is_empty() {
            break;
        }
        for run in &page {
            assert!(ids.insert(run.id().to_owned()), "pagination repeated a run");
        }
        after = page.last().map(|run| run.id().to_owned());
    }
    assert_eq!(ids.len(), total_count);
    let inspection = began.elapsed();
    let mut oldest_finished = std::collections::BTreeMap::new();
    for id in &ids {
        if id == "unreadable" {
            continue;
        }
        let identity: TaskRunIdentity = serde_json::from_slice(
            &store
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        if identity.operation == "finished" {
            oldest_finished.insert((identity.created_at, identity.id.clone()), identity.id);
        }
    }
    let now = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 86400);
    let mut cleanup = std::time::Duration::ZERO;
    let mut slowest_cleanup = std::time::Duration::ZERO;
    let mut surviving_ids = ids.clone();
    let mut remaining_finished = finished_count;
    let mut retention_calls = 0_usize;
    loop {
        let expected = remaining_finished.min(64);
        let began = std::time::Instant::now();
        assert_eq!(store.prune_completed(deployment, now).unwrap(), expected);
        retention_calls = retention_calls.checked_add(1).unwrap();
        remaining_finished = remaining_finished.checked_sub(expected).unwrap();
        let elapsed = began.elapsed();
        cleanup = cleanup.checked_add(elapsed).unwrap();
        slowest_cleanup = slowest_cleanup.max(elapsed);
        let mut deleted_cursor = None;
        for _ in 0..expected {
            let (_, id) = oldest_finished.pop_first().unwrap();
            assert!(
                store
                    .read(&format!("manifests/{id}"), 65536)
                    .unwrap()
                    .is_none(),
                "retention must preserve its oldest-first selection"
            );
            assert!(surviving_ids.remove(&id));
            deleted_cursor = Some(id);
        }
        if let Some(cursor) = deleted_cursor {
            let page = store
                .inspect_summaries(deployment, Some(&cursor), 100)
                .unwrap();
            let actual = page
                .iter()
                .map(|run| run.id().to_owned())
                .collect::<Vec<_>>();
            let expected_ids = surviving_ids
                .iter()
                .filter(|id| id.as_str() > cursor.as_str())
                .take(100)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                actual, expected_ids,
                "a deleted cursor must not reset or skip the next page"
            );
        }
        if expected == 0 {
            break;
        }
    }
    assert!(oldest_finished.is_empty());
    let remaining = store.inspect_summaries(deployment, None, 100).unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|run| matches!(run,
        status::TaskEvidenceEntry::Verified(summary) if summary.operation == "unfinished"
    )));
    assert!(remaining.iter().any(|run| matches!(run,
        status::TaskEvidenceEntry::Unreadable { id, .. } if id == "unreadable"
    )));
    assert_eq!(store.read("manifests/unreadable", 100).unwrap(), original);
    let mut retiring = 0_usize;
    store
        .visit_prefix(
            &ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}retiring/")).unwrap(),
            |_| {
                retiring = retiring.checked_add(1).unwrap();
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(
        retiring, 0,
        "completed cleanup must not leave retirement markers"
    );
    assert_eq!(std::fs::read(&payload).unwrap(), b"existing payload bytes");
    assert!(
        store
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .is_empty()
    );
    eprintln!(
        "{}-run metadata fixture: full paginated inspection {inspection:?}; {retention_calls} retention calls {cleanup:?}; slowest cleanup {slowest_cleanup:?}",
        total_count
    );
    // An invalid key beyond the requested page must not break inspection of
    // earlier verified history. Reaching that key still exposes the failure.
    let database = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    let invalid_key = format!("{TASK_EVIDENCE_PREFIX}manifests/zz\n");
    database
        .execute(
            "INSERT INTO shardline_task_artifacts_v1(artifact_key,payload) VALUES (?1,?2)",
            rusqlite::params![invalid_key, b"invalid later history".as_slice()],
        )
        .unwrap();
    let first = store.inspect_summaries(deployment, None, 1).unwrap();
    assert_eq!(first.len(), 1);
    assert!(
        matches!(first.first().unwrap(), status::TaskEvidenceEntry::Verified(summary)
        if summary.operation == "unfinished")
    );
    assert!(
        store
            .inspect_summaries(deployment, Some("unreadable"), 1)
            .is_err()
    );
    let untouched: Vec<u8> = database
        .query_row(
            "SELECT payload FROM shardline_task_artifacts_v1 WHERE artifact_key=?1",
            [invalid_key],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(untouched, b"invalid later history");
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
async fn postgres_logins_can_target_distinct_default_schemas_with_identical_url_options() {
    use std::str::FromStr;
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let migration_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    crate::apply_database_migrations(&migration_pool)
        .await
        .unwrap();
    migration_pool.close().await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let first_role = format!("sl_history_{}_{}_a", std::process::id(), nonce);
    let second_role = format!("sl_history_{}_{}_b", std::process::id(), nonce);
    // These identifiers contain only generated ASCII letters, digits and '_'.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {first_role} LOGIN PASSWORD 'owned_history_fixture';
         CREATE ROLE {second_role} LOGIN PASSWORD 'owned_history_fixture';
         CREATE SCHEMA {first_role} AUTHORIZATION {first_role};
         CREATE SCHEMA {second_role} AUTHORIZATION {second_role};
         CREATE TABLE {first_role}.shardline_resource_fences (LIKE public.shardline_resource_fences INCLUDING ALL);
         CREATE TABLE {second_role}.shardline_resource_fences (LIKE public.shardline_resource_fences INCLUDING ALL);"
    )))
    .execute(&pool)
    .await
    .unwrap();
    for schema in [&first_role, &second_role] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET search_path TO {schema}, public"
        )))
        .execute(&pool)
        .await
        .unwrap();
        shardline_index::PostgresIndexStore::new(pool.clone())
            .initialize_task_history_scope()
            .await
            .unwrap();
        let identity: String =
            sqlx::query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&pool)
                .await
                .unwrap();
        shardline_index::PostgresIndexStore::new(pool.clone())
            .initialize_task_history_scope()
            .await
            .unwrap();
        let reinstalled: String =
            sqlx::query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(identity, reinstalled);
    }
    sqlx::query("RESET search_path")
        .execute(&pool)
        .await
        .unwrap();
    let options = sqlx::postgres::PgConnectOptions::from_str(&database_url).unwrap();
    let first_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .clone()
                .username(&first_role)
                .password("owned_history_fixture"),
        )
        .await
        .unwrap();
    let second_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .username(&second_role)
                .password("owned_history_fixture"),
        )
        .await
        .unwrap();
    let first_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&first_pool)
        .await
        .unwrap();
    let second_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&second_pool)
        .await
        .unwrap();
    assert_eq!(first_schema, first_role);
    assert_eq!(second_schema, second_role);
    let root = tempfile::tempdir().unwrap();
    let mut first_url = url::Url::parse(&database_url).unwrap();
    first_url.set_username(&first_role).unwrap();
    first_url
        .set_password(Some("owned_history_fixture"))
        .unwrap();
    let mut second_url = first_url.clone();
    second_url.set_username(&second_role).unwrap();
    let first = config(root.path())
        .with_index_postgres_url(first_url.to_string())
        .unwrap();
    let second = config(root.path())
        .with_index_postgres_url(second_url.to_string())
        .unwrap();
    assert_ne!(
        deployment_fingerprint(&first).unwrap(),
        deployment_fingerprint(&second).unwrap()
    );
    let legacy_first = deployment_fingerprint(&first).unwrap();
    let legacy_second = deployment_fingerprint(&second).unwrap();
    let mut first_connection = first_pool.acquire().await.unwrap();
    let mut second_connection = second_pool.acquire().await.unwrap();
    let first_scope = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    let second_scope = resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
        .await
        .unwrap();
    assert_ne!(first_scope, second_scope);
    assert!(
        sqlx::query("SELECT * FROM shardline_resource_fences")
            .execute(&mut *first_connection)
            .await
            .is_err()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {first_role}.shardline_resource_fences ENABLE ROW LEVEL SECURITY"
    )))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
            .await
            .is_err()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {first_role}.shardline_resource_fences DISABLE ROW LEVEL SECURITY"
    )))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("SET search_path TO public")
        .execute(&mut *first_connection)
        .await
        .unwrap();
    sqlx::query("SET search_path TO public")
        .execute(&mut *second_connection)
        .await
        .unwrap();
    let shared_scope = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    assert_eq!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
            .await
            .unwrap()
    );
    assert_ne!(shared_scope, first_scope);
    assert_ne!(shared_scope, second_scope);
    assert_ne!(shared_scope, legacy_first);
    // Separate legacy logins retain their original views while new signed
    // bindings can associate their runs with the same resolved metadata scope.
    for role_config in [&first, &second] {
        let legacy = deployment_fingerprint(role_config).unwrap();
        let role_store = TaskEvidenceStore::from_config(role_config).unwrap();
        EmbeddedTaskTracker::new(object_store_from_config(role_config).unwrap(), legacy)
            .with_history_scope(shared_scope)
            .begin(
                "bound-role-job",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"role report")
            .unwrap();
        let own_history = role_store.summaries(legacy, None, 10).unwrap();
        assert_eq!(own_history.len(), 1);
        let role_manifest: TaskRunIdentity = serde_json::from_slice(
            &role_store
                .read(&format!("manifests/{}", own_history[0].id), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            role_store
                .history_scope_binding_matches(&role_manifest, shared_scope)
                .unwrap()
        );
        assert_eq!(own_history[0].state, TaskRunState::Completed);
        // Observation binds the actual barrier-owning work session, whose
        // default schema differs from our inspection connection's public schema.
        let work_pool = crate::maintenance_barrier::postgres_shared_work_pool(
            role_config.index_postgres_url().unwrap(),
        )
        .await
        .unwrap();
        let work_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&work_pool)
            .await
            .unwrap();
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let mut observed = role_manifest.clone();
        observed.id = format!("observed-{}", observed.id);
        observed.operation = "observed-work-session".to_owned();
        observed.public_key = *key.verifying_key().as_bytes();
        observed.scope.task = penelope_core::task::TaskId::from_digest(
            penelope_core::task::Digest::of(observed.id.as_bytes()),
        );
        role_store
            .publish(
                &format!("manifests/{}", observed.id),
                &serde_json::to_vec(&observed).unwrap(),
            )
            .unwrap();
        role_store
            .observe_history_scopes(observed.clone(), key.clone(), async {
                observe_work_pool(role_config, &work_pool).await;
                observe_work_pool(role_config, &work_pool).await;
            })
            .await;
        let actual_scope = if legacy == legacy_first {
            first_scope
        } else {
            second_scope
        };
        assert!(
            role_store
                .history_scope_binding_matches(&observed, actual_scope)
                .unwrap()
        );
        assert!(
            !role_store
                .history_scope_binding_matches(&observed, shared_scope)
                .unwrap()
        );
        assert_eq!(
            work_pid,
            sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
                .fetch_one(&work_pool)
                .await
                .unwrap()
        );
        // A later target change conflicts with the immutable first association.
        sqlx::query("SET search_path TO public")
            .execute(&work_pool)
            .await
            .unwrap();
        role_store
            .observe_history_scopes(observed.clone(), key.clone(), async {
                observe_work_pool(role_config, &work_pool).await;
            })
            .await;
        assert!(
            role_store
                .history_scope_binding_matches(&observed, actual_scope)
                .unwrap()
        );
        work_pool.close().await;
        // A lost session cannot be replaced by the observation layer.
        role_store
            .observe_history_scopes(observed.clone(), key, async {
                observe_work_pool(role_config, &work_pool).await;
                42
            })
            .await;
        assert!(
            role_store
                .history_scope_binding_matches(&observed, actual_scope)
                .unwrap()
        );
    }
    // Normal routers resolve their own configured connections and inspect the
    // other login's signed history. These schema-only readers cannot publish
    // metadata, so automatic startup must not add payload history instead.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "ALTER ROLE {first_role} SET search_path TO public;
         ALTER ROLE {second_role} SET search_path TO public;
         GRANT SELECT ON public.shardline_schema_migrations TO {first_role}, {second_role};"
    )))
    .execute(&pool)
    .await
    .unwrap();
    for role_config in [&first, &second] {
        use axum::{
            body::{Body, to_bytes},
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let role_store = TaskEvidenceStore::from_config(role_config).unwrap();
        let inventory_before = role_store
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .len();
        let router_config = role_config
            .clone()
            .with_server_frontends([crate::ServerFrontend::Lfs])
            .unwrap()
            .with_admin_read_token(b"history-admin".to_vec())
            .unwrap();
        let app = crate::app::router(router_config).await.unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tasks/maintenance")
                    .header("Authorization", "Bearer history-admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        let jobs = body["tasks"].as_array().unwrap();
        assert_eq!(body["history_grouping"], "shared_metadata");
        assert_eq!(
            jobs.iter()
                .filter(|job| job["operation"] == "bound-role-job")
                .count(),
            2
        );
        assert!(
            jobs.iter()
                .all(|job| job["operation"] != "lfs-startup-session-sweep")
        );
        assert_eq!(
            role_store
                .summaries_with_scope(
                    deployment_fingerprint(role_config).unwrap(),
                    Some(shared_scope),
                    None,
                    100,
                )
                .unwrap()
                .iter()
                .filter(|run| run.operation == "bound-role-job")
                .count(),
            2
        );
        assert_eq!(
            role_store
                .objects
                .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
                .unwrap()
                .len(),
            inventory_before
        );
    }
    // Simulate another initialized backend behind the same endpoint with the
    // same database/schema names. Only its durable namespace identity differs.
    let original_identity: String =
        sqlx::query_scalar("SELECT public.shardline_task_history_scope_v1('public')")
            .fetch_one(&pool)
            .await
            .unwrap();
    let replacement_identity = if original_identity == "00".repeat(32) {
        "01".repeat(32)
    } else {
        "00".repeat(32)
    };
    sqlx::query("UPDATE public.shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(replacement_identity).execute(&pool).await.unwrap();
    let other_backend = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    sqlx::query("UPDATE public.shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(original_identity).execute(&pool).await.unwrap();
    assert_ne!(other_backend, shared_scope);
    assert_eq!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
            .await
            .unwrap()
    );
    assert_eq!(deployment_fingerprint(&first).unwrap(), legacy_first);
    assert_eq!(deployment_fingerprint(&second).unwrap(), legacy_second);
    let other_root = tempfile::tempdir().unwrap();
    let other_target = config(other_root.path())
        .with_index_postgres_url(second.index_postgres_url().unwrap().to_owned())
        .unwrap();
    assert_ne!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&other_target, &mut second_connection)
            .await
            .unwrap()
    );
    assert!(
        resolved_postgres_deployment_fingerprint(&config(root.path()), &mut second_connection)
            .await
            .is_err()
    );
    sqlx::query("SET search_path TO nonexistent_history_scope")
        .execute(&mut *second_connection)
        .await
        .unwrap();
    assert!(
        resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
            .await
            .is_err()
    );
    // A deployment awaiting the owner upgrade must still start and expose its
    // original history. Ordinary startup never installs the missing function.
    sqlx::raw_sql(
        "ALTER FUNCTION public.shardline_task_history_scope_v1(text)
         RENAME TO owned_disabled_history_scope;",
    )
    .execute(&pool)
    .await
    .unwrap();
    {
        use axum::{
            body::{Body, to_bytes},
            http::{Request, StatusCode},
        };
        use tower::ServiceExt;
        let fallback_config = first
            .clone()
            .with_server_frontends([crate::ServerFrontend::Lfs])
            .unwrap()
            .with_admin_read_token(b"history-admin".to_vec())
            .unwrap();
        let app = crate::app::router(fallback_config).await.unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/tasks/maintenance")
                    .header("Authorization", "Bearer history-admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
        assert_eq!(body["history_grouping"], "legacy");
        assert_eq!(
            body["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|job| job["operation"] == "bound-role-job")
                .count(),
            1
        );
        let exists: bool = sqlx::query_scalar("SELECT to_regprocedure($1) IS NOT NULL")
            .bind("public.shardline_task_history_scope_v1(text)")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(!exists);
    }
    sqlx::raw_sql(
        "ALTER FUNCTION public.owned_disabled_history_scope(text)
         RENAME TO shardline_task_history_scope_v1;",
    )
    .execute(&pool)
    .await
    .unwrap();
    drop(first_connection);
    drop(second_connection);
    first_pool.close().await;
    second_pool.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA {first_role} CASCADE; DROP SCHEMA {second_role} CASCADE;
         REVOKE SELECT ON public.shardline_schema_migrations FROM {first_role}, {second_role};
         DROP ROLE {first_role}; DROP ROLE {second_role};"
    )))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
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
                && job["next_step"] == "No recovery is needed for this recorded run. Check /api/v1/status separately for current server readiness."
                && job["owner"]["pid"] == std::process::id())
    );
}

#[tokio::test]
async fn failed_startup_sweep_exposes_diagnostics_without_failing_readiness() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("tmp")).unwrap();
    std::fs::write(
        root.path().join("tmp/lfs-patch"),
        b"fixture prevents staging directory creation",
    )
    .unwrap();
    let config = config(root.path())
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"test-admin-token".to_vec())
        .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let get = |path: &str| {
        Request::builder()
            .uri(path)
            .header("Authorization", "Bearer test-admin-token")
            .body(Body::empty())
            .unwrap()
    };
    let response = app
        .clone()
        .oneshot(get("/api/v1/tasks/maintenance"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let history: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    let task = history["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["operation"] == "lfs-startup-session-sweep")
        .unwrap();
    assert_eq!(task["state"], "needs_reconciliation");
    assert_eq!(
        task["next_step"],
        "Inspect the affected data and original command logs before repeating a mutation. Check the saved report when available."
    );
    let response = app
        .clone()
        .oneshot(get(&format!(
            "/api/v1/tasks/maintenance/{}/report",
            task["id"].as_str().unwrap()
        )))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let diagnostic: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(diagnostic["task"]["state"], "needs_reconciliation");
    assert_eq!(diagnostic["task"]["next_step"], task["next_step"]);
    assert_eq!(
        diagnostic["task"]["report"],
        "sweep failed; removed count unavailable"
    );
    let response = app.oneshot(get("/api/v1/status")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let status: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(status["state"], "ready");
}

#[tokio::test]
async fn corrupt_manifests_remain_visible_without_hiding_healthy_jobs() {
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
    let app = crate::app::router(config.clone()).await.unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    for _ in 0..4 {
        tracker
            .begin("test-sweep", b"input", MaintenanceArtifactKind::Fsck)
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let original = store.summaries(deployment, None, 100).unwrap();
    assert!(original.len() >= 4);
    for (index, job) in original.iter().take(3).enumerate() {
        let suffix = format!("manifests/{}", job.id);
        let path = root
            .path()
            .join(format!("chunks/{TASK_EVIDENCE_PREFIX}manifests/{}", job.id));
        let bytes = match index {
            0 => b"broken JSON".to_vec(),
            1 => {
                let mut identity: TaskRunIdentity =
                    serde_json::from_slice(&store.read(&suffix, 65536).unwrap().unwrap()).unwrap();
                identity.schema = 999;
                serde_json::to_vec(&identity).unwrap()
            }
            _ => vec![b'x'; 65537],
        };
        let key = TaskEvidenceStore::key(&suffix).unwrap();
        if store
            .local_metadata
            .as_ref()
            .unwrap()
            .contains_task_artifact(key.as_str())
            .unwrap()
        {
            // Inject corruption into the actual storage surface. Normal
            // publication remains immutable and cannot replace this row.
            let connection =
                rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
            assert_eq!(connection.execute(
                "UPDATE shardline_task_artifacts_v1 SET payload = ?2 WHERE artifact_key = ?1",
                rusqlite::params![key.as_str(), bytes],
            ).unwrap(), 1);
        } else {
            std::fs::write(path, bytes).unwrap();
        }
    }
    // Existing library callers still receive an error rather than incomplete
    // typed summaries. The admin view can explicitly represent unknown identity.
    assert!(store.summaries(deployment, None, 100).is_err());
    let mut seen = Vec::new();
    let mut cursor = None;
    for index in 0..original.len() {
        let uri = cursor.as_ref().map_or_else(
            || "/api/v1/tasks/maintenance?limit=1".to_owned(),
            |cursor| format!("/api/v1/tasks/maintenance?limit=1&cursor={cursor}"),
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let jobs = body["tasks"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(body["page"]["returned"], 1);
        let job = &jobs[0];
        seen.push(job["id"].as_str().unwrap().to_owned());
        if index < 3 {
            assert_eq!(job["state"], "corrupt");
            assert_eq!(job["identity_verified"], false);
            for field in ["owner", "operation", "execution", "created_at"] {
                assert!(job.get(field).is_none());
            }
        } else {
            assert_eq!(job["state"], "completed");
        }
        cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(cursor.is_some(), index + 1 < original.len());
    }
    assert_eq!(
        seen,
        original
            .iter()
            .map(|job| job.id.clone())
            .collect::<Vec<_>>()
    );
    // Corrupt identities never become evidence-retention authorization.
    let expired = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60);
    assert_eq!(
        store.prune_completed(deployment, expired).unwrap(),
        original.len() - 3
    );
    assert_eq!(
        store
            .inspect_summaries(deployment, None, 100)
            .unwrap()
            .len(),
        3
    );
    // Losing access to the storage namespace is a request failure, not a
    // collection of corrupt jobs or a reassuring empty history.
    let chunks = root.path().join("chunks");
    std::fs::rename(&chunks, root.path().join("chunks-backup")).unwrap();
    std::fs::write(&chunks, b"not a directory").unwrap();
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
    assert!(response.status().is_server_error());
}
