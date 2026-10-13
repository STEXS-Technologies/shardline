//! Explicit large legacy-run cleanup with reopening between every batch.
use super::*;

#[test]
#[ignore = "large legacy artifact inventory; explicit capacity drill"]
fn large_legacy_run_retires_incrementally_without_touching_unresolved_evidence() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let configured = TaskEvidenceStore::from_config(&config).unwrap();
    let store = TaskEvidenceStore::from_objects(configured.objects);
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    tracker
        .begin(
            "large-legacy",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"verified completion")
        .unwrap();
    let id = store.summaries(deployment, None, 10).unwrap().remove(0).id;
    let unfinished = tracker
        .begin(
            "unfinished-legacy",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    store
        .publish("manifests/unreadable", b"preserve corrupt history")
        .unwrap();
    for number in 0..10_000 {
        store
            .publish(&format!("runs/{id}/extra/{number:05}"), b"legacy evidence")
            .unwrap();
    }
    let manifest = store
        .read(&format!("manifests/{id}"), 65536)
        .unwrap()
        .unwrap();
    let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
    let count = |store: &TaskEvidenceStore| {
        let mut count = 0_usize;
        store
            .visit_prefix(&prefix, |_| {
                count = count.checked_add(1).unwrap();
                Ok(())
            })
            .unwrap();
        count
    };
    let payload = root.path().join("chunks/user-payload");
    std::fs::write(&payload, b"unchanged user data").unwrap();
    let objects = store.objects.clone();
    let mut remaining = count(&store);
    assert!(remaining > 10_000);
    let started = std::time::Instant::now();
    let mut calls = 0_usize;
    let mut retired = 0_usize;
    drop(store);
    while remaining != 0 {
        let fresh = TaskEvidenceStore::from_objects(objects.clone());
        let finished = fresh
            .prune_completed_incrementally(deployment, u64::MAX)
            .unwrap();
        assert!(finished <= 1);
        retired = retired.checked_add(finished).unwrap();
        let after = count(&fresh);
        let deleted = remaining.checked_sub(after).unwrap();
        assert!(deleted > 0 && deleted <= 256);
        calls = calls.checked_add(1).unwrap();
        assert!(calls <= 100);
        assert!(
            fresh
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        if after != 0 {
            assert_eq!(
                fresh.read(&format!("retiring/{id}"), 65536).unwrap(),
                Some(manifest.clone())
            );
        } else {
            assert!(
                fresh
                    .read(&format!("retiring/{id}"), 65536)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            fresh.read("manifests/unreadable", 100).unwrap().unwrap(),
            b"preserve corrupt history"
        );
        assert_eq!(std::fs::read(&payload).unwrap(), b"unchanged user data");
        remaining = after;
    }
    assert_eq!(retired, 1);
    assert!(calls > 39);
    let fresh = TaskEvidenceStore::from_objects(objects);
    let summaries = fresh.inspect_summaries(deployment, None, 10).unwrap();
    assert_eq!(summaries.len(), 2);
    assert!(summaries.iter().any(|entry| matches!(entry, status::TaskEvidenceEntry::Verified(run) if run.operation == "unfinished-legacy")));
    eprintln!(
        "10000-artifact legacy retirement: {calls} calls in {:?}",
        started.elapsed()
    );
    drop(unfinished);
}
