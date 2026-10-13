//! Full SQLite admission capacity using the automatic retirement budget.
use super::*;

#[test]
#[ignore = "full automatic history capacity; run in release mode"]
fn full_incremental_metadata_capacity_preserves_unresolved_evidence() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config)
        .unwrap()
        .require_metadata_publication();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    let payload = root.path().join("chunks/user-payload");
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, b"existing payload bytes").unwrap();
    for _ in 0..4094 {
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
    assert!(
        tracker
            .begin(
                "over-capacity",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair
            )
            .is_err()
    );
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
        for entry in &page {
            assert!(ids.insert(entry.id().to_owned()));
        }
        after = page.last().map(|entry| entry.id().to_owned());
    }
    assert_eq!(ids.len(), 4096);
    let now = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 86400);
    let started = std::time::Instant::now();
    let mut retired = 0_usize;
    let mut calls = 0_usize;
    loop {
        let before = key_count(&store, "runs");
        let count = store
            .prune_completed_incrementally(deployment, now)
            .unwrap();
        let after = key_count(&store, "runs");
        assert!(before.checked_sub(after).unwrap() <= 256);
        assert!(count <= 64);
        retired = retired.checked_add(count).unwrap();
        calls = calls.checked_add(1).unwrap();
        assert!(calls <= 4096, "cleanup must make progress");
        if count == 0 {
            break;
        }
    }
    assert_eq!(retired, 4094);
    assert!(calls > 65, "automatic artifact budget must split cleanup");
    assert_eq!(key_count(&store, "retiring"), 0);
    let remaining = store.inspect_summaries(deployment, None, 100).unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|entry| matches!(entry,
        status::TaskEvidenceEntry::Verified(run) if run.operation == "unfinished"
    )));
    assert!(remaining.iter().any(|entry| matches!(entry,
        status::TaskEvidenceEntry::Unreadable { id, .. } if id == "unreadable"
    )));
    assert_eq!(
        store.read("manifests/unreadable", 100).unwrap().unwrap(),
        b"invalid legacy identity"
    );
    assert_eq!(std::fs::read(payload).unwrap(), b"existing payload bytes");
    assert!(
        store
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .is_empty()
    );
    tracker
        .begin(
            "fresh-after-cleanup",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"fresh report")
        .unwrap();
    eprintln!(
        "4096-run incremental SQLite history: {calls} cleanup calls {:?}",
        started.elapsed()
    );
    drop(unfinished);
}

fn key_count(store: &TaskEvidenceStore, directory: &str) -> usize {
    let mut count = 0_usize;
    store
        .visit_prefix(
            &ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}{directory}/")).unwrap(),
            |_| {
                count = count.checked_add(1).unwrap();
                Ok(())
            },
        )
        .unwrap();
    count
}
