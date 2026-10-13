//! Recovery after a SQLite retirement batch fails after an earlier batch commits.
use super::*;

#[test]
fn incremental_retirement_bounds_deletions_and_resumes_after_reopening() {
    for metadata_backed in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let deployment = deployment_fingerprint(&config).unwrap();
        let configured = TaskEvidenceStore::from_config(&config).unwrap();
        let store = if metadata_backed {
            configured
        } else {
            TaskEvidenceStore::from_objects(configured.objects)
        };
        EmbeddedTaskTracker::from_store(store.clone(), deployment)
            .begin(
                "incremental-retirement",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"original report")
            .unwrap();
        let id = store.summaries(deployment, None, 10).unwrap().remove(0).id;
        let manifest = store
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .unwrap();
        for number in 0..400 {
            let suffix = format!("runs/{id}/extra/{number:04}");
            store.publish(&suffix, b"original evidence").unwrap();
            if metadata_backed {
                TaskEvidenceStore::from_objects(store.objects.clone())
                    .publish(&suffix, b"original evidence")
                    .unwrap();
            }
        }
        let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        let count = |store: &TaskEvidenceStore| {
            let mut count = 0;
            store
                .visit_prefix(&prefix, |_| {
                    count += 1;
                    Ok(())
                })
                .unwrap();
            count
        };
        let original_count = count(&store);
        let payload = root.path().join("chunks/user-payload");
        std::fs::write(&payload, b"preserved user bytes").unwrap();
        assert_eq!(
            store
                .prune_completed_incrementally(deployment, u64::MAX)
                .unwrap(),
            0
        );
        assert_eq!(count(&store), original_count - 256);
        assert!(
            store
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .read(&format!("retiring/{id}"), 65536)
                .unwrap()
                .unwrap(),
            manifest
        );
        let objects = store.objects.clone();
        drop(store);
        let fresh = if metadata_backed {
            TaskEvidenceStore::from_config(&config).unwrap()
        } else {
            TaskEvidenceStore::from_objects(objects)
        };
        assert_eq!(
            fresh
                .prune_completed_incrementally(deployment, u64::MAX)
                .unwrap(),
            1
        );
        assert_eq!(count(&fresh), 0);
        assert!(
            fresh
                .read(&format!("retiring/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        assert_eq!(std::fs::read(&payload).unwrap(), b"preserved user bytes");
    }
}

#[test]
fn interrupted_sqlite_batches_preserve_authorization_and_resume() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    EmbeddedTaskTracker::from_store(store.clone(), deployment)
        .begin(
            "batched-retirement",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"original report")
        .unwrap();
    let id = store.summaries(deployment, None, 10).unwrap().remove(0).id;
    let manifest = store
        .read(&format!("manifests/{id}"), 65536)
        .unwrap()
        .unwrap();
    let legacy = TaskEvidenceStore::from_objects(store.objects.clone());
    for number in 0..130 {
        let suffix = format!("runs/{id}/extra/{number:04}");
        store.publish(&suffix, b"original evidence").unwrap();
        legacy.publish(&suffix, b"original evidence").unwrap();
    }
    let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
    let mut keys = std::collections::BTreeSet::new();
    store
        .visit_prefix(&prefix, |artifact| {
            keys.insert(artifact.key().as_str().to_owned());
            Ok(())
        })
        .unwrap();
    let keys: Vec<_> = keys.into_iter().collect();
    let failing_key = keys.get(80).unwrap().replace('\'', "''");
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute_batch(&format!(
        "CREATE TRIGGER reject_retirement_batch BEFORE DELETE ON shardline_task_artifacts_v1
         WHEN OLD.artifact_key = '{failing_key}' BEGIN SELECT RAISE(ABORT, 'fixture failure'); END;"
    ))
        .unwrap();
    let payload = root.path().join("chunks/user-payload");
    std::fs::write(&payload, b"preserved user bytes").unwrap();
    assert!(store.prune_completed(deployment, u64::MAX).is_err());
    assert!(
        store
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .read(&format!("retiring/{id}"), 65536)
            .unwrap()
            .unwrap(),
        manifest
    );
    let metadata = store.local_metadata.as_ref().unwrap();
    for (position, key) in keys.iter().enumerate() {
        assert_eq!(
            metadata.contains_task_artifact(key).unwrap(),
            position >= 64
        );
    }
    assert_eq!(std::fs::read(&payload).unwrap(), b"preserved user bytes");
    connection
        .execute_batch("DROP TRIGGER reject_retirement_batch")
        .unwrap();
    drop(connection);
    drop(store);
    let fresh = TaskEvidenceStore::from_config(&config).unwrap();
    assert_eq!(fresh.prune_completed(deployment, u64::MAX).unwrap(), 1);
    let mut remaining = 0;
    fresh
        .visit_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap(), |_| {
            remaining += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(remaining, 0);
    assert!(
        fresh
            .objects
            .list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .is_empty()
    );
    assert_eq!(std::fs::read(payload).unwrap(), b"preserved user bytes");
}
