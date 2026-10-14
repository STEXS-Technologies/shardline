use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn explicit_archive_inspection_preserves_scope_isolation_and_corrupt_evidence() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let foreign = penelope_core::task::Digest::of(b"original archive deployment");
    for target in [deployment, foreign] {
        EmbeddedTaskTracker::from_store(store.clone(), target)
            .begin(
                "archived-result",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"original signed result")
            .unwrap();
    }
    let (reader, _) = TaskEvidenceStore::for_inspection(&config, None)
        .await
        .unwrap();
    let normal = reader.inspection_page(deployment, None, None, 100).unwrap();
    assert_eq!(
        normal
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .unwrap()
            .len(),
        1
    );
    store
        .publish("manifests/zz-corrupt", b"damaged archived identity")
        .unwrap();
    let preserved = store.read("manifests/zz-corrupt", 65536).unwrap();
    let mut after = None;
    let mut states = Vec::new();
    loop {
        let page = reader.inspection_archive_page(after.as_deref(), 1).unwrap();
        assert_eq!(
            page.get("archive").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        for task in page
            .get("tasks")
            .and_then(serde_json::Value::as_array)
            .unwrap()
        {
            states.push(
                task.get("state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap()
                    .to_owned(),
            );
        }
        after = page
            .get("next_after")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        if after.is_none() {
            break;
        }
    }
    assert_eq!(
        states
            .iter()
            .filter(|state| state.as_str() == "completed")
            .count(),
        2
    );
    assert_eq!(
        states
            .iter()
            .filter(|state| state.as_str() == "corrupt")
            .count(),
        1
    );
    assert_eq!(
        store.read("manifests/zz-corrupt", 65536).unwrap(),
        preserved
    );
    assert!(reader.inspection_archive_page(None, 101).is_err());
    assert!(
        reader
            .inspection_archive_page(Some(&"x".repeat(129)), 1)
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn history_inspection_preserves_evidence_and_exposes_recovery_guidance() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
    for _ in 0..3 {
        tracker
            .begin(
                "inspection",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"report")
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
        .publish("manifests/zz-corrupt", b"damaged evidence")
        .unwrap();
    let before = store
        .summaries(deployment, None, 100)
        .unwrap_err()
        .to_string();
    let (reader, scope) = TaskEvidenceStore::for_inspection(&config, None)
        .await
        .unwrap();
    let mut after = None;
    let mut ids = std::collections::BTreeSet::new();
    let mut states = std::collections::BTreeSet::new();
    loop {
        let reader = reader.clone();
        let page = tokio::task::spawn_blocking(move || {
            reader.inspection_page(deployment, scope, after.as_deref(), 2)
        })
        .await
        .unwrap()
        .unwrap();
        for task in page["tasks"].as_array().unwrap() {
            assert!(ids.insert(task["id"].as_str().unwrap().to_owned()));
            states.insert(task["state"].as_str().unwrap().to_owned());
            assert!(!task["next_step"].as_str().unwrap().is_empty());
        }
        after = page["next_after"].as_str().map(str::to_owned);
        if after.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 5);
    assert!(states.contains("completed"));
    assert!(states.contains("running") || states.contains("running_or_interrupted"));
    assert!(states.contains("corrupt"));
    assert_eq!(
        store.read("manifests/zz-corrupt", 100).unwrap().unwrap(),
        b"damaged evidence"
    );
    assert_eq!(
        store
            .summaries(deployment, None, 100)
            .unwrap_err()
            .to_string(),
        before
    );
    assert!(reader.inspection_page(deployment, scope, None, 0).is_err());
    assert!(
        reader
            .inspection_page(deployment, scope, None, 101)
            .is_err()
    );
    drop(unfinished);
}

#[tokio::test(flavor = "multi_thread")]
async fn history_inspection_does_not_initialize_a_missing_deployment() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("absent");
    let config = config(&root);
    let (store, scope) = TaskEvidenceStore::for_inspection(&config, None)
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let page =
        tokio::task::spawn_blocking(move || store.inspection_page(deployment, scope, None, 20))
            .await
            .unwrap()
            .unwrap();
    assert!(page["tasks"].as_array().unwrap().is_empty());
    assert!(page["history_notice"].is_string());
    assert!(!root.exists());
}
