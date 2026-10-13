use super::*;

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
