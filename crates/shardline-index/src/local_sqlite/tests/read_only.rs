use super::super::LocalIndexStore;

#[test]
fn read_only_store_rejects_writes_and_leaves_missing_roots_absent() {
    let sandbox = tempfile::tempdir().unwrap();
    let root = sandbox.path().join("existing");
    let writer = LocalIndexStore::new(root.clone()).unwrap();
    writer
        .publish_task_artifact("original", b"evidence")
        .unwrap();
    let reader = LocalIndexStore::open_read_only(root);
    assert!(reader.contains_task_artifact("original").unwrap());
    assert!(reader.publish_task_artifact("forbidden", b"new").is_err());
    assert!(!writer.contains_task_artifact("forbidden").unwrap());
    let absent = sandbox.path().join("absent");
    assert!(
        LocalIndexStore::open_read_only(absent.clone())
            .contains_task_artifact("missing")
            .is_err()
    );
    assert!(!absent.exists());
}
