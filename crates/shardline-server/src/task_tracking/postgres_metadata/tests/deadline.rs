//! Blocked history I/O must time out while preserving evidence and recovery.
use super::*;
use crate::task_tracking::TASK_EVIDENCE_PREFIX;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exhausted_history_pool_reports_acquisition_timeout_and_recovers() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let (store, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    // Seed through the asynchronous index contract. This case measures the
    // synchronous reader's checkout deadline, not publication's separate
    // end-to-end deadline during fixture setup. Keep all SQL integrity checks.
    fixture
        .index()
        .publish_task_artifact(
            TaskEvidenceStore::key("fixture/acquisition")
                .unwrap()
                .as_str(),
            b"original",
        )
        .await
        .unwrap();
    // This fixture deliberately has one index connection. Hold it without
    // issuing SQL: the diagnostic must distinguish checkout from query waits.
    let checkout = fixture.index().pool().acquire().await.unwrap();
    let blocked = store.clone();
    let result = tokio::task::spawn_blocking(move || blocked.read("fixture/acquisition", 8))
        .await
        .unwrap();
    assert!(matches!(result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("phase=acquiring connection")
            && error.to_string().contains("pool_size=1, pool_idle=0")));
    drop(checkout);
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            store.read("fixture/acquisition", 8).unwrap().unwrap(),
            b"original"
        );
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_history_publish_times_out_and_preserves_original_evidence() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    fixture
        .index()
        .publish_task_artifact(
            TaskEvidenceStore::key("fixture/publication")
                .unwrap()
                .as_str(),
            b"original",
        )
        .await
        .unwrap();
    let (store, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let mut lock = fixture.admin.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {}.shardline_task_artifacts_v1 IN ACCESS EXCLUSIVE MODE",
        fixture.schema
    )))
    .execute(&mut *lock)
    .await
    .unwrap();
    let blocked = store.clone();
    let result =
        tokio::task::spawn_blocking(move || blocked.publish("fixture/publication", b"replacement"))
            .await
            .unwrap();
    assert!(
        matches!(&result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("task metadata publish exceeded")
            && error.to_string().contains("phase=executing")
            && std::error::Error::source(error).is_some()),
        "unexpected publication timeout: {result:?}"
    );
    lock.rollback().await.unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            store.read("fixture/publication", 8).unwrap().unwrap(),
            b"original"
        );
        store.publish("fixture/publication", b"original").unwrap();
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_history_read_times_out_and_recovers_after_lock_release() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let (store, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let original = store.clone();
    tokio::task::spawn_blocking(move || original.publish("fixture/deadline", b"original"))
        .await
        .unwrap()
        .unwrap();
    // Hold the SQL lock on the independent admin pool. Holding the sole
    // index connection would test acquisition starvation instead of SQL I/O.
    let mut lock = fixture.admin.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "LOCK TABLE {}.shardline_task_artifacts_v1 IN ACCESS EXCLUSIVE MODE",
        fixture.schema
    )))
    .execute(&mut *lock)
    .await
    .unwrap();
    let blocked = store.clone();
    let result = tokio::task::spawn_blocking(move || blocked.read("fixture/deadline", 8))
        .await
        .unwrap();
    assert!(
        matches!(&result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("task metadata read exceeded")
            && error.to_string().contains("phase=executing")
            && error.to_string().contains("pool_size=")
            && error.to_string().contains("pool_idle=")
            && std::error::Error::source(error).is_some()),
        "unexpected history timeout: {result:?}"
    );
    lock.commit().await.unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            store.read("fixture/deadline", 8).unwrap().unwrap(),
            b"original"
        );
        store
            .delete_artifact(&TaskEvidenceStore::key("fixture/deadline").unwrap())
            .unwrap();
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_history_delete_preserves_retirement_marker_and_resumes() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let (store, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let scope = scope.expect("PostgreSQL history scope");
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let payload = fixture
        .config
        .root_dir()
        .join("chunks/timeout-user-payload");
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, b"unchanged acknowledged bytes").unwrap();
    let prepare = store.clone();
    let (finished_id, unfinished) = tokio::task::spawn_blocking(move || {
        let tracker =
            EmbeddedTaskTracker::from_store(prepare.clone(), deployment).with_history_scope(scope);
        tracker
            .begin(
                "delete-deadline",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"verified report")
            .unwrap();
        let finished_id = prepare
            .summaries(deployment, None, 10)
            .unwrap()
            .remove(0)
            .id;
        let unfinished = tracker
            .begin(
                "unfinished-delete-deadline",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap();
        (finished_id, unfinished)
    })
    .await
    .unwrap();
    // Ordinary reads and publication of a different retirement-marker key
    // remain available. Only deletion of this completed manifest must wait.
    let mut lock = fixture.admin.begin().await.unwrap();
    let locked: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT artifact_key FROM {}.shardline_task_artifacts_v1
         WHERE artifact_key=$1 FOR UPDATE",
        fixture.schema
    )))
    .bind(format!("{TASK_EVIDENCE_PREFIX}manifests/{finished_id}"))
    .fetch_one(&mut *lock)
    .await
    .unwrap();
    assert!(locked.ends_with(&finished_id));
    let blocked = store.clone();
    let result = tokio::task::spawn_blocking(move || {
        blocked.prune_completed_incrementally(deployment, u64::MAX)
    })
    .await
    .unwrap();
    assert!(
        matches!(&result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("task metadata delete exceeded")
            && error.to_string().contains("phase=executing")
            && error.to_string().contains("pool_size=")
            && error.to_string().contains("pool_idle=")
            && std::error::Error::source(error).is_some()),
        "unexpected retirement timeout: {result:?}"
    );
    lock.rollback().await.unwrap();
    tokio::task::spawn_blocking(move || {
        for category in ["retiring", "scope-bindings"] {
            assert!(
                store
                    .read(&format!("{category}/{finished_id}"), 65536)
                    .unwrap()
                    .is_some()
            );
        }
        // Retry uses the durable marker after the timed-out connection returns;
        // it must consume the existing retirement authority.
        assert_eq!(
            store
                .prune_completed_incrementally(deployment, u64::MAX)
                .unwrap(),
            1
        );
        for category in ["manifests", "retiring", "scope-bindings"] {
            assert!(
                store
                    .read(&format!("{category}/{finished_id}"), 65536)
                    .unwrap()
                    .is_none()
            );
        }
        let summaries = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].operation, "unfinished-delete-deadline");
        assert_eq!(
            std::fs::read(payload).unwrap(),
            b"unchanged acknowledged bytes"
        );
        drop(unfinished);
    })
    .await
    .unwrap();
    fixture.finish().await;
}
