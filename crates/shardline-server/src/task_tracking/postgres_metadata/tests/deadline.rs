//! Blocked history I/O must time out while preserving evidence and recovery.
use super::*;
use crate::task_tracking::TASK_EVIDENCE_PREFIX;

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
    let mut lock = fixture.index().pool().begin().await.unwrap();
    sqlx::query("LOCK TABLE shardline_task_artifacts_v1 IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    let blocked = store.clone();
    let result = tokio::task::spawn_blocking(move || blocked.read("fixture/deadline", 8))
        .await
        .unwrap();
    assert!(matches!(result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("task metadata read exceeded")
            && std::error::Error::source(&error).is_some()));
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
    assert!(matches!(result, Err(ServerError::Io(error))
        if error.kind() == std::io::ErrorKind::TimedOut
            && error.to_string().contains("task metadata delete exceeded")
            && std::error::Error::source(&error).is_some()));
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
