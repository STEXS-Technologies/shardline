//! A blocked optional history read must time out without changing evidence.
use super::*;

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
