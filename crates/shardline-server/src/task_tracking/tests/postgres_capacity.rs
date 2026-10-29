//! Explicit shared-history capacity drill on an owned PostgreSQL fixture.
use super::*;
use crate::test_support::postgres_fixture::PostgresFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires configured disposable PostgreSQL; run in release mode"]
async fn full_postgres_history_capacity_preserves_unresolved_evidence() {
    verify_full_capacity(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires configured disposable PostgreSQL; run in release mode"]
async fn full_incremental_postgres_history_capacity_preserves_unresolved_evidence() {
    verify_full_capacity(true).await;
}

async fn verify_full_capacity(incremental: bool) {
    let fixture = PostgresFixture::new()
        .await
        .expect("DATABASE_URL must name an owned fixture");
    crate::run_database_migration(&crate::DatabaseMigrationOptions::new(
        fixture.config.index_postgres_url().unwrap().to_owned(),
        crate::DatabaseMigrationCommand::Up { steps: None },
    ))
    .await
    .unwrap();
    let (store, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    assert!(
        store.postgres_metadata.is_some(),
        "capacity must use PostgreSQL history"
    );
    let scope = scope.expect("actual PostgreSQL history scope");
    let store = store.require_metadata_publication();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let index_pool = fixture.index().pool().clone();
    let runtime = tokio::runtime::Handle::current();
    let payload = fixture
        .config
        .root_dir()
        .join("chunks/capacity-user-payload");
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, b"unchanged user payload").unwrap();
    tokio::task::spawn_blocking(move || {
        let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment).with_history_scope(scope);
        let started = std::time::Instant::now();
        for _ in 0..4094 {
            tracker.begin("finished-capacity", b"input", MaintenanceArtifactKind::LifecycleRepair)
                .unwrap().finish(true, b"verified report").unwrap();
        }
        let unfinished = tracker.begin("unfinished-capacity", b"input", MaintenanceArtifactKind::LifecycleRepair).unwrap();
        store.publish("manifests/unreadable", b"unreadable history").unwrap();
        assert!(tracker.begin("over-capacity", b"input", MaintenanceArtifactKind::LifecycleRepair).is_err());
        let creation = started.elapsed();
        let started = std::time::Instant::now();
        let mut ids = std::collections::BTreeSet::new();
        let mut after = None;
        loop {
            let page = store.inspect_summaries_with_scope(deployment, Some(scope), after.as_deref(), 100).unwrap();
            assert!(page.len() <= 100);
            if page.is_empty() { break; }
            for entry in &page { assert!(ids.insert(entry.id().to_owned())); }
            after = page.last().map(|entry| entry.id().to_owned());
        }
        assert_eq!(ids.len(), 4096);
        let inspection = started.elapsed();
        let now = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 86400);
        let started = std::time::Instant::now();
        let mut retired = 0_usize;
        let mut calls = 0_usize;
        let mut slowest = std::time::Duration::ZERO;
        loop {
            let batch_started = std::time::Instant::now();
            let before = incremental.then(|| run_artifact_count(&runtime, &index_pool));
            let count = if incremental {
                store.prune_completed_incrementally(deployment, now).unwrap()
            } else {
                store.prune_completed(deployment, now).unwrap()
            };
            if let Some(before) = before {
                let after = run_artifact_count(&runtime, &index_pool);
                assert!(before >= after);
                assert!(before.checked_sub(after).unwrap() <= 256, "automatic cleanup exceeded its artifact budget");
            }
            slowest = slowest.max(batch_started.elapsed());
            assert!(count <= 64);
            retired = retired.checked_add(count).unwrap();
            calls = calls.checked_add(1).unwrap();
            assert!(calls <= 4096, "cleanup must make progress");
            if count == 0 { break; }
        }
        assert_eq!(retired, 4094);
        if incremental {
            assert!(calls > 65, "the artifact budget must split full-capacity cleanup");
        } else {
            assert_eq!(calls, 65);
        }
        let mut retiring = 0_usize;
        store.visit_prefix(&ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}retiring/")).unwrap(), |_| {
            retiring = retiring.checked_add(1).unwrap();
            Ok(())
        }).unwrap();
        assert_eq!(retiring, 0, "all retirement markers must be settled");
        let cleanup = started.elapsed();
        let remaining = store.inspect_summaries_with_scope(deployment, Some(scope), None, 100).unwrap();
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().any(|entry| matches!(entry,
            status::TaskEvidenceEntry::Verified(run) if run.operation == "unfinished-capacity"
        )));
        assert!(remaining.iter().any(|entry| matches!(entry,
            status::TaskEvidenceEntry::Unreadable { id, .. } if id == "unreadable"
        )));
        assert_eq!(store.read("manifests/unreadable", 100).unwrap().unwrap(), b"unreadable history");
        assert_eq!(std::fs::read(payload).unwrap(), b"unchanged user payload");
        assert!(store.objects.list_prefix(&ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap()).unwrap().is_empty());
        // Reclaimed completed slots permit new recording without discarding
        // the interrupted or corrupt histories.
        tracker.begin("fresh-after-cleanup", b"input", MaintenanceArtifactKind::LifecycleRepair)
            .unwrap().finish(true, b"fresh report").unwrap();
        eprintln!("4096-run PostgreSQL history: creation {creation:?}; inspection {inspection:?}; {calls} cleanup calls {cleanup:?}; slowest {slowest:?}");
        drop(unfinished);
    }).await.unwrap();
    fixture.finish().await;
}

fn run_artifact_count(runtime: &tokio::runtime::Handle, pool: &sqlx::PgPool) -> usize {
    runtime.block_on(async {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM shardline_task_artifacts_v1
             WHERE artifact_key >= $1 COLLATE \"C\"
               AND artifact_key < $2 COLLATE \"C\"",
        )
        .bind(format!("{TASK_EVIDENCE_PREFIX}runs/"))
        .bind(format!("{TASK_EVIDENCE_PREFIX}runs0"))
        .fetch_one(pool)
        .await
        .unwrap();
        usize::try_from(count).unwrap()
    })
}
