//! Retirement across metadata batches must remain resumable after partial failure.
use super::*;
use crate::test_support::postgres_fixture::PostgresFixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_postgres_retirement_preserves_binding_until_resumed() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let (store, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let scope = scope.unwrap();
    let prepare = store.clone();
    let id = tokio::task::spawn_blocking(move || {
        EmbeddedTaskTracker::from_store(prepare.clone(), deployment)
            .with_history_scope(scope)
            .begin(
                "incremental-retirement",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"report")
            .unwrap();
        let id = prepare
            .summaries(deployment, None, 10)
            .unwrap()
            .remove(0)
            .id;
        let legacy = TaskEvidenceStore::from_objects(prepare.objects.clone());
        for number in 0..400 {
            let suffix = format!("runs/{id}/extra/{number:04}");
            prepare.publish(&suffix, b"evidence").unwrap();
            legacy.publish(&suffix, b"evidence").unwrap();
        }
        let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        let mut before = 0;
        prepare
            .visit_prefix(&prefix, |_| {
                before += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            prepare
                .prune_completed_incrementally(deployment, u64::MAX)
                .unwrap(),
            0
        );
        let mut after = 0;
        prepare
            .visit_prefix(&prefix, |_| {
                after += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(after, before - 256);
        assert!(
            prepare
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        for category in ["retiring", "scope-bindings"] {
            assert!(
                prepare
                    .read(&format!("{category}/{id}"), 65536)
                    .unwrap()
                    .is_some()
            );
        }
        id
    })
    .await
    .unwrap();
    drop(store);
    let (fresh, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            fresh
                .prune_completed_incrementally(deployment, u64::MAX)
                .unwrap(),
            1
        );
        for category in ["retiring", "scope-bindings"] {
            assert!(
                fresh
                    .read(&format!("{category}/{id}"), 65536)
                    .unwrap()
                    .is_none()
            );
        }
        let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        let mut remaining = 0;
        fresh
            .visit_prefix(&prefix, |_| {
                remaining += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(remaining, 0);
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_batched_retirement_preserves_marker_and_resumes() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let (store, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let scope = scope.unwrap();
    let payload = fixture.config.root_dir().join("chunks/user-payload");
    std::fs::create_dir_all(payload.parent().unwrap()).unwrap();
    std::fs::write(&payload, b"preserved user bytes").unwrap();
    let prepare = store.clone();
    let (id, keys) = tokio::task::spawn_blocking(move || {
        EmbeddedTaskTracker::from_store(prepare.clone(), deployment)
            .with_history_scope(scope)
            .begin(
                "batched-retirement",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"report")
            .unwrap();
        let id = prepare
            .summaries(deployment, None, 10)
            .unwrap()
            .remove(0)
            .id;
        let legacy = TaskEvidenceStore::from_objects(prepare.objects.clone());
        for number in 0..130 {
            let suffix = format!("runs/{id}/extra/{number:04}");
            prepare.publish(&suffix, b"extra evidence").unwrap();
            legacy.publish(&suffix, b"extra evidence").unwrap();
        }
        let prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        let mut keys = std::collections::BTreeSet::new();
        prepare
            .visit_prefix(&prefix, |artifact| {
                keys.insert(artifact.key().as_str().to_owned());
                Ok(())
            })
            .unwrap();
        (id, keys.into_iter().collect::<Vec<_>>())
    })
    .await
    .unwrap();
    let failing_key = keys.get(80).unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION {schema}.reject_retirement_batch() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF OLD.artifact_key = '{failing_key}' THEN RAISE EXCEPTION 'injected retirement batch failure'; END IF;
         RETURN OLD; END $$;
         CREATE TRIGGER reject_retirement_batch BEFORE DELETE ON {schema}.shardline_task_artifacts_v1
         FOR EACH ROW EXECUTE FUNCTION {schema}.reject_retirement_batch()", schema=fixture.schema
    ))).execute(&fixture.admin).await.unwrap();
    let failed = store.clone();
    let failed_id = id.clone();
    let original_count = keys.len();
    tokio::task::spawn_blocking(move || {
        assert!(failed.prune_completed(deployment, u64::MAX).is_err());
        assert!(
            failed
                .read(&format!("manifests/{failed_id}"), 65536)
                .unwrap()
                .is_none()
        );
        assert!(
            failed
                .read(&format!("retiring/{failed_id}"), 65536)
                .unwrap()
                .is_some()
        );
        assert!(
            failed
                .read(&format!("scope-bindings/{failed_id}"), 2048)
                .unwrap()
                .is_some()
        );
        let prefix =
            ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{failed_id}/")).unwrap();
        let mut remaining = 0;
        failed
            .visit_prefix(&prefix, |_| {
                remaining += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(remaining, original_count - 64);
    })
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER reject_retirement_batch ON {}.shardline_task_artifacts_v1",
        fixture.schema
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
        let prefix = ObjectPrefix::parse(TASK_EVIDENCE_PREFIX).unwrap();
        let mut remaining = 0;
        store
            .visit_prefix(&prefix, |_| {
                remaining += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(remaining, 0);
        assert!(store.objects.list_prefix(&prefix).unwrap().is_empty());
        assert_eq!(std::fs::read(payload).unwrap(), b"preserved user bytes");
    })
    .await
    .unwrap();
    fixture.finish().await;
}
