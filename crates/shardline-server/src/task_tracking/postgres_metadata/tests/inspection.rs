use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inspection_resolves_postgres_scope_without_changing_saved_artifacts() {
    let Some(fixture) = Fixture::new().await else {
        eprintln!("SKIPPED: configured PostgreSQL inspection fixture requires DATABASE_URL");
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let (writer, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        EmbeddedTaskTracker::from_store(writer, deployment)
            .begin(
                "inspect shared history",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"report")
            .unwrap();
    })
    .await
    .unwrap();
    let query = format!(
        "SELECT artifact_key,payload FROM {}.shardline_task_artifacts_v1 ORDER BY artifact_key",
        fixture.schema,
    );
    let before: Vec<(String, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(query.clone()))
        .fetch_all(&fixture.admin)
        .await
        .unwrap();
    let (reader, resolved) =
        TaskEvidenceStore::for_inspection(&fixture.config, Some(fixture.index().pool()))
            .await
            .unwrap();
    assert_eq!(resolved, scope);
    assert!(resolved.is_some());
    let page =
        tokio::task::spawn_blocking(move || reader.inspection_page(deployment, resolved, None, 20))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(page["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(page["tasks"][0]["state"], "completed");
    assert!(page["tasks"][0]["next_step"].is_string());
    let after: Vec<(String, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(query))
        .fetch_all(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(after, before);
    fixture.finish().await;
}
