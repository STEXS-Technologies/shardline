use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_reader_inspects_history_without_new_grants_or_cross_schema_access() {
    let Some(fixture) = Fixture::new().await else {
        eprintln!("SKIPPED: restricted PostgreSQL inspection requires DATABASE_URL");
        return;
    };
    let other = Fixture::new().await.unwrap();
    for target in [&fixture, &other] {
        target
            .index()
            .initialize_task_history_scope()
            .await
            .unwrap();
        let deployment = deployment_fingerprint(&target.config).unwrap();
        let (writer, scope) = TaskEvidenceStore::from_backend(&target.config, &target.backend)
            .await
            .unwrap();
        tokio::task::spawn_blocking(move || {
            EmbeddedTaskTracker::from_store(writer, deployment)
                .with_history_scope(scope.unwrap())
                .begin(
                    "restricted inspection",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"report")
                .unwrap();
        })
        .await
        .unwrap();
    }
    let role = format!("{}_reader", fixture.schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'owned_inspection_reader';
         GRANT USAGE ON SCHEMA {} TO {role};
         GRANT SELECT ON {}.shardline_reliability_events TO {role}",
        fixture.schema, fixture.schema,
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    let mut url = url::Url::parse(fixture.config.index_postgres_url().unwrap()).unwrap();
    url.set_username(&role).unwrap();
    url.set_password(Some("owned_inspection_reader")).unwrap();
    let config = fixture
        .config
        .clone()
        .with_index_postgres_url(url.to_string())
        .unwrap();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url.as_str())
        .await
        .unwrap();
    let snapshot = format!(
        "SELECT artifact_key,payload FROM {}.shardline_task_artifacts_v1 ORDER BY artifact_key",
        fixture.schema,
    );
    let before: Vec<(String, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(snapshot.clone()))
        .fetch_all(&fixture.admin)
        .await
        .unwrap();
    let direct = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT * FROM {}.shardline_task_artifacts_v1",
        fixture.schema,
    )))
    .fetch_all(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        direct.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    let cross_schema = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {}.shardline_task_artifact_read_v1('{}', 'missing', 0)",
        other.schema, other.schema,
    )))
    .fetch_all(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        cross_schema.as_database_error().unwrap().code().as_deref(),
        Some("42501")
    );
    let (reader, scope) = TaskEvidenceStore::for_inspection(&config, Some(&pool))
        .await
        .unwrap();
    let (_, owner_scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let (_, other_scope) = TaskEvidenceStore::from_backend(&other.config, &other.backend)
        .await
        .unwrap();
    assert_eq!(scope, owner_scope);
    assert_ne!(scope, other_scope);
    let deployment = deployment_fingerprint(&config).unwrap();
    let page =
        tokio::task::spawn_blocking(move || reader.inspection_page(deployment, scope, None, 20))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(page["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(page["tasks"][0]["state"], "completed");
    // A policy on ordinary metadata also prevents schema-wide grouping; the
    // optional evidence adapter must not use its definer privileges to bypass it.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {}.shardline_reliability_events ENABLE ROW LEVEL SECURITY;
         ALTER TABLE {}.shardline_reliability_events FORCE ROW LEVEL SECURITY;
         CREATE POLICY deny_inspection ON {}.shardline_reliability_events
         USING (false) WITH CHECK (false)",
        fixture.schema, fixture.schema, fixture.schema,
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    let protected_read = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {}.shardline_task_artifact_read_v1('{}', 'missing', 0)",
        fixture.schema, fixture.schema,
    )))
    .fetch_all(&pool)
    .await
    .unwrap_err();
    assert_eq!(
        protected_read
            .as_database_error()
            .unwrap()
            .code()
            .as_deref(),
        Some("SLD04")
    );
    let (reader, scope) = TaskEvidenceStore::for_inspection(&config, Some(&pool))
        .await
        .unwrap();
    assert!(scope.is_none());
    let page =
        tokio::task::spawn_blocking(move || reader.inspection_page(deployment, scope, None, 20))
            .await
            .unwrap()
            .unwrap();
    assert!(page["tasks"].as_array().unwrap().is_empty());
    assert!(
        page["history_notice"]
            .as_str()
            .unwrap()
            .contains("an empty list does not prove maintenance did not run")
    );
    let after: Vec<(String, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(snapshot))
        .fetch_all(&fixture.admin)
        .await
        .unwrap();
    assert_eq!(before, after);
    pool.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role}"
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    other.finish().await;
    fixture.finish().await;
}

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
