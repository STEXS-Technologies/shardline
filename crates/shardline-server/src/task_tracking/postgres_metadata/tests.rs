#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::task_tracking::{EmbeddedTaskTracker, TaskRunState, deployment_fingerprint};
use shardline_reliability::MaintenanceArtifactKind;
use shardline_storage::{ObjectPrefix, ObjectStore};
use std::num::NonZeroUsize;

struct Fixture {
    _root: tempfile::TempDir,
    config: ServerConfig,
    backend: ServerBackend,
    admin: PgPool,
    schema: String,
}

impl Fixture {
    async fn new() -> Option<Self> {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            return None;
        };
        let admin = PgPool::connect(&database_url).await.unwrap();
        let mut nonce = [0_u8; 8];
        getrandom::fill(&mut nonce).unwrap();
        let schema = format!("task_store_{}", hex::encode(nonce));
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
            .execute(&admin)
            .await
            .unwrap();
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'
             AND tablename LIKE 'shardline_%' AND tablename<>'shardline_task_artifacts_v1'
             ORDER BY tablename",
        )
        .fetch_all(&admin)
        .await
        .unwrap();
        for table in tables {
            let quoted = format!("\"{}\"", table.replace('"', "\"\""));
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {schema}.{quoted} (LIKE public.{quoted} INCLUDING ALL)"
            )))
            .execute(&admin)
            .await
            .unwrap();
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.shardline_schema_migrations SELECT * FROM public.shardline_schema_migrations"
        ))).execute(&admin).await.unwrap();
        let mut url = url::Url::parse(&database_url).unwrap();
        let options = url
            .query_pairs()
            .find(|(key, _)| key == "options")
            .map_or_else(String::new, |(_, value)| value.into_owned());
        let parameters: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "options")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(parameters)
            .append_pair("options", &format!("{options} -csearch_path={schema}"));
        let root = tempfile::tempdir().unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_path_buf(),
            NonZeroUsize::new(4096).unwrap(),
        )
        .with_index_postgres_url(url.to_string())
        .unwrap();
        let backend = crate::PostgresBackend::new(
            root.path().to_path_buf(),
            config.public_base_url().to_owned(),
            NonZeroUsize::new(4096).unwrap(),
            url.as_str(),
        )
        .await
        .unwrap()
        .with_single_connection_pools(url.as_str())
        .unwrap();
        Some(Self {
            _root: root,
            config,
            backend: ServerBackend::Postgres(backend),
            admin,
            schema,
        })
    }

    fn index(&self) -> &PostgresIndexStore {
        let backend = if let ServerBackend::Postgres(backend) = &self.backend {
            Some(backend)
        } else {
            None
        };
        backend
            .expect("fixture constructs PostgreSQL")
            .index_store()
    }

    async fn finish(self) {
        self.index().pool().close().await;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await
        .unwrap();
        self.admin.close().await;
    }
}

#[test]
fn library_backend_without_runtime_returns_an_error_without_starting_one() {
    use futures_util::FutureExt;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let Some(fixture) = runtime.block_on(Fixture::new()) else {
        return;
    };
    let error = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .now_or_never()
        .expect("runtime validation must return immediately")
        .unwrap_err();
    assert!(matches!(error, ServerError::Io(error)
        if error.get_ref().is_some_and(|source| source.is::<tokio::runtime::TryCurrentError>())));
    runtime.block_on(fixture.finish());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_defers_scope_reads_for_runs_outside_the_oldest_batch() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let (store, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let seed = store.clone();
    let newest = tokio::task::spawn_blocking(move || {
        let tracker = EmbeddedTaskTracker::from_store(seed.clone(), deployment);
        for _ in 0..65 {
            tracker
                .begin(
                    "completed",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"complete")
                .unwrap();
        }
        let mut ids: Vec<_> = seed
            .summaries(deployment, None, 100)
            .unwrap()
            .into_iter()
            .map(|summary| summary.id)
            .collect();
        ids.sort();
        for (position, id) in ids.iter().enumerate() {
            let suffix = format!("manifests/{id}");
            let mut identity: super::super::TaskRunIdentity =
                serde_json::from_slice(&seed.read(&suffix, 65536).unwrap().unwrap()).unwrap();
            identity.created_at = u64::try_from(position).unwrap();
            seed.delete_artifact(&TaskEvidenceStore::key(&suffix).unwrap())
                .unwrap();
            seed.publish(&suffix, &serde_json::to_vec(&identity).unwrap())
                .unwrap();
        }
        ids.last().unwrap().clone()
    })
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "ALTER FUNCTION {}.shardline_task_artifact_read_v1(text,text,bigint) RENAME TO original_artifact_read;
         CREATE FUNCTION {}.shardline_task_artifact_read_v1(index_schema text,artifact text,maximum_bytes bigint)
         RETURNS TABLE(byte_length bigint,body bytea) LANGUAGE plpgsql AS $$
         BEGIN IF artifact='{}scope-bindings/{newest}' THEN RAISE EXCEPTION 'synthetic deferred scope read failure'; END IF;
         RETURN QUERY SELECT * FROM {}.original_artifact_read(index_schema,artifact,maximum_bytes); END $$",
        fixture.schema, fixture.schema, super::super::TASK_EVIDENCE_PREFIX, fixture.schema,
    ))).execute(&fixture.admin).await.unwrap();
    let checked = store.clone();
    let checked_id = newest.clone();
    tokio::task::spawn_blocking(move || {
        assert_eq!(checked.prune_completed(deployment, u64::MAX).unwrap(), 64);
        assert!(checked.prune_completed(deployment, u64::MAX).is_err());
        assert!(
            checked
                .read(&format!("manifests/{checked_id}"), 65536)
                .unwrap()
                .is_some()
        );
    })
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP FUNCTION {}.shardline_task_artifact_read_v1(text,text,bigint);
         ALTER FUNCTION {}.original_artifact_read(text,text,bigint) RENAME TO shardline_task_artifact_read_v1",
        fixture.schema, fixture.schema,
    ))).execute(&fixture.admin).await.unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replacement_login_retires_shared_history_after_source_pool_closes() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let source_role = format!("{}_source", fixture.schema);
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {source_role} LOGIN PASSWORD 'owned_retention_source';
         GRANT USAGE ON SCHEMA {} TO {source_role};
         GRANT SELECT,INSERT,DELETE ON {}.shardline_reliability_events TO {source_role}",
        fixture.schema, fixture.schema,
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    let mut source_url = url::Url::parse(fixture.config.index_postgres_url().unwrap()).unwrap();
    source_url.set_username(&source_role).unwrap();
    source_url
        .set_password(Some("owned_retention_source"))
        .unwrap();
    let source_config = fixture
        .config
        .clone()
        .with_index_postgres_url(source_url.to_string())
        .unwrap();
    let source_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(source_url.as_str())
        .await
        .unwrap();
    let (source, source_scope) =
        TaskEvidenceStore::from_postgres_pool(&source_config, &source_pool)
            .await
            .unwrap();
    let source_scope = source_scope.unwrap();
    let source_deployment = deployment_fingerprint(&source_config).unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    assert_ne!(source_deployment, deployment);
    tokio::task::spawn_blocking(move || {
        let tracker = EmbeddedTaskTracker::from_store(source, source_deployment)
            .with_history_scope(source_scope);
        for _ in 0..2 {
            tracker
                .begin(
                    "previous-account-complete",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"complete")
                .unwrap();
        }
        drop(
            tracker
                .begin(
                    "previous-account-interrupted",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap(),
        );
    })
    .await
    .unwrap();
    source_pool.close().await;
    let (replacement, replacement_scope) =
        TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
            .await
            .unwrap();
    assert_eq!(replacement_scope, Some(source_scope));
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            replacement.prune_completed(deployment, u64::MAX).unwrap(),
            2
        );
        let history = replacement
            .summaries_with_scope(deployment, replacement_scope, None, 10)
            .unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(
            history.first().unwrap().operation,
            "previous-account-interrupted"
        );
        assert!(!matches!(
            history.first().unwrap().state,
            TaskRunState::Completed | TaskRunState::Failed
        ));
    })
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {source_role}; DROP ROLE {source_role}"
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_scope_retention_accepts_foreign_configuration_without_crossing_scopes() {
    let Some(fixture) = Fixture::new().await else {
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
    tokio::task::spawn_blocking(move || {
        let foreign = Digest::of(b"previous login configuration");
        let create = |target, association: Option<Digest>, operation: &str| {
            let source = if association.is_some_and(|associated| associated != scope) {
                // A metadata adapter correctly refuses publishing another
                // namespace's binding. Import genuine signed legacy history.
                TaskEvidenceStore::from_objects(store.objects.clone())
            } else {
                store.clone()
            };
            let tracker = EmbeddedTaskTracker::from_store(source, target);
            let tracker = if let Some(scope) = association {
                tracker.with_history_scope(scope)
            } else {
                tracker
            };
            tracker
                .begin(
                    operation,
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"complete")
                .unwrap();
            let id = store
                .summaries(target, None, 100)
                .unwrap()
                .into_iter()
                .find(|summary| summary.operation == operation)
                .unwrap()
                .id;
            let suffix = format!("manifests/{id}");
            let bytes = store.read(&suffix, 65536).unwrap();
            (suffix, bytes)
        };
        let accepted = create(foreign, Some(scope), "shared-complete");
        let unbound = create(foreign, None, "unbound-foreign");
        let legacy_copy = create(foreign, Some(scope), "legacy-shared-copy");
        TaskEvidenceStore::from_objects(store.objects.clone())
            .publish(&legacy_copy.0, legacy_copy.1.as_ref().unwrap())
            .unwrap();
        let other_scope = create(
            deployment,
            Some(Digest::of(b"another metadata namespace")),
            "other-scope",
        );
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
        assert!(store.read(&accepted.0, 65536).unwrap().is_none());
        assert_eq!(store.read(&unbound.0, 65536).unwrap(), unbound.1);
        assert_eq!(store.read(&legacy_copy.0, 65536).unwrap(), legacy_copy.1);
        assert_eq!(store.read(&other_scope.0, 65536).unwrap(), other_scope.1);
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_shared_retirement_keeps_exact_manifest_and_atomic_authorization() {
    let Some(fixture) = Fixture::new().await else {
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
    let (identity, exact_manifest) = tokio::task::spawn_blocking(move || {
        let foreign = Digest::of(b"retired login configuration");
        let receipt = EmbeddedTaskTracker::from_store(prepare.clone(), foreign)
            .begin(
                "shared-retirement",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap();
        let id = prepare
            .summaries(foreign, None, 10)
            .unwrap()
            .first()
            .unwrap()
            .id
            .clone();
        let suffix = format!("manifests/{id}");
        let identity: super::super::TaskRunIdentity =
            serde_json::from_slice(&prepare.read(&suffix, 65536).unwrap().unwrap()).unwrap();
        let exact_manifest = serde_json::to_vec_pretty(&identity).unwrap();
        prepare
            .delete_artifact(&TaskEvidenceStore::key(&suffix).unwrap())
            .unwrap();
        prepare.publish(&suffix, &exact_manifest).unwrap();
        receipt.bind_history_scope(scope).unwrap();
        receipt.finish(true, b"complete").unwrap();
        assert!(prepare.begin_retirement(&identity).unwrap());
        prepare
            .delete_artifact(&TaskEvidenceStore::key(&suffix).unwrap())
            .unwrap();
        (identity, exact_manifest)
    })
    .await
    .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE FUNCTION {}.reject_retirement_delete() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN IF OLD.artifact_key = '{}retiring/{}' THEN RAISE EXCEPTION 'synthetic retirement deletion failure'; END IF;
         RETURN OLD; END $$;
         CREATE TRIGGER reject_retirement_delete BEFORE DELETE ON {}.shardline_task_artifacts_v1
         FOR EACH ROW EXECUTE FUNCTION {}.reject_retirement_delete()",
        fixture.schema, super::super::TASK_EVIDENCE_PREFIX, identity.id, fixture.schema, fixture.schema,
    ))).execute(&fixture.admin).await.unwrap();
    let failed = store.clone();
    let checked_identity = identity.clone();
    let checked_manifest = exact_manifest.clone();
    tokio::task::spawn_blocking(move || {
        assert!(failed.prune_completed(deployment, u64::MAX).is_err());
        assert_eq!(
            failed
                .read(&format!("retiring/{}", checked_identity.id), 65536)
                .unwrap(),
            Some(checked_manifest.clone())
        );
        assert!(
            failed
                .history_scope_binding_match_for_manifest(
                    &checked_identity,
                    scope,
                    &checked_manifest
                )
                .unwrap()
                .unwrap()
        );
    })
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TRIGGER reject_retirement_delete ON {}.shardline_task_artifacts_v1",
        fixture.schema,
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
        for category in ["retiring", "scope-bindings", "manifests"] {
            assert!(
                store
                    .read(&format!("{category}/{}", identity.id), 65536)
                    .unwrap()
                    .is_none()
            );
        }
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batched_retention_crosses_pages_without_losing_oldest_first_or_unfinished_history() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    let (store, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    assert!(store.postgres_metadata.is_some());
    let store = store.require_metadata_publication();
    tokio::task::spawn_blocking(move || {
        let tracker = EmbeddedTaskTracker::from_store(store.clone(), deployment);
        for _ in 0..65 {
            tracker
                .begin(
                    "finished",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"complete")
                .unwrap();
        }
        let mut ids: Vec<_> = store
            .summaries(deployment, None, 100)
            .unwrap()
            .into_iter()
            .map(|summary| summary.id)
            .collect();
        ids.sort();
        for (position, id) in ids.iter().enumerate() {
            let suffix = format!("manifests/{id}");
            let mut identity: super::super::TaskRunIdentity =
                serde_json::from_slice(&store.read(&suffix, 65536).unwrap().unwrap()).unwrap();
            identity.created_at = 65_u64
                .checked_sub(u64::try_from(position).unwrap())
                .unwrap();
            store
                .delete_artifact(&super::super::TaskEvidenceStore::key(&suffix).unwrap())
                .unwrap();
            store
                .publish(&suffix, &serde_json::to_vec(&identity).unwrap())
                .unwrap();
        }
        let unfinished = tracker
            .begin(
                "unfinished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap();
        let unfinished_id = store
            .summaries(deployment, None, 100)
            .unwrap()
            .into_iter()
            .find(|summary| summary.operation == "unfinished")
            .unwrap()
            .id;
        let unfinished_suffix = format!("manifests/{unfinished_id}");
        let original = store.read(&unfinished_suffix, 65536).unwrap();
        tracker
            .begin(
                "conflicting",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
        let conflicting_id = store
            .summaries(deployment, None, 100)
            .unwrap()
            .into_iter()
            .find(|summary| summary.operation == "conflicting")
            .unwrap()
            .id;
        let conflicting_suffix = format!("manifests/{conflicting_id}");
        let metadata_original = store.read(&conflicting_suffix, 65536).unwrap();
        TaskEvidenceStore::from_objects(store.objects.clone())
            .publish(&conflicting_suffix, b"conflicting legacy identity")
            .unwrap();
        store
            .publish("manifests/unreadable", b"invalid identity")
            .unwrap();
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 64);
        for (position, id) in ids.iter().enumerate() {
            assert_eq!(
                store
                    .read(&format!("manifests/{id}"), 65536)
                    .unwrap()
                    .is_some(),
                position == 0
            );
        }
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 1);
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
        assert!(matches!(
            store.read(&conflicting_suffix, 65536),
            Err(ServerError::InvalidContentHash)
        ));
        assert_eq!(
            store
                .read_objects(&conflicting_suffix, 65536)
                .unwrap()
                .as_deref(),
            Some(b"conflicting legacy identity".as_slice())
        );
        assert_eq!(
            store
                .postgres_metadata
                .as_ref()
                .unwrap()
                .read(
                    TaskEvidenceStore::key(&conflicting_suffix)
                        .unwrap()
                        .as_str(),
                    65536
                )
                .unwrap()
                .unwrap()
                .bytes,
            metadata_original
        );
        assert_eq!(store.read(&unfinished_suffix, 65536).unwrap(), original);
        assert_eq!(
            store
                .read("manifests/unreadable", 65536)
                .unwrap()
                .as_deref(),
            Some(b"invalid identity".as_slice())
        );
        drop(unfinished);
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_policies_disable_optional_history_without_stopping_startup_work() {
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use tower::ServiceExt;
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
    let seed = store.clone();
    tokio::task::spawn_blocking(move || seed.publish("rls-original", b"existing history"))
        .await
        .unwrap()
        .unwrap();
    for statement in [
        format!(
            "ALTER TABLE {}.shardline_task_artifacts_v1 ENABLE ROW LEVEL SECURITY",
            fixture.schema
        ),
        format!(
            "ALTER TABLE {}.shardline_task_artifacts_v1 FORCE ROW LEVEL SECURITY",
            fixture.schema
        ),
        format!(
            "CREATE POLICY deny_history ON {}.shardline_task_artifacts_v1 USING (false) WITH CHECK (false)",
            fixture.schema
        ),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&fixture.admin)
            .await
            .unwrap();
    }
    let reader = store.clone();
    tokio::task::spawn_blocking(move || {
        assert!(reader.read("rls-original", 100).is_err());
        assert!(
            reader
                .publish("rls-new", b"must not bypass policy")
                .is_err()
        );
    })
    .await
    .unwrap();
    let (unsupported, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    assert!(scope.is_none());
    assert!(!unsupported.uses_metadata_history());

    let sessions = fixture.config.root_dir().join("tmp/lfs-patch");
    std::fs::create_dir_all(&sessions).unwrap();
    let oid = "01".repeat(32);
    let staged = sessions.join(&oid);
    let touched = sessions.join(format!("{oid}.meta"));
    std::fs::write(&staged, b"expired legacy staging").unwrap();
    std::fs::write(&touched, b"0").unwrap();
    let config = fixture
        .config
        .clone()
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"owned-rls-admin".to_vec())
        .unwrap();
    let app = crate::app::router(config).await.unwrap();
    assert!(
        !staged.exists(),
        "optional history must not prevent the original sweep"
    );
    assert!(!touched.exists());
    for path in ["/api/v1/status", "/api/v1/tasks/maintenance"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("Authorization", "Bearer owned-rls-admin")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success(), "{path}");
        if path == "/api/v1/tasks/maintenance" {
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap())
                    .unwrap();
            assert_eq!(body["history_grouping"], "legacy");
            assert!(body["tasks"].as_array().unwrap().is_empty());
            assert!(
                body["history_notice"]
                    .as_str()
                    .unwrap()
                    .contains("an empty list does not prove maintenance did not run")
            );
        }
    }
    assert!(
        fixture
            .backend
            .object_store()
            .list_prefix(&ObjectPrefix::parse("_shardline_tasks/v1/").unwrap())
            .unwrap()
            .is_empty()
    );
    let rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_task_artifacts_v1",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(
        rows, 1,
        "unavailable tracking must neither overwrite nor claim completion"
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {}.shardline_task_artifacts_v1 DISABLE ROW LEVEL SECURITY",
        fixture.schema
    )))
    .execute(&fixture.admin)
    .await
    .unwrap();
    let (restored, _) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || {
        assert_eq!(
            restored.read("rls-original", 100).unwrap(),
            Some(b"existing history".to_vec())
        );
        assert!(restored.read("rls-new", 100).unwrap().is_none());
    })
    .await
    .unwrap();
    drop(app);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_relocation_preserves_scope_associations_and_exact_history() {
    let Some(fixture) = Fixture::new().await else {
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
    let legacy = TaskEvidenceStore::from_objects(fixture.backend.object_store());
    tokio::task::spawn_blocking(move || {
        for (operation, legacy_deployment, history_scope) in [
            ("same-login", deployment, None),
            ("shared-login", Digest::of(b"another login"), Some(scope)),
            (
                "other-work-target",
                deployment,
                Some(Digest::of(b"another database/schema")),
            ),
        ] {
            let mut tracker = EmbeddedTaskTracker::from_store(legacy.clone(), legacy_deployment);
            if let Some(scope) = history_scope {
                tracker = tracker.with_history_scope(scope);
            }
            tracker
                .begin(
                    operation,
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap()
                .finish(true, b"original report")
                .unwrap();
        }
        let before: std::collections::BTreeMap<_, _> = legacy
            .objects
            .list_prefix(&ObjectPrefix::parse(super::super::TASK_EVIDENCE_PREFIX).unwrap())
            .unwrap()
            .into_iter()
            .map(|artifact| {
                let suffix = artifact
                    .key()
                    .as_str()
                    .strip_prefix(super::super::TASK_EVIDENCE_PREFIX)
                    .unwrap()
                    .to_owned();
                let bytes = legacy.read(&suffix, artifact.length()).unwrap().unwrap();
                (suffix, bytes)
            })
            .collect();
        assert!(store.relocate_finished_history(deployment).unwrap() > 0);
        let history = store
            .summaries_with_scope(deployment, Some(scope), None, 10)
            .unwrap();
        assert_eq!(history.len(), 2);
        assert!(
            history
                .iter()
                .all(|task| task.state == TaskRunState::Completed)
        );
        let legacy_history = legacy.summaries(deployment, None, 10).unwrap();
        assert_eq!(legacy_history.len(), 1);
        assert_eq!(legacy_history[0].operation, "other-work-target");
        for (suffix, bytes) in before {
            assert_eq!(
                store
                    .read(&suffix, u64::try_from(bytes.len()).unwrap())
                    .unwrap(),
                Some(bytes)
            );
        }
        assert_eq!(store.relocate_finished_history(deployment).unwrap(), 0);
    })
    .await
    .unwrap();
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_metadata_preserves_legacy_history_and_retention() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let deployment = deployment_fingerprint(&fixture.config).unwrap();
    // Ordinary startup before an owner upgrade never installs tables or breaks
    // the existing evidence writer. Its legacy run remains readable afterward.
    let (legacy, legacy_scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    assert!(legacy.postgres_metadata.is_none());
    assert!(legacy_scope.is_none());
    tokio::task::spawn_blocking(move || {
        EmbeddedTaskTracker::from_store(legacy, deployment)
            .begin(
                "legacy-server",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"old report")
            .unwrap();
    })
    .await
    .unwrap();
    let absent: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NULL")
        .bind(format!("{}.shardline_task_artifacts_v1", fixture.schema))
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
    assert!(absent);
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let (store, scope) = TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
        .await
        .unwrap();
    let scope = scope.unwrap();
    assert!(store.postgres_metadata.is_some());
    let writer = store.clone();
    tokio::task::spawn_blocking(move || {
        EmbeddedTaskTracker::from_store(writer, deployment)
            .with_history_scope(scope)
            .begin(
                "metadata-server",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"new verified report")
            .unwrap();
    })
    .await
    .unwrap();
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_task_artifacts_v1",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert!(count > 0);
    assert_eq!(fixture.index().pool().size(), 1);
    fixture.index().pool().close().await;
    let other_backend = ServerBackend::from_config(&fixture.config).await.unwrap();
    let (reader, reopened_scope) = TaskEvidenceStore::from_backend(&fixture.config, &other_backend)
        .await
        .unwrap();
    assert_eq!(reopened_scope, Some(scope));
    tokio::task::spawn_blocking(move || {
        let history = reader
            .summaries_with_scope(deployment, Some(scope), None, 10)
            .unwrap();
        assert_eq!(
            reader
                .inspect_summaries_with_scope(deployment, Some(scope), None, 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(history.len(), 2);
        assert!(
            history
                .iter()
                .all(|run| run.state == TaskRunState::Completed)
        );
        let new_run = history
            .iter()
            .find(|run| run.operation == "metadata-server")
            .unwrap();
        assert_eq!(
            reader
                .embedded_report(deployment, Some(scope), &new_run.id)
                .unwrap()
                .unwrap()["report"],
            "new verified report"
        );
        // Only the legacy manifest is present in payload storage.
        let prefix = ObjectPrefix::parse("_shardline_tasks/v1/manifests/").unwrap();
        assert_eq!(reader.objects.list_prefix(&prefix).unwrap().len(), 1);
        let suffix = format!("manifests/{}", new_run.id);
        let manifest = reader.read(&suffix, 65536).unwrap().unwrap();
        assert!(matches!(
            reader.read(&suffix, 0),
            Err(ServerError::RequestBodyTooLarge)
        ));
        assert!(reader.publish(&suffix, b"conflicting bytes").is_err());
        assert_eq!(reader.read(&suffix, 65536).unwrap(), Some(manifest));
        reader.prune_completed(deployment, u64::MAX).unwrap();
        assert!(reader.summaries(deployment, None, 10).unwrap().is_empty());
        assert!(
            reader
                .objects
                .list_prefix(&ObjectPrefix::parse("_shardline_tasks/v1/").unwrap())
                .unwrap()
                .is_empty()
        );
    })
    .await
    .unwrap();
    let remaining: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_task_artifacts_v1",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert_eq!(remaining, 0);
    assert_eq!(fixture.index().pool().size(), 0);
    if let ServerBackend::Postgres(backend) = other_backend {
        backend.index_store().pool().close().await;
    }
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changed_metadata_identity_never_redirects_evidence_or_falls_back_to_payloads() {
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
    assert!(scope.is_some());
    let original: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT resource FROM {}.shardline_resource_fences WHERE domain='maintenance-task-history-scope'", fixture.schema
    ))).fetch_one(&fixture.admin).await.unwrap();
    let replacement = if original == "00".repeat(32) {
        "01".repeat(32)
    } else {
        "00".repeat(32)
    };
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {}.shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'", fixture.schema
    ))).bind(replacement).execute(&fixture.admin).await.unwrap();
    let reader = store.clone();
    tokio::task::spawn_blocking(move || {
        assert!(
            reader
                .publish("manifests/changed-target", b"must not publish")
                .is_err()
        );
        assert!(reader.read("manifests/changed-target", 100).is_err());
        assert!(
            reader
                .objects
                .list_prefix(&ObjectPrefix::parse("_shardline_tasks/v1/").unwrap())
                .unwrap()
                .is_empty()
        );
    })
    .await
    .unwrap();
    assert!(
        fixture
            .index()
            .read_task_artifact("_shardline_tasks/v1/manifests/changed-target", 100)
            .await
            .unwrap()
            .is_none()
    );
    fixture.index().pool().close().await;
    assert!(
        TaskEvidenceStore::from_backend(&fixture.config, &fixture.backend)
            .await
            .is_err()
    );
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_router_publishes_and_serves_authenticated_metadata_history() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_task_history_scope()
        .await
        .unwrap();
    let config = fixture
        .config
        .clone()
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"owned-task-admin".to_vec())
        .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .header("Authorization", "Bearer owned-task-admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 65536).await.unwrap()).unwrap();
    assert_eq!(body["history_grouping"], "shared_metadata");
    assert!(body["history_notice"].is_null());
    let startup = body["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["operation"] == "lfs-startup-session-sweep")
        .unwrap();
    assert_eq!(startup["state"], "completed");
    let id = startup["id"].as_str().unwrap();
    let report_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/tasks/maintenance/{id}/report"))
                .header("Authorization", "Bearer owned-task-admin")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(report_response.status(), StatusCode::OK);
    let report: serde_json::Value =
        serde_json::from_slice(&to_bytes(report_response.into_body(), 65536).await.unwrap())
            .unwrap();
    assert_eq!(report["task"]["operation"], "lfs-startup-session-sweep");
    assert_eq!(report["task"]["state"], "completed");
    assert_eq!(report["task"]["report_available"], true);
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_task_artifacts_v1",
        fixture.schema
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    assert!(count > 0);
    assert!(
        fixture
            .backend
            .object_store()
            .list_prefix(&ObjectPrefix::parse("_shardline_tasks/v1/").unwrap())
            .unwrap()
            .is_empty()
    );
    drop(app);
    fixture.finish().await;
}
