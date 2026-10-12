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
