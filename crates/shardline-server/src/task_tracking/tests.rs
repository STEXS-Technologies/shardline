#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use shardline_reliability::MaintenanceArtifactKind;
use std::num::NonZeroUsize;

fn config(root: &std::path::Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

#[test]
fn history_scope_bindings_preserve_legacy_results_and_reject_substitution() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let scope = penelope_core::task::Digest::of(b"resolved scope");
    let other_scope = penelope_core::task::Digest::of(b"other scope");
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment)
        .with_history_scope(scope);
    let receipt = tracker
        .begin(
            "bound-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let summary = store.summaries(deployment, None, 10).unwrap();
    let id = &summary[0].id;
    let manifest = store
        .read(&format!("manifests/{id}"), 65536)
        .unwrap()
        .unwrap();
    let identity: TaskRunIdentity = serde_json::from_slice(&manifest).unwrap();
    let binding = store
        .read(&format!("scope-bindings/{id}"), 2048)
        .unwrap()
        .unwrap();
    assert_eq!(identity.deployment, deployment);
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    assert!(
        !store
            .history_scope_binding_matches(&identity, other_scope)
            .unwrap()
    );
    receipt.bind_history_scope(scope).unwrap();
    assert_eq!(
        store
            .read(&format!("scope-bindings/{id}"), 2048)
            .unwrap()
            .unwrap(),
        binding
    );
    assert!(receipt.bind_history_scope(other_scope).is_err());
    assert!(
        store
            .publish_history_scope_binding(
                &identity,
                &ed25519_dalek::SigningKey::from_bytes(&[1; 32]),
                scope
            )
            .is_err()
    );
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    receipt.finish(true, b"done").unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Completed
    );
    assert_eq!(
        store
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .unwrap(),
        manifest
    );
    let binding_path = root
        .path()
        .join("chunks/_shardline_tasks/v1/scope-bindings")
        .join(id);
    let mut tampered: serde_json::Value = serde_json::from_slice(&binding).unwrap();
    tampered["scope"] = serde_json::to_value(other_scope).unwrap();
    std::fs::write(&binding_path, serde_json::to_vec(&tampered).unwrap()).unwrap();
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .is_err()
    );
    assert!(
        store
            .history_scope_binding_matches(&identity, other_scope)
            .is_err()
    );
    std::fs::write(&binding_path, &binding).unwrap();
    let mut forged = identity.clone();
    forged.operation.push_str("-forged");
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/manifests")
            .join(id),
        serde_json::to_vec(&forged).unwrap(),
    )
    .unwrap();
    assert!(store.history_scope_binding_matches(&forged, scope).is_err());
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/manifests")
            .join(id),
        &manifest,
    )
    .unwrap();
    assert!(
        store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    std::fs::remove_file(&binding_path).unwrap();
    assert!(
        !store
            .history_scope_binding_matches(&identity, scope)
            .unwrap()
    );
    std::fs::write(&binding_path, &binding).unwrap();
}

#[test]
fn embedded_tasks_publish_verified_success_and_truthful_failure() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "test-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let before = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].state, TaskRunState::Running);
    assert_eq!(before[0].execution, TaskExecutionMode::Embedded);
    receipt
        .finish(true, b"inspected: 4; upgraded: 2; failures: 0")
        .unwrap();
    let failed = tracker
        .begin(
            "incomplete-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    failed.finish(false, b"failures: 1").unwrap();
    let after = store.summaries(deployment, None, 10).unwrap();
    assert!(
        after
            .iter()
            .any(|task| task.state == TaskRunState::Completed && task.operation == "test-sweep")
    );
    assert!(
        after
            .iter()
            .any(|task| task.state == TaskRunState::NeedsReconciliation
                && task.operation == "incomplete-sweep")
    );
    assert!(
        store
            .summaries(
                penelope_core::task::Digest::of(b"other-deployment"),
                None,
                10
            )
            .unwrap()
            .is_empty()
    );
}

#[test]
fn background_report_tampering_and_record_gaps_are_visible() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "test-sweep",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    receipt.finish(true, b"report").unwrap();
    let summary = store.summaries(deployment, None, 10).unwrap();
    let run = &summary[0].id;
    std::fs::write(
        root.path()
            .join("chunks/_shardline_tasks/v1/runs")
            .join(run)
            .join("artifacts")
            .join(hex::encode(penelope_core::task::Digest::of(b"report").0)),
        b"edited",
    )
    .unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Corrupt
    );
    std::fs::remove_file(
        root.path()
            .join("chunks/_shardline_tasks/v1/runs")
            .join(run)
            .join("records/0000000001"),
    )
    .unwrap();
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::Corrupt
    );
}

#[test]
fn dropping_an_unfinished_background_job_never_leaves_a_live_success_claim() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    let receipt = tracker
        .begin(
            "interrupted",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    drop(receipt);
    assert_eq!(
        store.summaries(deployment, None, 10).unwrap()[0].state,
        TaskRunState::NeedsReconciliation
    );
}

#[test]
fn evidence_retention_removes_only_verified_finished_runs() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment)
        .with_history_scope(penelope_core::task::Digest::of(b"retention scope"));
    tracker
        .begin(
            "finished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"complete")
        .unwrap();
    let unfinished = tracker
        .begin(
            "unfinished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    let corrupt = tracker
        .begin(
            "corrupt",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap();
    corrupt.finish(true, b"original").unwrap();
    let corrupt_id = store
        .summaries(deployment, None, 10)
        .unwrap()
        .into_iter()
        .find(|summary| summary.operation == "corrupt")
        .unwrap()
        .id;
    std::fs::write(
        root.path().join(format!(
            "chunks/_shardline_tasks/v1/runs/{corrupt_id}/artifacts/{}",
            hex::encode(penelope_core::task::Digest::of(b"original").0)
        )),
        b"tampered",
    )
    .unwrap();
    assert_eq!(
        store
            .prune_completed(deployment, shardline_protocol::unix_now_seconds_lossy())
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .prune_completed(
                deployment,
                shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60)
            )
            .unwrap(),
        1
    );
    let remaining = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().all(|task| task.operation != "finished"));
    for task in &remaining {
        assert!(
            store
                .read(&format!("scope-bindings/{}", task.id), 2048)
                .unwrap()
                .is_some()
        );
    }
    let prefix =
        shardline_storage::ObjectPrefix::parse("_shardline_tasks/v1/scope-bindings/").unwrap();
    use shardline_storage::ObjectStore as _;
    assert_eq!(store.objects.list_prefix(&prefix).unwrap().len(), 2);
    drop(unfinished);
}

#[test]
fn interrupted_evidence_cleanup_resumes_without_replaying_work() {
    use penelope_core::task::Digest;
    use shardline_storage::{ObjectPrefix, ObjectStore};
    // Crash after the marker, after hiding status, during artifact deletion,
    // and after all run objects were deleted but before clearing the marker.
    for boundary in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let deployment = deployment_fingerprint(&config).unwrap();
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        let objects = object_store_from_config(&config).unwrap();
        let tracker = EmbeddedTaskTracker::new(objects.clone(), deployment);
        tracker
            .begin(
                "finished",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
        let id = store.summaries(deployment, None, 10).unwrap()[0].id.clone();
        let identity: TaskRunIdentity = serde_json::from_slice(
            &store
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(store.begin_retirement(&identity).unwrap());
        let run_prefix = ObjectPrefix::parse(&format!("{TASK_EVIDENCE_PREFIX}runs/{id}/")).unwrap();
        if boundary >= 1 {
            std::fs::remove_file(
                root.path()
                    .join(format!("chunks/{TASK_EVIDENCE_PREFIX}manifests/{id}")),
            )
            .unwrap();
        }
        if boundary >= 2 {
            std::fs::remove_file(root.path().join(format!(
                "chunks/{TASK_EVIDENCE_PREFIX}runs/{id}/artifacts/{}",
                hex::encode(penelope_core::task::Digest::of(b"complete").0)
            )))
            .unwrap();
        }
        if boundary >= 3 {
            objects
                .visit_prefix(&run_prefix, |metadata| {
                    objects.delete_if_present(metadata.key()).unwrap();
                    Ok::<(), ServerError>(())
                })
                .unwrap();
        }
        let reopened = TaskEvidenceStore::from_config(&config).unwrap();
        let expired = identity.created_at.saturating_add(31 * 24 * 60 * 60);
        assert_eq!(
            reopened
                .prune_completed(Digest::of(b"another-deployment"), expired)
                .unwrap(),
            0
        );
        assert_eq!(reopened.prune_completed(deployment, expired).unwrap(), 1);
        assert!(reopened.summaries(deployment, None, 10).unwrap().is_empty());
        assert!(
            reopened
                .read(&format!("retiring/{id}"), 65536)
                .unwrap()
                .is_none()
        );
        let mut remaining = 0;
        objects
            .visit_prefix(&run_prefix, |_metadata| {
                remaining += 1;
                Ok::<(), ServerError>(())
            })
            .unwrap();
        assert_eq!(remaining, 0);
        assert_eq!(reopened.prune_completed(deployment, expired).unwrap(), 0);
    }
}

#[test]
fn unfinished_history_does_not_starve_finished_history_retention() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    for _ in 0..65 {
        drop(
            tracker
                .begin(
                    "unfinished",
                    b"input",
                    MaintenanceArtifactKind::LifecycleRepair,
                )
                .unwrap(),
        );
    }
    for summary in store.summaries(deployment, None, 100).unwrap() {
        let path = root.path().join(format!(
            "chunks/{TASK_EVIDENCE_PREFIX}manifests/{}",
            summary.id
        ));
        let mut identity: TaskRunIdentity =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        identity.created_at = 0;
        std::fs::write(path, serde_json::to_vec(&identity).unwrap()).unwrap();
    }
    tracker
        .begin(
            "finished",
            b"input",
            MaintenanceArtifactKind::LifecycleRepair,
        )
        .unwrap()
        .finish(true, b"complete")
        .unwrap();
    let now = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60);
    assert_eq!(store.prune_completed(deployment, now).unwrap(), 1);
    let remaining = store.summaries(deployment, None, 100).unwrap();
    assert_eq!(remaining.len(), 65);
    assert!(remaining.iter().all(|task| task.operation == "unfinished"));
}

#[test]
fn credential_source_changes_do_not_change_a_shared_deployment_target() {
    use shardline_protocol::SecretString;
    let root = tempfile::tempdir().unwrap();
    let s3 = shardline_storage::S3ObjectStoreConfig::new("bucket".to_owned(), "region".to_owned());
    let iam =
        config(root.path()).with_object_storage(crate::ObjectStorageAdapter::S3, Some(s3.clone()));
    let explicit = config(root.path()).with_object_storage(
        crate::ObjectStorageAdapter::S3,
        Some(s3.with_credentials(
            Some(SecretString::new("access".to_owned())),
            Some(SecretString::new("secret".to_owned())),
            None,
        )),
    );
    assert_eq!(
        deployment_fingerprint(&iam).unwrap(),
        deployment_fingerprint(&explicit).unwrap()
    );
}

#[test]
fn postgres_password_rotation_preserves_deployment_identity() {
    let root = tempfile::tempdir().unwrap();
    let initial = config(root.path())
        .with_index_postgres_url("postgres://service:old@localhost/shardline".to_owned())
        .unwrap();
    let rotated = config(root.path())
        .with_index_postgres_url("postgres://service:new@localhost/shardline".to_owned())
        .unwrap();
    let other = config(root.path())
        .with_index_postgres_url("postgres://service:new@localhost/another".to_owned())
        .unwrap();
    assert_eq!(
        deployment_fingerprint(&initial).unwrap(),
        deployment_fingerprint(&rotated).unwrap()
    );
    assert_ne!(
        deployment_fingerprint(&initial).unwrap(),
        deployment_fingerprint(&other).unwrap()
    );
}

#[tokio::test]
async fn postgres_logins_can_target_distinct_default_schemas_with_identical_url_options() {
    use std::str::FromStr;
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let migration_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    crate::apply_database_migrations(&migration_pool)
        .await
        .unwrap();
    migration_pool.close().await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .unwrap();
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let first_role = format!("sl_history_{}_{}_a", std::process::id(), nonce);
    let second_role = format!("sl_history_{}_{}_b", std::process::id(), nonce);
    // These identifiers contain only generated ASCII letters, digits and '_'.
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {first_role} LOGIN PASSWORD 'owned_history_fixture';
         CREATE ROLE {second_role} LOGIN PASSWORD 'owned_history_fixture';
         CREATE SCHEMA {first_role} AUTHORIZATION {first_role};
         CREATE SCHEMA {second_role} AUTHORIZATION {second_role};
         CREATE TABLE {first_role}.shardline_resource_fences (LIKE public.shardline_resource_fences INCLUDING ALL);
         CREATE TABLE {second_role}.shardline_resource_fences (LIKE public.shardline_resource_fences INCLUDING ALL);"
    )))
    .execute(&pool)
    .await
    .unwrap();
    for schema in [&first_role, &second_role] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET search_path TO {schema}, public"
        )))
        .execute(&pool)
        .await
        .unwrap();
        shardline_index::PostgresIndexStore::new(pool.clone())
            .initialize_task_history_scope()
            .await
            .unwrap();
        let identity: String =
            sqlx::query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&pool)
                .await
                .unwrap();
        shardline_index::PostgresIndexStore::new(pool.clone())
            .initialize_task_history_scope()
            .await
            .unwrap();
        let reinstalled: String =
            sqlx::query_scalar("SELECT shardline_task_history_scope_v1(current_schema())")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(identity, reinstalled);
    }
    sqlx::query("RESET search_path")
        .execute(&pool)
        .await
        .unwrap();
    let options = sqlx::postgres::PgConnectOptions::from_str(&database_url).unwrap();
    let first_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .clone()
                .username(&first_role)
                .password("owned_history_fixture"),
        )
        .await
        .unwrap();
    let second_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            options
                .username(&second_role)
                .password("owned_history_fixture"),
        )
        .await
        .unwrap();
    let first_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&first_pool)
        .await
        .unwrap();
    let second_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(&second_pool)
        .await
        .unwrap();
    assert_eq!(first_schema, first_role);
    assert_eq!(second_schema, second_role);
    let root = tempfile::tempdir().unwrap();
    let mut first_url = url::Url::parse(&database_url).unwrap();
    first_url.set_username(&first_role).unwrap();
    first_url
        .set_password(Some("owned_history_fixture"))
        .unwrap();
    let mut second_url = first_url.clone();
    second_url.set_username(&second_role).unwrap();
    let first = config(root.path())
        .with_index_postgres_url(first_url.to_string())
        .unwrap();
    let second = config(root.path())
        .with_index_postgres_url(second_url.to_string())
        .unwrap();
    assert_ne!(
        deployment_fingerprint(&first).unwrap(),
        deployment_fingerprint(&second).unwrap()
    );
    let legacy_first = deployment_fingerprint(&first).unwrap();
    let legacy_second = deployment_fingerprint(&second).unwrap();
    let mut first_connection = first_pool.acquire().await.unwrap();
    let mut second_connection = second_pool.acquire().await.unwrap();
    let first_scope = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    let second_scope = resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
        .await
        .unwrap();
    assert_ne!(first_scope, second_scope);
    assert!(
        sqlx::query("SELECT * FROM shardline_resource_fences")
            .execute(&mut *first_connection)
            .await
            .is_err()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {first_role}.shardline_resource_fences ENABLE ROW LEVEL SECURITY"
    )))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
            .await
            .is_err()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {first_role}.shardline_resource_fences DISABLE ROW LEVEL SECURITY"
    )))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("SET search_path TO public")
        .execute(&mut *first_connection)
        .await
        .unwrap();
    sqlx::query("SET search_path TO public")
        .execute(&mut *second_connection)
        .await
        .unwrap();
    let shared_scope = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    assert_eq!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
            .await
            .unwrap()
    );
    assert_ne!(shared_scope, first_scope);
    assert_ne!(shared_scope, second_scope);
    assert_ne!(shared_scope, legacy_first);
    // Separate legacy logins retain their original views while new signed
    // bindings can associate their runs with the same resolved metadata scope.
    for role_config in [&first, &second] {
        let legacy = deployment_fingerprint(role_config).unwrap();
        let role_store = TaskEvidenceStore::from_config(role_config).unwrap();
        EmbeddedTaskTracker::new(object_store_from_config(role_config).unwrap(), legacy)
            .with_history_scope(shared_scope)
            .begin(
                "bound-role-job",
                b"input",
                MaintenanceArtifactKind::LifecycleRepair,
            )
            .unwrap()
            .finish(true, b"role report")
            .unwrap();
        let own_history = role_store.summaries(legacy, None, 10).unwrap();
        assert_eq!(own_history.len(), 1);
        let role_manifest: TaskRunIdentity = serde_json::from_slice(
            &role_store
                .read(&format!("manifests/{}", own_history[0].id), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            role_store
                .history_scope_binding_matches(&role_manifest, shared_scope)
                .unwrap()
        );
        assert_eq!(own_history[0].state, TaskRunState::Completed);
    }
    // Simulate another initialized backend behind the same endpoint with the
    // same database/schema names. Only its durable namespace identity differs.
    let original_identity: String =
        sqlx::query_scalar("SELECT public.shardline_task_history_scope_v1('public')")
            .fetch_one(&pool)
            .await
            .unwrap();
    let replacement_identity = if original_identity == "00".repeat(32) {
        "01".repeat(32)
    } else {
        "00".repeat(32)
    };
    sqlx::query("UPDATE public.shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(replacement_identity).execute(&pool).await.unwrap();
    let other_backend = resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
        .await
        .unwrap();
    sqlx::query("UPDATE public.shardline_resource_fences SET resource=$1 WHERE domain='maintenance-task-history-scope'")
        .bind(original_identity).execute(&pool).await.unwrap();
    assert_ne!(other_backend, shared_scope);
    assert_eq!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&first, &mut first_connection)
            .await
            .unwrap()
    );
    assert_eq!(deployment_fingerprint(&first).unwrap(), legacy_first);
    assert_eq!(deployment_fingerprint(&second).unwrap(), legacy_second);
    let other_root = tempfile::tempdir().unwrap();
    let other_target = config(other_root.path())
        .with_index_postgres_url(second.index_postgres_url().unwrap().to_owned())
        .unwrap();
    assert_ne!(
        shared_scope,
        resolved_postgres_deployment_fingerprint(&other_target, &mut second_connection)
            .await
            .unwrap()
    );
    assert!(
        resolved_postgres_deployment_fingerprint(&config(root.path()), &mut second_connection)
            .await
            .is_err()
    );
    sqlx::query("SET search_path TO nonexistent_history_scope")
        .execute(&mut *second_connection)
        .await
        .unwrap();
    assert!(
        resolved_postgres_deployment_fingerprint(&second, &mut second_connection)
            .await
            .is_err()
    );
    drop(first_connection);
    drop(second_connection);
    first_pool.close().await;
    second_pool.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA {first_role} CASCADE; DROP SCHEMA {second_role} CASCADE;
         DROP ROLE {first_role}; DROP ROLE {second_role};"
    )))
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
}

#[tokio::test]
async fn normal_router_starts_tracker_and_exposes_authenticated_job_status() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path())
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"test-admin-token".to_vec())
        .unwrap();
    let app = crate::app::router(config).await.unwrap();
    let unauthenticated = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .header("Authorization", "Bearer test-admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["supervision"], "embedded");
    let jobs = body["tasks"].as_array().unwrap();
    assert!(
        jobs.iter()
            .any(|job| job["operation"] == "lfs-startup-session-sweep"
                && job["state"] == "completed"
                && job["execution"] == "embedded"
                && job["owner"]["pid"] == std::process::id())
    );
}

#[tokio::test]
async fn corrupt_manifests_remain_visible_without_hiding_healthy_jobs() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path())
        .with_server_frontends([crate::ServerFrontend::Lfs])
        .unwrap()
        .with_admin_read_token(b"test-admin-token".to_vec())
        .unwrap();
    let app = crate::app::router(config.clone()).await.unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let tracker = EmbeddedTaskTracker::new(object_store_from_config(&config).unwrap(), deployment);
    for _ in 0..4 {
        tracker
            .begin("test-sweep", b"input", MaintenanceArtifactKind::Fsck)
            .unwrap()
            .finish(true, b"complete")
            .unwrap();
    }
    let original = store.summaries(deployment, None, 100).unwrap();
    assert!(original.len() >= 4);
    for (index, job) in original.iter().take(3).enumerate() {
        let path = root
            .path()
            .join(format!("chunks/{TASK_EVIDENCE_PREFIX}manifests/{}", job.id));
        let bytes = match index {
            0 => b"broken JSON".to_vec(),
            1 => {
                let mut identity: TaskRunIdentity =
                    serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
                identity.schema = 999;
                serde_json::to_vec(&identity).unwrap()
            }
            _ => vec![b'x'; 65537],
        };
        std::fs::write(path, bytes).unwrap();
    }
    // Existing library callers still receive an error rather than incomplete
    // typed summaries. The admin view can explicitly represent unknown identity.
    assert!(store.summaries(deployment, None, 100).is_err());
    let mut seen = Vec::new();
    let mut cursor = None;
    for index in 0..original.len() {
        let uri = cursor.as_ref().map_or_else(
            || "/api/v1/tasks/maintenance?limit=1".to_owned(),
            |cursor| format!("/api/v1/tasks/maintenance?limit=1&cursor={cursor}"),
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("Authorization", "Bearer test-admin-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), 65536).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let jobs = body["tasks"].as_array().unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(body["page"]["returned"], 1);
        let job = &jobs[0];
        seen.push(job["id"].as_str().unwrap().to_owned());
        if index < 3 {
            assert_eq!(job["state"], "corrupt");
            assert_eq!(job["identity_verified"], false);
            for field in ["owner", "operation", "execution", "created_at"] {
                assert!(job.get(field).is_none());
            }
        } else {
            assert_eq!(job["state"], "completed");
        }
        cursor = body["page"]["next_cursor"].as_str().map(str::to_owned);
        assert_eq!(cursor.is_some(), index + 1 < original.len());
    }
    assert_eq!(
        seen,
        original
            .iter()
            .map(|job| job.id.clone())
            .collect::<Vec<_>>()
    );
    // Corrupt identities never become evidence-retention authorization.
    let expired = shardline_protocol::unix_now_seconds_lossy().saturating_add(31 * 24 * 60 * 60);
    assert_eq!(
        store.prune_completed(deployment, expired).unwrap(),
        original.len() - 3
    );
    assert_eq!(
        store
            .inspect_summaries(deployment, None, 100)
            .unwrap()
            .len(),
        3
    );
    // Losing access to the storage namespace is a request failure, not a
    // collection of corrupt jobs or a reassuring empty history.
    let chunks = root.path().join("chunks");
    std::fs::rename(&chunks, root.path().join("chunks-backup")).unwrap();
    std::fs::write(&chunks, b"not a directory").unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/tasks/maintenance")
                .header("Authorization", "Bearer test-admin-token")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.status().is_server_error());
}
