#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use crate::{
    ServerObjectStore,
    task_tracking::{
        TaskEvidenceStore, TaskRunIdentity, TaskRunState, deployment_fingerprint,
        resolved_postgres_deployment_fingerprint,
    },
};
use shardline_index::hub::HubRepoType;
use shardline_protocol::SecretBytes;
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread")]
async fn local_upgrade_preserves_encrypted_secret_when_final_history_recording_fails() {
    for denied_suffix in ["artifacts/*", "records/0000000002"] {
        let root = tempfile::tempdir().unwrap();
        shardline_index::hub::ensure_hub_tables(root.path()).unwrap();
        let store = BoxedHubStore::from_store(shardline_index::LocalIndexStore::open(
            root.path().to_owned(),
        ));
        store
            .create_repo(HubRepoType::Model, "work/local", false)
            .unwrap();
        store
            .create_webhook(
                "work/local",
                "https://example.com/hook",
                &["push".to_owned()],
                Some("original-plaintext"),
            )
            .unwrap();
        let config = ServerConfig::new(
            "127.0.0.1:8080".parse().unwrap(),
            "http://localhost:8080".to_owned(),
            root.path().to_owned(),
            NonZeroUsize::new(4096).unwrap(),
        );
        let deployment = deployment_fingerprint(&config).unwrap();
        let evidence = TaskEvidenceStore::from_config(&config).unwrap();
        evidence
            .publish("runs/fixture/blobs/seed", b"original")
            .unwrap();
        let database = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        database
            .execute_batch(&format!(
                "CREATE TRIGGER fixture_reject_final BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/{denied_suffix}'
             BEGIN SELECT RAISE(ABORT, 'fixture denies final history'); END;"
            ))
            .unwrap();
        let tracker = EmbeddedTaskTracker::from_store(evidence.clone(), deployment);
        let cipher = WebhookSecretCipher::new(SecretBytes::new(vec![41; 32])).unwrap();
        let work_config = config.clone();
        let work_store = store.clone();
        let work_tracker = tracker.clone();
        let work_cipher = cipher.clone();
        let report = tokio::task::spawn_blocking(move || {
            run(&work_config, &work_store, &work_cipher, &work_tracker)
        })
        .await
        .unwrap();
        assert_eq!(
            (report.inspected, report.upgraded, report.failures),
            (1, 1, 0)
        );
        let hooks = store.list_webhooks("work/local").unwrap();
        let encrypted = hooks
            .first()
            .unwrap()
            .secret
            .as_ref()
            .unwrap()
            .expose_secret()
            .to_owned();
        assert_ne!(encrypted, "original-plaintext");
        let decrypted = cipher.decrypt("work/local", &encrypted).unwrap();
        assert_eq!(decrypted.secret.expose_secret(), "original-plaintext");
        assert!(!decrypted.needs_upgrade);
        let history = evidence.summaries(deployment, None, 10).unwrap();
        assert_eq!(history.len(), 1);
        let first_id = history.first().unwrap().id.clone();
        assert!(!matches!(
            history.first().unwrap().state,
            TaskRunState::Completed | TaskRunState::Failed
        ));
        assert_eq!(evidence.prune_completed(deployment, u64::MAX).unwrap(), 0);
        database
            .execute_batch("DROP TRIGGER fixture_reject_final")
            .unwrap();
        let work_store = store.clone();
        let repeated =
            tokio::task::spawn_blocking(move || run(&config, &work_store, &cipher, &tracker))
                .await
                .unwrap();
        assert_eq!(
            (repeated.inspected, repeated.upgraded, repeated.failures),
            (1, 0, 0)
        );
        let hooks = store.list_webhooks("work/local").unwrap();
        assert_eq!(
            hooks
                .first()
                .unwrap()
                .secret
                .as_ref()
                .unwrap()
                .expose_secret(),
            encrypted
        );
        let history = evidence.summaries(deployment, None, 10).unwrap();
        assert_eq!(history.len(), 2);
        assert!(
            history
                .iter()
                .any(|task| task.id != first_id && task.state == TaskRunState::Completed)
        );
        assert!(history.iter().any(|task| task.id == first_id
            && !matches!(task.state, TaskRunState::Completed | TaskRunState::Failed)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_hub_session_binds_history_and_owner_loss_does_not_reconnect() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let admin = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("owned Hub work fixture");
    PostgresIndexStore::new(admin.clone())
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let role = format!(
        "hub_work_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let schema = role.clone();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN CONNECTION LIMIT 1 PASSWORD 'owned_hub_work_fixture'; CREATE SCHEMA {schema};
         ALTER ROLE {role} SET search_path TO {schema},public; GRANT USAGE ON SCHEMA {schema} TO {role};",
    ))).execute(&admin).await.unwrap();
    let tables: Vec<String> = sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'shardline_%' AND tablename <> 'shardline_task_artifacts_v1'")
        .fetch_all(&admin).await.unwrap();
    assert!(!tables.is_empty(), "fixture must have released migrations");
    for table in tables {
        let table = format!("\"{}\"", table.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)"
        )))
        .execute(&admin)
        .await
        .unwrap();
    }
    let owner_schema = schema.clone();
    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |connection, _metadata| {
            let session_schema = owner_schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {session_schema},public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect(&url)
        .await
        .unwrap();
    PostgresIndexStore::new(owner.clone())
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};
         REVOKE SELECT ON {schema}.shardline_resource_fences FROM {role};
         REVOKE ALL ON {schema}.shardline_task_artifacts_v1 FROM {role};
         GRANT UPDATE ON {schema}.shardline_hub_repos, {schema}.shardline_hub_refs TO {role};
         GRANT INSERT ON {schema}.shardline_reliability_events TO {role};
         GRANT UPDATE(secret) ON {schema}.shardline_hub_webhooks TO {role};",
    )))
    .execute(&admin)
    .await
    .unwrap();
    let owned_store = BoxedHubStore::from_store(PostgresIndexStore::new(owner.clone()));
    owned_store
        .create_repo(HubRepoType::Model, "work/session", false)
        .unwrap();
    let first = owned_store
        .create_webhook(
            "work/session",
            "https://example.com/one",
            &["push".to_owned()],
            Some("legacy-one"),
        )
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let mut work_url = url::Url::parse(&url).unwrap();
    work_url.set_username(&role).unwrap();
    work_url
        .set_password(Some("owned_hub_work_fixture"))
        .unwrap();
    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.path().to_path_buf(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_index_postgres_url(work_url.to_string())
    .unwrap();
    let objects = ServerObjectStore::local(root.path().join("chunks")).unwrap();
    let deployment = deployment_fingerprint(&config).unwrap();
    let mut inspection = admin.acquire().await.unwrap();
    let inherited = resolved_postgres_deployment_fingerprint(&config, &mut inspection)
        .await
        .unwrap();
    drop(inspection);
    let mut actual = owner.acquire().await.unwrap();
    let expected = resolved_postgres_deployment_fingerprint(&config, &mut actual)
        .await
        .unwrap();
    drop(actual);
    assert_ne!(expected, inherited);
    let (startup_evidence, startup_scope) = TaskEvidenceStore::from_postgres_pool(&config, &admin)
        .await
        .unwrap();
    assert_eq!(startup_scope, Some(inherited));
    let (evidence, work_scope) = TaskEvidenceStore::from_postgres_pool(&config, &owner)
        .await
        .unwrap();
    assert_eq!(work_scope, Some(expected));
    let tracker = EmbeddedTaskTracker::from_store(startup_evidence.clone(), deployment)
        .with_history_scope(inherited);
    let fallback = owned_store.clone();
    let run_config = config.clone();
    let run_tracker = tracker.clone();
    let healthy = tokio::task::spawn_blocking(move || {
        run(
            &run_config,
            &fallback,
            &WebhookSecretCipher::new(SecretBytes::new(vec![41; 32])).unwrap(),
            &run_tracker,
        )
    })
    .await
    .unwrap();
    assert_eq!(
        (healthy.inspected, healthy.upgraded, healthy.failures),
        (1, 1, 0)
    );
    tokio::task::block_in_place(|| {
        let summaries = evidence.summaries(deployment, None, 10).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].state, TaskRunState::Completed);
        let identity: TaskRunIdentity = serde_json::from_slice(
            &evidence
                .read(&format!("manifests/{}", summaries[0].id), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(identity.owner.pid, std::process::id());
        assert!(
            startup_evidence
                .summaries(deployment, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            TaskEvidenceStore::from_objects(objects.clone())
                .summaries(deployment, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            evidence
                .history_scope_binding_matches(&identity, expected)
                .unwrap()
        );
        assert!(
            !evidence
                .history_scope_binding_matches(&identity, inherited)
                .unwrap()
        );
    });
    // A webhook-only writer keeps the sweep available without new privileges
    // or adding tracker objects to its payload inventory.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "REVOKE INSERT ON {schema}.shardline_reliability_events FROM {role}"
    )))
    .execute(&admin)
    .await
    .unwrap();
    let legacy_config = config.clone();
    let legacy_store = owned_store.clone();
    let legacy_tracker = tracker.clone();
    let legacy_report = tokio::task::spawn_blocking(move || {
        run(
            &legacy_config,
            &legacy_store,
            &WebhookSecretCipher::new(SecretBytes::new(vec![41; 32])).unwrap(),
            &legacy_tracker,
        )
    })
    .await
    .unwrap();
    assert_eq!(legacy_report.failures, 0);
    let legacy = TaskEvidenceStore::from_objects(objects);
    let legacy_history = legacy.summaries(deployment, None, 10).unwrap();
    assert!(legacy_history.is_empty());
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "GRANT INSERT ON {schema}.shardline_reliability_events TO {role}"
    )))
    .execute(&admin)
    .await
    .unwrap();
    // Hold the table until the real source SELECT is waiting. Its history is
    // bound before work, and terminating that PID must not create a replacement.
    owned_store
        .update_webhook_secret("work/session", &first.id, Some("still-plaintext"))
        .unwrap();
    let mut locked = owner.begin().await.unwrap();
    sqlx::query("LOCK TABLE shardline_hub_webhooks IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *locked)
        .await
        .unwrap();
    let fallback = owned_store.clone();
    let run_config = config.clone();
    let run_tracker = tracker.clone();
    let source = tokio::task::spawn_blocking(move || {
        run(
            &run_config,
            &fallback,
            &WebhookSecretCipher::new(SecretBytes::new(vec![41; 32])).unwrap(),
            &run_tracker,
        )
    });
    let pid = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if let Some(pid) = sqlx::query_scalar::<_, i32>(
                "SELECT pid FROM pg_stat_activity WHERE usename=$1 AND wait_event_type='Lock'",
            )
            .bind(&role)
            .fetch_optional(&admin)
            .await
            .unwrap()
            {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(&admin)
            .await
            .unwrap()
    );
    let failed = tokio::time::timeout(Duration::from_secs(15), source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.upgraded, 0);
    assert_eq!(failed.failures, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_stat_activity WHERE usename=$1")
            .bind(&role)
            .fetch_one(&admin)
            .await
            .unwrap(),
        0
    );
    locked.rollback().await.unwrap();
    assert_eq!(
        owned_store
            .list_webhooks("work/session")
            .unwrap()
            .remove(0)
            .secret
            .unwrap()
            .expose_secret(),
        "still-plaintext"
    );
    tokio::task::block_in_place(|| {
        let history = evidence.summaries(deployment, None, 10).unwrap();
        let failed = history
            .iter()
            .find(|entry| entry.state == TaskRunState::NeedsReconciliation)
            .unwrap();
        let identity: TaskRunIdentity = serde_json::from_slice(
            &evidence
                .read(&format!("manifests/{}", failed.id), 65536)
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert!(
            evidence
                .history_scope_binding_matches(&identity, expected)
                .unwrap()
        );
        assert_eq!(identity.owner.pid, std::process::id());
    });
    owner.close().await;
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "DROP OWNED BY {role}; DROP ROLE {role}; DROP SCHEMA {schema} CASCADE;"
    )))
    .execute(&admin)
    .await
    .unwrap();
    admin.close().await;
}
