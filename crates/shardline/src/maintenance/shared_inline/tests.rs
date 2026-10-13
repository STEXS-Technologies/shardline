#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
use super::*;
use shardline_server::{ServerConfig, task_tracking::TaskRunState};
use std::{num::NonZeroUsize, path::Path};

fn config(root: &Path) -> ServerConfig {
    ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost:8080".to_owned(),
        root.to_path_buf(),
        NonZeroUsize::new(4096).unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_cli_tracking_paths_bind_the_actual_postgres_work_session() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        return;
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).unwrap();
    let role = format!("cli_history_{}", hex::encode(nonce));
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&pool)
        .await
        .unwrap();
    let owner = owner.replace('"', "\"\"");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE ROLE {role} LOGIN PASSWORD 'owned_history_login_password' CONNECTION LIMIT 1 IN ROLE \"{owner}\""
    )))
    .execute(&pool)
    .await
    .unwrap();
    let mut runtime_url = url::Url::parse(&database_url).unwrap();
    runtime_url.set_username(&role).unwrap();
    runtime_url
        .set_password(Some("owned_history_login_password"))
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path())
        .with_index_postgres_url(runtime_url.to_string())
        .unwrap();
    let expected_scope = {
        let mut connection = pool.acquire().await.unwrap();
        shardline_server::task_tracking::resolved_postgres_deployment_fingerprint(
            &config,
            &mut connection,
        )
        .await
        .unwrap()
    };
    let (store, scope) = TaskEvidenceStore::from_postgres_pool(&config, &pool)
        .await
        .unwrap();
    assert_eq!(scope, Some(expected_scope));
    let legacy = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    for (shared_only, operation_kind) in [true, false]
        .into_iter()
        .flat_map(|shared_only| (0..3).map(move |kind| (shared_only, kind)))
    {
        let invocation_config = config.clone();
        let run_id = format!("ordinary-bound-session-{role}-{operation_kind}");
        let result = tokio::task::spawn_blocking(move || {
            let work_root = invocation_config.root_dir().to_path_buf();
            let (operation, command) = match operation_kind {
                0 => (
                    MaintenanceOperation::IndexRebuild {
                        root: work_root.clone(),
                    },
                    crate::CliCommand::IndexRebuild {
                        root: Some(work_root),
                    },
                ),
                1 => (
                    MaintenanceOperation::Gc {
                        root: work_root.clone(),
                        mark: true,
                        sweep: false,
                        retention_seconds: 3600,
                    },
                    crate::CliCommand::Gc {
                        root: Some(work_root),
                        mark: true,
                        sweep: false,
                        retention_seconds: 3600,
                        orphan_inventory: None,
                        retention_report: None,
                    },
                ),
                _ => (
                    MaintenanceOperation::LifecycleRepair {
                        root: work_root.clone(),
                        webhook_retention_seconds: 604800,
                    },
                    crate::CliCommand::RepairLifecycle {
                        root: Some(work_root),
                        webhook_retention_seconds: 604800,
                    },
                ),
            };
            let configuration = super::super::configuration::Configuration {
                command,
                config: invocation_config.clone(),
                config_path: None,
                migration: None,
            };
            super::super::configuration::scope(configuration, || {
                let arguments = vec![OsString::from("shardline"), OsString::from("index-rebuild")];
                if shared_only {
                    Ok(run(&operation, arguments, &invocation_config))
                } else {
                    super::super::inline::run(
                        &invocation_config.root_dir().join("existing-private-state"),
                        &run_id,
                        &operation,
                        vec![b"index-rebuild".to_vec()],
                        arguments,
                    )
                }
            })
            .unwrap()
        })
        .await
        .unwrap();
        assert!(result.success);
        assert!(result.evidence_published);
    }
    let artifacts: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_task_artifacts_v1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(artifacts > 0);
    tokio::task::spawn_blocking(move || {
        let objects = shardline_server::object_store_from_config(&config).unwrap();
        assert!(
            shardline_storage::ObjectStore::list_prefix(
                &objects,
                &shardline_storage::ObjectPrefix::parse(
                    shardline_server::task_tracking::TASK_EVIDENCE_PREFIX
                )
                .unwrap(),
            )
            .unwrap()
            .is_empty()
        );
        let tasks = store
            .summaries_with_scope(legacy, Some(expected_scope), None, 10)
            .unwrap();
        assert_eq!(tasks.len(), 6);
        for task in tasks {
            assert_eq!(task.state, TaskRunState::Completed);
            let identity: TaskRunIdentity = serde_json::from_slice(
                &store
                    .read(&format!("manifests/{}", task.id), 65536)
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(identity.owner.pid, std::process::id());
            assert!(
                store
                    .history_scope_binding_matches(&identity, expected_scope)
                    .unwrap()
            );
        }
    })
    .await
    .unwrap();
    let remaining_sessions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE usename=$1")
            .bind(&role)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(remaining_sessions, 0);
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

#[test]
fn shared_receipts_verify_native_success_failure_and_materialized_tampering() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let operation = MaintenanceOperation::Fsck {
        root: root.path().to_owned(),
    };
    let store = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    for exit_code in [0_u8, 1, 2] {
        let id = format!("shared-inline-{exit_code}");
        let (input, definition, scope) =
            super::super::inline::prepare(&config, &id, &operation, vec![b"fsck".to_vec()])
                .unwrap();
        let mut receipt =
            Receipt::begin(&operation, &id, &config, &input, definition, scope).unwrap();
        receipt.finish(operation.kind(), exit_code).unwrap();
        let tasks = store.summaries(deployment, None, 10).unwrap();
        let task = tasks.iter().find(|task| task.id == id).unwrap();
        assert_eq!(
            task.state,
            if exit_code == 0 {
                TaskRunState::Completed
            } else {
                TaskRunState::Failed
            }
        );
    }
    // No signing key, private task journal or scratch directory is needed.
    // Native body verification also detects damage in metadata-backed evidence.
    assert!(!root.path().join(".shardline-maintenance").exists());
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    assert!(
        connection
            .execute(
                "UPDATE shardline_task_artifacts_v1 SET payload = ?1
         WHERE artifact_key GLOB '_shardline_tasks/v1/runs/shared-inline-0/artifacts/*'",
                [b"damaged materialized result".as_slice()],
            )
            .unwrap()
            > 0
    );
    let tasks = store.summaries(deployment, None, 10).unwrap();
    assert_eq!(
        tasks
            .iter()
            .find(|task| task.id == "shared-inline-0")
            .unwrap()
            .state,
        TaskRunState::Corrupt
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_commands_finish_before_history_housekeeping_and_preserve_cleanup_failures() {
    for shared_only in [true, false] {
        for deny_cleanup in [true, false] {
            let root = tempfile::tempdir().unwrap();
            let configured = config(root.path());
            let work_config = configured.clone();
            let output = tokio::task::spawn_blocking(move || {
                let operation = MaintenanceOperation::IndexRebuild {
                    root: work_config.root_dir().to_owned(),
                };
                let id = "expired-before-work";
                let (input, definition, scope) = super::super::inline::prepare(
                    &work_config, id, &operation, vec![b"index-rebuild".to_vec()],
                ).unwrap();
                let mut old = Receipt::begin(&operation, id, &work_config, &input, definition, scope).unwrap();
                old.finish(operation.kind(), 0).unwrap();
                let connection = rusqlite::Connection::open(work_config.root_dir().join("metadata.sqlite3")).unwrap();
                let manifest = format!("_shardline_tasks/v1/manifests/{id}");
                let mut identity = old.identity.clone();
                identity.created_at = 0;
                connection.execute(
                    "UPDATE shardline_task_artifacts_v1 SET payload = ?1 WHERE artifact_key = ?2",
                    rusqlite::params![serde_json::to_vec(&identity).unwrap(), manifest],
                ).unwrap();
                // Cleanup is permitted only after the fresh command's report
                // is saved. Moving housekeeping before work leaves the old
                // manifest behind and fails the assertion below.
                connection.execute_batch(
                    "CREATE TRIGGER verify_cleanup_order BEFORE DELETE ON shardline_task_artifacts_v1
                     WHEN OLD.artifact_key = '_shardline_tasks/v1/manifests/expired-before-work'
                     AND NOT EXISTS (
                         SELECT 1 FROM shardline_task_artifacts_v1
                         WHERE artifact_key LIKE '_shardline_tasks/v1/runs/%/artifacts/%'
                         AND artifact_key NOT LIKE '_shardline_tasks/v1/runs/expired-before-work/%'
                         AND CAST(payload AS TEXT) LIKE '%\"exit_code\":0%'
                     ) BEGIN SELECT RAISE(ABORT, 'fresh work has not reported its result'); END;",
                ).unwrap();
                if deny_cleanup {
                    connection.execute_batch(
                        "CREATE TRIGGER deny_history_cleanup BEFORE DELETE ON shardline_task_artifacts_v1
                         WHEN OLD.artifact_key = '_shardline_tasks/v1/manifests/expired-before-work'
                         BEGIN SELECT RAISE(ABORT, 'owned cleanup fault'); END;",
                    ).unwrap();
                }
                let configuration = super::super::configuration::Configuration {
                    command: crate::CliCommand::IndexRebuild { root: Some(work_config.root_dir().to_owned()) },
                    config: work_config.clone(), config_path: None, migration: None,
                };
                super::super::configuration::scope(configuration, || {
                    let arguments = vec![OsString::from("shardline"), OsString::from("index-rebuild")];
                    if shared_only { Ok(run(&operation, arguments, &work_config)) }
                    else {
                        let command = crate::CliCommand::IndexRebuild { root: Some(work_config.root_dir().to_owned()) };
                        let prepared = super::super::automatic::prepare(&command)?.unwrap();
                        super::super::automatic::run(&prepared, arguments)
                    }
                }).unwrap()
            }).await.unwrap();
            assert!(output.success);
            assert_eq!(output.exit_code, Some(0));
            assert!(output.evidence_published);
            let store = TaskEvidenceStore::from_config(&configured).unwrap();
            let deployment =
                shardline_server::task_tracking::deployment_fingerprint(&configured).unwrap();
            let history = store.summaries(deployment, None, 10).unwrap();
            assert_eq!(history.len(), if deny_cleanup { 2 } else { 1 });
            assert!(
                history
                    .iter()
                    .all(|run| run.state == TaskRunState::Completed)
            );
            assert!(
                history
                    .iter()
                    .any(|run| run.id != "expired-before-work"
                        && run.owner.pid == std::process::id())
            );
            if deny_cleanup {
                let connection =
                    rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
                connection
                    .execute_batch("DROP TRIGGER deny_history_cleanup")
                    .unwrap();
                assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 2);
            }
        }
    }
}

#[test]
fn interrupted_admission_preserves_unfinished_history_at_each_record_boundary() {
    for sequence in 0..=2 {
        let root = tempfile::tempdir().unwrap();
        let config = config(root.path());
        let operation = MaintenanceOperation::Fsck {
            root: root.path().to_owned(),
        };
        let id = "shared-inline-admission";
        let store = TaskEvidenceStore::from_config(&config).unwrap();
        // Initialize the optional table before installing the write fault.
        store
            .publish("runs/fixture/blobs/seed", b"fixture")
            .unwrap();
        let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
        connection
            .execute_batch(&format!(
            "CREATE TRIGGER fixture_reject_admission BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN NEW.artifact_key = '_shardline_tasks/v1/runs/{id}/records/{sequence:010}'
             BEGIN SELECT RAISE(ABORT, 'fixture interrupts admission'); END;"
        ))
            .unwrap();
        let (input, definition, scope) =
            super::super::inline::prepare(&config, id, &operation, vec![b"fsck".to_vec()]).unwrap();
        assert!(Receipt::begin(&operation, id, &config, &input, definition, scope).is_err());
        drop(store);
        let reopened = TaskEvidenceStore::from_config(&config).unwrap();
        let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
        let tasks = reopened.summaries(deployment, None, 10).unwrap();
        let task = tasks.iter().find(|task| task.id == id).unwrap();
        assert!(!matches!(
            task.state,
            TaskRunState::Completed | TaskRunState::Failed
        ));
        assert!(
            reopened
                .read(&format!("runs/{id}/records/{sequence:010}"), 65536)
                .unwrap()
                .is_none()
        );
        for preceding in 0..sequence {
            assert!(
                reopened
                    .read(&format!("runs/{id}/records/{preceding:010}"), 65536)
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(reopened.prune_completed(deployment, u64::MAX).unwrap(), 0);
        assert!(
            reopened
                .read(&format!("manifests/{id}"), 65536)
                .unwrap()
                .is_some()
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_command_succeeds_when_shared_admission_records_are_denied() {
    for sequence in 0..=2 {
        let root = tempfile::tempdir().unwrap();
        let configured = config(root.path());
        let work_config = configured.clone();
        let output = tokio::task::spawn_blocking(move || {
            let store = TaskEvidenceStore::from_config(&work_config).unwrap();
            store.publish("runs/fixture/blobs/seed", b"fixture").unwrap();
            let connection = rusqlite::Connection::open(work_config.root_dir().join("metadata.sqlite3")).unwrap();
            connection.execute_batch(&format!(
                "CREATE TRIGGER fixture_reject_admission BEFORE INSERT ON shardline_task_artifacts_v1
                 WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/records/{sequence:010}'
                 BEGIN SELECT RAISE(ABORT, 'fixture rejects admission'); END;"
            )).unwrap();
            let operation = MaintenanceOperation::IndexRebuild { root: work_config.root_dir().to_owned() };
            let configuration = super::super::configuration::Configuration {
                command: crate::CliCommand::IndexRebuild { root: Some(work_config.root_dir().to_owned()) },
                config: work_config.clone(), config_path: None, migration: None,
            };
            super::super::configuration::scope(configuration, || -> Result<_> {
                Ok(run(&operation, vec![OsString::from("shardline"), OsString::from("index-rebuild")], &work_config))
            }).unwrap()
        }).await.unwrap();
        assert!(output.success);
        assert_eq!(output.exit_code, Some(0));
        assert!(!output.evidence_published);
        assert!(output.capture.is_none());
        let store = TaskEvidenceStore::from_config(&configured).unwrap();
        let deployment =
            shardline_server::task_tracking::deployment_fingerprint(&configured).unwrap();
        let history = store.summaries(deployment, None, 10).unwrap();
        assert_eq!(history.len(), 1);
        assert!(!matches!(
            history[0].state,
            TaskRunState::Completed | TaskRunState::Failed
        ));
        assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
    }
}

#[test]
fn saved_report_without_completion_acknowledgement_stays_unresolved() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let operation = MaintenanceOperation::Fsck {
        root: root.path().to_owned(),
    };
    let id = "shared-inline-unacknowledged";
    let (input, definition, scope) =
        super::super::inline::prepare(&config, id, &operation, vec![b"fsck".to_vec()]).unwrap();
    let mut receipt = Receipt::begin(&operation, id, &config, &input, definition, scope).unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fixture_reject_completion BEFORE INSERT ON shardline_task_artifacts_v1
             WHEN NEW.artifact_key = '_shardline_tasks/v1/runs/shared-inline-unacknowledged/records/0000000003'
             BEGIN SELECT RAISE(ABORT, 'fixture rejects completion acknowledgement'); END;",
        )
        .unwrap();
    assert!(receipt.finish(operation.kind(), 0).is_err());
    let reports: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM shardline_task_artifacts_v1
             WHERE artifact_key GLOB '_shardline_tasks/v1/runs/shared-inline-unacknowledged/artifacts/*'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    // The checkpoint and materialized result persisted before acknowledgement failed.
    assert_eq!(reports, 2);
    assert!(
        receipt
            .store
            .read(&format!("runs/{id}/records/0000000003"), 65536)
            .unwrap()
            .is_none()
    );
    drop(receipt);
    let reopened = TaskEvidenceStore::from_config(&config).unwrap();
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let tasks = reopened.summaries(deployment, None, 10).unwrap();
    let task = tasks.iter().find(|task| task.id == id).unwrap();
    assert!(!matches!(
        task.state,
        TaskRunState::Completed | TaskRunState::Failed
    ));
    assert_eq!(reopened.prune_completed(deployment, u64::MAX).unwrap(), 0);
    assert!(
        reopened
            .read(&format!("manifests/{id}"), 65536)
            .unwrap()
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn ordinary_command_preserves_success_when_report_or_acknowledgement_is_denied() {
    #[derive(Clone, Copy)]
    enum CommandKind {
        Fsck,
        Rebuild,
        Repair,
        Gc,
    }
    for command_kind in [
        CommandKind::Fsck,
        CommandKind::Rebuild,
        CommandKind::Repair,
        CommandKind::Gc,
    ] {
        for denied_suffix in ["artifacts/*", "records/0000000003"] {
            let root = tempfile::tempdir().unwrap();
            let configured = config(root.path());
            let work_config = configured.clone();
            let output = tokio::task::spawn_blocking(move || {
                let store = TaskEvidenceStore::from_config(&work_config).unwrap();
                store
                    .publish("runs/fixture/blobs/seed", b"fixture")
                    .unwrap();
                let connection =
                    rusqlite::Connection::open(work_config.root_dir().join("metadata.sqlite3"))
                        .unwrap();
                connection
                    .execute_batch(&format!(
                "CREATE TRIGGER fixture_reject_result BEFORE INSERT ON shardline_task_artifacts_v1
                 WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/runs/*/{denied_suffix}'
                 AND ('{denied_suffix}' <> 'artifacts/*'
                      OR CAST(NEW.payload AS TEXT) LIKE 'maintenance.success:%')
                 BEGIN SELECT RAISE(ABORT, 'fixture rejects result recording'); END;"
            ))
                    .unwrap();
                let root = work_config.root_dir().to_owned();
                let (operation, command, argument) = match command_kind {
                    CommandKind::Fsck => (
                        MaintenanceOperation::Fsck { root: root.clone() },
                        crate::CliCommand::Fsck { root: Some(root) },
                        "fsck",
                    ),
                    CommandKind::Rebuild => (
                        MaintenanceOperation::IndexRebuild { root: root.clone() },
                        crate::CliCommand::IndexRebuild { root: Some(root) },
                        "index-rebuild",
                    ),
                    CommandKind::Repair => (
                        MaintenanceOperation::LifecycleRepair {
                            root: root.clone(),
                            webhook_retention_seconds: 604800,
                        },
                        crate::CliCommand::RepairLifecycle {
                            root: Some(root),
                            webhook_retention_seconds: 604800,
                        },
                        "repair",
                    ),
                    CommandKind::Gc => (
                        MaintenanceOperation::Gc {
                            root: root.clone(),
                            mark: true,
                            sweep: false,
                            retention_seconds: 3600,
                        },
                        crate::CliCommand::Gc {
                            root: Some(root),
                            mark: true,
                            sweep: false,
                            retention_seconds: 3600,
                            retention_report: None,
                            orphan_inventory: None,
                        },
                        "gc",
                    ),
                };
                let configuration = super::super::configuration::Configuration {
                    command,
                    config: work_config.clone(),
                    config_path: None,
                    migration: None,
                };
                super::super::configuration::scope(configuration, || -> Result<_> {
                    Ok(run(
                        &operation,
                        vec![OsString::from("shardline"), OsString::from(argument)],
                        &work_config,
                    ))
                })
                .unwrap()
            })
            .await
            .unwrap();
            assert!(output.success);
            assert_eq!(output.exit_code, Some(0));
            assert!(!output.evidence_published);
            assert!(output.capture.is_none());
            let store = TaskEvidenceStore::from_config(&configured).unwrap();
            let deployment =
                shardline_server::task_tracking::deployment_fingerprint(&configured).unwrap();
            let history = store.summaries(deployment, None, 10).unwrap();
            assert_eq!(history.len(), 1);
            let task = history.first().unwrap();
            assert!(!matches!(
                task.state,
                TaskRunState::Completed | TaskRunState::Failed
            ));
            for sequence in 0..=2 {
                assert!(
                    store
                        .read(&format!("runs/{}/records/{sequence:010}", task.id), 65536)
                        .unwrap()
                        .is_some(),
                    "result fault must happen after durable admission and checkpoint"
                );
            }
            let connection =
                rusqlite::Connection::open(configured.root_dir().join("metadata.sqlite3")).unwrap();
            let materialized: i64 = connection
                .query_row(
                    "SELECT count(*) FROM shardline_task_artifacts_v1
                 WHERE artifact_key GLOB ?1",
                    [format!("_shardline_tasks/v1/runs/{}/artifacts/*", task.id)],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                materialized,
                if denied_suffix == "artifacts/*" { 1 } else { 2 }
            );
            assert!(
                store
                    .read(&format!("runs/{}/records/0000000003", task.id), 65536)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
            assert_eq!(
                store
                    .read("runs/fixture/blobs/seed", 65536)
                    .unwrap()
                    .as_deref(),
                Some(b"fixture".as_slice())
            );
        }
    }
}

#[test]
fn shared_completion_write_failure_never_claims_a_finished_task() {
    let root = tempfile::tempdir().unwrap();
    let config = config(root.path());
    let operation = MaintenanceOperation::Fsck {
        root: root.path().to_owned(),
    };
    let id = "shared-inline-incomplete";
    let (input, definition, scope) =
        super::super::inline::prepare(&config, id, &operation, vec![b"fsck".to_vec()]).unwrap();
    let mut receipt = Receipt::begin(&operation, id, &config, &input, definition, scope).unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER fixture_reject_artifacts BEFORE INSERT ON shardline_task_artifacts_v1
         WHEN NEW.artifact_key GLOB '_shardline_tasks/v1/runs/shared-inline-incomplete/artifacts/*'
         BEGIN SELECT RAISE(ABORT, 'fixture rejects task artifact writes'); END;",
        )
        .unwrap();
    assert!(receipt.finish(operation.kind(), 0).is_err());
    assert!(
        receipt
            .store
            .read(&format!("runs/{id}/records/0000000003"), 65536)
            .unwrap()
            .is_none()
    );
    let deployment = shardline_server::task_tracking::deployment_fingerprint(&config).unwrap();
    let tasks = receipt.store.summaries(deployment, None, 10).unwrap();
    assert!(!matches!(
        tasks[0].state,
        TaskRunState::Completed | TaskRunState::Failed
    ));
}
