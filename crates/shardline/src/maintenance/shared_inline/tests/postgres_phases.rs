#![allow(clippy::unwrap_used, clippy::expect_used)]
//! PostgreSQL must preserve unfinished receipts at the same boundaries as SQLite.
use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn postgres_receipt_faults_preserve_each_durable_boundary() {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIPPED: PostgreSQL receipt phases require DATABASE_URL");
        return;
    };
    let admin = sqlx::PgPool::connect(&database_url).await.unwrap();
    let mut nonce = [0_u8; 8];
    getrandom::fill(&mut nonce).unwrap();
    let nonce = hex::encode(nonce);
    for phase in 0..5 {
        let root = tempfile::tempdir().unwrap();
        let configured = config(root.path())
            .with_index_postgres_url(database_url.clone())
            .unwrap();
        let id = format!("pg-receipt-{nonce}-{phase}");
        let inspected_id = id.clone();
        let trigger = format!("receipt_fault_{nonce}_{phase}");
        let condition = match phase {
            0..=2 => format!("NEW.artifact_key LIKE '%/runs/{id}/records/{phase:010}'"),
            3 => format!(
                "NEW.artifact_key LIKE '%/runs/{id}/artifacts/%' AND convert_from(NEW.payload,'UTF8') LIKE 'maintenance.success:%'"
            ),
            _ => format!("NEW.artifact_key LIKE '%/runs/{id}/records/0000000003'"),
        };
        // DDL contains only generated hex, numeric phases and fixed literals.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "CREATE FUNCTION {trigger}() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN IF {condition} THEN RAISE EXCEPTION 'owned receipt phase fault'; END IF;
             RETURN NEW; END $$;
             CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE ON shardline_task_artifacts_v1
             FOR EACH ROW EXECUTE FUNCTION {trigger}();"
        )))
        .execute(&admin)
        .await
        .unwrap();
        let result = tokio::task::spawn_blocking(move || {
            let operation = MaintenanceOperation::Fsck {
                root: configured.root_dir().to_owned(),
            };
            let (input, definition, scope) = super::super::super::inline::prepare(
                &configured,
                &id,
                &operation,
                vec![b"fsck".to_vec()],
            )
            .unwrap();
            let receipt = Receipt::begin(&operation, &id, &configured, &input, definition, scope);
            if phase <= 2 {
                assert!(receipt.is_err());
            } else {
                let mut receipt = receipt.unwrap();
                assert!(receipt.finish(operation.kind(), 0).is_err());
            }
            let store =
                super::super::super::evidence::store(&operation, &configured, None).unwrap();
            let deployment =
                shardline_server::task_tracking::deployment_fingerprint(&configured).unwrap();
            let tasks = store.summaries(deployment, None, 100).unwrap();
            let task = tasks.iter().find(|task| task.id == id).unwrap();
            assert!(!matches!(
                task.state,
                TaskRunState::Completed | TaskRunState::Failed
            ));
            let committed = phase.min(3);
            for sequence in 0..committed {
                assert!(
                    store
                        .read(&format!("runs/{id}/records/{sequence:010}"), 65536)
                        .unwrap()
                        .is_some()
                );
            }
            assert!(
                store
                    .read(&format!("runs/{id}/records/{committed:010}"), 65536)
                    .unwrap()
                    .is_none()
            );
            assert_eq!(store.prune_completed(deployment, u64::MAX).unwrap(), 0);
            assert!(
                store
                    .read(&format!("manifests/{id}"), 65536)
                    .unwrap()
                    .is_some()
            );
        })
        .await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP TRIGGER {trigger} ON shardline_task_artifacts_v1; DROP FUNCTION {trigger}();"
        )))
        .execute(&admin)
        .await
        .unwrap();
        result.unwrap();
        let (records, artifacts): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE artifact_key LIKE $1),
                    count(*) FILTER (WHERE artifact_key LIKE $2)
             FROM shardline_task_artifacts_v1",
        )
        .bind(format!("%/runs/{inspected_id}/records/%"))
        .bind(format!("%/runs/{inspected_id}/artifacts/%"))
        .fetch_one(&admin)
        .await
        .unwrap();
        assert_eq!(records, i64::from(phase.min(3)));
        assert_eq!(
            artifacts,
            if phase == 4 {
                2
            } else if phase >= 2 {
                1
            } else {
                0
            }
        );
    }
    admin.close().await;
}
