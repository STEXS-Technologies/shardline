//! Persistent PostgreSQL repair recovery at each metadata mutation boundary.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::{
    LifecycleRepairBoundary, LifecycleRepairOptions, fault_injection::interrupt_at,
    orchestrator::run_lifecycle_repair_with_stores_at_time,
};
use crate::{ServerError, ServerFrontend, ServerObjectStore};
use shardline_index::{
    AsyncIndexStore, PostgresIndexStore, PostgresRecordStore, QuarantineCandidate, RetentionHold,
    WebhookDelivery,
};
use shardline_protocol::RepositoryProvider;
use shardline_storage::ObjectKey;
use sqlx::{PgPool, postgres::PgPoolOptions};

async fn connect(url: &str, schema: &str) -> PgPool {
    let schema = schema.to_owned();
    PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |connection, _| {
            let schema = schema.clone();
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "SET search_path TO {schema}, public"
                )))
                .execute(connection)
                .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
        .unwrap()
}

#[tokio::test]
async fn mutation_interruptions_survive_new_postgres_sessions() {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("SKIPPED: PostgreSQL lifecycle recovery requires owned fixture");
        return;
    };
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let schema = format!(
        "repair_recovery_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let tables: Vec<String> = sqlx::query_scalar("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'shardline_%'").fetch_all(&admin).await.unwrap();
    assert!(!tables.is_empty(), "fixture requires metadata migrations");
    for table in tables {
        let table = format!("\"{}\"", table.replace('"', "\"\""));
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {schema}.{table} (LIKE public.{table} INCLUDING ALL)"
        )))
        .execute(&admin)
        .await
        .unwrap();
    }
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("chunks")).unwrap();
    let payload = root.path().join("chunks/user-payload");
    std::fs::write(&payload, b"unchanged payload").unwrap();
    for boundary in [
        LifecycleRepairBoundary::AfterRetentionHoldMutation,
        LifecycleRepairBoundary::AfterQuarantineCandidateMutation,
        LifecycleRepairBoundary::AfterWebhookDeliveryMutation,
    ] {
        let pool = connect(&url, &schema).await;
        let index = PostgresIndexStore::new(pool.clone());
        index.initialize_remote_delete_guard().await.unwrap();
        let records = PostgresRecordStore::new(pool.clone());
        for suffix in ["a", "b"] {
            index
                .upsert_retention_hold(
                    &RetentionHold::new(
                        ObjectKey::parse(&format!("repair/hold-{suffix}")).unwrap(),
                        "expired".to_owned(),
                        0,
                        Some(999),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            index
                .upsert_quarantine_candidate(
                    &QuarantineCandidate::new(
                        ObjectKey::parse(&format!("repair/quarantine-{suffix}")).unwrap(),
                        1,
                        0,
                        1001,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            index
                .record_webhook_delivery(
                    &WebhookDelivery::new(
                        RepositoryProvider::GitHub,
                        "owner".to_owned(),
                        "repo".to_owned(),
                        format!("repair-delivery-{suffix}"),
                        0,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        let interrupted = interrupt_at(
            boundary,
            run_lifecycle_repair_with_stores_at_time(
                &records,
                &index,
                &objects,
                &[ServerFrontend::Xet],
                LifecycleRepairOptions::default(),
                1000,
            ),
        )
        .await;
        assert!(
            matches!(interrupted, Err(ServerError::InjectedLifecycleRepairInterruption { boundary: observed }) if observed == boundary)
        );
        pool.close().await;
        drop(index);
        drop(records);
        let fresh = connect(&url, &schema).await;
        let index = PostgresIndexStore::new(fresh.clone());
        let records = PostgresRecordStore::new(fresh.clone());
        let expected = match boundary {
            LifecycleRepairBoundary::AfterRetentionHoldMutation => (1, 2, 2),
            LifecycleRepairBoundary::AfterQuarantineCandidateMutation => (0, 1, 2),
            LifecycleRepairBoundary::AfterWebhookDeliveryMutation => (0, 0, 1),
        };
        assert_eq!(
            (
                index.list_retention_holds().await.unwrap().len(),
                index.list_quarantine_candidates().await.unwrap().len(),
                index.list_webhook_deliveries().await.unwrap().len()
            ),
            expected
        );
        let resumed = run_lifecycle_repair_with_stores_at_time(
            &records,
            &index,
            &objects,
            &[ServerFrontend::Xet],
            LifecycleRepairOptions::default(),
            1000,
        )
        .await
        .unwrap();
        assert_eq!(
            resumed.removed_expired_retention_holds,
            u64::try_from(expected.0).unwrap()
        );
        assert_eq!(
            resumed.removed_missing_quarantine_candidates,
            u64::try_from(expected.1).unwrap()
        );
        assert_eq!(
            resumed.removed_stale_webhook_deliveries,
            u64::try_from(expected.2).unwrap()
        );
        let converged = run_lifecycle_repair_with_stores_at_time(
            &records,
            &index,
            &objects,
            &[ServerFrontend::Xet],
            LifecycleRepairOptions::default(),
            1000,
        )
        .await
        .unwrap();
        assert_eq!(
            (
                converged.scanned_retention_holds,
                converged.scanned_quarantine_candidates,
                converged.scanned_webhook_deliveries
            ),
            (0, 0, 0)
        );
        assert_eq!(std::fs::read(&payload).unwrap(), b"unchanged payload");
        fresh.close().await;
    }
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
