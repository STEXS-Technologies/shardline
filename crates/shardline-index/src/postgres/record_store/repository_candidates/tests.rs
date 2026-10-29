//! Cancellation must release the traversal's transaction and pool connection.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::postgres::test_support::connect_isolated_postgres;
use shardline_protocol::RepositoryProvider;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_blocked_page_releases_its_transaction() {
    let Some(pool) = connect_isolated_postgres().await else {
        return;
    };
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("LOCK TABLE shardline_file_records IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let store = PostgresRecordStore::new(pool.clone());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "cancel", None).unwrap();
    let pending_store = store.clone();
    let pending_scope = scope.clone();
    let pending = tokio::spawn(async move {
        visit(&pending_store, &pending_scope, &"a".repeat(64), |_| {
            Ok::<(), PostgresMetadataStoreError>(())
        })
        .await
    });
    let reader_pid: i32 = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let pid = sqlx::query_scalar(
                "SELECT pid FROM pg_locks
                 WHERE relation='shardline_file_records'::regclass
                   AND mode='AccessShareLock' AND NOT granted LIMIT 1",
            )
            .fetch_optional(&pool)
            .await
            .unwrap();
            if let Some(pid) = pid {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("candidate page did not reach its blocking database read");
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    blocker.rollback().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let released: Option<bool> =
                sqlx::query_scalar("SELECT xact_start IS NULL FROM pg_stat_activity WHERE pid=$1")
                    .bind(reader_pid)
                    .fetch_optional(&pool)
                    .await
                    .unwrap();
            if released.unwrap_or(true) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("aborted candidate page retained its database transaction");
    let mut count = 0_usize;
    visit(&store, &scope, &"a".repeat(64), |_| {
        count = count.saturating_add(1);
        Ok::<(), PostgresMetadataStoreError>(())
    })
    .await
    .unwrap();
    assert_eq!(count, 0);
    pool.close().await;
}
