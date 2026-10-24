//! Cancel after the first container was repaired inside an uncommitted batch.
use super::*;
use shardline_index::{PostgresRecordStore, XetXorbDescriptor};

pub(super) async fn qualify(
    root: &Path,
    remote: &RemoteStorage,
    observer: &sqlx::PgPool,
    first: &XetXorbDescriptor,
    second: &XetXorbDescriptor,
) -> Result<()> {
    for disconnect in [false, true] {
        interrupted_repair(root, remote, observer, first, second, disconnect).await?;
    }
    Ok(())
}

async fn interrupted_repair(
    root: &Path,
    remote: &RemoteStorage,
    observer: &sqlx::PgPool,
    first: &XetXorbDescriptor,
    second: &XetXorbDescriptor,
    disconnect: bool,
) -> Result<()> {
    let (low, high) = if first.hash < second.hash {
        (first, second)
    } else {
        (second, first)
    };
    let hashes = vec![low.hash.clone(), high.hash.clone()];
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=ANY($1)")
        .bind(&hashes)
        .execute(observer)
        .await?;
    let mut blocker = observer.begin().await?;
    sqlx::query("SELECT xorb_hash FROM shardline_xet_xorbs WHERE xorb_hash=$1 FOR UPDATE")
        .bind(&high.hash)
        .fetch_one(&mut *blocker)
        .await?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&remote.database_url)
        .await?;
    let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    let interrupted_records = PostgresRecordStore::new(pool.clone());
    let batch = vec![high.clone(), low.clone()];
    let worker =
        tokio::spawn(async move { interrupted_records.cache_xet_xorb_descriptors(&batch).await });
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(10))
        .ok_or("repair cancellation deadline overflow")?;
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",
        )
        .bind(pid)
        .fetch_one(observer)
        .await?;
        if waiting {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "repair must reach the blocked second container"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let wrote_members: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND relation='shardline_xet_chunks'::regclass AND mode='RowExclusiveLock' AND granted)")
        .bind(pid).fetch_one(observer).await?;
    assert!(
        wrote_members,
        "cancellation must happen after member writes started"
    );
    expect_scan(root, remote, 1, "partial repair remains uncommitted")?;
    if disconnect {
        let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
            .bind(pid)
            .fetch_one(observer)
            .await?;
        assert!(terminated, "repair session must be terminated");
        let result = tokio::time::timeout(std::time::Duration::from_secs(10), worker).await??;
        assert!(result.is_err(), "terminated repair must not report success");
    } else {
        worker.abort();
        match worker.await {
            Err(error) => assert!(error.is_cancelled(), "repair must be cancelled"),
            Ok(_) => return Err("repair completed before cancellation".into()),
        }
    }
    blocker.rollback().await?;
    // Reusing the sole connection forces the driver's pending rollback to
    // finish; a cancelled future alone does not prove server-side cleanup.
    let connection =
        tokio::time::timeout(std::time::Duration::from_secs(10), pool.acquire()).await??;
    drop(connection);
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM shardline_xet_chunks WHERE xorb_hash=ANY($1)")
            .bind(&hashes)
            .fetch_one(observer)
            .await?;
    assert_eq!(
        count, 0,
        "cancelled repair must roll back the first container too"
    );
    expect_scan(root, remote, 1, "cancelled repair leaves findings visible")?;
    let records = PostgresRecordStore::new(pool.clone());
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        records.cache_xet_xorb_descriptors(&[first.clone(), second.clone()]),
    )
    .await??;
    expect_scan(root, remote, 0, "retry after cancelled repair")?;
    pool.close().await;
    Ok(())
}
