//! Force overlapping repair transactions to contend before releasing either.
use super::*;
use shardline_index::{PostgresRecordStore, XetXorbDescriptor};

pub(super) async fn qualify(
    root: &Path,
    remote: &RemoteStorage,
    observer: &sqlx::PgPool,
    first: &XetXorbDescriptor,
    second: &XetXorbDescriptor,
) -> Result<()> {
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=ANY($1)")
        .bind(vec![first.hash.clone(), second.hash.clone()])
        .execute(observer)
        .await?;
    let mut blocker = observer.begin().await?;
    sqlx::query("SELECT xorb_hash FROM shardline_xet_xorbs WHERE xorb_hash=ANY($1) ORDER BY xorb_hash FOR UPDATE")
        .bind(vec![first.hash.clone(), second.hash.clone()])
        .fetch_all(&mut *blocker)
        .await?;
    let mut workers = Vec::new();
    let mut pids = Vec::new();
    let mut pools = Vec::new();
    for batch in [
        vec![first.clone(), second.clone()],
        vec![second.clone(), first.clone()],
    ] {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&remote.database_url)
            .await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await?;
        pids.push(pid);
        let records = PostgresRecordStore::new(pool.clone());
        workers.push(tokio::spawn(async move {
            records.cache_xet_xorb_descriptors(&batch).await
        }));
        pools.push(pool);
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(std::time::Duration::from_secs(10))
        .ok_or("repair wait deadline overflow")?;
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE pid=ANY($1) AND wait_event_type='Lock'",
        )
        .bind(&pids)
        .fetch_one(observer)
        .await?;
        if waiting == 2 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "both repair workers must contend"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    // Uncommitted repairs must never hide the unresolved missing copy.
    expect_scan(root, remote, 1, "repairs waiting for source ownership")?;
    blocker.commit().await?;
    for worker in workers {
        tokio::time::timeout(std::time::Duration::from_secs(10), worker).await???;
    }
    let records = PostgresRecordStore::new(observer.clone());
    for descriptor in [first, second] {
        assert_eq!(
            records
                .xet_indexed_xorb_descriptor(&descriptor.hash)
                .await?
                .as_ref(),
            Some(descriptor)
        );
    }
    expect_scan(root, remote, 0, "concurrent repairs committed")?;
    for pool in pools {
        pool.close().await;
    }
    Ok(())
}
