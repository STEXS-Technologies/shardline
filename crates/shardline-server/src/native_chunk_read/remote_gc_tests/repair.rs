//! Overlap a derived-member repair with a real fenced remote sweep.

use shardline_index::{PostgresIndexStore, PostgresRecordStore};
use std::time::Duration;

struct Worker(tokio::task::JoinHandle<Result<(), &'static str>>);

impl Drop for Worker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn qualify(
    database: &str,
    work: sqlx::PgPool,
    objects: crate::ServerObjectStore,
    live: &str,
) {
    let observer = sqlx::PgPool::connect(database).await.unwrap();
    let records = PostgresRecordStore::new(observer.clone());
    let key = shardline_xet_adapter::xorb_object_key(live).unwrap();
    let length = shardline_storage::AsyncObjectStore::metadata(&objects, &key)
        .await
        .unwrap()
        .unwrap()
        .length();
    let bytes = objects.read_full_object(&key, length).unwrap();
    let validated = shardline_xet_adapter::validate_serialized_xorb(
        &mut std::io::Cursor::new(bytes),
        shardline_index::parse_xet_hash_hex(live).unwrap(),
    )
    .unwrap();
    let descriptor = shardline_index::XetXorbDescriptor {
        hash: live.to_owned(),
        total_bytes: validated.total_length(),
        chunks: validated
            .chunks()
            .iter()
            .map(|member| shardline_index::XetXorbChunk {
                hash: shardline_index::xet_hash_hex_string(member.hash()),
                packed_start: member.packed_start(),
                packed_end: member.packed_end(),
                unpacked_start: member.unpacked_start(),
                unpacked_end: member.unpacked_end(),
            })
            .collect(),
    };
    records
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1")
        .bind(live)
        .execute(&observer)
        .await
        .unwrap();
    let mut blocker = observer.begin().await.unwrap();
    sqlx::query("SELECT xorb_hash FROM shardline_xet_xorbs WHERE xorb_hash=$1 FOR UPDATE")
        .bind(live)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let repair_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database)
        .await
        .unwrap();
    let repair_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&repair_pool)
        .await
        .unwrap();
    let repairing = PostgresRecordStore::new(repair_pool.clone());
    let batch = vec![descriptor.clone()];
    let mut repair = Worker(tokio::spawn(async move {
        repairing
            .cache_xet_xorb_descriptors(&batch)
            .await
            .map_err(|_error| "derived member repair failed")
    }));
    wait_for_lock(&observer, repair_pid).await;
    let gc_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&work)
        .await
        .unwrap();
    let mut gc = Worker(tokio::spawn(async move {
        shardline_gc::run_postgres_gc_with_remote_delete_protection(
            &PostgresRecordStore::new(work.clone()),
            &PostgresIndexStore::new(work),
            &objects,
            &[crate::ServerFrontend::Xet],
            shardline_gc::LocalGcOptions::mark_and_sweep(0),
        )
        .await
        .map(|_| ())
        .map_err(|_error| "remote GC during repair failed")
    }));
    // GC may recover directly from source bytes or wait while publishing its
    // own verified descriptor. Either must overlap the blocked repair, rather
    // than testing a sweep which begins only after repair has committed.
    tokio::time::timeout(Duration::from_secs(15), async {
        while !gc.0.is_finished() && !waiting(&observer, gc_pid).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !repair.0.is_finished(),
        "repair must remain uncommitted during GC"
    );
    blocker.rollback().await.unwrap();
    for worker in [&mut repair, &mut gc] {
        tokio::time::timeout(Duration::from_secs(20), &mut worker.0)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    assert_eq!(
        records
            .xet_indexed_xorb_descriptor(live)
            .await
            .unwrap()
            .as_ref(),
        Some(&descriptor),
        "repair and GC must preserve the complete authenticated member index",
    );
    repair_pool.close().await;
    observer.close().await;
}

async fn waiting(observer: &sqlx::PgPool, pid: i32) -> bool {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",
    )
    .bind(pid)
    .fetch_one(observer)
    .await
    .unwrap()
}

async fn wait_for_lock(observer: &sqlx::PgPool, pid: i32) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !waiting(observer, pid).await {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
