//! Real SQLx rows exercise bounded buffering and stalled-stream behavior.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::read;
use crate::postgres::test_support::connect_isolated_postgres;
use crate::{FileRecord, StorageRepresentation};
use futures_util::{StreamExt, stream};
use sqlx::{PgPool, postgres::PgRow, types::Json};
use std::time::Duration;

async fn rows(pool: &PgPool, size: usize, count: i32) -> Vec<PgRow> {
    let record = FileRecord {
        file_id: "a".repeat(size),
        content_hash: "b".repeat(64),
        total_bytes: 0,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: None,
        chunks: Vec::new(),
    };
    sqlx::query(
        "SELECT 'key'::text AS record_key, 'latest'::text AS record_kind,
                'scope'::text AS scope_key, 'id'::text AS file_id,
                $1::jsonb AS record, 0::bigint AS modified_since_epoch
         FROM generate_series(1,$2::integer)",
    )
    .bind(Json(record))
    .bind(count)
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn oversized_row_is_converted_alone_without_consuming_its_successor() {
    let Some(pool) = connect_isolated_postgres().await else {
        return;
    };
    let mut source = stream::iter(rows(&pool, 100_000, 2).await.into_iter().map(Ok));
    let first = read(&mut source).await.unwrap();
    assert_eq!(first.records.len(), 1);
    assert!(first.records.into_iter().all(|record| record.is_ok()));
    assert!(!first.finished);
    assert!(first.error.is_none());
    assert!(source.next().await.unwrap().is_ok());
    assert!(source.next().await.is_none());
    pool.close().await;
}

#[tokio::test]
async fn ready_record_does_not_wait_for_a_stalled_successor() {
    let Some(pool) = connect_isolated_postgres().await else {
        return;
    };
    let mut source =
        stream::iter(rows(&pool, 8, 1).await.into_iter().map(Ok)).chain(stream::pending());
    let batch = tokio::time::timeout(Duration::from_secs(1), read(&mut source))
        .await
        .expect("batch waited for a speculative network row")
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(batch.records.into_iter().all(|record| record.is_ok()));
    assert!(!batch.finished);
    assert!(batch.error.is_none());
    pool.close().await;
}

#[tokio::test]
async fn later_transport_failure_preserves_the_prior_record() {
    let Some(pool) = connect_isolated_postgres().await else {
        return;
    };
    let mut source =
        stream::iter(rows(&pool, 8, 1).await.into_iter().map(Ok)).chain(stream::iter([Err(
            sqlx::Error::Protocol("synthetic transport failure".into()),
        )]));
    let batch = read(&mut source).await.unwrap();
    assert_eq!(batch.records.len(), 1);
    assert!(batch.records.into_iter().all(|record| record.is_ok()));
    assert!(batch.error.is_some());
    pool.close().await;
}
