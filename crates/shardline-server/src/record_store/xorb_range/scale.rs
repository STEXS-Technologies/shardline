//! Explicit synthetic application-level PostgreSQL authorization diagnostic.

use super::repository_covers_xorb_range;
use shardline_index::{
    FileChunkRecord, FileRecord, PostgresRecordStore, RecordTraversal, StorageRepresentation,
};
use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};
use std::time::{Duration, Instant};

mod late;

#[tokio::test]
#[ignore = "large synthetic PostgreSQL diagnostic; requires an owner-migrated disposable database"]
async fn shared_hash_range_authorization_scale() {
    verify_scale(false).await;
}

#[tokio::test]
#[ignore = "large synthetic PostgreSQL late/multi-record/negative authorization diagnostic"]
async fn shared_hash_late_and_negative_authorization_scale() {
    verify_scale(true).await;
}

async fn verify_scale(include_late: bool) {
    let fixture = crate::test_support::postgres_fixture::PostgresFixture::new()
        .await
        .expect("range scale diagnostic requires DATABASE_URL");
    fixture.migrate().await;
    let pool = fixture.index().pool().clone();
    let store = PostgresRecordStore::new(pool.clone());
    let scope = RepositoryScope::new(RepositoryProvider::Generic, "team", "range", None).unwrap();
    let hash = "a".repeat(64);
    let record = FileRecord {
        file_id: "scale-0000000".into(),
        content_hash: "b".repeat(64),
        total_bytes: 4,
        chunk_size: 0,
        storage_repr: StorageRepresentation::WholeFileV1,
        repository_scope: Some(scope.clone()),
        chunks: vec![FileChunkRecord {
            hash: hash.clone(),
            offset: 0,
            length: 4,
            range_start: 0,
            range_end: 1,
            packed_start: 0,
            packed_end: 12,
        }],
    };
    store.commit_file_version_metadata(&record).await.unwrap();
    let template = store.version_record_locator(&record);
    let mut inserted = 2_i64;
    let maximum = std::env::var("SHARDLINE_RANGE_SCALE_MAX_RECORDS")
        .map_or(1_000_000, |value| value.parse::<i64>().unwrap());
    assert!([2, 10_000, 100_000, 1_000_000].contains(&maximum));
    for count in [2_i64, 10_000, 100_000, 1_000_000] {
        if count > maximum {
            break;
        }
        // Equal-width IDs preserve the real locator encoding. Every generated
        // row is valid FileRecord JSON, unlike a SQL-only candidate fixture.
        sqlx::query(
            "INSERT INTO shardline_file_records
             (record_key,record_kind,scope_key,file_id,content_hash,record)
             SELECT replace(source.record_key, source.file_id, generated.file_id),
                    source.record_kind,source.scope_key,generated.file_id,source.content_hash,
                    jsonb_set(source.record,'{file_id}',to_jsonb(generated.file_id))
             FROM shardline_file_records source
             CROSS JOIN (SELECT 'scale-' || lpad(n::text,7,'0') AS file_id
                         FROM generate_series($2::bigint,$3::bigint) n) generated
             WHERE source.record_key=$1",
        )
        .bind(template.record_key())
        .bind(inserted.checked_sub(1).unwrap())
        .bind(count.checked_sub(2).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        inserted = count;
        sqlx::raw_sql("ANALYZE shardline_file_records; ANALYZE shardline_record_membership")
            .execute(&pool)
            .await
            .unwrap();
        let actual: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_file_records")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(actual, count);
        for sample in 0..3 {
            let started = Instant::now();
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(120),
                    repository_covers_xorb_range(
                        &store,
                        &hash,
                        &scope,
                        ByteRange::new(0, 11).unwrap(),
                    ),
                )
                .await
                .unwrap()
                .unwrap()
            );
            let proof_millis = milliseconds(started.elapsed());
            // Completion includes reusing the same one-connection pool: a
            // dropped SQL stream may still require draining backend results.
            tokio::time::timeout(
                Duration::from_secs(120),
                sqlx::query("SELECT 1").execute(&pool),
            )
            .await
            .unwrap()
            .unwrap();
            eprintln!(
                "range_scale records={count} sample={sample} proof_ms={proof_millis:.3} reused_pool_ms={:.3}",
                milliseconds(started.elapsed()),
            );
        }
        if include_late {
            let last_key = template.record_key().replace(
                &record.file_id,
                &format!("scale-{:07}", count.checked_sub(2).unwrap()),
            );
            late::verify(
                &store,
                &pool,
                &scope,
                &hash,
                &last_key,
                count,
                count == maximum,
            )
            .await;
        }
    }
    fixture.finish().await;
}

#[allow(
    clippy::float_arithmetic,
    reason = "Diagnostic duration formatting in milliseconds"
)]
fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
