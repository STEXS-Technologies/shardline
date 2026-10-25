//! Synthetic late matches, combined proofs, denials and committed revocation.
use super::{Duration, Instant, milliseconds, repository_covers_xorb_range};
use shardline_index::PostgresRecordStore;
use shardline_protocol::{ByteRange, RepositoryProvider, RepositoryScope};

pub(super) async fn verify(
    store: &PostgresRecordStore,
    pool: &sqlx::PgPool,
    scope: &RepositoryScope,
    hash: &str,
    last_key: &str,
    count: i64,
    revoke: bool,
) {
    // Only the last candidate covers the second serialized term. Every earlier
    // record remains a valid, authorized reference to the first term.
    assert_eq!(
        sqlx::query(
            "UPDATE shardline_file_records SET record=
             jsonb_set(jsonb_set(record,'{chunks,0,packed_start}','12'::jsonb),
                       '{chunks,0,packed_end}','24'::jsonb)
             WHERE record_key=$1",
        )
        .bind(last_key)
        .execute(pool)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    for (label, start, end, expected) in [
        ("late", 12, 23, true),
        ("combined", 0, 23, true),
        ("uncovered", 24, 35, false),
    ] {
        measure(
            store,
            pool,
            scope,
            hash,
            (start, end),
            (label, count, expected),
        )
        .await;
    }
    let outsider =
        RepositoryScope::new(RepositoryProvider::Generic, "outsider", "range", None).unwrap();
    measure(
        store,
        pool,
        &outsider,
        hash,
        (0, 11),
        ("wrong_scope", count, false),
    )
    .await;
    measure(
        store,
        pool,
        scope,
        &"c".repeat(64),
        (0, 11),
        ("missing_hash", count, false),
    )
    .await;
    if revoke {
        assert_eq!(
            sqlx::query("DELETE FROM shardline_file_records WHERE record_key=$1")
                .bind(last_key)
                .execute(pool)
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        measure(
            store,
            pool,
            scope,
            hash,
            (12, 23),
            ("revoked", count.checked_sub(1).unwrap(), false),
        )
        .await;
        measure(
            store,
            pool,
            scope,
            hash,
            (0, 11),
            ("remaining", count.checked_sub(1).unwrap(), true),
        )
        .await;
    } else {
        // Preserve the next size's original template and population.
        sqlx::query(
            "UPDATE shardline_file_records SET record=
             jsonb_set(jsonb_set(record,'{chunks,0,packed_start}','0'::jsonb),
                       '{chunks,0,packed_end}','12'::jsonb)
             WHERE record_key=$1",
        )
        .bind(last_key)
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn measure(
    store: &PostgresRecordStore,
    pool: &sqlx::PgPool,
    scope: &RepositoryScope,
    hash: &str,
    range: (u64, u64),
    case: (&str, i64, bool),
) {
    let (label, count, expected) = case;
    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        repository_covers_xorb_range(
            store,
            hash,
            scope,
            ByteRange::new(range.0, range.1).unwrap(),
        ),
    )
    .await
    .expect("synthetic authorization proof exceeded 120 seconds")
    .unwrap();
    assert_eq!(
        result, expected,
        "incorrect {label} proof at {count} records"
    );
    let proof_millis = milliseconds(started.elapsed());
    tokio::time::timeout(
        Duration::from_secs(120),
        sqlx::query("SELECT 1").execute(pool),
    )
    .await
    .unwrap()
    .unwrap();
    eprintln!(
        "range_scale case={label} records={count} proof_ms={proof_millis:.3} reused_pool_ms={:.3}",
        milliseconds(started.elapsed()),
    );
}
