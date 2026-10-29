//! Indexed lookup bounds and snapshot consistency with real PostgreSQL.
use super::*;

#[tokio::test]
async fn maximum_members_include_last_corruption_and_excess_rows() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = crate::PostgresRecordStore::new(pool.clone());
    let mut target = descriptor();
    target.chunks = (0..crate::MAX_XET_DESCRIPTOR_CHUNKS)
        .map(|index| {
            let offset = u64::try_from(index).unwrap();
            XetXorbChunk {
                // Repeated members are legal; membership is ordered, not a set.
                hash: if index % 2 == 0 { "b" } else { "c" }.repeat(64),
                packed_start: offset * 16,
                packed_end: (offset + 1) * 16,
                unpacked_start: offset * 8,
                unpacked_end: (offset + 1) * 8,
            }
        })
        .collect();
    target.total_bytes = u64::try_from(target.chunks.len()).unwrap() * 16 + 32;
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target.clone())
    );
    sqlx::query(
        "UPDATE shardline_xet_chunks SET chunk_hash=$2 WHERE xorb_hash=$1 AND chunk_index=$3",
    )
    .bind(&target.hash)
    .bind("d".repeat(64))
    .bind(i64::try_from(target.chunks.len() - 1).unwrap())
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES($1,$2,$3)",
    )
    .bind(&target.hash)
    .bind(i64::try_from(target.chunks.len()).unwrap())
    .bind("e".repeat(64))
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap()
            .is_none()
    );
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&target))
        .await
        .unwrap();
    assert_eq!(
        store
            .xet_indexed_xorb_descriptor(&target.hash)
            .await
            .unwrap(),
        Some(target)
    );
    pool.close().await;
}

#[tokio::test]
async fn concurrent_descriptor_and_member_changes_never_mix_snapshots() {
    let Some(pool) = crate::postgres::connect_isolated_postgres().await else {
        return;
    };
    let store = crate::PostgresRecordStore::new(pool.clone());
    let original = descriptor();
    let mut replacement = original.clone();
    replacement.chunks.first_mut().unwrap().hash = "c".repeat(64);
    // These are derived-index fixtures, not authenticated payload descriptors.
    // The lookup must observe descriptor/member pairs from one committed state.
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&original))
        .await
        .unwrap();
    let writer = async {
        for iteration in 0..64 {
            let value = if iteration % 2 == 0 {
                &replacement
            } else {
                &original
            };
            let mut transaction = pool.begin().await.unwrap();
            sqlx::query("UPDATE shardline_xet_xorbs SET descriptor=$2 WHERE xorb_hash=$1")
                .bind(&value.hash)
                .bind(serde_json::to_value(value).unwrap())
                .execute(&mut *transaction)
                .await
                .unwrap();
            tokio::task::yield_now().await;
            sqlx::query("UPDATE shardline_xet_chunks SET chunk_hash=$2 WHERE xorb_hash=$1")
                .bind(&value.hash)
                .bind(&value.chunks.first().unwrap().hash)
                .execute(&mut *transaction)
                .await
                .unwrap();
            transaction.commit().await.unwrap();
        }
    };
    let reader = async {
        for _ in 0..128 {
            let observed = store
                .xet_indexed_xorb_descriptor(&original.hash)
                .await
                .unwrap()
                .unwrap();
            assert!(observed == original || observed == replacement);
        }
    };
    tokio::join!(writer, reader);
    pool.close().await;
}
