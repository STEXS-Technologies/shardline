//! Physical native-source reclamation through the fenced production GC path.

mod publication;
mod repair;

use crate::ServerObjectStore;
use shardline_index::{
    FileChunkRecord, FileRecord, PostgresIndexStore, PostgresRecordStore, RecordMutation,
    RecordTraversal, StorageRepresentation,
};
use shardline_storage::{AsyncObjectStore, S3ObjectStoreConfig};

async fn replace_source(
    objects: &ServerObjectStore,
    key: &shardline_storage::ObjectKey,
    bytes: &[u8],
) {
    AsyncObjectStore::delete_if_present(objects, key)
        .await
        .unwrap();
    AsyncObjectStore::put_if_absent(
        objects,
        key,
        shardline_storage::ObjectBody::from_slice(bytes),
        &shardline_storage::ObjectIntegrity::new(
            shardline_server_core::chunk_hash(bytes),
            bytes.len() as u64,
        ),
    )
    .await
    .unwrap();
}
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

async fn source(objects: &ServerObjectStore, content: &[u8]) -> String {
    let mut data = content.to_vec();
    let mut leaves = vec![(compute_data_hash(content), content.len() as u64)];
    let mut boundaries = vec![(leaves[0].0, content.len() as u64)];
    // Exercise repair of a real multi-member payload, including members shared
    // by the retained source and the orphan which will be physically swept.
    for index in 0_u8..32 {
        let member = vec![index; 64 * 1024];
        let leaf = compute_data_hash(&member);
        leaves.push((leaf, member.len() as u64));
        data.extend_from_slice(&member);
        boundaries.push((leaf, data.len() as u64));
    }
    let hash = xorb_hash(&leaves);
    let bytes =
        serialized_xorb_object_from_components(&hash, data, boundaries, CompressionScheme::None)
            .unwrap()
            .serialized_data;
    shardline_xet_adapter::store_uploaded_xorb_bytes(objects, &hash.hex(), &bytes)
        .await
        .unwrap();
    hash.hex()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_source_physical_sweep_survives_lost_index_and_copy() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .unwrap()
            .expect("native GC qualification requires Docker")
    })
    .await
    .unwrap();
    let database = stack.postgres_url().unwrap();
    let owner = sqlx::PgPool::connect(&database).await.unwrap();
    crate::apply_database_migrations(&owner).await.unwrap();
    let raw = stack.s3_raw_config(Some("native-gc-recovery")).unwrap();
    let config = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(raw.allow_http)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let objects = ServerObjectStore::s3(config).unwrap();
    let content = b"historically retained native payload";
    let live = source(&objects, content).await;
    let orphan = source(&objects, b"reclaimable native payload").await;
    let member = compute_data_hash(content).hex();
    AsyncObjectStore::delete_if_present(
        &objects,
        &crate::chunk_store::chunk_object_key(&member).unwrap(),
    )
    .await
    .unwrap();
    let pool = crate::maintenance_barrier::postgres_exclusive_work_pool(&database)
        .await
        .unwrap();
    let records = PostgresRecordStore::new(pool.clone());
    records
        .write_version_record(&FileRecord {
            file_id: "historical-native-member".to_owned(),
            content_hash: member.clone(),
            total_bytes: content.len() as u64,
            chunk_size: content.len() as u64,
            storage_repr: StorageRepresentation::FixedChunkV1,
            repository_scope: None,
            chunks: vec![FileChunkRecord {
                hash: member.clone(),
                offset: 0,
                length: content.len() as u64,
                range_start: 0,
                range_end: 1,
                packed_start: 0,
                packed_end: 0,
            }],
        })
        .await
        .unwrap();
    assert!(
        records
            .native_chunk_reference_page(None)
            .await
            .unwrap()
            .is_empty()
    );
    let live_key = shardline_xet_adapter::xorb_object_key(&live).unwrap();
    let orphan_key = shardline_xet_adapter::xorb_object_key(&orphan).unwrap();
    // Exercise a real (short fixture) retention interval without changing the
    // clock or editing quarantine timestamps. This is not the default 24h gate.
    let retention_index = PostgresIndexStore::new(pool.clone());
    shardline_gc::run_postgres_gc_with_remote_delete_protection(
        &records,
        &retention_index,
        &objects,
        &[crate::ServerFrontend::Xet],
        shardline_gc::LocalGcOptions::mark_only(5),
    )
    .await
    .unwrap();
    shardline_gc::run_postgres_gc_with_remote_delete_protection(
        &records,
        &retention_index,
        &objects,
        &[crate::ServerFrontend::Xet],
        shardline_gc::LocalGcOptions::sweep_only(),
    )
    .await
    .unwrap();
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_some(),
        "unexpired quarantine must prevent physical deletion"
    );
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // Stop the old invocation inside its live-record scan, then transfer real
    // maintenance ownership. No MinIO delete may escape this stale owner.
    let old_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let mut blocker = owner.begin().await.unwrap();
    sqlx::query("LOCK TABLE shardline_file_records IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .unwrap();
    let work = pool.clone();
    let old_objects = objects.clone();
    let stale = tokio::spawn(async move {
        shardline_gc::run_postgres_gc_with_remote_delete_protection(
            &PostgresRecordStore::new(work.clone()),
            &PostgresIndexStore::new(work),
            &old_objects,
            &[crate::ServerFrontend::Xet],
            shardline_gc::LocalGcOptions::mark_and_sweep(0),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND query LIKE '%shardline_file_records%')")
                .bind(old_pid).fetch_one(&owner).await.unwrap();
            if blocked { break; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_terminate_backend($1)")
            .bind(old_pid)
            .fetch_one(&owner)
            .await
            .unwrap()
    );
    let replacement = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        crate::maintenance_barrier::postgres_exclusive_work_pool(&database),
    )
    .await
    .unwrap()
    .unwrap();
    blocker.rollback().await.unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(15), stale)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(
        sqlx::query("SELECT 1").execute(&pool).await.is_err(),
        "stale work pool must not reconnect after takeover"
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_some()
    );
    pool.close().await;
    let pool = replacement;
    let records = PostgresRecordStore::new(pool.clone());
    let index = PostgresIndexStore::new(pool.clone());
    let publication = publication::Pending::start(&database, objects.clone(), &owner).await;
    repair::qualify(&database, pool.clone(), objects.clone(), &live).await;
    publication.assert_blocked(&owner).await;
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &orphan_key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        shardline_xet_adapter::read_verified_xorb_chunk(&objects, &live, &member)
            .unwrap()
            .unwrap(),
        content
    );
    let length = AsyncObjectStore::metadata(&objects, &live_key)
        .await
        .unwrap()
        .unwrap()
        .length();
    let original = objects.read_full_object(&live_key, length).unwrap();
    // The overlapping repair restored the derived source membership. Remove
    // it again before testing the separate lost-index recovery failure: an
    // intact index legitimately protects a live source without scanning its
    // current payload as a health check.
    sqlx::query("DELETE FROM shardline_xet_xorbs WHERE xorb_hash=$1")
        .bind(&live)
        .execute(&pool)
        .await
        .unwrap();
    assert!(
        records
            .native_chunk_reference_page(None)
            .await
            .unwrap()
            .is_empty()
    );
    let mut corrupt = original.clone();
    *corrupt.get_mut(8).unwrap() ^= 1;
    replace_source(&objects, &live_key, &corrupt).await;
    assert!(
        shardline_gc::run_postgres_gc_with_remote_delete_protection(
            &records,
            &index,
            &objects,
            &[crate::ServerFrontend::Xet],
            shardline_gc::LocalGcOptions::mark_and_sweep(0),
        )
        .await
        .is_err(),
        "corrupt recovery candidate must stop remote sweep"
    );
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_some()
    );
    replace_source(&objects, &live_key, &original).await;
    for locator in records.list_version_record_locators().await.unwrap() {
        records.delete_record_locator(&locator).await.unwrap();
    }
    shardline_gc::run_postgres_gc_with_remote_delete_protection(
        &records,
        &index,
        &objects,
        &[crate::ServerFrontend::Xet],
        shardline_gc::LocalGcOptions::mark_and_sweep(0),
    )
    .await
    .unwrap();
    assert!(
        AsyncObjectStore::metadata(&objects, &live_key)
            .await
            .unwrap()
            .is_none()
    );
    pool.close().await;
    publication.finish().await;
    owner.close().await;
}
