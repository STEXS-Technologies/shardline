//! Live backend qualification; prerequisites are mandatory when selected.

use crate::{PostgresBackend, ServerError, ServerObjectStore};
use shardline_storage::{AsyncObjectStore, S3ObjectStoreConfig};
use std::num::NonZeroUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_members_survive_replica_restart_without_standalone_copies() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .unwrap()
            .expect("native member qualification requires Docker")
    })
    .await
    .unwrap();
    let database = stack.postgres_url().unwrap();
    let owner = sqlx::PgPool::connect(&database).await.unwrap();
    crate::apply_database_migrations(&owner).await.unwrap();
    let raw = stack.s3_raw_config(Some("native-member-read")).unwrap();
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
    let root = tempfile::tempdir().unwrap();
    let open = || {
        PostgresBackend::new_with_object_store(
            root.path().to_owned(),
            "http://localhost".to_owned(),
            NonZeroUsize::new(65536).unwrap(),
            &database,
            objects.clone(),
        )
    };
    let backend = open().await.unwrap();
    let content = b"a native member survives independent replica construction";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(content);
    backend.upload_xorb(&hash, xorb).await.unwrap();
    let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(content, &hash)]);
    backend
        .upload_shard_stream(
            crate::upload_ingest::RequestBodyReader::from_bytes(shard),
            None,
            crate::ShardMetadataLimits::default(),
        )
        .await
        .unwrap();
    let member = shardline_xet_core::merklehash::compute_data_hash(content).hex();
    let copy = crate::chunk_store::chunk_object_key(&member).unwrap();
    AsyncObjectStore::delete_if_present(&objects, &copy)
        .await
        .unwrap();
    assert!(
        AsyncObjectStore::metadata(&objects, &copy)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(backend.read_chunk(&member).await.unwrap(), content);
    assert_eq!(
        backend.chunk_length(&member).await.unwrap(),
        content.len() as u64
    );
    assert_eq!(
        backend.download_file(&file_hash, None, None).await.unwrap(),
        content
    );
    let report = shardline_fsck::run_fsck_with_stores(
        backend.record_store(),
        backend.index_store(),
        &root.path().join("chunks"),
        &objects,
        crate::ShardMetadataLimits::default(),
    )
    .await
    .unwrap();
    assert!(
        report.is_clean(),
        "Xorb-only source must pass FSCK: {:?}",
        report.issues
    );
    backend.index_store().pool().close().await;
    backend.resource_lock_pool.close().await;
    drop(backend);
    let restarted = open().await.unwrap();
    let generic = blake3::hash(content).to_hex().to_string();
    assert_ne!(generic, member);
    // A derived discovery row cannot turn equal payload bytes into a valid
    // identity from a different hash domain. Exercise the actual SQL lookup,
    // source verification and MinIO read after replacing the backend instance.
    sqlx::query(
        "INSERT INTO shardline_xet_chunks (xorb_hash,chunk_index,chunk_hash) VALUES ($1,99,$2)",
    )
    .bind(&hash)
    .bind(&generic)
    .execute(&owner)
    .await
    .unwrap();
    assert!(
        !restarted
            .record_store()
            .xet_chunk_candidates_after(&generic, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        restarted.read_chunk(&generic).await,
        Err(ServerError::NotFound)
    ));
    assert!(matches!(
        restarted.chunk_length(&generic).await,
        Err(ServerError::NotFound)
    ));
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1 AND chunk_index=99")
        .bind(&hash)
        .execute(&owner)
        .await
        .unwrap();
    let native_references = shardline_index::RecordTraversal::native_chunk_reference_page(
        restarted.record_store(),
        None,
    )
    .await
    .unwrap();
    assert!(native_references.contains(&(hash.clone(), 0, member.clone())));
    assert!(
        shardline_index::RecordTraversal::native_chunk_reference_page(
            restarted.record_store(),
            Some((hash.clone(), 0)),
        )
        .await
        .unwrap()
        .is_empty()
    );
    assert_eq!(restarted.read_chunk(&member).await.unwrap(), content);
    assert_eq!(
        restarted
            .download_file(&file_hash, None, None)
            .await
            .unwrap(),
        content
    );
    assert!(matches!(
        restarted.read_chunk(&"ab".repeat(32)).await,
        Err(ServerError::NotFound)
    ));
    let descriptor = restarted
        .record_store()
        .xet_xorb_descriptor(&hash)
        .await
        .unwrap()
        .unwrap();
    sqlx::query("DELETE FROM shardline_xet_chunks WHERE xorb_hash=$1")
        .bind(&hash)
        .execute(&owner)
        .await
        .unwrap();
    assert_eq!(
        restarted
            .record_store()
            .xet_xorb_descriptor(&hash)
            .await
            .unwrap(),
        Some(descriptor.clone())
    );
    assert!(matches!(
        restarted.read_chunk(&member).await,
        Err(ServerError::NotFound)
    ));
    let report = shardline_fsck::run_fsck_with_stores(
        restarted.record_store(),
        restarted.index_store(),
        &root.path().join("chunks"),
        &objects,
        crate::ShardMetadataLimits::default(),
    )
    .await
    .unwrap();
    assert!(
        !report.is_clean(),
        "missing native location must remain an integrity finding"
    );
    let rebuilt = crate::rebuild::run_index_rebuild_with_stores(
        restarted.record_store(),
        restarted.index_store(),
        &objects,
        crate::ShardMetadataLimits::default(),
    )
    .await
    .unwrap();
    assert!(rebuilt.is_clean());
    assert_eq!(restarted.read_chunk(&member).await.unwrap(), content);
    sqlx::query("UPDATE shardline_xet_chunks SET chunk_hash=$2 WHERE xorb_hash=$1")
        .bind(&hash)
        .bind("dd".repeat(32))
        .execute(&owner)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO shardline_xet_chunks (xorb_hash,chunk_index,chunk_hash) VALUES ($1,99,$2)",
    )
    .bind(&hash)
    .bind("ee".repeat(32))
    .execute(&owner)
    .await
    .unwrap();
    restarted
        .record_store()
        .cache_xet_xorb_descriptors(&[descriptor])
        .await
        .unwrap();
    assert_eq!(restarted.read_chunk(&member).await.unwrap(), content);
    let count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM shardline_xet_chunks WHERE xorb_hash=$1")
            .bind(&hash)
            .fetch_one(&owner)
            .await
            .unwrap();
    assert_eq!(count.0, 1, "replay must remove stale extra member rows");
    AsyncObjectStore::delete_if_present(
        &objects,
        &crate::xet_adapter::xorb_object_key(&hash).unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        restarted.read_chunk(&member).await,
        Err(ServerError::NotFound)
    ));
    let report = shardline_fsck::run_fsck_with_stores(
        restarted.record_store(),
        restarted.index_store(),
        &root.path().join("chunks"),
        &objects,
        crate::ShardMetadataLimits::default(),
    )
    .await
    .unwrap();
    assert!(!report.is_clean(), "missing backing Xorb must fail FSCK");
    restarted.index_store().pool().close().await;
    restarted.resource_lock_pool.close().await;
    owner.close().await;
}
