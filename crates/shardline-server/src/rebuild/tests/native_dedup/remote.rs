//! Legacy source recovery against owned PostgreSQL and MinIO services.

use super::*;
use crate::{ObjectStorageAdapter, PostgresBackend, ServerConfig, ServerObjectStore};
use shardline_storage::{AsyncObjectStore, S3ObjectStoreConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO"]
async fn native_members_rebuild_all_legacy_scoped_sources_on_postgres_minio() {
    let stack = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .unwrap()
            .expect("legacy recovery qualification requires Docker")
    })
    .await
    .unwrap();
    let database = stack.postgres_url().unwrap();
    let owner = sqlx::PgPool::connect(&database).await.unwrap();
    crate::apply_database_migrations(&owner).await.unwrap();
    let raw = stack.s3_raw_config(Some("legacy-source-rebuild")).unwrap();
    let s3 = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(raw.allow_http)
        .with_key_prefix(raw.key_prefix.as_deref())
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let objects = ServerObjectStore::s3(s3.clone()).unwrap();
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
    let left =
        RepositoryScope::new(RepositoryProvider::GitHub, "alice", "model", Some("main")).unwrap();
    let right =
        RepositoryScope::new(RepositoryProvider::GitHub, "bob", "model", Some("main")).unwrap();
    let denied =
        RepositoryScope::new(RepositoryProvider::GitHub, "eve", "model", Some("main")).unwrap();
    let mut expected = Vec::new();
    for (scope, tail) in [
        (&left, b"left tail".as_slice()),
        (&right, b"right tail".as_slice()),
    ] {
        let (object, hash, shard) = fixture(tail);
        backend.upload_xorb(&hash, object).await.unwrap();
        backend
            .upload_shard_stream(
                RequestBodyReader::from_bytes(shard),
                Some(scope),
                ShardMetadataLimits::default(),
            )
            .await
            .unwrap();
        expected.push(hash);
    }
    let member = compute_data_hash(SHARED).hex();
    sqlx::query("DELETE FROM shardline_xet_xorbs")
        .execute(&owner)
        .await
        .unwrap();
    for payload in [SHARED, b"left tail".as_slice(), b"right tail".as_slice()] {
        AsyncObjectStore::delete_if_present(
            &objects,
            &crate::chunk_store::chunk_object_key(&compute_data_hash(payload).hex()).unwrap(),
        )
        .await
        .unwrap();
    }
    assert!(
        backend
            .record_store()
            .xet_chunk_candidates_after(&member, None, None)
            .await
            .unwrap()
            .is_empty()
    );
    backend.index_store().pool().close().await;
    backend.resource_lock_pool.close().await;
    drop(backend);

    let config = ServerConfig::new(
        "127.0.0.1:8080".parse().unwrap(),
        "http://localhost".to_owned(),
        root.path().to_owned(),
        NonZeroUsize::new(65536).unwrap(),
    )
    .with_object_storage(ObjectStorageAdapter::S3, Some(s3))
    .with_index_postgres_url(database.clone())
    .unwrap();
    let report = crate::rebuild::run_index_rebuild(config).await.unwrap();
    assert!(report.is_clean());
    let restarted = open().await.unwrap();
    assert_eq!(
        restarted
            .record_store()
            .xet_chunk_candidates_after(&member, None, None)
            .await
            .unwrap()
            .len(),
        2
    );
    for (scope, hash) in [(&left, &expected[0]), (&right, &expected[1])] {
        let candidates = restarted
            .record_store()
            .xet_chunk_candidates_after(&member, Some(scope), None)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(&candidates[0].hash, hash);
    }
    assert_eq!(restarted.read_chunk(&member).await.unwrap(), SHARED);
    let backend = crate::ServerBackend::Postgres(restarted);
    for scope in [&left, &right] {
        assert!(
            !backend
                .scoped_dedup_shard(&member, Some(scope), ShardMetadataLimits::default())
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert!(matches!(
        backend
            .scoped_dedup_shard(&member, Some(&denied), ShardMetadataLimits::default())
            .await,
        Err(ServerError::NotFound)
    ));
    if let crate::ServerBackend::Postgres(backend) = backend {
        backend.index_store().pool().close().await;
        backend.resource_lock_pool.close().await;
    }
    owner.close().await;
}
