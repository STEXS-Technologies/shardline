use axum::body::Bytes;
use shardline_protocol::ShardlineHash;
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore};

use super::*;

#[tokio::test]
async fn postgres_stats_count_latest_files_and_physical_objects_and_propagate_failures() {
    let Some(fixture) = crate::test_support::postgres_fixture::PostgresFixture::new().await else {
        return;
    };
    fixture.migrate().await;
    let backend = fixture.postgres();
    assert_eq!(
        backend.stats().await.unwrap(),
        ServerStatsResponse {
            objects: 0,
            object_bytes: 0,
            chunks: 0,
            chunk_bytes: 0,
            files: 0
        }
    );
    backend
        .upload_file("file-one", Bytes::from_static(b"first"), None)
        .await
        .unwrap();
    let first = backend.stats().await.unwrap();
    assert_eq!(first.files, 1);
    assert_eq!(first.chunks, 1);
    assert!(first.objects >= first.chunks);
    assert!(first.object_bytes >= first.chunk_bytes);
    assert!(first.chunk_bytes > 0);

    backend
        .upload_file("file-one", Bytes::from_static(b"second"), None)
        .await
        .unwrap();
    let replaced = backend.stats().await.unwrap();
    assert_eq!(replaced.files, 1, "versions must not count as new files");
    assert_eq!(
        replaced.chunks, 2,
        "immutable old chunks remain physical objects"
    );
    assert!(replaced.chunk_bytes > first.chunk_bytes);

    backend
        .upload_file("file-two", Bytes::from_static(b"first"), None)
        .await
        .unwrap();
    let deduplicated = backend.stats().await.unwrap();
    assert_eq!(
        deduplicated,
        ServerStatsResponse {
            files: 2,
            ..replaced
        }
    );

    let bytes = b"auxiliary";
    backend
        .object_store()
        .put_if_absent(
            &ObjectKey::parse("health/stats-fixture").unwrap(),
            ObjectBody::from_slice(bytes),
            &ObjectIntegrity::new(
                ShardlineHash::from_bytes(*blake3::hash(bytes).as_bytes()),
                u64::try_from(bytes.len()).unwrap(),
            ),
        )
        .unwrap();
    assert_eq!(
        backend.stats().await.unwrap(),
        ServerStatsResponse {
            objects: deduplicated.objects + 1,
            object_bytes: deduplicated.object_bytes + u64::try_from(bytes.len()).unwrap(),
            ..deduplicated
        }
    );
    backend.record_store().pool().close().await;
    assert!(
        backend.stats().await.is_err(),
        "metadata failure must not return partial statistics"
    );
    fixture.finish().await;
}
