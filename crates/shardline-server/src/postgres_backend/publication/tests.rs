#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Publication must not expose chunks removed after immutable staging.
use crate::{test_support::postgres_fixture::PostgresFixture, upload_ingest::RequestBodyReader};
use axum::body::Bytes;
use shardline_index::{ResourceLockKey, S3ObjectEntry, S3PublishCondition};
use shardline_storage::{ObjectKey, ObjectStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn s3_publication_rejects_a_chunk_deleted_after_staging() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let namespace = "01".repeat(32);
    let logical = ObjectKey::parse(&format!("protocols/s3/{namespace}/staged-object")).unwrap();
    let bytes = Bytes::from_static(b"orphan bytes prepared before the delete completed");
    let seed = fixture
        .backend
        .prepare_s3_object_stream(&logical, RequestBodyReader::from_bytes(bytes.clone()))
        .await
        .unwrap();
    let hash = &seed.record.chunks[0].hash;
    let physical = crate::xet_adapter::xorb_object_key(hash).unwrap();
    let backend = fixture.postgres();
    let objects = backend.object_store();
    assert!(
        objects.metadata(&physical).unwrap().is_some(),
        "delete must target the actual staged backing object"
    );
    let permit = fixture
        .index()
        .begin_remote_delete(&physical)
        .await
        .unwrap();
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&namespace, "staged-object"),
        )
        .await
        .unwrap();
    let prepared = fixture
        .backend
        .prepare_s3_object_stream(&logical, RequestBodyReader::from_bytes(bytes))
        .await
        .unwrap();
    let entry = S3ObjectEntry {
        scope_namespace: namespace,
        object_key: "staged-object".to_owned(),
        file_id: prepared.record.file_id.clone(),
        size_bytes: prepared.record.total_bytes,
        content_hash: prepared.record.content_hash.clone(),
        etag: "02".repeat(16),
        user_metadata: Vec::new(),
        updated_at_unix_seconds: 1,
    };
    let pending = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    guard.release().await.unwrap();
    assert_eq!(
        objects.delete_if_present(&physical).unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    );
    assert!(fixture.index().finish_remote_delete(&permit).await.unwrap());
    assert!(objects.metadata(&physical).unwrap().is_none());
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&entry.scope_namespace, "staged-object"),
        )
        .await
        .unwrap();
    let publication = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    let conditional = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &prepared,
            &entry,
            &S3PublishCondition::IfUnchanged(Some(entry.clone())),
            None,
        )
        .await;
    guard.release().await.unwrap();
    let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_s3_objects",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    let mut connection = fixture.index().pool().acquire().await.unwrap();
    let file_publication = backend
        .commit_staged_file_on_connection(&mut connection, &prepared.record)
        .await;
    connection.close().await.unwrap();
    let visible_files: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_file_records",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    let retry = fixture
        .backend
        .prepare_s3_object_stream(
            &logical,
            RequestBodyReader::from_bytes(Bytes::from_static(
                b"orphan bytes prepared before the delete completed",
            )),
        )
        .await
        .unwrap();
    let mut guard = fixture
        .backend
        .acquire_resource_write_lock(
            fixture.config.root_dir(),
            &ResourceLockKey::s3_object(&entry.scope_namespace, "staged-object"),
        )
        .await
        .unwrap();
    let retried = fixture
        .backend
        .publish_s3_object_locked(
            &mut guard,
            &retry,
            &entry,
            &S3PublishCondition::Unconditional,
            None,
        )
        .await;
    guard.release().await.unwrap();
    fixture.finish().await;
    assert!(pending.unwrap_err().is_remote_delete_pending());
    assert!(
        !conditional.unwrap(),
        "a failed precondition must retain its existing result"
    );
    assert!(
        publication.is_err(),
        "deleted staged content must not become visible: {publication:?}"
    );
    let error = publication.unwrap_err();
    assert!(
        !error.is_remote_delete_pending(),
        "the completed delete is no longer pending"
    );
    assert_eq!(
        error.status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(error.to_string().contains("retry the upload"));
    assert_eq!(visible, 0);
    assert_eq!(
        visible_files, 0,
        "both publication paths must roll back file metadata"
    );
    assert_eq!(
        file_publication.unwrap_err().status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(
        retried.unwrap(),
        "a fresh retry restores the chunks and publishes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staged_content_locks_last_until_the_publication_transaction_ends() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let (record, _) = backend
        .prepare_file_stream(
            "locked-file",
            RequestBodyReader::from_bytes(Bytes::from_static(b"locked backing bytes")),
            None,
        )
        .await
        .unwrap();
    let physical = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
    let mut publication = fixture.index().pool().begin().await.unwrap();
    backend
        .verify_staged_file_on_connection(&mut publication, &record)
        .await
        .unwrap();
    let mut contender = fixture.admin.begin().await.unwrap();
    let statement = "SELECT pg_try_advisory_xact_lock(1397507140, (hashtextextended('shardline:remote-delete:' || $1, 0) & 255)::INTEGER)";
    let before: bool = sqlx::query_scalar(statement)
        .bind(physical.as_str())
        .fetch_one(&mut *contender)
        .await
        .unwrap();
    publication.rollback().await.unwrap();
    let after: bool = sqlx::query_scalar(statement)
        .bind(physical.as_str())
        .fetch_one(&mut *contender)
        .await
        .unwrap();
    contender.rollback().await.unwrap();
    fixture.finish().await;
    assert!(
        !before,
        "DELETE admission must wait through metadata publication"
    );
    assert!(
        after,
        "the backing-object lock must release with the transaction"
    );
}
