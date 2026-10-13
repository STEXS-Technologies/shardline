#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Publication must not expose chunks removed after immutable staging.
use crate::{test_support::postgres_fixture::PostgresFixture, upload_ingest::RequestBodyReader};
use axum::body::Bytes;
use shardline_index::{ResourceLockKey, S3ObjectEntry, S3PublishCondition};
use shardline_storage::{ObjectKey, ObjectStore};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn guarded_record_reads_do_not_reenter_the_request_connection() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    let mut backend = fixture.postgres().clone();
    backend
        .upload_file("guarded-read", Bytes::from_static(b"guarded bytes"), None)
        .await
        .unwrap();
    // Production has a separate record-read pool. Keep one connection per pool
    // to expose mutex re-entry without requiring a second ownership connection.
    let read_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            backend
                .record_store
                .pool()
                .connect_options()
                .as_ref()
                .clone(),
        )
        .await
        .unwrap();
    backend.record_store = shardline_index::PostgresRecordStore::new(read_pool.clone());
    let session = crate::maintenance_barrier::RequestSession::acquire(
        backend.index_store.pool(),
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            let guard = crate::maintenance_barrier::acquire_postgres_request_resources(
                backend.index_store.pool(),
                &backend.request_session_identity,
                &[ResourceLockKey::s3_object("guarded-scope", "guarded-read")],
            )
            .await
            .unwrap();
            let record = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                backend.read_record("guarded-read", None, None),
            )
            .await
            .expect("record reads must not re-enter the held request mutex")
            .unwrap();
            assert_eq!(record.total_bytes, 13);
            drop(guard);
        })
        .await;
    session.finish().await;
    read_pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_reference_copy_cannot_replace_a_lost_request_owner() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    let backend = fixture.postgres();
    backend
        .upload_file("copy-source", Bytes::from_static(b"copy ownership"), None)
        .await
        .unwrap();
    let source = backend
        .read_record("copy-source", None, None)
        .await
        .unwrap();
    let session = crate::maintenance_barrier::RequestSession::acquire(
        backend.index_store.pool(),
        backend.request_session_identity.clone(),
    )
    .await
    .unwrap();
    session
        .scope(async {
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-owned")
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .delete_file_reference_if_latest("copy-owned", &source.content_hash)
                    .await
                    .unwrap()
            );
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-delete")
                    .await
                    .unwrap()
            );
            assert!(backend.delete_file_reference("copy-delete").await.unwrap());
            assert!(!backend.delete_file_reference("copy-delete").await.unwrap());
            let mut owner = crate::maintenance_barrier::RequestSession::connection_for(
                &backend.request_session_identity,
            )
            .await
            .unwrap();
            let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut **owner.as_mut().unwrap())
                .await
                .unwrap();
            drop(owner);
            let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
                .bind(pid)
                .fetch_one(&fixture.admin)
                .await
                .unwrap();
            assert!(terminated);
            assert!(backend.delete_file_reference("copy-source").await.is_err());
            assert!(
                backend
                    .delete_file_reference_if_latest("copy-source", &source.content_hash)
                    .await
                    .is_err()
            );
            assert!(
                backend
                    .copy_file_reference("copy-source", "copy-owner-lost")
                    .await
                    .is_err()
            );
        })
        .await;
    session.finish().await;
    assert!(backend.read_record("copy-source", None, None).await.is_ok());
    assert!(matches!(
        backend.read_record("copy-owner-lost", None, None).await,
        Err(crate::ServerError::NotFound)
    ));
    assert!(
        backend
            .copy_file_reference("copy-source", "copy-fresh")
            .await
            .unwrap()
    );
    assert!(
        !backend
            .copy_file_reference("copy-source", "copy-fresh")
            .await
            .unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_transaction_snapshot_cannot_hide_a_pending_delete() {
    for (protocol, isolation) in ["REPEATABLE READ", "SERIALIZABLE", "REPEATABLE READ"]
        .into_iter()
        .enumerate()
    {
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
                "snapshot-file",
                RequestBodyReader::from_bytes(Bytes::from_static(
                    b"pending deletion after snapshot",
                )),
                None,
            )
            .await
            .unwrap();
        let physical = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
        assert!(
            backend
                .object_store()
                .metadata(&physical)
                .unwrap()
                .is_some()
        );
        let mut snapshot = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *snapshot)
        .await
        .unwrap();
        let before: i64 = sqlx::query_scalar("SELECT count(*) FROM shardline_resource_fences WHERE domain='maintenance-remote-delete'")
            .fetch_one(&mut *snapshot).await.unwrap();
        assert_eq!(before, 0);
        let epoch: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_begin_v1($1, $2)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind(physical.as_str())
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let publication = backend
            .commit_staged_file_on_connection(&mut snapshot, &record)
            .await;
        if publication.is_ok() {
            snapshot.commit().await.unwrap();
        } else {
            snapshot.rollback().await.unwrap();
        }
        let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(*) FROM {}.shardline_file_records",
            fixture.schema,
        )))
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let settled: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_finish_v1($1, $2, $3)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind(physical.as_str())
        .bind(epoch)
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        let mut retry = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *retry)
        .await
        .unwrap();
        let retried = backend
            .commit_staged_file_on_connection(&mut retry, &record)
            .await;
        let retained_isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
            .fetch_one(&mut *retry)
            .await
            .unwrap();
        retry.commit().await.unwrap();
        fixture.finish().await;
        assert!(settled);
        assert!(
            publication.is_err(),
            "{isolation} must not publish through an invisible pending marker: {publication:?}"
        );
        assert_eq!(visible, 0, "{isolation} must preserve publication safety");
        assert!(
            retried.is_ok(),
            "a fresh {isolation} retry must succeed: {retried:?}"
        );
        assert_eq!(retained_isolation, isolation.to_lowercase());
        let error = publication.unwrap_err();
        assert!(!error.is_remote_delete_pending());
        use axum::response::IntoResponse;
        let response = match protocol {
            0 => error.into_response(),
            1 => crate::error::OciError(error).into_response(),
            _ => shardline_s3_adapter::S3Error::from(error).into_response(),
        };
        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(
            std::str::from_utf8(&body)
                .unwrap()
                .contains("retry the request in a new transaction")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_publication_does_not_wait_on_a_delete_holding_physical_locks() {
    for isolation in ["REPEATABLE READ", "SERIALIZABLE"] {
        let Some(fixture) = PostgresFixture::new().await else {
            return;
        };
        fixture
            .index()
            .initialize_remote_delete_guard()
            .await
            .unwrap();
        let mut publication = fixture.index().pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "SET TRANSACTION ISOLATION LEVEL {isolation}"
        )))
        .execute(&mut *publication)
        .await
        .unwrap();
        sqlx::query("SELECT shardline_remote_delete_guard_v1(current_schema()::text, $1)")
            .bind("snapshot-order/first")
            .execute(&mut *publication)
            .await
            .unwrap();
        let statement = sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_begin_v1($1, $2)",
            fixture.schema,
        ));
        let mut admission = Box::pin(
            sqlx::query_scalar::<_, i64>(statement)
                .bind(&fixture.schema)
                .bind("snapshot-order/second")
                .fetch_one(&fixture.admin),
        );
        let waiting = tokio::time::timeout(std::time::Duration::from_millis(100), &mut admission)
            .await
            .is_err();
        // While admission waits on the snapshot lock, this same transaction
        // must still be able to publish a second physical key.
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            sqlx::query("SELECT shardline_remote_delete_guard_v1(current_schema()::text, $1)")
                .bind("snapshot-order/second")
                .execute(&mut *publication),
        )
        .await;
        publication.rollback().await.unwrap();
        let epoch = tokio::time::timeout(std::time::Duration::from_secs(5), admission)
            .await
            .unwrap()
            .unwrap();
        let settled: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {}.shardline_remote_delete_finish_v1($1, $2, $3)",
            fixture.schema,
        )))
        .bind(&fixture.schema)
        .bind("snapshot-order/second")
        .bind(epoch)
        .fetch_one(&fixture.admin)
        .await
        .unwrap();
        fixture.finish().await;
        assert!(
            waiting,
            "{isolation} must hold delete admission until publication ends"
        );
        assert!(
            matches!(second, Ok(Ok(_))),
            "{isolation} must avoid reversed lock ordering: {second:?}"
        );
        assert!(settled);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_shard_publication_rejects_a_deleted_validated_xorb() {
    let Some(fixture) = PostgresFixture::new().await else {
        return;
    };
    fixture
        .index()
        .initialize_remote_delete_guard()
        .await
        .unwrap();
    let backend = fixture.postgres();
    let payload = b"validated native backing bytes";
    let (xorb, hash) = crate::test_fixtures::single_chunk_xorb(payload);
    backend.upload_xorb(&hash, xorb.clone()).await.unwrap();
    let (shard, file_hash) = crate::test_fixtures::single_file_shard(&[(payload, &hash)]);
    let mut validated = None;
    crate::xet_adapter::register_uploaded_shard_bytes_with_member_references(
        &backend.object_store(),
        &shard,
        None,
        crate::ShardMetadataLimits::default(),
        |records, mappings, references| {
            validated = Some((records, mappings, references));
            async { Ok(()) }
        },
    )
    .await
    .unwrap();
    let (records, mappings, references) = validated.unwrap();
    assert!(!records.is_empty());
    assert!(!mappings.is_empty());
    let physical = crate::xet_adapter::xorb_object_key(&hash).unwrap();
    assert!(
        backend
            .object_store()
            .metadata(&physical)
            .unwrap()
            .is_some()
    );
    let permit = fixture
        .index()
        .begin_remote_delete(&physical)
        .await
        .unwrap();
    let pending = backend
        .commit_validated_native_shard(&records, &mappings, &references)
        .await;
    let deleted = backend.object_store().delete_if_present(&physical).unwrap();
    assert_eq!(deleted, shardline_storage::DeleteOutcome::Deleted);
    assert!(fixture.index().finish_remote_delete(&permit).await.unwrap());
    let publication = backend
        .commit_validated_native_shard(&records, &mappings, &references)
        .await;
    let visible: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {}.shardline_file_records",
        fixture.schema,
    )))
    .fetch_one(&fixture.admin)
    .await
    .unwrap();
    backend.upload_xorb(&hash, xorb).await.unwrap();
    let shard_key = mappings.first().unwrap().shard_object_key().clone();
    assert!(
        backend
            .object_store()
            .metadata(&shard_key)
            .unwrap()
            .is_some()
    );
    let shard_permit = fixture
        .index()
        .begin_remote_delete(&shard_key)
        .await
        .unwrap();
    assert_eq!(
        backend
            .object_store()
            .delete_if_present(&shard_key)
            .unwrap(),
        shardline_storage::DeleteOutcome::Deleted
    );
    assert!(
        fixture
            .index()
            .finish_remote_delete(&shard_permit)
            .await
            .unwrap()
    );
    let missing_shard = backend
        .commit_validated_native_shard(&records, &mappings, &references)
        .await;
    let partial_rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT (SELECT count(*) FROM {}.shardline_file_records) + (SELECT count(*) FROM {}.shardline_dedupe_shards)",
        fixture.schema, fixture.schema,
    ))).fetch_one(&fixture.admin).await.unwrap();
    let retried = backend
        .upload_shard_stream(
            RequestBodyReader::from_bytes(shard),
            None,
            crate::ShardMetadataLimits::default(),
        )
        .await;
    let downloaded = backend.download_file(&file_hash, None, None).await;
    fixture.finish().await;
    assert!(crate::ServerError::from(pending.unwrap_err()).is_remote_delete_pending());
    assert!(
        publication.is_err(),
        "native metadata must not expose deleted backing bytes: {publication:?}"
    );
    assert_eq!(visible, 0);
    assert_eq!(
        partial_rows, 0,
        "missing normalized shards must roll back file and mapping rows"
    );
    let missing_shard = crate::ServerError::from(missing_shard.unwrap_err());
    assert!(!missing_shard.is_remote_delete_pending());
    assert_eq!(
        missing_shard.status_code(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let error = crate::ServerError::from(publication.unwrap_err());
    assert!(!error.is_remote_delete_pending());
    use axum::response::IntoResponse;
    let response = error.into_response();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(response.headers()[axum::http::header::RETRY_AFTER], "1");
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    assert!(
        std::str::from_utf8(&body)
            .unwrap()
            .contains("upload missing backing content again, then retry the shard")
    );
    assert!(
        retried.is_ok(),
        "a fresh native upload retry must succeed: {retried:?}"
    );
    assert_eq!(downloaded.unwrap().as_slice(), payload);
}

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
