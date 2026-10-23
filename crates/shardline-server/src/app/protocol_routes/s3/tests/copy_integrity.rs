//! Damaged copy sources cannot replace an acknowledged multipart part.
use super::*;

#[tokio::test]
async fn corrupt_copy_source_preserves_acknowledged_part() {
    let (state, _root) = build_test_state().await;
    exercise(state).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_corrupt_copy_source_preserves_acknowledged_part() {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let (state, _root) = build_postgres_test_state()
        .await
        .expect("configured PostgreSQL");
    exercise(state).await;
}

#[tokio::test]
async fn copy_source_snapshot_stays_pinned_if_source_is_replaced_before_read() {
    let (state, _root) = build_test_state().await;
    exercise_snapshot_pinning(state).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_copy_source_snapshot_stays_pinned_if_source_is_replaced_before_read() {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let (state, _root) = build_postgres_test_state()
        .await
        .expect("configured PostgreSQL");
    exercise_snapshot_pinning(state).await;
}

#[tokio::test]
async fn copy_part_keeps_snapshot_bytes_when_source_is_replaced_in_flight() {
    let (state, _root) = build_test_state().await;
    exercise_inflight_copy_overwrite(state).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_copy_part_keeps_snapshot_bytes_when_source_is_replaced_in_flight() {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let (state, _root) = build_postgres_test_state()
        .await
        .expect("configured PostgreSQL");
    exercise_inflight_copy_overwrite(state).await;
}

#[tokio::test]
async fn cancelled_copy_part_preserves_acknowledged_part() {
    let (state, _root) = build_test_state().await;
    exercise_cancelled_copy_part(state).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_cancelled_copy_part_preserves_acknowledged_part() {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let (state, _root) = build_postgres_test_state()
        .await
        .expect("configured PostgreSQL");
    exercise_cancelled_copy_part(state).await;
}

async fn exercise_cancelled_copy_part(state: Arc<AppState>) {
    let app = s3_router(state.clone());
    let source = "copy-source-cancelled-at-snapshot";
    let uploaded = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            b"copy source bytes".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);

    let upload = create_upload_id(&app).await;
    let acknowledged = b"previously acknowledged part";
    assert_eq!(
        upload_part(&app, &upload, 1, acknowledged).await.status(),
        StatusCode::OK
    );

    let repository = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repository));
    let source_key = shardline_s3_adapter::s3_object_key(&namespace, source).unwrap();
    let (copy_paused, resume_copy) = super::super::object::copy_part_test_hooks::arm(&source_key)
        .expect("copy-part pause hook must be available");
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/{BUCKET}/{KEY}?uploadId={upload}&partNumber=1"))
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header("x-amz-copy-source", format!("/{BUCKET}/{source}"))
        .body(Body::empty())
        .unwrap();
    let copying = {
        let app = app.clone();
        tokio::spawn(async move { app.oneshot(request).await.unwrap() })
    };
    copy_paused.await.unwrap();

    // Cancel while the request has a source snapshot but before it can read or
    // publish replacement part bytes.
    copying.abort();
    let cancelled = copying.await.unwrap_err();
    assert!(cancelled.is_cancelled());
    drop(resume_copy);

    let completed = complete_upload(&app, &upload, complete_body(&upload, &[1])).await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert_eq!(
        body_bytes(get_etag_request(&app, KEY).await).await,
        acknowledged
    );
}

async fn exercise_inflight_copy_overwrite(state: Arc<AppState>) {
    let app = s3_router(state.clone());
    let source = "copy-source-inflight-overwrite";
    let original = b"original-bytes-pinned-by-copy-part";
    let uploaded = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            original.to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);

    let upload = create_upload_id(&app).await;
    let repository = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repository));
    let source_key = shardline_s3_adapter::s3_object_key(&namespace, source).unwrap();
    let (copy_paused, resume_copy) = super::super::object::copy_part_test_hooks::arm(&source_key)
        .expect("copy-part pause hook must be available");
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/{BUCKET}/{KEY}?uploadId={upload}&partNumber=1"))
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header("x-amz-copy-source", format!("/{BUCKET}/{source}"))
        .body(Body::empty())
        .unwrap();
    let copying = {
        let app = app.clone();
        tokio::spawn(async move { app.oneshot(request).await.unwrap() })
    };
    copy_paused.await.unwrap();

    // The handler has committed to the old source snapshot but has not opened
    // its byte stream yet. Replace the source through an independent request.
    let replaced = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            b"new-source-version-with-a-different-length".to_vec(),
        ))
        .await
        .unwrap();
    let replace_status = replaced.status();
    let _resume_sent = resume_copy.send(());
    let copied = copying.await.unwrap();
    assert_eq!(replace_status, StatusCode::OK);
    assert_eq!(copied.status(), StatusCode::OK);

    let copied_xml = String::from_utf8(body_bytes(copied).await).unwrap();
    let copied_etag = copied_xml
        .split_once("<ETag>")
        .unwrap()
        .1
        .split_once("</ETag>")
        .unwrap()
        .0;
    let completion_body = format!(
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{copied_etag}</ETag></Part></CompleteMultipartUpload>"
    );
    let completed = complete_upload(&app, &upload, completion_body).await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert_eq!(
        body_bytes(get_etag_request(&app, KEY).await).await,
        original
    );
}

async fn exercise_snapshot_pinning(state: Arc<AppState>) {
    let app = s3_router(state.clone());
    let source = "copy-source-snapshot-race";
    let original = b"snapshot-pinned-original-bytes";
    let uploaded = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            original.to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);

    // UploadPartCopy resolves the source's listing row and content version
    // before it opens the byte stream. Reproduce that exact race window by
    // committing a replacement after snapshot resolution but before the read.
    let repository = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repository));
    let object_key = shardline_s3_adapter::s3_object_key(&namespace, source).unwrap();
    let snapshot = state
        .backend
        .s3_object_read_snapshot(&namespace, source, &object_key)
        .await
        .unwrap();
    assert!(snapshot.record_content_hash.is_some());

    let replacement = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            b"replacement-version-with-different-length".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(replacement.status(), StatusCode::OK);

    let pinned = state
        .backend
        .read_object_stream_pinned(
            &object_key,
            snapshot.total_bytes,
            None,
            snapshot.record_content_hash.as_deref(),
        )
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(Body::from_stream(pinned), usize::MAX)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), original);
}

async fn exercise(state: Arc<AppState>) {
    let app = s3_router(state.clone());
    let source = "copy-integrity-source";
    let uploaded = app
        .clone()
        .oneshot(put_request(
            format!("/{BUCKET}/{source}"),
            b"source-original".to_vec(),
        ))
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    let upload = create_upload_id(&app).await;
    let prior = upload_part(&app, &upload, 1, b"acknowledged-part").await;
    assert_eq!(prior.status(), StatusCode::OK);
    let repository = RepositoryScope::new(RepositoryProvider::Generic, OWNER, NAME, None).unwrap();
    let namespace = crate::protocol_support::scope_namespace(Some(&repository));
    let key = shardline_s3_adapter::s3_object_key(&namespace, source).unwrap();
    let record = state
        .backend
        .protocol_file_record(
            &shardline_server_core::protocol_support::protocol_object_file_id(&key),
        )
        .await
        .unwrap();
    assert_eq!(record.chunks.len(), 1);
    let chunk = crate::xet_adapter::xorb_object_key(&record.chunks[0].hash).unwrap();
    let storage_directory = if matches!(&state.backend, crate::ServerBackend::Postgres(_)) {
        "objects"
    } else {
        "chunks"
    };
    let path = state
        .config
        .root_dir()
        .join(storage_directory)
        .join(chunk.as_str());
    let original = tokio::fs::read(&path).await.unwrap();
    let mut damaged = original.clone();
    *damaged.last_mut().unwrap() ^= 1;
    tokio::fs::write(&path, damaged).await.unwrap();
    let request = Request::builder()
        .method("PUT")
        .uri(format!("/{BUCKET}/{KEY}?uploadId={upload}&partNumber=1"))
        .header(
            header::AUTHORIZATION,
            sigv4_auth(&mint_token(TokenScope::Write, OWNER, NAME)),
        )
        .header("x-amz-copy-source", format!("/{BUCKET}/{source}"))
        .body(Body::empty())
        .unwrap();
    let rejected = app.clone().oneshot(request).await.unwrap();
    assert_eq!(rejected.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let completed = complete_upload(&app, &upload, complete_body(&upload, &[1])).await;
    assert_eq!(completed.status(), StatusCode::OK);
    assert_eq!(
        body_bytes(get_etag_request(&app, KEY).await).await,
        b"acknowledged-part"
    );
    tokio::fs::write(path, original).await.unwrap();
    assert_eq!(
        body_bytes(get_etag_request(&app, source).await).await,
        b"source-original"
    );
}
