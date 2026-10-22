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
async fn copy_source_snapshot_stays_pinned_across_concurrent_overwrite() {
    let (state, _root) = build_test_state().await;
    exercise_snapshot_pinning(state).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn postgres_copy_source_snapshot_stays_pinned_across_overwrite() {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be configured");
    let (state, _root) = build_postgres_test_state()
        .await
        .expect("configured PostgreSQL");
    exercise_snapshot_pinning(state).await;
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
