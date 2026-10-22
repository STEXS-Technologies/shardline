//! Continuation after deletion of the upload that anchored a previous page.
use super::*;
use shardline_index::{PostgresIndexStore, ResumableSession, ResumableSessionProtocol};

#[tokio::test]
async fn local_completion_abort_race_preserves_published_bytes() {
    completion_abort_race(None, None).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn postgres_completion_abort_race_preserves_published_bytes() {
    let database = std::env::var("DATABASE_URL").unwrap();
    completion_abort_race(Some(&database), None).await;
}

#[tokio::test]
#[ignore = "requires DATABASE_URL and TEST_S3_* disposable shared storage"]
async fn minio_completion_abort_race_preserves_published_bytes() {
    let database = std::env::var("DATABASE_URL").unwrap();
    let storage = S3ObjectStoreConfig::new(
        std::env::var("TEST_S3_BUCKET").unwrap(),
        "us-east-1".to_owned(),
    )
    .with_endpoint(Some(std::env::var("TEST_S3_ENDPOINT").unwrap()))
    .with_allow_http(true)
    .with_credentials(
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_ACCESS_KEY").unwrap(),
        )),
        Some(shardline_protocol::SecretString::new(
            std::env::var("TEST_S3_SECRET_KEY").unwrap(),
        )),
        None,
    );
    completion_abort_race(Some(&database), Some(storage)).await;
}

async fn completion_abort_race(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let peer_root = tempfile::tempdir().unwrap();
    let repository = format!("race-{}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
    let credential = token("writer", &repository, TokenScope::Write);
    let server = start_with_storage(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage.clone(),
    )
    .await;
    // Share filesystem payloads only for local storage. The S3 case uses
    // independent local roots, so peer reads must use shared remote bytes.
    let peer_path = if storage.is_some() {
        peer_root.path()
    } else {
        root.path()
    };
    let peer = start_with_storage(peer_path, ServerFrontend::S3, true, database, storage).await;
    let client = Client::new();
    for round in 0..8 {
        let url = format!("{}/{}.locks/object-{round}", server.base, repository);
        let peer_url = format!("{}/{}.locks/object-{round}", peer.base, repository);
        let old = b"previous published content";
        assert!(
            client
                .put(&url)
                .bearer_auth(&credential)
                .body(old.as_slice())
                .send()
                .await
                .unwrap()
                .status()
                .is_success()
        );
        let created = client
            .post(format!("{url}?uploads"))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::OK);
        let xml = created.text().await.unwrap();
        let id = xml
            .split_once("<UploadId>")
            .unwrap()
            .1
            .split_once("</UploadId>")
            .unwrap()
            .0;
        let new = format!("complete replacement content {round}");
        let part = client
            .put(format!("{url}?uploadId={id}&partNumber=1"))
            .bearer_auth(&credential)
            .body(new.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(part.status(), StatusCode::OK);
        let etag = part.headers().get("etag").unwrap().to_str().unwrap();
        let complete = client.post(format!("{url}?uploadId={id}")).bearer_auth(&credential).body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"));
        let abort = client
            .delete(format!("{peer_url}?uploadId={id}"))
            .bearer_auth(&credential);
        let (completed, aborted) = tokio::join!(complete.send(), abort.send());
        let completed = completed.unwrap();
        let aborted = aborted.unwrap();
        let abort_status = aborted.status();
        let abort_body = aborted.text().await.unwrap();
        assert!(
            matches!(
                completed.status(),
                StatusCode::OK | StatusCode::NOT_FOUND | StatusCode::CONFLICT
            ),
            "unexpected completion: {}",
            completed.status()
        );
        assert!(
            matches!(
                abort_status,
                StatusCode::NO_CONTENT | StatusCode::NOT_FOUND | StatusCode::CONFLICT
            ),
            "unexpected abort: {abort_status}: {abort_body}"
        );
        let success = completed.status() == StatusCode::OK;
        let body = completed.text().await.unwrap();
        if success {
            assert!(body.contains("<CompleteMultipartUploadResult "), "{body}");
        }
        let read = client
            .get(&peer_url)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(read.status(), StatusCode::OK);
        let bytes = read.bytes().await.unwrap();
        if success {
            assert_eq!(bytes.as_ref(), new.as_bytes());
        } else {
            assert_eq!(bytes.as_ref(), old);
        }
    }
    peer.stop().await;
    server.stop().await;
}

#[tokio::test]
async fn filesystem_page_survives_deleted_marker_and_restart_with_legacy_ids() {
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "local-marker", TokenScope::Write);
    let client = Client::new();
    let server = start(root.path(), ServerFrontend::S3, true, None).await;
    let created = client
        .post(format!(
            "{}/local-marker.locks/same-key?uploads",
            server.base
        ))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let xml = created.text().await.unwrap();
    let original_id = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let template = shardline_s3_adapter::read_session(
        root.path(),
        original_id,
        std::num::NonZeroU64::new(86400).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        client
            .delete(format!(
                "{}/local-marker.locks/same-key?uploadId={original_id}",
                server.base
            ))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    let now = shardline_protocol::unix_now_seconds_lossy();
    let first_id = "f".repeat(32);
    let later_id = "0".repeat(32);
    for (id, created_at) in [(&first_id, now - 10), (&later_id, now - 5)] {
        let mut session = template.clone();
        session.upload_id.clone_from(id);
        session.created_at_unix_seconds = created_at;
        session.last_touched_unix_seconds = created_at;
        let directory = shardline_s3_adapter::session_dir(root.path(), id).unwrap();
        tokio::fs::create_dir_all(directory).await.unwrap();
        let evidence = shardline_reliability::SessionEvidenceLog::new(
            &session.scope_namespace,
            &session.upload_id,
            &session.key,
        )
        .unwrap();
        // Released-format fixture, upgraded through the actual evidence repair
        // API. No forged digest or raw SQLite state is used to seed uploads.
        tokio::fs::write(
            shardline_s3_adapter::session_metadata_path(root.path(), id).unwrap(),
            serde_json::to_vec(&json!({"session":session, "evidence":evidence})).unwrap(),
        )
        .await
        .unwrap();
        shardline_s3_adapter::repair_session_evidence(root.path(), id)
            .await
            .unwrap();
    }
    let first_page = client
        .get(format!(
            "{}/local-marker.locks?uploads&max-uploads=1",
            server.base
        ))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(first_page.status(), StatusCode::OK);
    let first_page = first_page.text().await.unwrap();
    assert!(first_page.contains(&format!(
        "<NextUploadIdMarker>{first_id}</NextUploadIdMarker>"
    )));
    assert_eq!(
        client
            .delete(format!(
                "{}/local-marker.locks/same-key?uploadId={first_id}",
                server.base
            ))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    server.stop().await;
    let restarted = start(root.path(), ServerFrontend::S3, true, None).await;
    let page = client
        .get(format!("{}/local-marker.locks", restarted.base))
        .bearer_auth(&credential)
        .query(&[
            ("uploads", ""),
            ("key-marker", "same-key"),
            ("upload-id-marker", first_id.as_str()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let page = page.text().await.unwrap();
    assert!(
        page.contains(&format!("<UploadId>{later_id}</UploadId>")),
        "later upload was skipped: {page}"
    );
    assert!(!page.contains(&format!("<UploadId>{first_id}</UploadId>")));
    // Seed an honestly committed position whose retention already elapsed.
    // HTTP must treat it as an unknown manual marker, not revive its earlier
    // chronological position or turn the position into upload authority.
    let expired_id = "e".repeat(32);
    let anchor = serde_json::to_vec(&json!({
        "scope": template.scope_namespace,
        "bucket": "local-marker.locks",
        "key": "same-key",
        "upload_id": expired_id,
        "initiated": now - 10,
    }))
    .unwrap();
    shardline_index::LocalIndexStore::open(root.path().to_owned())
        .retain_s3_listing_anchor(&expired_id, &anchor, now - 2, now - 1)
        .unwrap();
    let expired_page = client
        .get(format!("{}/local-marker.locks", restarted.base))
        .bearer_auth(&credential)
        .query(&[
            ("uploads", ""),
            ("key-marker", "same-key"),
            ("upload-id-marker", expired_id.as_str()),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(expired_page.status(), StatusCode::OK);
    let expired_page = expired_page.text().await.unwrap();
    assert!(!expired_page.contains("<Upload>"), "{expired_page}");
    let expired_abort = client
        .delete(format!(
            "{}/local-marker.locks/same-key?uploadId={expired_id}",
            restarted.base
        ))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(expired_abort.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        client
            .delete(format!(
                "{}/local-marker.locks/same-key?uploadId={later_id}",
                restarted.base
            ))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL via DATABASE_URL"]
async fn postgres_aborted_marker_keeps_initiation_order_for_later_lower_id() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let root = tempfile::tempdir().unwrap();
    let repository = format!(
        "marker-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let credential = token("writer", &repository, TokenScope::Write);
    let server = start(root.path(), ServerFrontend::S3, true, Some(&database)).await;
    let client = Client::new();
    let bucket = format!("{repository}.locks");
    let url = format!("{}/{bucket}/same-key", server.base);
    let created = client
        .post(format!("{url}?uploads"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let xml = created.text().await.unwrap();
    let first_id = xml
        .split_once("<UploadId>")
        .unwrap()
        .1
        .split_once("</UploadId>")
        .unwrap()
        .0;
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    let store = PostgresIndexStore::new(pool.clone());
    let first = store
        .resumable_session_by_id(first_id)
        .await
        .unwrap()
        .unwrap();
    let mut attributes: Value = serde_json::from_str(first.attributes_json()).unwrap();
    attributes["initiated_at"] = json!(attributes["initiated_at"].as_u64().unwrap() + 1);
    // Seed valid durable state through the index API, not raw SQL or modified
    // evidence. Force the initiation and ID orders to differ deterministically.
    let later_id = format!("0-{:x}", chrono::Utc::now().timestamp_nanos_opt().unwrap());
    assert!(later_id.as_str() < first_id);
    let later = ResumableSession::new(
        later_id.clone(),
        ResumableSessionProtocol::S3Multipart,
        first.scope_namespace().to_owned(),
        first.target_key().to_owned(),
        first.expires_at(),
    )
    .with_attributes_json(serde_json::to_string(&attributes).unwrap())
    .unwrap();
    assert!(store.create_resumable_session(&later).await.unwrap());
    let aborted = client
        .delete(format!("{url}?uploadId={first_id}"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(aborted.status(), StatusCode::NO_CONTENT);
    let listed = client
        .get(format!("{}/{bucket}", server.base))
        .bearer_auth(&credential)
        .query(&[
            ("uploads", ""),
            ("key-marker", "same-key"),
            ("upload-id-marker", first_id),
        ])
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    let listed = listed.text().await.unwrap();
    assert!(
        listed.contains(&format!("<UploadId>{later_id}</UploadId>")),
        "later upload was skipped: {listed}"
    );
    assert!(!listed.contains(&format!("<UploadId>{first_id}</UploadId>")));
    assert_eq!(
        client
            .delete(format!("{url}?uploadId={later_id}"))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    server.stop().await;
    pool.close().await;
}
