//! Multipart copies spanning packed-container boundaries retain exact bytes.
use super::*;

#[tokio::test]
async fn large_copy_range_survives_restart() {
    exercise(None, None).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL DATABASE_URL"]
async fn large_copy_range_survives_restart_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL required");
    exercise(Some(&database), None).await;
}

fn element<'xml>(xml: &'xml str, name: &str) -> &'xml str {
    xml.split_once(&format!("<{name}>"))
        .unwrap()
        .1
        .split_once(&format!("</{name}>"))
        .unwrap()
        .0
}

#[tokio::test]
#[ignore = "requires PostgreSQL DATABASE_URL and TEST_S3_* storage"]
async fn minio_large_copy_range_survives_replica_restart() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL required");
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
    )
    .with_key_prefix(Some(&format!(
        "large-copy-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    )));
    exercise(Some(&database), Some(storage)).await;
}

async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let root = tempfile::tempdir().unwrap();
    let replica_root = tempfile::tempdir().unwrap();
    let repository = format!(
        "large-copy-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let credential = token("writer", &repository, TokenScope::Write);
    let server = start_with_storage(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage.clone(),
    )
    .await;
    let client = Client::new();
    let mut bytes = vec![0_u8; 36 * 1024 * 1024];
    let mut random = 0x1234_5678_9abc_def0_u64;
    for chunk in bytes.as_chunks_mut::<8>().0 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        chunk.copy_from_slice(&random.to_le_bytes());
    }
    let source_key = format!("/{repository}.locks/source");
    let source = format!("{}{source_key}", server.base);
    let uploaded = client
        .put(source)
        .bearer_auth(&credential)
        .body(bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(uploaded.status(), StatusCode::OK);
    let etag = uploaded.headers()["etag"].clone();
    let destination_key = format!("/{repository}.locks/destination");
    let destination = format!("{}{destination_key}", server.base);
    let created = client
        .post(format!("{destination}?uploads"))
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::OK);
    let created = created.text().await.unwrap();
    let upload = element(&created, "UploadId");
    let start = 31 * 1024 * 1024 + 17;
    let end = 33 * 1024 * 1024 + 29;
    let copied = client
        .put(format!("{destination}?uploadId={upload}&partNumber=1"))
        .bearer_auth(&credential)
        .header("x-amz-copy-source", source_key)
        .header("x-amz-copy-source-if-match", etag)
        .header("x-amz-copy-source-range", format!("bytes={start}-{end}"))
        .send()
        .await
        .unwrap();
    assert_eq!(copied.status(), StatusCode::OK);
    let copied = copied.text().await.unwrap();
    let part_etag = element(&copied, "ETag");
    server.stop().await;
    let restart_root = if storage.is_some() {
        replica_root.path()
    } else {
        root.path()
    };
    let server =
        start_with_storage(restart_root, ServerFrontend::S3, true, database, storage).await;
    let destination = format!("{}{destination_key}", server.base);
    let completed = client.post(format!("{destination}?uploadId={upload}"))
        .bearer_auth(&credential)
        .body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{part_etag}</ETag></Part></CompleteMultipartUpload>"))
        .send().await.unwrap();
    assert_eq!(completed.status(), StatusCode::OK);
    let downloaded = client
        .get(destination)
        .bearer_auth(&credential)
        .send()
        .await
        .unwrap();
    assert_eq!(downloaded.status(), StatusCode::OK);
    assert_eq!(
        downloaded.bytes().await.unwrap().as_ref(),
        &bytes[start..=end]
    );
    server.stop().await;
}
