use super::super::*;

mod session;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MINIO_SIGNER_BIN built from e2e/clients/minio-signed-stream"]
async fn native_minio_signed_streaming() {
    exercise(None, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MINIO_SIGNER_BIN and disposable PostgreSQL DATABASE_URL"]
async fn native_minio_signed_streaming_on_postgres() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    exercise(Some(&database), None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires MINIO_SIGNER_BIN, disposable DATABASE_URL and TEST_S3_* storage"]
async fn native_minio_signed_streaming_on_postgres_and_minio() {
    let database = std::env::var("DATABASE_URL").expect("DATABASE_URL is required");
    let prefix = format!(
        "native-stream-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
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
    .with_key_prefix(Some(&prefix));
    exercise(Some(&database), Some(storage)).await;
}

async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let binary = std::env::var("MINIO_SIGNER_BIN").expect("MINIO_SIGNER_BIN is required");
    let root = tempfile::tempdir().unwrap();
    let credential = token("writer", "aws-resume", TokenScope::Write);
    let server = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage.clone(),
        Some(signing_identity(credential.clone())),
    )
    .await;
    let url = format!("{}/aws-resume.locks/native-minio.bin", server.base);
    let expected = b"native MinIO signed streaming payload\n".repeat(5000);
    let client = Client::new();
    for checksum in ["", "crc32", "crc32c", "crc64nvme", "sha1", "sha256"] {
        for (corrupt, bad_checksum, disconnect) in [
            (false, false, false),
            (true, false, false),
            (false, true, false),
            (false, false, true),
        ] {
            if bad_checksum && checksum.is_empty() {
                continue;
            }
            upload(&binary, &url, checksum, corrupt, bad_checksum, disconnect).await;
            let response = client
                .get(&url)
                .bearer_auth(&credential)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
        }
        let destination = format!(
            "{}/aws-resume.locks/native-minio-part-{checksum}.bin",
            server.base
        );
        let response = client
            .post(format!("{destination}?uploads"))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let document = response.text().await.unwrap();
        let upload_id = document
            .split_once("<UploadId>")
            .unwrap()
            .1
            .split_once("</UploadId>")
            .unwrap()
            .0;
        let part = format!("{destination}?uploadId={upload_id}&partNumber=1");
        let etag = upload(&binary, &part, checksum, false, false, false).await;
        assert!(!etag.is_empty());
        upload(&binary, &part, checksum, true, false, false).await;
        upload(&binary, &part, checksum, false, false, true).await;
        if !checksum.is_empty() {
            upload(&binary, &part, checksum, false, true, false).await;
        }
        let response = client.post(format!("{destination}?uploadId={upload_id}"))
            .bearer_auth(&credential).body(format!("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>{etag}</ETag></Part></CompleteMultipartUpload>"))
            .send().await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "{}",
            response.text().await.unwrap()
        );
        let response = client
            .get(&destination)
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    }
    server.stop().await;
    let restarted = start_configured(
        root.path(),
        ServerFrontend::S3,
        true,
        database,
        storage,
        Some(signing_identity(credential.clone())),
    )
    .await;
    for key in [
        "native-minio.bin",
        "native-minio-part-.bin",
        "native-minio-part-crc32.bin",
        "native-minio-part-crc32c.bin",
        "native-minio-part-crc64nvme.bin",
        "native-minio-part-sha1.bin",
        "native-minio-part-sha256.bin",
    ] {
        let response = client
            .get(format!("{}/aws-resume.locks/{key}", restarted.base))
            .bearer_auth(&credential)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), expected);
    }
    restarted.stop().await;
}

async fn upload(
    binary: &str,
    endpoint: &str,
    checksum: &str,
    corrupt: bool,
    bad_checksum: bool,
    disconnect: bool,
) -> String {
    let binary = binary.to_owned();
    let endpoint = endpoint.to_owned();
    let checksum = checksum.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = std::process::Command::new(binary);
        if !checksum.is_empty() {
            command.args(["--trailer", "--checksum", &checksum]);
        }
        if bad_checksum {
            command.arg("--bad-checksum");
        }
        if corrupt {
            command.arg("--corrupt");
        }
        if disconnect {
            command.arg("--disconnect");
        }
        shardline_test_support::native_client_output(
            command.arg(endpoint),
            std::time::Duration::from_secs(180),
        )
        .unwrap()
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "native signer corrupt={corrupt} bad_checksum={bad_checksum} disconnect={disconnect}: {}",
        output.status
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
