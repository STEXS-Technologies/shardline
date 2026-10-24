use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(feature = "docker")]
#[ignore = "requires Docker and the pinned OCI_CONFORMANCE_BIN"]
async fn official_oci_distribution_on_postgres_and_minio() {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()
        .expect("start disposable OCI backends")
        .expect("Docker is required for remote OCI qualification");
    let database = services.postgres_url().expect("PostgreSQL fixture");
    let pool = sqlx::PgPool::connect(&database).await.unwrap();
    shardline_server::apply_database_migrations(&pool)
        .await
        .unwrap();
    let prefix = services.unique_s3_key_prefix("oci-conformance");
    let raw = services
        .s3_raw_config(Some(&prefix))
        .expect("MinIO fixture");
    let environment = [
        ("TEST_S3_BUCKET", raw.bucket.clone()),
        (
            "TEST_S3_ENDPOINT",
            raw.endpoint.clone().expect("fixture endpoint"),
        ),
        (
            "TEST_S3_ACCESS_KEY",
            raw.access_key.clone().expect("fixture access key"),
        ),
        (
            "TEST_S3_SECRET_KEY",
            raw.secret_key.clone().expect("fixture secret key"),
        ),
    ];
    let storage = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(true)
        .with_key_prefix(Some(&prefix))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    let root = tempfile::tempdir().unwrap();
    let server = start_with_storage(
        root.path(),
        ServerFrontend::Oci,
        false,
        Some(&database),
        Some(storage.clone()),
    )
    .await;
    run_official_oci_distribution(root.path(), &server).await;
    server.stop().await;
    exercise_referrers(Some(&database), Some(storage.clone())).await;
    oci_index::qualify_remote_reclamation(&database, storage, &environment).await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oci_missing_cross_mount_returns_resumable_upload() {
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let client = Client::new();
    for (ordinal, source) in [Some("source/repository"), None].into_iter().enumerate() {
        let payload = format!("missing cross-mount fallback payload {ordinal}").into_bytes();
        let digest = format!("sha256:{}", hex::encode(Sha256::digest(&payload)));
        let mut request = client
            .post(format!(
                "{}/v2/destination/repository/blobs/uploads/",
                server.base
            ))
            .query(&[("mount", digest.as_str())]);
        if let Some(source) = source {
            request = request.query(&[("from", source)]);
        }
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let location = response.headers()["location"].to_str().unwrap();
        let location = if location.starts_with('/') {
            format!("{}{location}", server.base)
        } else {
            location.to_owned()
        };
        let response = client
            .put(location)
            .query(&[("digest", &digest)])
            .body(payload.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let response = client
            .post(format!(
                "{}/v2/destination/repository/blobs/uploads/",
                server.base
            ))
            .query(&[("mount", &digest)])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let response = client
            .get(format!(
                "{}/v2/destination/repository/blobs/{digest}",
                server.base
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.bytes().await.unwrap().as_ref(), payload);
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires OCI_CONFORMANCE_BIN from distribution-spec v1.1.1"]
async fn official_oci_distribution_prepopulated_repository() {
    let executable = std::env::var("OCI_CONFORMANCE_BIN").unwrap();
    let root = tempfile::tempdir().unwrap();
    let server = start(root.path(), ServerFrontend::Oci, false, None).await;
    let client = Client::new();
    let repository = "preseed/main";
    let blob = b"{}";
    let blob_digest = format!("sha256:{}", hex::encode(Sha256::digest(blob)));
    let response = client
        .post(format!("{}/v2/{repository}/blobs/uploads/", server.base))
        .query(&[("digest", &blob_digest)])
        .body(blob.to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let media = "application/vnd.oci.image.manifest.v1+json";
    let manifest = serde_json::to_vec(&json!({"schemaVersion":2,"mediaType":media,
        "config":{"mediaType":"application/vnd.oci.image.config.v1+json","digest":blob_digest,"size":2},"layers":[]})).unwrap();
    let digest = format!("sha256:{}", hex::encode(Sha256::digest(&manifest)));
    for tag in ["tag-a", "tag-b", "tag-c", "tag-d"] {
        let response = client
            .put(format!("{}/v2/{repository}/manifests/{tag}", server.base))
            .header("content-type", media)
            .body(manifest.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
    }
    let mut command = std::process::Command::new(executable);
    command.current_dir(root.path())
        .args(["-ginkgo.timeout=5m", "-ginkgo.no-color", "-ginkgo.v",
            "-ginkgo.focus=Pull|Test content discovery endpoints \\(listing tags\\)|Populate registry with test tags \\(no push\\)"])
        .env("OCI_ROOT_URL", &server.base).env("OCI_NAMESPACE", repository)
        .env("OCI_TEST_PULL", "1").env("OCI_TEST_CONTENT_DISCOVERY", "1")
        .env("OCI_TEST_PUSH", "0").env("OCI_TEST_CONTENT_MANAGEMENT", "0")
        .env("OCI_TAG_NAME", "tag-a").env("OCI_TAG_LIST", "tag-a,tag-b,tag-c,tag-d")
        .env("OCI_BLOB_DIGEST", blob_digest).env("OCI_MANIFEST_DIGEST", digest)
        .env("OCI_REPORT_DIR", "none");
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    println!("{}", String::from_utf8_lossy(&output.stdout));
    server.stop().await;
}
