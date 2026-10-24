//! Mandatory checksum/restart qualification owns every remote prerequisite.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned botocore and disposable Docker PostgreSQL/MinIO"]
async fn native_botocore_checksum_with_owned_postgres_and_minio() {
    let services = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .expect("start checksum backends")
            .expect("Docker required for checksum qualification")
    })
    .await
    .unwrap();
    let database = services.postgres_url().unwrap();
    let prefix = services.unique_s3_key_prefix("botocore-checksum");
    let raw = services.s3_raw_config(Some(&prefix)).unwrap();
    let storage = S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_allow_http(true)
        .with_key_prefix(Some(&prefix))
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            None,
        );
    exercise_botocore_session_credentials(Some(&database), Some(storage)).await;
}
