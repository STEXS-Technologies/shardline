//! Mandatory checksum/restart qualification owns every remote prerequisite.
use super::*;
mod range_restart;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native AWS CLI and owned PostgreSQL/MinIO"]
async fn native_aws_multipart_resume_with_owned_postgres_and_minio() {
    let (_services, database, storage) = fixture("aws-multipart-resume").await;
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        exercise_native_aws_resume(Some(&database), Some(storage)),
    )
    .await
    .expect("remote AWS multipart resume deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires native AWS CLI and owned PostgreSQL/MinIO"]
async fn native_aws_presigned_get_stays_on_shardline_with_remote_storage() {
    let (_services, database, storage) = fixture("aws-presigned-shardline-only").await;
    tokio::time::timeout(
        std::time::Duration::from_secs(120),
        exercise_aws_cli_presigned_get(Some(&database), Some(storage)),
    )
    .await
    .expect("remote Shardline presigned GET deadline");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires pinned botocore and disposable Docker PostgreSQL/MinIO"]
async fn native_botocore_checksum_with_owned_postgres_and_minio() {
    let (_services, database, storage) = fixture("botocore-checksum").await;
    exercise_botocore_session_credentials(Some(&database), Some(storage)).await;
}

async fn fixture(
    label: &str,
) -> (
    shardline_test_support::DockerLocalStack,
    String,
    S3ObjectStoreConfig,
) {
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
    let prefix = services.unique_s3_key_prefix(label);
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
    (services, database, storage)
}
