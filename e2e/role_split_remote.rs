//! Remote storage stays private behind the API/transfer public origin.
use super::*;
use shardline_test_support::DockerLocalStack;

pub(super) fn configure(
    config: ServerConfig,
    services: Option<&DockerLocalStack>,
) -> Result<ServerConfig, Box<dyn Error>> {
    let Some(services) = services else {
        return Ok(config);
    };
    let raw = services
        .s3_raw_config(None)
        .ok_or("MinIO fixture missing")?;
    let objects = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
        .with_endpoint(raw.endpoint)
        .with_credentials(
            raw.access_key.map(shardline_protocol::SecretString::new),
            raw.secret_key.map(shardline_protocol::SecretString::new),
            raw.session_token.map(shardline_protocol::SecretString::new),
        )
        .with_allow_http(raw.allow_http);
    Ok(config
        .with_index_postgres_url(
            services
                .postgres_url()
                .ok_or("PostgreSQL fixture missing")?,
        )?
        .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(objects)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned PostgreSQL and MinIO"]
async fn remote_native_xet_signed_urls_stay_on_shardline() -> Result<(), Box<dyn Error>> {
    let services = DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("Docker is required")?;
    let pool = sqlx::PgPool::connect(
        &services
            .postgres_url()
            .ok_or("PostgreSQL fixture missing")?,
    )
    .await?;
    shardline_server::apply_database_migrations(&pool).await?;
    pool.close().await;
    timeout(
        Duration::from_secs(120),
        exercise_split_role_native_xet_flow(Some(&services)),
    )
    .await??;
    Ok(())
}
