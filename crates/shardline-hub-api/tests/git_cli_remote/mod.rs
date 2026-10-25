//! Stock-client contracts against disposable durable remote backends.

use super::*;
use shardline_index::{PostgresIndexStore, hub::BoxedHubStore};
use shardline_server_core::ServerObjectStore;
use shardline_storage::S3ObjectStoreConfig;
use shardline_test_support::DockerLocalStack;

async fn setup_remote() -> (DockerLocalStack, sqlx::PgPool, common::HubTestContext) {
    let stack = tokio::task::spawn_blocking(|| {
        DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .unwrap()
            .expect("remote Git qualification requires Docker")
    })
    .await
    .unwrap();
    let database = stack.postgres_url().unwrap();
    let binary = std::env::var_os("SHARDLINE_BIN").expect("current SHARDLINE_BIN is required");
    let migration = shardline_test_support::native_client_output(
        std::process::Command::new(binary)
            .args(["db", "migrate", "up"])
            .env("SHARDLINE_INDEX_POSTGRES_URL", &database),
        std::time::Duration::from_secs(120),
    )
    .unwrap();
    assert!(migration.status.success(), "remote Git migrations failed");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET statement_timeout = '30s'")
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&database)
        .await
        .unwrap();
    let raw = stack.s3_raw_config(Some("git-cli-matrix")).unwrap();
    let objects = ServerObjectStore::s3(
        S3ObjectStoreConfig::new(raw.bucket, raw.region)
            .with_endpoint(raw.endpoint)
            .with_allow_http(raw.allow_http)
            .with_key_prefix(raw.key_prefix.as_deref())
            .with_credentials(
                raw.access_key.map(shardline_protocol::SecretString::new),
                raw.secret_key.map(shardline_protocol::SecretString::new),
                None,
            ),
    )
    .unwrap();
    let context = common::HubTestContext::with_backend(
        tempfile::tempdir().unwrap(),
        BoxedHubStore::from_store(PostgresIndexStore::new(pool.clone())),
        objects,
    );
    (stack, pool, context)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL/MinIO and current SHARDLINE_BIN"]
async fn postgres_minio_clone_and_fetch_matrix() {
    let (_stack, pool, context) = setup_remote().await;
    exercise_clone_and_fetch(context).await;
    pool.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires disposable PostgreSQL/MinIO and current SHARDLINE_BIN"]
async fn postgres_minio_protocol_v2_matrix() {
    let (_stack, pool, context) = setup_remote().await;
    exercise_protocol_v2(context).await;
    pool.close().await;
}
