//! Stock Git LFS at the basic-request limit through separate server roles.
use super::*;
use shardline_server::ServerRole;

async fn exercise(database: Option<&str>, storage: Option<S3ObjectStoreConfig>) {
    let available = std::process::Command::new("git-lfs")
        .arg("version")
        .output()
        .is_ok_and(|output| output.status.success());
    if !shardline_test_support::require_native_client("git-lfs", available) {
        return;
    }
    let payload = vec![101_u8; 64 * 1024 * 1024];
    for frontend in [ServerFrontend::Lfs, ServerFrontend::Hub] {
        let root = tempfile::tempdir().unwrap();
        let transfer_root = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public = format!("http://{}", listener.local_addr().unwrap());
        let api = start_routed(
            root.path(),
            frontend,
            true,
            database,
            storage.clone(),
            None,
            Some((ServerRole::Api, &public)),
        )
        .await;
        let transfer = start_routed(
            if database.is_some() {
                transfer_root.path()
            } else {
                root.path()
            },
            frontend,
            true,
            database,
            storage.clone(),
            None,
            Some((ServerRole::Transfer, &public)),
        )
        .await;
        let proxy = split_roles::start_proxy(listener, &api.base, &transfer.base);
        exercise_native_git_lfs_at(&public, frontend, &payload).await;
        proxy.abort();
        let _stopped = proxy.await;
        transfer.stop().await;
        api.stop().await;
    }
}

#[tokio::test]
#[ignore = "requires stock git-lfs"]
async fn stock_git_lfs_maximum_basic_split_roles() {
    exercise(None, None).await;
}

#[cfg(feature = "docker")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires stock git-lfs and disposable PostgreSQL/MinIO"]
async fn stock_git_lfs_maximum_basic_split_roles_postgres_minio() {
    let services = tokio::task::spawn_blocking(|| {
        shardline_test_support::DockerLocalStack::builder()
            .with_postgres()
            .with_minio()
            .start()
            .expect("start split-role native LFS backends")
            .expect("Docker required for native LFS qualification")
    })
    .await
    .unwrap();
    let database = services.postgres_url().unwrap();
    let prefix = services.unique_s3_key_prefix("lfs-stock-split");
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
    exercise(Some(&database), Some(storage)).await;
}
