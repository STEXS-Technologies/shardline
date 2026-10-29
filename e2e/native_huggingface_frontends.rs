//! Stock-client selection and verified transfers with combined frontends.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker MinIO plus real hf/hf_xet clients"]
async fn native_huggingface_cli_flows_with_sqlite_and_minio() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "SQLite/MinIO qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_minio()
        .start()?
        .ok_or("SQLite/MinIO qualification requires Docker")?;
    assert!(services.postgres_url().is_none());
    assert!(
        services.s3_raw_config(None).is_some(),
        "SQLite/MinIO qualification requires S3 objects"
    );
    let runtime = start_runtime_with_services(
        &[ServerFrontend::Hub, ServerFrontend::Xet, ServerFrontend::S3],
        Some(services),
    )
    .await?;
    assert!(runtime.maintenance_config.index_postgres_url().is_none());
    assert_eq!(
        runtime.maintenance_config.object_storage_adapter(),
        shardline_server::ObjectStorageAdapter::S3
    );
    exercise_huggingface_cli_flows(&runtime).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_huggingface_cli_flows_with_hub_and_xet() -> Result<(), TestError> {
    if !command_available("hf") {
        return Ok(());
    }
    let runtime = start_runtime(&[ServerFrontend::Hub, ServerFrontend::Xet]).await?;
    exercise_huggingface_cli_flows(&runtime).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO plus real hf/hf_xet clients"]
async fn native_huggingface_cli_flows_with_split_roles_postgres_minio() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "split-role qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("split-role qualification requires Docker")?;
    assert!(services.postgres_url().is_some());
    assert!(services.s3_raw_config(None).is_some());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let public = format!("http://{}", listener.local_addr()?);
    let mut runtime = start_runtime_with_roles(
        &[ServerFrontend::Hub, ServerFrontend::Xet, ServerFrontend::S3],
        Some(services),
        Some(&public),
        true,
    )
    .await?;
    let transfer = &runtime.transfer.as_ref().ok_or("missing transfer role")?.0;
    let (_proxy, observations) =
        native_huggingface_proxy::start_observing_ranges(listener, &runtime.base_url, transfer);
    runtime.base_url = public;
    exercise_huggingface_cli_flows(&runtime).await?;
    assert!(
        observations
            .lock()
            .map_err(|_| "range observations poisoned")?
            .iter()
            .any(|(path, status)| path.starts_with("/transfer/xorb/")
                && *status == reqwest::StatusCode::PARTIAL_CONTENT),
        "native download must traverse the Shardline transfer role"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO plus real hf/hf_xet clients"]
async fn native_huggingface_cli_flows_with_combined_frontends_postgres_minio()
-> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "remote combined-frontend qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote combined-frontend qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "remote combined-frontend qualification requires S3 objects"
    );
    let runtime = start_runtime_with_services(
        &[ServerFrontend::Hub, ServerFrontend::Xet, ServerFrontend::S3],
        Some(services),
    )
    .await?;
    exercise_huggingface_cli_flows(&runtime).await
}
