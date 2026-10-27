//! Stock-client selection and verified transfers with combined frontends.
use super::*;

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
