use super::*;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Node and SHARDLINE_HF_JS_ROOT with @huggingface/hub 2.17.1"]
async fn official_huggingface_js_edits_use_native_delta() {
    let runtime = start_hub_runtime().await.expect("Hub runtime must start");
    exercise(&runtime);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL and pinned official JavaScript client"]
async fn official_huggingface_js_edits_use_native_delta_postgres() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .start()?
        .ok_or("JavaScript PostgreSQL qualification requires Docker")?;
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    exercise(&runtime);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL/MinIO and pinned official JavaScript client"]
async fn official_huggingface_js_edits_use_native_delta_postgres_minio() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio_tmpfs()
        .start()?
        .ok_or("JavaScript PostgreSQL/MinIO qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "S3 objects are required"
    );
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    exercise(&runtime);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL/disk-backed MinIO and pinned official JavaScript client"]
async fn official_huggingface_js_edits_use_native_delta_disk_minio() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("disk-backed JavaScript qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "disk-backed S3 objects are required"
    );
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    exercise(&runtime);
    Ok(())
}

fn exercise(runtime: &HubRuntime) {
    exercise_at(runtime, &runtime.base_url);
}

fn exercise_at(runtime: &HubRuntime, endpoint: &str) {
    let package_root =
        std::env::var("SHARDLINE_HF_JS_ROOT").expect("pinned JS package root is required");
    for (repo_type, token) in [
        ("model", &runtime.model_token),
        ("dataset", &runtime.dataset_token),
    ] {
        let output = shardline_test_support::native_client_output(
            Command::new("node")
                .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("native_huggingface_delta.mjs"))
                .env("SHARDLINE_HF_JS_ROOT", &package_root)
                .env("SHARDLINE_ENDPOINT", endpoint)
                .env("SHARDLINE_JS_REPO_TYPE", repo_type)
                .env(
                    "SHARDLINE_TOKEN",
                    shardline_protocol::format_hub_token(token).unwrap(),
                ),
            Duration::from_secs(120),
        )
        .expect("pinned official JavaScript client must finish within its deadline");
        assert!(
            output.status.success(),
            "official JavaScript delta workflow failed for {repo_type}"
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let verified = stdout
            .lines()
            .find(|line| line.starts_with("delta verified;"))
            .expect("client must finish all delta and integrity assertions");
        println!("{repo_type}: {verified}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker PostgreSQL/MinIO and pinned official JavaScript client"]
async fn official_huggingface_js_edits_use_native_delta_split_roles() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio_tmpfs()
        .start()?
        .ok_or("split-role JavaScript qualification requires Docker")?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let public_url = format!("http://{}", listener.local_addr()?);
    let runtime = start_runtime_with_roles(
        &[ServerFrontend::Hub],
        Some(services),
        Some(&public_url),
        true,
    )
    .await?;
    assert!(
        runtime.transfer.is_some(),
        "separate transfer role is required"
    );
    let transfer = &runtime.transfer.as_ref().expect("transfer role").0;
    let _proxy = super::native_huggingface_proxy::start(listener, &runtime.base_url, transfer);
    exercise_at(&runtime, &public_url);
    Ok(())
}
