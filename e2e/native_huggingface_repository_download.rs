//! Repository-wide credentials retain ordinary HTTP access to native files.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_huggingface_repository_wide_http_local() -> Result<(), TestError> {
    qualify(None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires owned PostgreSQL/MinIO and stock hf/hf_xet clients"]
async fn native_huggingface_repository_wide_http_remote() -> Result<(), TestError> {
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("repository-wide download qualification requires Docker")?;
    qualify(Some(services)).await
}

async fn qualify(
    services: Option<shardline_test_support::DockerLocalStack>,
) -> Result<(), TestError> {
    assert!(command_available("hf"), "stock hf is required");
    assert!(
        python_package_available("hf_xet"),
        "stock hf_xet is required"
    );
    let _measurements = MEASUREMENTS.lock().await;
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], services).await?;
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()?;
    client
        .post(format!("{}/api/repos/create", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .json(&serde_json::json!({"name":"team/cli-model", "private":true}))
        .send()
        .await?
        .error_for_status()?;
    let working = tempfile::tempdir()?;
    let source = working.path().join("native.bin");
    let mut bytes = vec![0_u8; 9 * 1024 * 1024];
    let mut state = 0x1234_5678_9abc_def0_u64;
    for word in bytes.as_chunks_mut::<8>().0 {
        state ^= state.wrapping_shl(13);
        state ^= state.wrapping_shr(7);
        state ^= state.wrapping_shl(17);
        word.copy_from_slice(&state.to_le_bytes());
    }
    write(&source, &bytes)?;
    let home = tempfile::tempdir()?;
    run_hf(
        &runtime,
        home.path(),
        [
            "upload",
            "team/cli-model",
            path_as_str(&source)?,
            "native.bin",
            "--token",
            runtime.model_token.as_str(),
        ],
    )?;
    let digest = hex::encode(Sha256::digest(&bytes));
    let root = bearer_token(
        "github-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "cli-model",
        None,
    )?;
    let denied = bearer_token(
        "other",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "other",
        "private",
        None,
    )?;
    let hidden = bearer_token(
        "github-user-1",
        TokenScope::Read,
        RepositoryProvider::GitHub,
        "team",
        "cli-model",
        Some("hidden"),
    )?;
    for path in [
        format!("/v1/hub/xet-files/{digest}"),
        "/team/cli-model/resolve/main/native.bin".to_owned(),
    ] {
        let url = format!("{}{path}", runtime.base_url);
        for token in [&denied, &hidden] {
            let rejected = client.get(&url).bearer_auth(token).send().await?;
            assert!(
                [
                    reqwest::StatusCode::FORBIDDEN,
                    reqwest::StatusCode::NOT_FOUND
                ]
                .contains(&rejected.status()),
                "unrelated repository or ref must remain denied"
            );
        }
        let response = client.get(&url).bearer_auth(&root).send().await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::OK,
            "repository-wide read must select the authorized native storage scope"
        );
        assert_eq!(response.bytes().await?.as_ref(), bytes.as_slice());
        let response = client
            .get(&url)
            .bearer_auth(&root)
            .header("Range", "bytes=65530-65549")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.bytes().await?.as_ref(), &bytes[65530..65550]);
    }
    Ok(())
}
