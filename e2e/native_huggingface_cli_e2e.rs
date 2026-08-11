#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

mod support;

use std::{
    error::Error,
    fs::{create_dir_all, read, write},
    io::Error as IoError,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    process::{Command, Output},
};

use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{ServerConfig, ServerError, ServerFrontend, serve_with_listener};
use tokio::{net::TcpListener, task::JoinHandle};

use support::{ServerE2eInvariantError, bearer_token, wait_for_health, write_provider_config};

type TestError = Box<dyn Error>;

struct HubRuntime {
    base_url: String,
    // Per-repo tokens: repo-scoped Hub-API routes (upload/commit/download/
    // resolve/delete/webhooks) enforce `require_repository_binding`, so each
    // repo this flow touches needs a token whose `RepositoryScope` matches it
    // exactly. `repo create` is deliberately-global (Write scope only).
    created_token: String,
    model_token: String,
    dataset_token: String,
    _storage: tempfile::TempDir,
    server: JoinHandle<Result<(), ServerError>>,
}

impl Drop for HubRuntime {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_huggingface_cli_model_and_dataset_flows_work_against_shardline() {
    if !command_available("hf") {
        return;
    }

    let result = exercise_huggingface_cli_flows().await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native Hugging Face CLI e2e failed: {error:?}"
    );
}

async fn exercise_huggingface_cli_flows() -> Result<(), TestError> {
    let runtime = start_hub_runtime().await?;
    let client_home = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let created_repo = "team/cli-created";
    let model_repo = "team/cli-model";
    let dataset_repo = "team/cli-dataset";

    run_hf(
        &runtime,
        client_home.path(),
        [
            "repo",
            "create",
            created_repo,
            "--private",
            "--token",
            runtime.created_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;

    let single_file = working.path().join("single.txt");
    write(&single_file, b"single upload via hf cli\n")?;
    run_hf(
        &runtime,
        client_home.path(),
        [
            "upload",
            model_repo,
            path_as_str(&single_file)?,
            "single.txt",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "single file upload",
            "--format",
            "quiet",
        ],
    )?;

    let folder = working.path().join("folder");
    create_dir_all(folder.join("nested"))?;
    write(folder.join("config.json"), b"{\"layers\": 3}\n")?;
    write(
        folder.join("nested").join("weights.json"),
        b"{\"weight\": 7}\n",
    )?;
    write(folder.join("ignored.bin"), b"must not be uploaded")?;
    run_hf(
        &runtime,
        client_home.path(),
        [
            "upload",
            model_repo,
            path_as_str(&folder)?,
            ".",
            "--include",
            "*.json",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "folder upload with include filter",
            "--format",
            "quiet",
        ],
    )?;

    let single_download = working.path().join("single-download");
    run_hf(
        &runtime,
        client_home.path(),
        [
            "download",
            model_repo,
            "single.txt",
            "--local-dir",
            path_as_str(&single_download)?,
            "--force-download",
            "--token",
            runtime.model_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(
        read(single_download.join("single.txt"))?,
        b"single upload via hf cli\n"
    );

    let filtered_download = working.path().join("filtered-download");
    run_hf(
        &runtime,
        client_home.path(),
        [
            "download",
            model_repo,
            "--include",
            "*.json",
            "--exclude",
            "nested/*",
            "--local-dir",
            path_as_str(&filtered_download)?,
            "--force-download",
            "--token",
            runtime.model_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(
        read(filtered_download.join("config.json"))?,
        b"{\"layers\": 3}\n"
    );
    assert!(
        !filtered_download
            .join("nested")
            .join("weights.json")
            .exists()
    );

    run_hf(
        &runtime,
        client_home.path(),
        [
            "repos",
            "delete-files",
            model_repo,
            "single.txt",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "delete single file",
            "--format",
            "quiet",
        ],
    )?;

    run_hf(
        &runtime,
        client_home.path(),
        [
            "repo",
            "create",
            dataset_repo,
            "--repo-type",
            "dataset",
            "--private",
            "--token",
            runtime.dataset_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    let dataset_file = working.path().join("rows.jsonl");
    write(&dataset_file, b"{\"id\":1,\"value\":\"ok\"}\n")?;
    run_hf(
        &runtime,
        client_home.path(),
        [
            "upload",
            dataset_repo,
            path_as_str(&dataset_file)?,
            "data/rows.jsonl",
            "--repo-type",
            "dataset",
            "--token",
            runtime.dataset_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    let dataset_download = working.path().join("dataset-download");
    run_hf(
        &runtime,
        client_home.path(),
        [
            "download",
            dataset_repo,
            "data/rows.jsonl",
            "--repo-type",
            "dataset",
            "--local-dir",
            path_as_str(&dataset_download)?,
            "--force-download",
            "--token",
            runtime.dataset_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(
        read(dataset_download.join("data").join("rows.jsonl"))?,
        b"{\"id\":1,\"value\":\"ok\"}\n"
    );

    for (repo, repo_type, repo_token) in [
        (created_repo, "model", runtime.created_token.as_str()),
        (model_repo, "model", runtime.model_token.as_str()),
        (dataset_repo, "dataset", runtime.dataset_token.as_str()),
    ] {
        run_hf(
            &runtime,
            client_home.path(),
            [
                "repo",
                "delete",
                repo,
                "--repo-type",
                repo_type,
                "--yes",
                "--token",
                repo_token,
                "--format",
                "quiet",
            ],
        )?;
    }

    Ok(())
}

async fn start_hub_runtime() -> Result<HubRuntime, TestError> {
    let storage = tempfile::tempdir()?;
    let hub_root = storage.path().join("hub");
    create_dir_all(&hub_root)?;
    let _connection = rusqlite::Connection::open(hub_root.join("metadata.sqlite3"))?;
    let provider_config = write_provider_config(storage.path())?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let address = listener.local_addr()?;
    let base_url = format!("http://{address}");
    let config = ServerConfig::new(
        address,
        base_url.clone(),
        storage.path().to_path_buf(),
        NonZeroUsize::new(128).ok_or("test chunk size must be non-zero")?,
    )
    .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
    .with_server_frontends([ServerFrontend::Hub])?
    .with_provider_runtime(
        provider_config,
        b"test-api-key".to_vec(),
        "test-issuer".to_owned(),
        NonZeroU64::new(3600).ok_or("test provider ttl must be non-zero")?,
    )?;
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;
    // One Write token per repo the flow touches; `require_repository_binding`
    // requires each repo's operations to use the token scoped to that exact
    // `{ns}/{repo}`.
    let created_token = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "cli-created",
        Some("main"),
    )?;
    let model_token = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "cli-model",
        Some("main"),
    )?;
    let dataset_token = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::GitHub,
        "team",
        "cli-dataset",
        Some("main"),
    )?;

    Ok(HubRuntime {
        base_url,
        created_token,
        model_token,
        dataset_token,
        _storage: storage,
        server,
    })
}

fn run_hf<const N: usize>(
    runtime: &HubRuntime,
    home: &Path,
    arguments: [&str; N],
) -> Result<Output, TestError> {
    let output = Command::new("hf")
        .args(arguments)
        .env("HF_ENDPOINT", &runtime.base_url)
        .env("HF_HOME", home)
        .env("HF_HUB_DISABLE_TELEMETRY", "1")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()?;
    if output.status.success() {
        return Ok(output);
    }

    let command = arguments
        .iter()
        .map(|argument| {
            if *argument == runtime.created_token
                || *argument == runtime.model_token
                || *argument == runtime.dataset_token
            {
                "<TOKEN>"
            } else {
                argument
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    Err(ServerE2eInvariantError::new(format!(
        "hf {} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        command,
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    ))
    .into())
}

fn path_as_str(path: &Path) -> Result<&str, TestError> {
    path.to_str()
        .ok_or_else(|| IoError::other("test path was not valid UTF-8").into())
}

fn command_available(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}
