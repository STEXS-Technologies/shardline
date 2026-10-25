#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

mod native_client_policy;
mod native_huggingface_cards;
mod native_huggingface_commits;
mod native_huggingface_credentials;
mod native_huggingface_dedup_rebuild;
mod native_huggingface_diagnostics;
mod native_huggingface_expiry;
mod native_huggingface_http_analytics;
mod native_huggingface_identity;
mod native_huggingface_js;
#[cfg(target_os = "linux")]
mod native_huggingface_large;
mod native_huggingface_preupload;
mod native_huggingface_proxy;
mod native_huggingface_requirements;
mod native_huggingface_small;
mod native_huggingface_whoami;
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

use sha2::{Digest, Sha256};
use shardline_protocol::{RepositoryProvider, TokenScope};
use shardline_server::{ServerConfig, ServerError, ServerFrontend, serve_with_listener};
use tokio::{net::TcpListener, task::JoinHandle};

use support::{ServerE2eInvariantError, bearer_token, wait_for_health, write_provider_config};

type TestError = Box<dyn Error>;

static MEASUREMENTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct HubRuntime {
    base_url: String,
    // Per-repo tokens: repo-scoped Hub-API routes (upload/commit/download/
    // resolve/delete/webhooks) enforce `require_repository_binding`, so each
    // repo this flow touches needs a token whose `RepositoryScope` matches it
    // exactly. Repository creation is scoped to the exact new repository too.
    created_token: String,
    model_token: String,
    dataset_token: String,
    postgres_url: Option<String>,
    maintenance_config: ServerConfig,
    _storage: tempfile::TempDir,
    server: JoinHandle<Result<(), ServerError>>,
    transfer: Option<(String, JoinHandle<Result<(), ServerError>>)>,
    _services: Option<shardline_test_support::DockerLocalStack>,
}

impl Drop for HubRuntime {
    fn drop(&mut self) {
        self.server.abort();
        if let Some((_, server)) = &self.transfer {
            server.abort();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_huggingface_cli_model_and_dataset_flows_work_against_shardline() {
    if !command_available("hf") {
        return;
    }

    let runtime = match start_hub_runtime().await {
        Ok(runtime) => runtime,
        Err(error) => {
            panic!("hub runtime failed to start: {error}");
        }
    };
    let result = exercise_huggingface_cli_flows(&runtime).await;
    let error = result.as_ref().err().map(ToString::to_string);
    assert!(
        result.is_ok(),
        "native Hugging Face CLI e2e failed: {error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL and MinIO plus real hf/hf_xet clients"]
async fn native_huggingface_cli_flows_with_postgres_and_minio() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "remote Hub qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .with_minio()
        .start()?
        .ok_or("remote Hub qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_some(),
        "MinIO qualification must use S3 objects"
    );
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    exercise_huggingface_cli_flows(&runtime).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Docker PostgreSQL plus real hf/hf_xet clients"]
async fn native_huggingface_cli_flows_with_postgres_and_local_objects() -> Result<(), TestError> {
    assert!(
        command_available("hf"),
        "PostgreSQL Hub qualification requires hf"
    );
    let services = shardline_test_support::DockerLocalStack::builder()
        .with_postgres()
        .start()?
        .ok_or("PostgreSQL Hub qualification requires Docker")?;
    assert!(
        services.s3_raw_config(None).is_none(),
        "local-object qualification must not use S3"
    );
    let runtime = start_runtime_with_services(&[ServerFrontend::Hub], Some(services)).await?;
    exercise_huggingface_cli_flows(&runtime).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_hf_cli_and_s3_clients_coexist_on_one_server() {
    if !command_available("hf") || !command_available("mc") {
        return;
    }

    // One server with Hub, Xet, and S3 frontends enabled together: the real
    // `hf` and `mc` clients must work while Hub token refresh shares the server
    // with native Xet routes.
    let runtime = match start_runtime(&[
        ServerFrontend::Hub,
        ServerFrontend::Xet,
        ServerFrontend::S3,
    ])
    .await
    {
        Ok(runtime) => runtime,
        Err(error) => {
            panic!("hub+s3 runtime failed to start: {error}");
        }
    };
    if let Err(error) = exercise_huggingface_cli_flows(&runtime).await {
        panic!("hf CLI flows failed against the hub+s3 server: {error}");
    }
    if let Err(error) = exercise_mc_s3_flows(&runtime).await {
        panic!("mc S3 flows failed against the hub+s3 server: {error}");
    }
}

/// Exercises S3 with the real `mc` client against the same server the `hf` CLI
/// just used (bucket = `{owner}.{name}` = `team.cli-model`, access-key = a
/// generic-provider repo-scoped token).
async fn exercise_mc_s3_flows(runtime: &HubRuntime) -> Result<(), TestError> {
    let tmp = tempfile::tempdir()?;
    let config_dir = tmp.path().join("mc-config");
    create_dir_all(&config_dir)?;
    let alias = "coexist";
    let s3_token = bearer_token(
        "github-user-1",
        TokenScope::Write,
        RepositoryProvider::Generic,
        "team",
        "cli-model",
        Some("main"),
    )?;
    let bucket = "team.cli-model";

    let output = native_output(
        Command::new("mc")
            .arg("--config-dir")
            .arg(&config_dir)
            .args([
                "alias",
                "set",
                alias,
                &runtime.base_url,
                s3_token.as_str(),
                "dummy-secret",
            ]),
    )?;
    assert!(
        output.status.success(),
        "mc alias set failed: {}",
        output.status
    );

    let output = native_output(
        Command::new("mc")
            .arg("--config-dir")
            .arg(&config_dir)
            .args(["mb", &format!("{alias}/{bucket}")]),
    )?;
    assert!(output.status.success(), "mc mb failed: {}", output.status);

    let file = tmp.path().join("coexist.txt");
    write(&file, b"s3 coexist payload\n")?;
    let output = native_output(
        Command::new("mc")
            .arg("--config-dir")
            .arg(&config_dir)
            .args([
                "cp",
                path_as_str(&file)?,
                &format!("{alias}/{bucket}/coexist.txt"),
            ]),
    )?;
    assert!(output.status.success(), "mc cp failed: {}", output.status);

    let output = native_output(
        Command::new("mc")
            .arg("--config-dir")
            .arg(&config_dir)
            .args(["cat", &format!("{alias}/{bucket}/coexist.txt")]),
    )?;
    assert!(output.status.success(), "mc cat failed: {}", output.status);
    assert_eq!(output.stdout, b"s3 coexist payload\n");

    Ok(())
}

async fn exercise_huggingface_cli_flows(runtime: &HubRuntime) -> Result<(), TestError> {
    // Preserve backend error causes when a stock client retries a failed upload.
    // Test output is captured unless the workflow fails or --nocapture is used.
    let _subscriber = tracing_subscriber::fmt()
        .with_env_filter("shardline_server=warn,shardline_hub_api=warn,shardline_storage=warn,shardline_server::app::operational=debug")
        .with_test_writer()
        .try_init();
    // The production Prometheus registry is process-wide. Keep benchmark
    // intervals disjoint when this test binary hosts several server fixtures.
    let _measurements = MEASUREMENTS.lock().await;
    let client_home = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let created_repo = "team/cli-created";
    let model_repo = "team/cli-model";
    let dataset_repo = "team/cli-dataset";

    assert!(
        python_package_available("hf_xet"),
        "native Hugging Face CLI E2E requires hf_xet to verify Xet uploads"
    );
    let wire_whoami: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/api/whoami-v2", runtime.base_url))
        .bearer_auth(&runtime.created_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(wire_whoami["name"], "github-user-1");
    assert_eq!(wire_whoami["orgs"], serde_json::json!([]));

    let whoami = native_output(
        Command::new("hf")
            .args(["auth", "whoami", "--format", "json"])
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_HOME", client_home.path())
            .env("HF_TOKEN", &runtime.created_token)
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    assert!(
        whoami.status.success(),
        "hf auth whoami failed: {}",
        whoami.status
    );
    let whoami_json: serde_json::Value = serde_json::from_slice(&whoami.stdout)?;
    assert_eq!(
        whoami_json["user"], "github-user-1",
        "unexpected hf auth whoami response: {whoami_json}"
    );
    // The CLI renders an empty org list as JSON null; the protocol response
    // above verifies that the Hub itself sends the required empty array.
    assert!(whoami_json["orgs"].is_null());

    run_hf(
        runtime,
        client_home.path(),
        [
            "repos",
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
        runtime,
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
    // Token exchange is repository- and revision-aware, so check it only after
    // the first upload has created the model repository's main revision.
    verify_hf_xet_token_headers(runtime, model_repo)?;
    verify_hub_preupload_selects_lfs_for_large_files(runtime, model_repo, &runtime.model_token)
        .await?;
    verify_hf_xet_lfs_batch_negotiation(runtime, model_repo, &runtime.model_token).await?;

    let folder = working.path().join("folder");
    create_dir_all(folder.join("nested"))?;
    write(folder.join("config.json"), b"{\"layers\": 3}\n")?;
    write(
        folder.join("nested").join("weights.json"),
        b"{\"weight\": 7}\n",
    )?;
    write(folder.join("ignored.bin"), b"must not be uploaded")?;
    run_hf(
        runtime,
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

    // Exercise Hugging Face's complete large-file path. With hf_xet installed,
    // the native CLI negotiates the Hub's Xet token endpoint and transfers the
    // Xorbs and shard through the CAS routes.
    let large_file = working.path().join("large-model.bin");
    let large_content: Vec<u8> = (0..9 * 1024 * 1024)
        .map(|index| ((index * 31 + index / 251) % 256) as u8)
        .collect();
    write(&large_file, &large_content)?;
    let xet_shards_before = read_metric_counter(runtime, "shardline_shards_total").await?;
    let xet_xorbs_before = read_metric_counter(runtime, "shardline_xorbs_total").await?;
    let hub_lfs_uploads_before =
        read_metric_counter(runtime, "shardline_hub_api_file_uploads_total").await?;
    run_hf(
        runtime,
        client_home.path(),
        [
            "upload",
            model_repo,
            path_as_str(&large_file)?,
            "large-model.bin",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "large LFS upload",
        ],
    )?;
    let xet_shards_after = read_metric_counter(runtime, "shardline_shards_total").await?;
    let xet_xorbs_after = read_metric_counter(runtime, "shardline_xorbs_total").await?;
    let hub_lfs_uploads_after =
        read_metric_counter(runtime, "shardline_hub_api_file_uploads_total").await?;
    assert!(
        xet_shards_after > xet_shards_before,
        "hf upload did not send a shard through the native Xet CAS API"
    );
    assert!(
        xet_xorbs_after > xet_xorbs_before,
        "hf upload did not send Xorbs through the native Xet CAS API"
    );
    assert_eq!(
        hub_lfs_uploads_after, hub_lfs_uploads_before,
        "hf_xet negotiation fell back to uploading the file body through LFS"
    );
    verify_hub_paths_info(runtime, model_repo, &runtime.model_token)?;
    native_huggingface_commits::roundtrip(runtime, model_repo, &runtime.model_token)?;
    native_huggingface_credentials::roundtrip(runtime, model_repo)?;
    native_huggingface_identity::reject_forged_identities(runtime).await?;
    native_huggingface_small::roundtrip(runtime, model_repo)?;
    native_huggingface_preupload::roundtrip(runtime, model_repo)?;
    assert_hf_xet_file_identity(
        runtime,
        "models",
        "team",
        "cli-model",
        &runtime.model_token,
        &large_content,
    )
    .await?;
    verify_hf_xet_cas_read(runtime, &runtime.model_token, &large_content).await?;
    let large_download = working.path().join("large-download");
    let reconstructions_before =
        read_metric_counter(runtime, "shardline_reconstruction_requests_total").await?;
    run_hf(
        runtime,
        client_home.path(),
        [
            "download",
            model_repo,
            "large-model.bin",
            "--local-dir",
            path_as_str(&large_download)?,
            "--force-download",
            "--token",
            runtime.model_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(read(large_download.join("large-model.bin"))?, large_content);
    assert!(
        read_metric_counter(runtime, "shardline_reconstruction_requests_total").await?
            > reconstructions_before,
        "ordinary hf download did not select the advertised native Xet reconstruction path"
    );

    let single_download = working.path().join("single-download");
    run_hf(
        runtime,
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
        runtime,
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
        runtime,
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
        runtime,
        client_home.path(),
        [
            "repos",
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
        runtime,
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
    let dataset_large_file = working.path().join("dataset-large-model.bin");
    let dataset_large_content: Vec<u8> = (0..9 * 1024 * 1024)
        .map(|index| ((index * 29 + index / 197 + 17) % 256) as u8)
        .collect();
    write(&dataset_large_file, &dataset_large_content)?;
    let dataset_xet_shards_before = read_metric_counter(runtime, "shardline_shards_total").await?;
    let dataset_xet_xorbs_before = read_metric_counter(runtime, "shardline_xorbs_total").await?;
    let dataset_lfs_uploads_before =
        read_metric_counter(runtime, "shardline_hub_api_file_uploads_total").await?;
    run_hf(
        runtime,
        client_home.path(),
        [
            "upload",
            dataset_repo,
            path_as_str(&dataset_large_file)?,
            "data/large-model.bin",
            "--repo-type",
            "dataset",
            "--token",
            runtime.dataset_token.as_str(),
            "--commit-message",
            "large hf_xet dataset upload",
            "--format",
            "quiet",
        ],
    )?;
    assert!(
        read_metric_counter(runtime, "shardline_shards_total").await? > dataset_xet_shards_before,
        "hf dataset upload did not send a shard through the native Xet CAS API"
    );
    assert!(
        read_metric_counter(runtime, "shardline_xorbs_total").await? > dataset_xet_xorbs_before,
        "hf dataset upload did not send Xorbs through the native Xet CAS API"
    );
    assert_eq!(
        read_metric_counter(runtime, "shardline_hub_api_file_uploads_total").await?,
        dataset_lfs_uploads_before,
        "hf_xet dataset negotiation fell back to uploading the file body through LFS"
    );
    assert_hf_xet_file_identity(
        runtime,
        "datasets",
        "team",
        "cli-dataset",
        &runtime.dataset_token,
        &dataset_large_content,
    )
    .await?;
    verify_hf_xet_cas_read(runtime, &runtime.dataset_token, &dataset_large_content).await?;
    let dataset_large_download = working.path().join("dataset-large-download");
    run_hf(
        runtime,
        client_home.path(),
        [
            "download",
            dataset_repo,
            "data/large-model.bin",
            "--repo-type",
            "dataset",
            "--local-dir",
            path_as_str(&dataset_large_download)?,
            "--force-download",
            "--token",
            runtime.dataset_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(
        read(dataset_large_download.join("data/large-model.bin"))?,
        dataset_large_content
    );

    let dataset_download = working.path().join("dataset-download");
    run_hf(
        runtime,
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

    verify_cold_client_chunk_dedup(runtime, model_repo, working.path()).await?;
    native_huggingface_cards::roundtrip(runtime, model_repo, &runtime.model_token)?;

    for (repo, repo_type, repo_token) in [
        (created_repo, "model", runtime.created_token.as_str()),
        (model_repo, "model", runtime.model_token.as_str()),
        (dataset_repo, "dataset", runtime.dataset_token.as_str()),
    ] {
        let denied = run_hf(
            runtime,
            client_home.path(),
            [
                "repos",
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
        );
        assert!(
            denied.is_err(),
            "branch credential unexpectedly administered repository"
        );
        // Check the protocol status directly rather than parsing SDK stderr,
        // which may contain credentials and varies between client versions.
        let rejected = reqwest::Client::new()
            .delete(format!("{}/api/repos/delete", runtime.base_url))
            .bearer_auth(repo_token)
            .json(&serde_json::json!({"name": repo, "type": repo_type}))
            .send()
            .await?;
        assert_eq!(rejected.status(), reqwest::StatusCode::FORBIDDEN);
        reqwest::Client::new()
            .get(format!("{}/api/{repo_type}s/{repo}", runtime.base_url))
            .bearer_auth(repo_token)
            .send()
            .await?
            .error_for_status()?;
        let (owner, name) = repo.split_once('/').ok_or("invalid fixture repository")?;
        let administrator = bearer_token(
            "github-user-1",
            TokenScope::Write,
            RepositoryProvider::GitHub,
            owner,
            name,
            None,
        )?;
        run_hf(
            runtime,
            client_home.path(),
            [
                "repos",
                "delete",
                repo,
                "--repo-type",
                repo_type,
                "--yes",
                "--token",
                administrator.as_str(),
                "--format",
                "quiet",
            ],
        )?;
    }

    Ok(())
}

/// Exercise the public Python client's paths-info parser against file, LFS,
/// directory and missing-path entries. This catches wire-shape mistakes that
/// raw JSON assertions would miss (for example `blobId` instead of `oid`).
fn verify_hub_paths_info(runtime: &HubRuntime, repo: &str, token: &str) -> Result<(), TestError> {
    let script = r#"
import os, re, hashlib
from huggingface_hub import HfApi
from huggingface_hub.hf_api import RepoFile, RepoFolder
api = HfApi(endpoint=os.environ["HF_ENDPOINT"], token=os.environ["HF_TOKEN"])
assert api.whoami()["orgs"] == []
entries = api.get_paths_info(os.environ["HF_REPO"], ["single.txt", "large-model.bin", "nested", "missing"], expand=False)
by_path = {entry.path: entry for entry in entries}
assert set(by_path) == {"single.txt", "large-model.bin", "nested"}, by_path
assert isinstance(by_path["single.txt"], RepoFile), by_path["single.txt"]
assert re.fullmatch(r"[0-9a-f]{40}", by_path["single.txt"].blob_id), by_path["single.txt"]
assert isinstance(by_path["large-model.bin"], RepoFile), by_path["large-model.bin"]
assert by_path["large-model.bin"].lfs is not None, by_path["large-model.bin"]
assert by_path["large-model.bin"].lfs.sha256 and by_path["large-model.bin"].lfs.pointer_size > 0
assert isinstance(by_path["nested"], RepoFolder), by_path["nested"]
assert re.fullmatch(r"[0-9a-f]{40}", by_path["nested"].tree_id), by_path["nested"]
def git_oid(kind, content):
    return hashlib.sha1(kind.encode() + b" " + str(len(content)).encode() + b"\0" + content).hexdigest()
assert by_path["single.txt"].blob_id == git_oid("blob", b"single upload via hf cli\n"), by_path["single.txt"]
weight_oid = git_oid("blob", b'{"weight": 7}\n')
assert by_path["nested"].tree_id == git_oid("tree", b"100644 weights.json\0" + bytes.fromhex(weight_oid)), by_path["nested"]
large = by_path["large-model.bin"]
pointer = f"version https://git-lfs.github.com/spec/v1\noid sha256:{large.lfs.sha256}\nsize {large.size}\n".encode()
assert large.blob_id == git_oid("blob", pointer), large
assert large.xet_hash and re.fullmatch(r"[0-9a-f]{64}", large.xet_hash), large
assert by_path["single.txt"].xet_hash is None, by_path["single.txt"]
from huggingface_hub import get_hf_file_metadata, hf_hub_url
metadata = get_hf_file_metadata(hf_hub_url(os.environ["HF_REPO"], "large-model.bin", endpoint=os.environ["HF_ENDPOINT"]), token=os.environ["HF_TOKEN"])
assert metadata.xet_file_data is not None, metadata
assert metadata.xet_file_data.file_hash == large.xet_hash, metadata
assert metadata.xet_file_data.refresh_route.startswith(os.environ["HF_ENDPOINT"] + "/api/models/"), metadata
expanded = {entry.path: entry for entry in api.get_paths_info(os.environ["HF_REPO"], list(by_path), expand=True)}
assert expanded["single.txt"].last_commit.title == "single file upload", expanded
assert expanded["nested"].last_commit.title == "folder upload with include filter", expanded
assert expanded["large-model.bin"].last_commit.title == "large LFS upload", expanded
historical = api.get_paths_info(os.environ["HF_REPO"], list(by_path), revision=expanded["single.txt"].last_commit.oid, expand=True)
assert [entry.path for entry in historical] == ["single.txt"], historical
assert historical[0].blob_id == by_path["single.txt"].blob_id, historical
"#;
    let output = native_output(
        Command::new(hf_python())
            .arg("-c")
            .arg(script)
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_TOKEN", token)
            .env("HF_REPO", repo),
    )?;
    assert!(
        output.status.success(),
        "official huggingface_hub get_paths_info failed: {}",
        output.status
    );
    Ok(())
}

/// Older `huggingface_hub` clients select Xet from the Git LFS batch response;
/// keep that negotiation contract covered alongside the current client's
/// direct token-and-CAS upload path.
async fn verify_hf_xet_lfs_batch_negotiation(
    runtime: &HubRuntime,
    repo: &str,
    token: &str,
) -> Result<(), TestError> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| IoError::other("test repository id was malformed"))?;
    let oid = hex::encode(Sha256::digest(b"hf_xet negotiation probe"));
    let response = reqwest::Client::new()
        .post(format!(
            "{}/{owner}/{name}.git/info/lfs/objects/batch",
            runtime.base_url
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "operation": "upload",
            "transfers": ["basic", "multipart", "xet"],
            "objects": [{"oid": oid, "size": 24}],
            "hash_algo": "sha256",
            "ref": {"name": "main"}
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(response["transfer"], "xet");
    assert_eq!(response["objects"][0]["oid"], oid);
    let upload = &response["objects"][0]["actions"]["upload"];
    let cas_base = runtime.base_url.trim_end_matches('/');
    assert_eq!(
        upload["href"],
        format!("{cas_base}/api/models/{repo}/xet-write-token/main")
    );
    assert_eq!(upload["header"]["X-Xet-Cas-Url"], cas_base);
    assert!(
        upload["header"]["X-Xet-Access-Token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
    // The advertised action renews credentials; it is not the CAS upload base.
    // Follow it with the original repository credential, as the Xet client does.
    let refreshed = reqwest::Client::new()
        .get(upload["href"].as_str().ok_or("missing Xet refresh URL")?)
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(refreshed.headers()["X-Xet-Cas-Url"], cas_base);
    assert!(!refreshed.headers()["X-Xet-Access-Token"].is_empty());
    let expiry = refreshed.headers()["X-Xet-Token-Expiration"]
        .to_str()?
        .parse::<u64>()?;
    assert!(
        expiry
            > std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs()
    );
    assert!(
        upload["header"]["X-Xet-Token-Expiration"]
            .as_str()
            .is_some_and(|expiry| expiry.parse::<u64>().is_ok())
    );
    Ok(())
}

/// Large files must enter the Hub's LFS negotiation path. That is the point
/// where compatible clients can select Xet; returning `regular` bypasses it
/// and makes the CLI send an ordinary commit payload instead.
async fn verify_hub_preupload_selects_lfs_for_large_files(
    runtime: &HubRuntime,
    repo: &str,
    token: &str,
) -> Result<(), TestError> {
    let (owner, name) = repo
        .split_once('/')
        .ok_or_else(|| IoError::other("test repository id was malformed"))?;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/api/models/{owner}/{name}/preupload/main",
            runtime.base_url
        ))
        .bearer_auth(token)
        .json(&serde_json::json!({
            "files": [
                {"path": "probe-small.txt", "size": 1},
                {"path": "probe-large.bin", "size": 8 * 1024 * 1024 + 1}
            ]
        }))
        .send()
        .await?
        .error_for_status()?
        .json::<serde_json::Value>()
        .await?;
    assert_eq!(response["files"][0]["uploadMode"], "regular");
    assert_eq!(response["files"][1]["uploadMode"], "lfs");
    Ok(())
}

async fn assert_hf_xet_file_identity(
    runtime: &HubRuntime,
    repo_type: &str,
    owner: &str,
    name: &str,
    token: &str,
    content: &[u8],
) -> Result<(), TestError> {
    let scope =
        shardline_protocol::RepositoryScope::new(RepositoryProvider::GitHub, owner, name, None)?;
    let sha256 = hex::encode(Sha256::digest(content));
    let identity = if let Some(url) = &runtime.postgres_url {
        let pool = sqlx::PgPool::connect(url).await?;
        let records = shardline_index::PostgresRecordStore::new(pool.clone());
        let identity = records.xet_file_identity(&scope, &sha256).await?;
        pool.close().await;
        identity
    } else {
        let records =
            shardline_index::LocalRecordStore::open(runtime._storage.path().to_path_buf());
        records.xet_file_identity(&scope, &sha256)?
    };
    let identity = identity.ok_or_else(|| {
        ServerE2eInvariantError::new(
            "hf CLI upload did not publish a native Xet file identity for the large file",
        )
    })?;
    assert_eq!(identity.total_bytes, content.len() as u64);
    assert!(!identity.file_id.is_empty());
    let client = reqwest::Client::new();
    let refreshed: serde_json::Value = client
        .get(format!(
            "{}/api/{repo_type}/{owner}/{name}/xet-read-token/main",
            runtime.base_url
        ))
        .bearer_auth(token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let access = refreshed["accessToken"]
        .as_str()
        .ok_or_else(|| ServerE2eInvariantError::new("Hub read refresh omitted CAS access token"))?;
    let cas = refreshed["casUrl"]
        .as_str()
        .ok_or_else(|| ServerE2eInvariantError::new("Hub read refresh omitted CAS URL"))?;
    let head = client
        .head(format!("{cas}/v1/files/{}", identity.file_id))
        .bearer_auth(access)
        .send()
        .await?;
    assert_eq!(head.status(), reqwest::StatusCode::OK);
    assert_eq!(
        head.headers()["content-length"],
        identity.total_bytes.to_string()
    );
    assert!(head.bytes().await?.is_empty());
    Ok(())
}

async fn verify_hf_xet_cas_read(
    runtime: &HubRuntime,
    token: &str,
    content: &[u8],
) -> Result<(), TestError> {
    let sha256 = hex::encode(Sha256::digest(content));
    let response = reqwest::Client::new()
        .get(format!("{}/v1/hub/xet-files/{sha256}", runtime.base_url))
        .bearer_auth(token)
        .send()
        .await?;
    let status = response.status();
    let body = response.bytes().await?;
    assert!(
        status.is_success(),
        "Hub Xet CAS read returned {status}: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(body.as_ref(), content);
    Ok(())
}

/// Validate the documented response headers used by the installed Hub client's
/// Xet refresh flow. This is deliberately an HTTP-level contract check rather
/// than an import of a private, version-specific Python helper.
fn verify_hf_xet_token_headers(runtime: &HubRuntime, repo: &str) -> Result<(), TestError> {
    let token_url = format!(
        "{}/api/models/{repo}/xet-write-token/main",
        runtime.base_url
    );
    let script = r#"
import os, time, httpx
response = httpx.get(
    os.environ["SHARDLINE_XET_TOKEN_URL"],
    headers={"Authorization": "Bearer " + os.environ["SHARDLINE_XET_TOKEN"]},
)
response.raise_for_status()
headers = {key.lower(): value for key, value in response.headers.items()}
assert headers["x-xet-cas-url"] == os.environ["SHARDLINE_EXPECTED_CAS_URL"]
assert headers["x-xet-access-token"]
assert int(headers["x-xet-token-expiration"]) > int(time.time())
body = response.json()
assert body["casUrl"] == os.environ["SHARDLINE_EXPECTED_CAS_URL"]
assert body["accessToken"] == headers["x-xet-access-token"]
assert int(body["exp"]) == int(headers["x-xet-token-expiration"])
from huggingface_hub import HfApi
api = HfApi(endpoint=os.environ["SHARDLINE_EXPECTED_CAS_URL"], token=os.environ["SHARDLINE_REPO_WRITE_TOKEN"])
refs = api.list_repo_refs("team/cli-model", include_pull_requests=True)
assert len(refs.branches) == 1, refs
assert refs.branches[0].name == "main", refs
assert refs.branches[0].ref == "refs/heads/main", refs
info = api.repo_info("team/cli-model")
assert refs.branches[0].target_commit == info.sha, refs
assert any(sibling.rfilename == "single.txt" for sibling in info.siblings), info
history = api.list_repo_commits("team/cli-model")
assert history[0].commit_id == info.sha, history
assert history[0].title == "single file upload", history
assert refs.tags == [] and refs.converts == [] and refs.pull_requests == [], refs
from huggingface_hub import hf_hub_download
from huggingface_hub.utils import HfHubHTTPError, RevisionNotFoundError, EntryNotFoundError
import pathlib, tempfile
try:
    api.list_repo_commits("team/cli-model", revision="missing-sdk-branch")
    raise AssertionError("missing revision unexpectedly resolved")
except RevisionNotFoundError:
    pass
api.create_branch("team/cli-model", branch="feature/sdk", revision=info.sha)
try:
    api.create_branch("team/cli-model", branch="feature/sdk")
    raise AssertionError("duplicate branch creation unexpectedly succeeded")
except HfHubHTTPError as error:
    assert error.response.status_code == 409, error
api.create_branch("team/cli-model", branch="feature/sdk", exist_ok=True)
api.create_tag("team/cli-model", tag="release/sdk", revision="feature/sdk", tag_message="SDK release")
api.create_tag("team/cli-model", tag="release/sdk", exist_ok=True)
refs = api.list_repo_refs("team/cli-model")
assert {ref.name: ref.target_commit for ref in refs.branches} == {"main": info.sha, "feature/sdk": info.sha}, refs
assert [(ref.name, ref.target_commit) for ref in refs.tags] == [("release/sdk", info.sha)], refs
api.upload_file(path_or_fileobj=b"branch only\n", path_in_repo="branch-only.txt",
    repo_id="team/cli-model", revision="feature/sdk", commit_message="branch-only commit")
branch_head = api.repo_info("team/cli-model", revision="feature/sdk").sha
assert branch_head != info.sha
branch_history = api.list_repo_commits("team/cli-model", revision="feature/sdk", formatted=True)
assert [commit.commit_id for commit in branch_history] == [branch_head] + [commit.commit_id for commit in history], branch_history
assert branch_history[0].title == "branch-only commit", branch_history
assert [commit.commit_id for commit in api.list_repo_commits("team/cli-model")] == [commit.commit_id for commit in history]
assert api.repo_info("team/cli-model").sha == info.sha
try:
    api.upload_file(path_or_fileobj=b"stale update\n", path_in_repo="stale.txt",
        repo_id="team/cli-model", revision="feature/sdk", parent_commit=info.sha)
    raise AssertionError("stale branch update unexpectedly committed")
except HfHubHTTPError as error:
    assert error.response.status_code == 409, error
assert api.repo_info("team/cli-model", revision="feature/sdk").sha == branch_head
updated = api.upload_file(path_or_fileobj=b"conditional update\n", path_in_repo="conditional.txt",
    repo_id="team/cli-model", revision="feature/sdk", parent_commit=branch_head,
    commit_message="conditional branch commit")
assert len(updated.oid) == 40
assert api.repo_info("team/cli-model", revision="feature/sdk").sha == updated.oid
assert api.list_repo_commits("team/cli-model", revision=updated.oid)[1].commit_id == branch_head
branch_head = updated.oid
try:
    api.upload_file(path_or_fileobj=b"tag write\n", path_in_repo="tag-write.txt",
        repo_id="team/cli-model", revision="release/sdk")
    raise AssertionError("tag write unexpectedly committed")
except HfHubHTTPError as error:
    assert error.response.status_code in (400, 409), error
assert api.repo_info("team/cli-model").sha == info.sha
with tempfile.TemporaryDirectory() as cache:
    downloaded = hf_hub_download("team/cli-model", "single.txt", revision="feature/sdk",
        endpoint=api.endpoint, token=api.token, cache_dir=cache)
    assert pathlib.Path(downloaded).read_bytes() == b"single upload via hf cli\n"
    try:
        hf_hub_download("team/cli-model", "missing-sdk-file.txt", revision="main",
            endpoint=api.endpoint, token=api.token, cache_dir=cache)
        raise AssertionError("missing file unexpectedly downloaded")
    except EntryNotFoundError:
        pass
api.delete_branch("team/cli-model", branch="feature/sdk")
assert api.repo_info("team/cli-model", revision=info.sha).sha == info.sha
assert api.list_repo_commits("team/cli-model", revision=branch_head)[0].commit_id == branch_head
assert len(api.list_repo_refs("team/cli-model").tags) == 1
api.delete_tag("team/cli-model", tag="release/sdk")
assert api.list_repo_refs("team/cli-model").tags == []
api.create_tag("team/cli-model", tag="HEAD")
api.delete_tag("team/cli-model", tag="HEAD")
try:
    api.delete_branch("team/cli-model", branch="main")
    raise AssertionError("default branch deletion unexpectedly succeeded")
except HfHubHTTPError as error:
    assert error.response.status_code == 403, error
"#;
    let output = native_output(
        Command::new(hf_python())
            .args(["-c", script])
            .env("SHARDLINE_XET_TOKEN_URL", token_url)
            .env("SHARDLINE_XET_TOKEN", &runtime.model_token)
            .env(
                "SHARDLINE_REPO_WRITE_TOKEN",
                bearer_token(
                    "github-user-1",
                    TokenScope::Write,
                    RepositoryProvider::GitHub,
                    "team",
                    "cli-model",
                    None,
                )?,
            )
            .env("SHARDLINE_EXPECTED_CAS_URL", &runtime.base_url)
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    if !output.status.success() {
        return Err(ServerE2eInvariantError::new(format!(
            "hf_xet token refresh response did not satisfy the Hub header contract: {}",
            output.status
        ))
        .into());
    }
    Ok(())
}

async fn start_runtime(frontends: &[ServerFrontend]) -> Result<HubRuntime, TestError> {
    start_runtime_with_services(frontends, None).await
}

async fn start_runtime_with_services(
    frontends: &[ServerFrontend],
    services: Option<shardline_test_support::DockerLocalStack>,
) -> Result<HubRuntime, TestError> {
    start_runtime_with_public_base(frontends, services, None).await
}

async fn start_runtime_with_public_base(
    frontends: &[ServerFrontend],
    services: Option<shardline_test_support::DockerLocalStack>,
    public_base: Option<&str>,
) -> Result<HubRuntime, TestError> {
    start_runtime_with_roles(frontends, services, public_base, false).await
}

async fn start_runtime_with_roles(
    frontends: &[ServerFrontend],
    services: Option<shardline_test_support::DockerLocalStack>,
    public_base: Option<&str>,
    split: bool,
) -> Result<HubRuntime, TestError> {
    let storage = tempfile::tempdir()?;
    let hub_root = storage.path().join("hub");
    create_dir_all(&hub_root)?;
    let _connection = rusqlite::Connection::open(hub_root.join("metadata.sqlite3"))?;
    let provider_config = write_provider_config(storage.path())?;
    // The SDK's provider-token exchange needs an explicit grant for the same
    // repository that the Hub CLI fixture uploads into.
    let mut provider: serde_json::Value = serde_json::from_slice(&read(&provider_config)?)?;
    let repositories = provider
        .pointer_mut("/providers/0/repositories")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("fixture repository grants missing")?;
    let mut model = repositories
        .first()
        .cloned()
        .ok_or("fixture provider repository missing")?;
    model["name"] = "cli-model".into();
    repositories.push(model);
    write(&provider_config, serde_json::to_vec(&provider)?)?;
    let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)).await?;
    let address = listener.local_addr()?;
    let base_url = format!("http://{address}");
    let build_config = |bind_address| -> Result<ServerConfig, TestError> {
        Ok(ServerConfig::new(
            bind_address,
            public_base.unwrap_or(&base_url).to_owned(),
            storage.path().to_path_buf(),
            NonZeroUsize::new(128).ok_or("test chunk size must be non-zero")?,
        )
        .with_token_signing_key(b"test-signing-key-32-bytes-long!!".to_vec())?
        .with_server_frontends(frontends.iter().copied())?
        .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
        .with_provider_runtime(
            provider_config.clone(),
            b"test-api-key-16bytes".to_vec(),
            "test-issuer".to_owned(),
            NonZeroU64::new(3600).ok_or("test provider ttl must be non-zero")?,
        )?)
    };
    let mut config = build_config(address)?;
    let mut remote_object_config = None;
    let postgres_url = services.as_ref().and_then(|stack| stack.postgres_url());
    if let Some(url) = &postgres_url {
        let pool = sqlx::PgPool::connect(url).await?;
        shardline_server::apply_database_migrations(&pool).await?;
        pool.close().await;
        config = config.with_index_postgres_url(url.clone())?;
        let stack = services.as_ref().ok_or("remote Hub services missing")?;
        let prefix = stack.unique_s3_key_prefix("native-hf-cli");
        if let Some(raw) = stack.s3_raw_config(Some(&prefix)) {
            let object_config = shardline_storage::S3ObjectStoreConfig::new(raw.bucket, raw.region)
                .with_endpoint(raw.endpoint)
                .with_credentials(
                    raw.access_key.map(shardline_protocol::SecretString::new),
                    raw.secret_key.map(shardline_protocol::SecretString::new),
                    raw.session_token.map(shardline_protocol::SecretString::new),
                )
                .with_key_prefix(raw.key_prefix.as_deref())
                .with_allow_http(raw.allow_http);
            config = config.with_object_storage(
                shardline_server::ObjectStorageAdapter::S3,
                Some(object_config.clone()),
            );
            remote_object_config = Some(object_config);
        }
    }
    if split {
        config = config.with_server_role(shardline_server::ServerRole::Api);
    }
    let maintenance_config = config.clone();
    let server = tokio::spawn(async move { serve_with_listener(config, listener).await });
    wait_for_health(&base_url).await?;
    let transfer = if split {
        let transfer_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let transfer_address = transfer_listener.local_addr()?;
        let transfer_url = format!("http://{transfer_address}");
        let mut transfer_config = build_config(transfer_address)?
            .with_server_role(shardline_server::ServerRole::Transfer);
        if let Some(url) = &postgres_url {
            transfer_config = transfer_config.with_index_postgres_url(url.clone())?;
        }
        if let Some(objects) = remote_object_config {
            transfer_config = transfer_config
                .with_object_storage(shardline_server::ObjectStorageAdapter::S3, Some(objects));
        }
        let task =
            tokio::spawn(
                async move { serve_with_listener(transfer_config, transfer_listener).await },
            );
        wait_for_health(&transfer_url).await?;
        Some((transfer_url, task))
    } else {
        None
    };
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
        postgres_url,
        maintenance_config,
        _storage: storage,
        server,
        transfer,
        _services: services,
    })
}

async fn start_hub_runtime() -> Result<HubRuntime, TestError> {
    // The Hub frontend itself must expose the CAS routes needed by hf_xet;
    // users should not need to know to enable an additional frontend.
    start_runtime(&[ServerFrontend::Hub]).await
}

async fn read_metric_counter(runtime: &HubRuntime, name: &str) -> Result<u64, TestError> {
    let response = reqwest::get(format!("{}/metrics", runtime.base_url)).await?;
    let metrics = response.error_for_status()?.text().await?;
    let value = metrics
        .lines()
        .find_map(|line| {
            let mut fields = line.split_whitespace();
            (fields.next()? == name).then(|| fields.next()?.parse::<u64>().ok())?
        })
        .ok_or_else(|| ServerE2eInvariantError::new(format!("metric {name} was not exposed")))?;
    Ok(value)
}

async fn verify_cold_client_chunk_dedup(
    runtime: &HubRuntime,
    repository: &str,
    working: &Path,
) -> Result<(), TestError> {
    verify_cold_client_chunk_dedup_with_rebuild(runtime, repository, working, false).await
}

async fn verify_cold_client_chunk_dedup_with_rebuild(
    runtime: &HubRuntime,
    repository: &str,
    working: &Path,
    rebuild: bool,
) -> Result<(), TestError> {
    // Incompressible, deterministic data prevents compression or repeated-byte
    // fixtures from masquerading as network deduplication. Each upload runs in
    // a new process with a genuinely empty Hub and Xet cache.
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut content = Vec::with_capacity(36 * 1024 * 1024);
    while content.len() < 36 * 1024 * 1024 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        content.extend_from_slice(&state.to_le_bytes());
    }
    let original = working.join("dedup-original.bin");
    write(&original, &content)?;
    let first_home = tempfile::tempdir()?;
    run_hf(
        runtime,
        first_home.path(),
        [
            "upload",
            repository,
            path_as_str(&original)?,
            "dedup-original.bin",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "dedup baseline",
        ],
    )?;
    if rebuild {
        native_huggingface_dedup_rebuild::recover_legacy_descriptors(runtime).await?;
    }
    let before = read_metric_counter(runtime, "shardline_xet_xorb_upload_bytes_total").await?;
    let hits = read_metric_counter(runtime, "shardline_xet_dedupe_shard_hits_total").await?;
    for byte in content.iter_mut().rev().take(256 * 1024) {
        *byte ^= 0x5a;
    }
    let modified = working.join("dedup-edited.bin");
    write(&modified, &content)?;
    let second_home = tempfile::tempdir()?;
    run_hf(
        runtime,
        second_home.path(),
        [
            "upload",
            repository,
            path_as_str(&modified)?,
            "dedup-edited.bin",
            "--token",
            runtime.model_token.as_str(),
            "--commit-message",
            "cold client incremental upload",
        ],
    )?;
    let after = read_metric_counter(runtime, "shardline_xet_xorb_upload_bytes_total").await?;
    let uploaded = after
        .checked_sub(before)
        .ok_or_else(|| ServerE2eInvariantError::new("upload byte counter decreased"))?;
    eprintln!(
        "cold-client hf_xet: {uploaded} Xorb upload bytes for {} logical bytes after a 256 KiB edit",
        content.len()
    );
    assert!(
        read_metric_counter(runtime, "shardline_xet_dedupe_shard_hits_total").await? > hits,
        "stock hf_xet did not use the global dedup endpoint"
    );
    assert!(
        uploaded > 0 && uploaded < u64::try_from(content.len())? / 2,
        "cold client sent {uploaded} Xorb bytes for {} logical bytes",
        content.len()
    );
    let download = working.join("dedup-download");
    let third_home = tempfile::tempdir()?;
    run_hf(
        runtime,
        third_home.path(),
        [
            "download",
            repository,
            "dedup-edited.bin",
            "--local-dir",
            path_as_str(&download)?,
            "--force-download",
            "--token",
            runtime.model_token.as_str(),
            "--format",
            "quiet",
        ],
    )?;
    assert_eq!(read(download.join("dedup-edited.bin"))?, content);
    verify_plain_xet_downloads(runtime, repository, &content).await?;
    Ok(())
}

async fn verify_plain_xet_downloads(
    runtime: &HubRuntime,
    repository: &str,
    content: &[u8],
) -> Result<(), TestError> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/{repository}/resolve/main/dedup-edited.bin",
        runtime.base_url
    );
    let etag = format!("\"{}\"", hex::encode(Sha256::digest(content)));
    let total = content.len();
    for (field, start, end) in [
        ("bytes=65530-65549", 65530, 65550),
        ("bytes=-19", total - 19, total),
    ] {
        let response = client
            .get(&url)
            .bearer_auth(&runtime.model_token)
            .header("Range", field)
            .header("If-Range", &etag)
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
        assert_eq!(response.headers()["etag"], etag);
        assert_eq!(
            response.headers()["content-range"],
            format!("bytes {start}-{}/{total}", end - 1)
        );
        assert_eq!(response.bytes().await?.as_ref(), &content[start..end]);
    }
    let response = client
        .get(&url)
        .bearer_auth(&runtime.model_token)
        .header("Range", "bytes=0-3")
        .header("If-Range", "\"stale\"")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await?.as_ref(), content);
    for (name, expected) in [
        ("If-Match", reqwest::StatusCode::PARTIAL_CONTENT),
        ("If-None-Match", reqwest::StatusCode::NOT_MODIFIED),
    ] {
        let mut validators = b"\"\x80,opaque\", ".to_vec();
        validators.extend_from_slice(etag.as_bytes());
        let response = client
            .get(&url)
            .bearer_auth(&runtime.model_token)
            .header("Range", "bytes=0-3")
            .header(name, reqwest::header::HeaderValue::from_bytes(&validators)?)
            .send()
            .await?;
        assert_eq!(response.status(), expected);
        if expected == reqwest::StatusCode::PARTIAL_CONTENT {
            assert_eq!(response.bytes().await?.as_ref(), &content[..4]);
        } else {
            assert!(!response.headers().contains_key("content-range"));
            assert!(response.bytes().await?.is_empty());
        }
    }
    let response = client
        .get(&url)
        .bearer_auth(&runtime.model_token)
        .header("Range", "bytes=0-3")
        .header(
            "If-Range",
            reqwest::header::HeaderValue::from_bytes(b"\"\xff\"")?,
        )
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert!(!response.headers().contains_key("content-range"));
    assert_eq!(response.bytes().await?.as_ref(), content);
    for (name, value, status) in [
        (
            "If-None-Match",
            format!("W/{etag}"),
            reqwest::StatusCode::NOT_MODIFIED,
        ),
        (
            "If-Match",
            "\"stale\"".to_owned(),
            reqwest::StatusCode::PRECONDITION_FAILED,
        ),
        (
            "Range",
            format!("bytes={total}-"),
            reqwest::StatusCode::RANGE_NOT_SATISFIABLE,
        ),
    ] {
        let response = client
            .get(&url)
            .bearer_auth(&runtime.model_token)
            .header(name, value)
            .send()
            .await?;
        assert_eq!(response.status(), status);
        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            assert_eq!(
                response.headers()["content-range"],
                format!("bytes */{total}")
            );
        }
        let body = response.bytes().await?;
        if status == reqwest::StatusCode::NOT_MODIFIED {
            assert!(body.is_empty());
        } else {
            assert!(body.len() < 4096, "error response returned file payload");
        }
    }
    let sha256 = hex::encode(Sha256::digest(content));
    let response = client
        .head(format!("{}/lfs/objects/{sha256}", runtime.base_url))
        .bearer_auth(&runtime.model_token)
        .header("Range", "bytes=0-3")
        .send()
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-length"], total.to_string());
    assert_eq!(response.headers()["etag"], etag);
    assert!(response.bytes().await?.is_empty());
    Ok(())
}

fn run_hf<const N: usize>(
    runtime: &HubRuntime,
    home: &Path,
    arguments: [&str; N],
) -> Result<Output, TestError> {
    let output = native_output(
        Command::new("hf")
            .args(arguments)
            .env("HF_ENDPOINT", &runtime.base_url)
            .env("HF_HOME", home)
            .env("HF_XET_CACHE", home.join("xet"))
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("HF_HUB_VERBOSITY", "debug")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env("no_proxy", "127.0.0.1,localhost"),
    )?;
    if output.status.success() {
        return Ok(output);
    }

    Err(ServerE2eInvariantError::new(format!(
        "hf failed with status {}; diagnostic categories: {}; raw client output omitted because it can contain transfer credentials",
        output.status,
        native_huggingface_diagnostics::categories(&output.stdout, &output.stderr),
    ))
    .into())
}

fn native_output(command: &mut Command) -> Result<Output, IoError> {
    shardline_test_support::native_client_output(command, std::time::Duration::from_secs(600))
}

fn path_as_str(path: &Path) -> Result<&str, TestError> {
    path.to_str()
        .ok_or_else(|| IoError::other("test path was not valid UTF-8").into())
}

fn command_available(program: &str) -> bool {
    let available = shardline_test_support::native_client_output(
        Command::new(program).arg("--version"),
        std::time::Duration::from_secs(10),
    )
    .is_ok_and(|output| output.status.success());
    native_client_policy::require_available(program, available)
}

fn python_package_available(package: &str) -> bool {
    shardline_test_support::native_client_output(Command::new(hf_python())
        .args([
            "-c",
            "import importlib.util, sys; sys.exit(importlib.util.find_spec(sys.argv[1]) is None)",
        ])
        .arg(package), std::time::Duration::from_secs(10))
        .is_ok_and(|output| output.status.success())
}

/// Uses the interpreter from the `hf` launcher so package checks exercise the
/// same environment as the CLI instead of an unrelated system Python install.
fn hf_python() -> std::path::PathBuf {
    let Ok(output) = shardline_test_support::native_client_output(
        Command::new("which").arg("hf"),
        std::time::Duration::from_secs(10),
    ) else {
        return "python3".into();
    };
    if !output.status.success() {
        return "python3".into();
    }
    let Ok(executable) = std::str::from_utf8(&output.stdout) else {
        return "python3".into();
    };
    let Ok(contents) = std::fs::read_to_string(executable.trim()) else {
        return "python3".into();
    };
    contents
        .lines()
        .next()
        .and_then(|line| line.strip_prefix("#!"))
        .map(str::trim)
        .filter(|interpreter| !interpreter.is_empty())
        .map(Into::into)
        .unwrap_or_else(|| "python3".into())
}
