#![allow(clippy::expect_used, clippy::indexing_slicing, clippy::unwrap_used)]

mod native_client_policy;
mod native_huggingface_requirements;
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

    let output = Command::new("mc")
        .arg("--config-dir")
        .arg(&config_dir)
        .args([
            "alias",
            "set",
            alias,
            &runtime.base_url,
            s3_token.as_str(),
            "dummy-secret",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "mc alias set failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new("mc")
        .arg("--config-dir")
        .arg(&config_dir)
        .args(["mb", &format!("{alias}/{bucket}")])
        .output()?;
    assert!(
        output.status.success(),
        "mc mb failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let file = tmp.path().join("coexist.txt");
    write(&file, b"s3 coexist payload\n")?;
    let output = Command::new("mc")
        .arg("--config-dir")
        .arg(&config_dir)
        .args([
            "cp",
            path_as_str(&file)?,
            &format!("{alias}/{bucket}/coexist.txt"),
        ])
        .output()?;
    assert!(
        output.status.success(),
        "mc cp failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let output = Command::new("mc")
        .arg("--config-dir")
        .arg(&config_dir)
        .args(["cat", &format!("{alias}/{bucket}/coexist.txt")])
        .output()?;
    assert!(
        output.status.success(),
        "mc cat failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"s3 coexist payload\n");

    Ok(())
}

async fn exercise_huggingface_cli_flows(runtime: &HubRuntime) -> Result<(), TestError> {
    let client_home = tempfile::tempdir()?;
    let working = tempfile::tempdir()?;
    let created_repo = "team/cli-created";
    let model_repo = "team/cli-model";
    let dataset_repo = "team/cli-dataset";

    assert!(
        python_package_available("hf_xet"),
        "native Hugging Face CLI E2E requires hf_xet to verify Xet uploads"
    );
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
    assert_hf_xet_file_identity(runtime, "team", "cli-model", &large_content)?;
    verify_hf_xet_cas_read(runtime, &runtime.model_token, &large_content).await?;
    let large_download = working.path().join("large-download");
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
    assert_hf_xet_file_identity(runtime, "team", "cli-dataset", &dataset_large_content)?;
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

    for (repo, repo_type, repo_token) in [
        (created_repo, "model", runtime.created_token.as_str()),
        (model_repo, "model", runtime.model_token.as_str()),
        (dataset_repo, "dataset", runtime.dataset_token.as_str()),
    ] {
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
                repo_token,
                "--format",
                "quiet",
            ],
        )?;
    }

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
    assert!(response["objects"][0]["actions"].is_null());
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

fn assert_hf_xet_file_identity(
    runtime: &HubRuntime,
    owner: &str,
    name: &str,
    content: &[u8],
) -> Result<(), TestError> {
    let scope =
        shardline_protocol::RepositoryScope::new(RepositoryProvider::GitHub, owner, name, None)?;
    let sha256 = hex::encode(Sha256::digest(content));
    let records = shardline_index::LocalRecordStore::open(runtime._storage.path().to_path_buf());
    let identity = records.xet_file_identity(&scope, &sha256)?;
    let identity = identity.ok_or_else(|| {
        ServerE2eInvariantError::new(
            "hf CLI upload did not publish a native Xet file identity for the large file",
        )
    })?;
    assert_eq!(identity.total_bytes, content.len() as u64);
    assert!(!identity.file_id.is_empty());
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
api = HfApi(endpoint=os.environ["SHARDLINE_EXPECTED_CAS_URL"], token=os.environ["SHARDLINE_XET_TOKEN"])
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
    let output = Command::new(hf_python())
        .args(["-c", script])
        .env("SHARDLINE_XET_TOKEN_URL", token_url)
        .env("SHARDLINE_XET_TOKEN", &runtime.model_token)
        .env("SHARDLINE_EXPECTED_CAS_URL", &runtime.base_url)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .output()?;
    if !output.status.success() {
        return Err(ServerE2eInvariantError::new(format!(
            "hf_xet token refresh response did not satisfy the Hub header contract: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
        .into());
    }
    Ok(())
}

async fn start_runtime(frontends: &[ServerFrontend]) -> Result<HubRuntime, TestError> {
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
    .with_server_frontends(frontends.iter().copied())?
    .with_deployment_mode(shardline_server::DeploymentMode::Insecure)
    .with_provider_runtime(
        provider_config,
        b"test-api-key-16bytes".to_vec(),
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
        .env("HF_HUB_VERBOSITY", "debug")
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
    let stdout = redact_cli_secrets(&String::from_utf8_lossy(&output.stdout), runtime);
    let stderr = redact_cli_secrets(&String::from_utf8_lossy(&output.stderr), runtime);
    Err(ServerE2eInvariantError::new(format!(
        "hf {} failed with status {}\nstdout:\n{}\nstderr:\n{}",
        command, output.status, stdout, stderr,
    ))
    .into())
}

fn redact_cli_secrets(output: &str, runtime: &HubRuntime) -> String {
    let mut redacted = output
        .replace(&runtime.created_token, "<TOKEN>")
        .replace(&runtime.model_token, "<TOKEN>")
        .replace(&runtime.dataset_token, "<TOKEN>");
    let mut search_from = 0;
    while let Some(start) = redacted[search_from..]
        .find("upload_token=")
        .map(|offset| search_from + offset)
    {
        let value_start = start + "upload_token=".len();
        let value_end = redacted[value_start..]
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '&' | '"' | '\'')
            })
            .map_or(redacted.len(), |offset| value_start + offset);
        redacted.replace_range(value_start..value_end, "<REDACTED>");
        search_from = value_start + "<REDACTED>".len();
    }
    redacted
}

fn path_as_str(path: &Path) -> Result<&str, TestError> {
    path.to_str()
        .ok_or_else(|| IoError::other("test path was not valid UTF-8").into())
}

fn command_available(program: &str) -> bool {
    let available = Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success());
    native_client_policy::require_available(program, available)
}

fn python_package_available(package: &str) -> bool {
    Command::new(hf_python())
        .args([
            "-c",
            "import importlib.util, sys; sys.exit(importlib.util.find_spec(sys.argv[1]) is None)",
        ])
        .arg(package)
        .output()
        .is_ok_and(|output| output.status.success())
}

/// Uses the interpreter from the `hf` launcher so package checks exercise the
/// same environment as the CLI instead of an unrelated system Python install.
fn hf_python() -> std::path::PathBuf {
    let Ok(output) = Command::new("which").arg("hf").output() else {
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
